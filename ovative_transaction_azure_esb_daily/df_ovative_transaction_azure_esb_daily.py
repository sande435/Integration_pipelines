import os
import tempfile
import datetime
import json
import base64
import logging
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from google.cloud import storage
from google.auth import default
from google.auth.transport.requests import Request
import snowflake.connector
from azure.storage.blob import BlobClient
from bef_integration.bef_integration import generate_bef_log, publish_bef_log


# =========================
# Custom Pipeline Options
# =========================
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--parameter_name",
            type=str,
            required=False,
            help="Optional Parameter Manager name (can be omitted for template build)"
        )


# =========================
# Helpers: Parameter & Secret Loaders
# =========================
def _gcp_auth_headers():
    credentials, project_id = default()
    credentials.refresh(Request())
    return credentials, project_id, {"Authorization": f"Bearer {credentials.token}"}

def load_parameter_json(name: str):
    """Load JSON parameter from Parameter Manager."""
    credentials, project_id, headers = _gcp_auth_headers()
    base_url = f"https://parametermanager.googleapis.com/v1/projects/{project_id}/locations/global/parameters/{name}/versions"
    resp = requests.get(base_url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "parameterVersions" not in data or not data["parameterVersions"]:
        raise KeyError(f"No versions found for parameter: {name}")

    version_name = data["parameterVersions"][0]["name"]
    payload_resp = requests.get(f"https://parametermanager.googleapis.com/v1/{version_name}", headers=headers)
    payload_resp.raise_for_status()
    payload = payload_resp.json()["payload"]["data"]
    return json.loads(base64.b64decode(payload).decode())

def load_secret(secret_id: str):
    """Load a secret from Secret Manager. Returns dict if JSON, else string."""
    credentials, project_id, headers = _gcp_auth_headers()
    url = f"https://secretmanager.googleapis.com/v1/projects/{project_id}/secrets/{secret_id}/versions/latest:access"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    payload_b64 = resp.json()["payload"]["data"]
    text = base64.b64decode(payload_b64).decode()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

def resolve_parameter_name(vp):
    """Resolve parameter name from ValueProvider if accessible, else env/default."""
    # Default/fallback if not supplied at runtime
    fallback = os.getenv("DEFAULT_PARAMETER_NAME", "ovative_transaction_azure_esb_daily_parameter")
    try:
        # In template build, is_accessible() is False; at job runtime it is True
        if hasattr(vp, "is_accessible") and vp.is_accessible():
            val = vp.get()
            return val if val else fallback
    except Exception:
        pass
    return fallback


# =========================
# Snowflake
# =========================
def get_curr_day_from_snowflake(sf_config: dict) -> str:
    """
    Reads CURR_DAY from {DB}.{SCHEMA}.DWH_C_PARAM where PARAM_NAME='CURR_DAY'
    Returns 'YYYY-MM-DD'
    """
    ctx = snowflake.connector.connect(
        user=sf_config["SNOWFLAKE_USER"],
        password=sf_config["SNOWFLAKE_PASSWORD"],
        account=sf_config["SNOWFLAKE_ACCOUNT"],
        warehouse=sf_config["SNOWFLAKE_WAREHOUSE"],
        database=sf_config["SNOWFLAKE_DATABASE"],
        schema=sf_config["SNOWFLAKE_SCHEMA"]
    )
    try:
        with ctx.cursor() as cs:
            query = f"""
            SELECT PARAM_VALUE
            FROM {sf_config['SNOWFLAKE_DATABASE']}.{sf_config['SNOWFLAKE_SCHEMA']}.DWH_C_PARAM
            WHERE PARAM_NAME = 'CURR_DAY'
            """
            cs.execute(query)
            row = cs.fetchone()
            if not row:
                raise RuntimeError("No CURR_DAY found in Snowflake")
            return row[0]
    finally:
        ctx.close()


# =========================
# DoFn: Compute blob path (defers all runtime deps)
# =========================
class ComputeBlobPathDoFn(beam.DoFn):
    def __init__(self, parameter_name_vp):
        self.parameter_name_vp = parameter_name_vp
        self.params = None
        self.sf_config = None
        self.blob_path = None

    def setup(self):
        # Resolve parameter name safely for both template build and runtime
        param_name = resolve_parameter_name(self.parameter_name_vp)
        self.params = load_parameter_json(param_name)

        # Load and merge Snowflake creds
        sf_secret = load_secret(self.params["SF_SECRET_ID"])
        if not isinstance(sf_secret, dict):
            raise ValueError("Snowflake secret must be a JSON object with credentials")
        self.sf_config = {**self.params, **sf_secret}

        required_keys = [
            "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"
        ]
        missing = [k for k in required_keys if k not in self.sf_config or not self.sf_config[k]]
        if missing:
            raise KeyError(f"Missing required Snowflake keys: {missing}")

        # Compute date and build source blob path
        curr_date = get_curr_day_from_snowflake(self.sf_config)  # 'YYYY-MM-DD'
        y, m, d = curr_date.split('-')
        azure_blob_name = self.params["AZURE_BLOB_NAME"]
        self.blob_path = f"CDW/QA/outbound/{y}/{m}/{d}/ovative/{azure_blob_name}.txt"

    def process(self, _):
        # Emit the blob path for downstream DoFns
        yield self.blob_path


# =========================
# DoFn: Download from Azure
# =========================
class DownloadAzureBlobDoFn(beam.DoFn):
    def __init__(self, parameter_name_vp):
        self.parameter_name_vp = parameter_name_vp
        self.params = None
        self.azure_sas_url = None

    def setup(self):
        param_name = resolve_parameter_name(self.parameter_name_vp)
        self.params = load_parameter_json(param_name)

        # Load Azure SAS secret (can be JSON or plain string)
        azure_secret = load_secret(self.params["AZURE_SECRET_ID"])
        if isinstance(azure_secret, dict):
            if "Azure_SAS_qa_secret" not in azure_secret:
                raise KeyError("Key 'Azure_SAS_qa_secret' not found in Azure secret")
            self.azure_sas_url = azure_secret["Azure_SAS_qa_secret"]
        else:
            self.azure_sas_url = azure_secret

        if "?" not in self.azure_sas_url:
            raise ValueError("Azure SAS URL invalid — expected '?' separator")

    def process(self, blob_relative_path):
        container_url, sas_token = self.azure_sas_url.split('?', 1)
        full_blob_url = f"{container_url}/{blob_relative_path}?{sas_token}"
        blob_client = BlobClient.from_blob_url(full_blob_url)

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        renamed_file = f"{self.params['AZURE_BLOB_NAME']}_{timestamp}.txt"

        tmp_dir = tempfile.mkdtemp()
        local_path = os.path.join(tmp_dir, renamed_file)

        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

        yield {
            "local_path": local_path,
            "renamed_file": renamed_file,
            "blob_path": blob_relative_path
        }


# =========================
# DoFn: Upload to GCS
# =========================
class UploadToGCSDoFn(beam.DoFn):
    def __init__(self, parameter_name_vp):
        self.parameter_name_vp = parameter_name_vp
        self.params = None
        self.client = None

    def setup(self):
        param_name = resolve_parameter_name(self.parameter_name_vp)
        self.params = load_parameter_json(param_name)
        self.client = storage.Client()

    def process(self, element):
        bucket = self.client.bucket(self.params["GCS_BUCKET"])
        outbound_path = self.params["GCS_OUTBOUND_PREFIX"] + element["renamed_file"]
        archive_path = self.params["GCS_ARCHIVE_PREFIX"] + element["renamed_file"]

        bucket.blob(outbound_path).upload_from_filename(element["local_path"])
        bucket.blob(archive_path).upload_from_filename(element["local_path"])

        yield outbound_path


# =========================
# Main Function with BEF Logging (template-safe)
# =========================
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    correlation_id = f"df-ovative-azure-esb-{timestamp}"

    bef_flow_name = "df_ovative_transaction_azure_esb_daily"  # safe default to avoid UnboundLocalError

    try:
        opts = PipelineOptions(save_main_session=True)
        _ = opts.view_as(GoogleCloudOptions)
        opts.view_as(SetupOptions).save_main_session = True
        custom_opts = opts.view_as(CustomOptions)

        # Try to resolve BEF_FLOW_NAME for nicer logs when running (not during template build)
        try:
            param_name_snapshot = resolve_parameter_name(custom_opts.parameter_name)
            params_snapshot = load_parameter_json(param_name_snapshot)
            bef_flow_name = params_snapshot.get("BEF_FLOW_NAME", bef_flow_name)
            logging.info(f"Loaded parameters: {list(params_snapshot.keys())}")
        except Exception as e:
            # Fine during template build or if defaults are used; keep default flow name
            logging.info(f"Skipping param snapshot (likely template build or default): {e}")

        # === BEF: Pipeline Start ===
        start_log = generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=10,
            severity=1,
            flow_name=bef_flow_name,
            message="Pipeline started",
            application_component="Azure_Integration"
        )
        publish_bef_log(start_log)
        logging.info("BEF start log published.")

        # Build pipeline — all runtime deps are deferred to DoFn.setup()
        with beam.Pipeline(options=opts) as p:
            (
                p
                | "Seed" >> beam.Create([None])  # singleton seed
                | "Compute Blob Path" >> beam.ParDo(ComputeBlobPathDoFn(custom_opts.parameter_name))
                | "Download Azure Blob" >> beam.ParDo(DownloadAzureBlobDoFn(custom_opts.parameter_name))
                | "Upload to GCS" >> beam.ParDo(UploadToGCSDoFn(custom_opts.parameter_name))
            )

        # === BEF: Success ===
        success_log = generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=42,
            severity=2,
            flow_name=bef_flow_name,
            message="Dataflow job completed successfully",
            application_component="Azure_Integration"
        )
        publish_bef_log(success_log)
        logging.info("BEF success log published.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        error_log = generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=42,
            severity=3,
            flow_name=bef_flow_name,
            message=f"Pipeline error: {e}",
            application_component="Azure_Integration"
        )
        publish_bef_log(error_log)
        logging.error("BEF error log published.")
        raise


# =========================
# Entry Point
# =========================
if __name__ == "__main__":
    run()
