import os
import json
import base64
import logging
import datetime
import requests
import uuid
import pandas as pd
import oracledb
import apache_beam as beam

from google.cloud import storage, secretmanager
from google.auth import default
from google.auth.transport.requests import Request

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from bef_integration.bef_integration import generate_bef_log, publish_bef_log

# Dynamically get the path relative to the current working directory
current_dir = os.getcwd()
instant_client_path = os.path.join(current_dir, "instantclient_23_8")

# Initialize Oracle Client dynamically
oracledb.init_oracle_client(lib_dir=instant_client_path)

#oracledb.init_oracle_client(lib_dir=r"C:\Users\ANDESRI\Documents\BelkMain\Belktestfolder\masterextract_cmn_brd_daily\instantclient_23_8")

# =========================
# Custom Pipeline Options
# =========================
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--parameter_name",
            type=str,
            help="Parameter Manager name containing JSON configuration",
            required=False
        )

# =========================
# Parameter Manager Loader
# =========================
def load_parameter_json(name: str):
    credentials, project_id = default()
    credentials.refresh(Request())

    base_url = f"https://parametermanager.googleapis.com/v1/projects/{project_id}/locations/global/parameters/{name}/versions"
    headers = {"Authorization": f"Bearer {credentials.token}"}

    resp = requests.get(base_url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    if "parameterVersions" not in data:
        raise KeyError(f"No parameterVersions found for parameter: {name}")

    version = data["parameterVersions"][0]["name"]
    payload_resp = requests.get(f"https://parametermanager.googleapis.com/v1/{version}", headers=headers)
    payload_resp.raise_for_status()
    payload = payload_resp.json()["payload"]["data"]

    return json.loads(base64.b64decode(payload).decode())

# =========================
# Secret Manager Loader
# =========================
def load_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    project_id = default()[1]
    secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(name=secret_path)
    secret_payload = response.payload.data.decode("UTF-8")
    return json.loads(secret_payload)

# =========================
# Oracle Data Fetcher
# =========================
def fetch_from_oracle(creds: dict, query: str):
    dsn = oracledb.makedsn(
        creds["oracle_host"],
        creds["oracle_port"],
        service_name=creds["oracle_service"]
    )
    with oracledb.connect(
        user=creds["oracle_username"],
        password=creds["oracle_password"],
        dsn=dsn
    ) as conn:
        df = pd.read_sql(query, con=conn)
        return [row.to_dict() for _, row in df.iterrows()]

# =========================
# Beam DoFn to Convert Row to CSV Line
# =========================
class RowToCSVLine(beam.DoFn):
    def __init__(self, header):
        self.header = header

    def process(self, row):
        yield ','.join(str(row.get(col, '') or '') for col in self.header)

# =========================
# GCS File Management
# =========================
def archive_file(bucket_name, source_prefix, archive_prefix, archive_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=source_prefix):
        if blob.name.endswith(".csv"):
            new_name = f"{archive_prefix}/{archive_name}.csv"
            bucket.copy_blob(blob, bucket, new_name)
            logging.info(f"Archived: gs://{bucket_name}/{new_name}")

# =========================
# Main Function
# =========================
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    opts = PipelineOptions()
    google_opts = opts.view_as(GoogleCloudOptions)
    custom_opts = opts.view_as(CustomOptions)

    if not google_opts.job_name:
        google_opts.job_name = f"df-masterextract-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

    param_name = custom_opts.parameter_name.get()
    if not param_name:
        raise ValueError("--parameter_name must be provided at runtime")

    # Load parameters
    params = load_parameter_json(param_name)
    bucket_name = params["GCS_BUCKET"]
    target_path = params["GCS_OUTBOUND_PREFIX"]
    archive_path = params["ARCHIVE_GCS_OUTBOUND_PREFIX"]

    # Load Oracle creds from Secret Manager
    oracle_creds = load_secret(params["ORACLE_SECRET_ID"])

    timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')
    target_file_name = f"master_extract_{timestamp}"

    # SQL Query
    query = """
    SELECT ACCOUNT_NUMBER, SERIAL_NUMBER, ISSUE_DT, EXPIRATION_DT, REDEMPTION_DT,
           CYCLE_AMT_REWARDED, AMT_REWARD_USED, PURCHASE_AMT, PURCHASE_STORE,
           REGISTER_NUMBER, TRANS_NUMBER, REASON_CODE, REDEMPTION_ACCOUNT_NUMBER,
           LAST_MAINT_USERID, WEB_HOLD_FLAG, WEB_HOLD_DT, BATCH_FLAG, ACTION_FLAG,
           INSERTED_TMSTMP, UPD_TMSTMP, EVENT_ID
    FROM brd.brd_master
    WHERE ACCOUNT_NUMBER <> '6045839064716168'
    """

    correlation_id = f"brd-extract-{timestamp}"

    try:
        # Step 1: Pre-clean
        #remove_existing_files(bucket_name, target_path)

        # Step 2: Fetch Oracle Data
        oracle_data = fetch_from_oracle(oracle_creds, query)

        # Step 3: Pipeline
        header = [
            "ACCOUNT_NUMBER", "SERIAL_NUMBER", "ISSUE_DT", "EXPIRATION_DT", "REDEMPTION_DT",
            "CYCLE_AMT_REWARDED", "AMT_REWARD_USED", "PURCHASE_AMT", "PURCHASE_STORE",
            "REGISTER_NUMBER", "TRANS_NUMBER", "REASON_CODE", "REDEMPTION_ACCOUNT_NUMBER",
            "LAST_MAINT_USERID", "WEB_HOLD_FLAG", "WEB_HOLD_DT", "BATCH_FLAG", "ACTION_FLAG",
            "INSERTED_TMSTMP", "UPD_TMSTMP", "EVENT_ID"
        ]

        with beam.Pipeline(options=opts) as p:
            (
                p
                | "Create Oracle Data" >> beam.Create(oracle_data)
                | "Convert to CSV Line" >> beam.ParDo(RowToCSVLine(header))
                | "Write to GCS" >> beam.io.WriteToText(
                    f"gs://{bucket_name}/{target_path}/{target_file_name}",
                    file_name_suffix='.csv',
                    header=','.join(header),
                    num_shards=1,
                    shard_name_template=''
                )
            )

        # Step 4: Archive
        archive_file(bucket_name, target_path, archive_path, target_file_name)

        # Step 5: BEF success log
        publish_bef_log(generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=42,
            severity=2,
            flow_name="df_masterextract_cmn_brd_daily",
            message="Dataflow job completed successfully",
            application_component="DataflowPipeline"
        ))

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        publish_bef_log(generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=42,
            severity=3,
            flow_name="df_masterextract_cmn_brd_daily",
            message=f"Pipeline error: {e}",
            application_component="DataflowPipeline"
        ))
        raise

if __name__ == "__main__":
    run()
