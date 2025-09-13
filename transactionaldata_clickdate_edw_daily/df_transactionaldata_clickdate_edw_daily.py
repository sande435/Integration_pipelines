import os
import re
import csv
import json
import base64
import logging
import smtplib
import requests
import fnmatch
import tempfile
from email.mime.text import MIMEText
from datetime import datetime, timedelta, date, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

from google.cloud import storage, secretmanager
from google.auth import default
from google.auth.transport.requests import Request

from bef_integration.bef_integration import generate_bef_log, publish_bef_log


# ---------------------------
# Custom Pipeline Options
# ---------------------------
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--parameter_name",
            type=str,
            help="Parameter Manager name containing JSON configuration",
            required=True,
        )


# ---------------------------
# Parameter Manager Loader
# ---------------------------
def load_parameter_json(name: str):
    credentials, project_id = default()
    credentials.refresh(Request())

    base_url = (
        f"https://parametermanager.googleapis.com/v1/projects/{project_id}/locations/global/parameters/{name}/versions"
    )
    headers = {"Authorization": f"Bearer {credentials.token}"}
    resp = requests.get(base_url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if "parameterVersions" not in data or not data["parameterVersions"]:
        raise KeyError(f"No parameterVersions found for parameter: {name}")

    version = data["parameterVersions"][0]["name"]
    payload_resp = requests.get(
        f"https://parametermanager.googleapis.com/v1/{version}", headers=headers, timeout=60
    )
    payload_resp.raise_for_status()
    payload = payload_resp.json()["payload"]["data"]

    return json.loads(base64.b64decode(payload).decode())


# ---------------------------
# Secret Manager Loader
# ---------------------------
def load_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    project_id = default()[1]
    secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return json.loads(response.payload.data.decode("UTF-8"))


# ---------------------------
# Helpers (parsing, cleaning)
# ---------------------------

def to_numeric_order_id(val):
    """Convert ORDER_ID to integer-like string. Return empty string if not convertible."""
    if val is None:
        return ""
    s = str(val).strip()
    if s == "":
        return ""
    # try numeric conversion
    try:
        num = int(float(s))
        return str(num)
    except Exception:
        # fallback: strip non-digits
        digits = re.sub(r"\D", "", s)
        return digits if digits else ""


def parse_row(parts, original_line=None):
    """Convert a CSV parts list into a dict. Pad missing fields instead of dropping."""
    EXPECTED_FIELDS = 21

    # if empty or blank line, skip
    if not parts or all((p is None or p.strip() == "") for p in parts):
        logging.warning(f"Skipping empty row: {original_line}")
        return None

    # Pad with empty strings if too short
    if len(parts) < EXPECTED_FIELDS:
        logging.warning(
            f"Row has only {len(parts)} fields (expected {EXPECTED_FIELDS}), padding: {original_line or parts}"
        )
        parts = parts + [""] * (EXPECTED_FIELDS - len(parts))

    parts = [p.strip() for p in parts]

    try:
        return {
            "POSTING_DATE": parts[0],
            "EVENT_DATE": parts[1],
            "ID": parts[2],
            "ACTION_NAME": parts[3],
            "TYPE": parts[4],
            "CORRELATION_BASIS": parts[5],
            "STATUS": parts[6],
            "CORRECTED": parts[7],
            "SALE_AMOUNT": parts[8],
            "ORDER_DISCOUNT": parts[9],
            "PUBLISHER_COMMISSION": parts[10],
            "CJ_FEE": parts[11],
            "PUBLISHER_ID": parts[12],
            "PUBLISHER_NAME": parts[13],
            "WEBSITE_ID": parts[14],
            "WEBSITE_NAME": parts[15],
            "LINK_ID": parts[16],
            "ORDER_ID": parts[17],
            "CLICK_DATE": parts[18],
            "ACTION_ID": parts[19],
            "AD_OWNER_ADVERTISER_ID": parts[20],
        }
    except Exception as e:
        logging.exception(
            f"Unexpected error parsing row: {e}. Row: {original_line or parts}"
        )
        return None


def clean_date(date_str):
    """Return yyyy-MM-dd HH:mm:ss or None if unparseable (keeps original behavior)."""
    if not date_str or date_str.strip() == "":
        return None
    s = date_str.strip()
    fmts = [
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    # try ISO-ish fallback
    try:
        s2 = re.sub(r"Z|(\+|-)\d{2}:?\d{2}$", "", s)
        s2 = s2.replace("T", " ")
        dt = datetime.fromisoformat(s2.strip())
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        logging.debug(f"clean_date: unable to parse date '{date_str}'")
        return None


def clean_string(value):
    """Replace every pipe '|' with single space (and collapse extra spaces)."""
    if value is None:
        return ""
    replaced = value.replace("|", " ")
    return re.sub(r"\s+", " ", replaced).strip()


def is_valid(record):
    # preserve your business rule: exclude action ids 361305,405252 and require CLICK_DATE parseable
    try:
        return (
            record["ACTION_ID"] not in ["361305", "405252"]
            and clean_date(record["CLICK_DATE"]) is not None
        )
    except Exception:
        return False


def is_reject(record):
    try:
        return clean_date(record["CLICK_DATE"]) is None
    except Exception:
        return True


def process_valid(record):
    """Apply transformations required for valid rows:
       - date conversions for POSTING_DATE & EVENT_DATE & CLICK_DATE
       - replace pipes in ACTION_NAME,PUBLISHER_NAME,WEBSITE_NAME
       - convert ORDER_ID to numeric-like string
    """
    return {
        "POSTING_DATE": clean_date(record.get("POSTING_DATE", "")),
        "EVENT_DATE": clean_date(record.get("EVENT_DATE", "")),
        "CJ_ID": record.get("ID", ""),
        "ACTION_NAME": clean_string(record.get("ACTION_NAME", "")),
        "CJ_TYPE": record.get("TYPE", ""),
        "CORRELATION_BASIS": record.get("CORRELATION_BASIS", ""),
        "CJ_STATUS": record.get("STATUS", ""),
        "CJ_CORRECTED": record.get("CORRECTED", ""),
        "SALE_AMOUNT": record.get("SALE_AMOUNT", ""),
        "ORDER_DISCOUNT": record.get("ORDER_DISCOUNT", ""),
        "PUBLISHER_COMMISSION": record.get("PUBLISHER_COMMISSION", ""),
        "CJ_FEE": record.get("CJ_FEE", ""),
        "PUBLISHER_ID": record.get("PUBLISHER_ID", ""),
        "PUBLISHER_NAME": clean_string(record.get("PUBLISHER_NAME", "")),
        "WEBSITE_ID": record.get("WEBSITE_ID", ""),
        "WEBSITE_NAME": clean_string(record.get("WEBSITE_NAME", "")),
        "CJ_LINK_ID": record.get("LINK_ID", ""),
        "ORDER_ID": to_numeric_order_id(record.get("ORDER_ID", "")),
        "CLICK_DATE": clean_date(record.get("CLICK_DATE", "")),
        "ACTION_ID": record.get("ACTION_ID", ""),
        "AD_OWNER_ADVERTISER_ID": record.get("AD_OWNER_ADVERTISER_ID", ""),
    }


def process_reject(record):
    # keep original field names, clean CLICK_DATE only and convert ORDER_ID numeric
    r = dict(record)
    r["CLICK_DATE"] = clean_date(r.get("CLICK_DATE", ""))
    r["ORDER_ID"] = to_numeric_order_id(r.get("ORDER_ID", ""))
    # Also replace pipes in textual fields for consistency
    r["ACTION_NAME"] = clean_string(r.get("ACTION_NAME", ""))
    r["PUBLISHER_NAME"] = clean_string(r.get("PUBLISHER_NAME", ""))
    r["WEBSITE_NAME"] = clean_string(r.get("WEBSITE_NAME", ""))
    return r


# ---------------------------
# Output Headers
# ---------------------------
VALID_OUTPUT_HEADERS = [
    "POSTING_DATE",
    "EVENT_DATE",
    "CJ_ID",
    "ACTION_NAME",
    "CJ_TYPE",
    "CORRELATION_BASIS",
    "CJ_STATUS",
    "CJ_CORRECTED",
    "SALE_AMOUNT",
    "ORDER_DISCOUNT",
    "PUBLISHER_COMMISSION",
    "CJ_FEE",
    "PUBLISHER_ID",
    "PUBLISHER_NAME",
    "WEBSITE_ID",
    "WEBSITE_NAME",
    "CJ_LINK_ID",
    "ORDER_ID",
    "CLICK_DATE",
    "ACTION_ID",
    "AD_OWNER_ADVERTISER_ID",
]

REJECT_OUTPUT_HEADERS = [
    "POSTING_DATE",
    "EVENT_DATE",
    "ID",
    "ACTION_NAME",
    "TYPE",
    "CORRELATION_BASIS",
    "STATUS",
    "CORRECTED",
    "SALE_AMOUNT",
    "ORDER_DISCOUNT",
    "PUBLISHER_COMMISSION",
    "CJ_FEE",
    "PUBLISHER_ID",
    "PUBLISHER_NAME",
    "WEBSITE_ID",
    "WEBSITE_NAME",
    "LINK_ID",
    "ORDER_ID",
    "CLICK_DATE",
    "ACTION_ID",
    "AD_OWNER_ADVERTISER_ID",
]


def format_valid_output(row_dict):
    return "|".join(
        "" if row_dict.get(h) is None else str(row_dict.get(h))
        for h in VALID_OUTPUT_HEADERS
    )


def format_reject_output(row_dict):
    return "|".join(
        "" if row_dict.get(h) is None else str(row_dict.get(h))
        for h in REJECT_OUTPUT_HEADERS
    )


def with_header(pcoll, header, name_suffix=""):
    """Prepends a header row to the given PCollection."""
    header_label = f"CreateHeader{name_suffix}"
    concat_label = f"ConcatHeader{name_suffix}"
    header_pcoll = pcoll.pipeline | header_label >> beam.Create([header])
    return (header_pcoll, pcoll) | concat_label >> beam.Flatten()


# ---------------------------
# Email + Archive (DoFns)
# ---------------------------
class BuildAndSendEmail(beam.DoFn):
    def __init__(self, smtp_creds, params, correlation_id, reject_file):
        self.smtp_creds = smtp_creds
        self.params = params
        self.correlation_id = correlation_id
        self.reject_file = reject_file

    def process(self, reject_count):
        # reject_count is an int from Count.Globally()
        today = date.today().isoformat()

        subject = f"CJ Transactional data click date null records reject: {today}"

        body = f"""
        <html>
        <body>
        <p>Team-<br><br>
        Please find below click date null record reject details for today.<br><br>
        Reject file: {self.reject_file}<br>
        Reject count: {reject_count}<br>
        </p>
        <p>Thanks,<br>ESB Support</p>
        </body>
        </html>
        """

        msg = MIMEText(body, "html")
        msg["Subject"] = subject
        msg["From"] = self.params.get("EMAIL_FROM")
        msg["To"] = self.params.get("EMAIL_TO")
        if self.params.get("EMAIL_CC"):
            msg["Cc"] = self.params.get("EMAIL_CC")

        recipients = self.params.get("EMAIL_TO", "").split(",")
        if self.params.get("EMAIL_CC"):
            recipients += self.params.get("EMAIL_CC", "").split(",")

        try:
            smtp_host = self.smtp_creds.get("SMTP_HOST")
            smtp_port = int(self.smtp_creds.get("SMTP_PORT", 25))

            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                if self.smtp_creds.get("SMTP_TLS", "false").lower() == "true":
                    server.starttls()

                if self.smtp_creds.get("SMTP_USER") and self.smtp_creds.get("SMTP_PASSWORD"):
                    server.login(self.smtp_creds["SMTP_USER"], self.smtp_creds["SMTP_PASSWORD"])

                server.sendmail(self.params.get("EMAIL_FROM"), recipients, msg.as_string())

            yield {"status": "email_sent", "reject_count": reject_count}

        except Exception as e:
            logging.error(f"Failed to send email: {e}", exc_info=True)
            yield {"status": "email_failed", "error": str(e)}


class ArchiveInputFilesDoFn(beam.DoFn):
    """Archives inbound objects (rewrite -> delete) inside worker."""
    def __init__(self, bucket_name, src_prefix, archive_prefix):
        self.bucket_name = bucket_name
        self.src_prefix = src_prefix
        self.archive_prefix = archive_prefix
        self.client = None

    def setup(self):
        self.client = storage.Client()

    def process(self, element):
        # element is just a trigger (like count)
        bucket = self.client.bucket(self.bucket_name)
        for blob in bucket.list_blobs(prefix=self.src_prefix):
            if blob.name.endswith("_KEEP"):
                continue
            dest_name = blob.name.replace(self.src_prefix, self.archive_prefix, 1)
            dest_blob = bucket.blob(dest_name)
            try:
                dest_blob.rewrite(blob)
                blob.delete()
                logging.info(f"Archived and deleted {blob.name} -> {dest_name}")
            except Exception as e:
                logging.error(f"Failed to archive {blob.name}: {e}", exc_info=True)
        # ensure _KEEP marker
        inbound_prefix = self.src_prefix.rstrip("/") + "/"
        keep_blob = bucket.blob(inbound_prefix + "_KEEP")
        if not keep_blob.exists():
            try:
                keep_blob.upload_from_string("")
            except Exception:
                logging.debug("Failed to write _KEEP marker")
        yield f"archived:{self.src_prefix}"


class PublishSuccessBEFDoFn(beam.DoFn):
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

    def process(self, element):
        try:
            publish_bef_log(
                generate_bef_log(
                    correlation_id=self.correlation_id,
                    parent_correlation_id="",
                    payload="",
                    code=42,
                    severity=2,
                    flow_name="df_transactionaldata_clickdate_adhoc_cj_edw_daily",
                    message="Pipeline completed successfully",
                    application_component="Mule_MAO",
                )
            )
            logging.info("Published BEF success log")
        except Exception as e:
            logging.error(f"Failed to publish BEF success: {e}", exc_info=True)
        yield "bef_published"


# ---------------------------
# New: DoFn to safely read blob lines (supports multiple files / patterns)
# ---------------------------
class ReadGCSFileLines(beam.DoFn):
    """Given a blob name (full name within bucket), download it to a temp file and yield lines.
       Skips the first line (assumed header) for each file. Uses a temp file to avoid OOM on large blobs.
    """
    def __init__(self, bucket_name: str, encoding: str = "utf-8"):
        self.bucket_name = bucket_name
        self.encoding = encoding
        self.client = None
        self.bucket = None

    def setup(self):
        # initialize client in worker
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def process(self, blob_name):
        temp_path = None
        try:
            blob = self.bucket.blob(blob_name)
            # write to temp file
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                temp_path = tmp.name
            blob.download_to_filename(temp_path)

            with open(temp_path, "r", encoding=self.encoding, errors="replace") as fh:
                first = True
                for line in fh:
                    if first:
                        first = False
                        continue  # skip header line per file
                    # strip trailing newline but keep commas for csv.reader to work
                    yield line.rstrip("\n").rstrip("\r")
        except Exception as e:
            logging.error(f"Error reading GCS object {blob_name}: {e}", exc_info=True)
            # swallow error so pipeline can proceed with other files; consider failing if desired
            return
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    logging.debug(f"Failed to delete temp file {temp_path}")


# ---------------------------
# Main runner
# ---------------------------
def run():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    opts = PipelineOptions()
    google_opts = opts.view_as(GoogleCloudOptions)
    custom_opts = opts.view_as(CustomOptions)

    if not google_opts.job_name:
        google_opts.job_name = f"df-cj-clickdate-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    param_name = custom_opts.parameter_name.get()
    if not param_name:
        raise ValueError("--parameter_name must be provided")

    params = load_parameter_json(param_name)
    smtp_creds = load_secret(params["SMTP_SECRET_ID"])

    bucket = params["GCS_BUCKET"]
    inbound_prefix = params["GCS_INBOUND_PREFIX"]
    archive_prefix = params["GCS_ARCHIVE_PREFIX"]
    input_file = f"gs://{bucket}/{inbound_prefix}{params['GCS_INBOUND_FILE']}"
    output_prefix = f"gs://{bucket}/{params['GCS_OUTBOUND_PREFIX']}"
    reject_output_prefix = f"gs://{bucket}/{params['REJECT_PREFIX']}"

    prev_day_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    valid_file_prefix = f"{output_prefix}T_SLSORDAFFLCOMM_CPLOD_{prev_day_str}"
    reject_file_prefix = f"{reject_output_prefix}Clickdate_Reject_{prev_day_str}"
    reject_file_full_path = f"{reject_file_prefix}.TXT"

    correlation_id = f"cj-clickdate-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    publish_bef_log(
        generate_bef_log(
            correlation_id=correlation_id,
            parent_correlation_id="",
            payload="",
            code=10,
            severity=2,
            flow_name="df_transactionaldata_clickdate_adhoc_cj_edw_daily",
            message=f"File pickup: {input_file}, starting processing.",
            application_component="Mule_MAO",
        )
    )
    logging.info("BEF log (pickup) sent")


    # Build list of matching blobs for pipeline Create()
    if not input_file.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {input_file}")

    gcs_path = input_file.replace(f"gs://{bucket}/", "", 1)
    list_prefix = gcs_path.split("*", 1)[0]

    storage_client = storage.Client()
    all_blobs = list(storage_client.list_blobs(bucket, prefix=list_prefix))
    matching_blob_names = [b.name for b in all_blobs if fnmatch.fnmatch(b.name, gcs_path)]

    if not matching_blob_names:
        logging.warning(f"No files matched pattern {input_file}")

    try:
        with beam.Pipeline(options=opts) as p:
            # Create a PC of blob names, then read each file with ReadGCSFileLines
            raw_lines = (
                p
                | "CreateFileList" >> beam.Create(matching_blob_names)
                | "ReadCSVFiles" >> beam.ParDo(ReadGCSFileLines(bucket))
            )

            parsed = (
                raw_lines
                | "Split"
                >> beam.Map(
                    lambda line: (
                        next(csv.reader([line], delimiter=",")),  # read CSV with comma delimiter
                        line,
                    )
                )
                | "ToDict"
                >> beam.Map(
                    lambda parts_line: parse_row(
                        parts_line[0], original_line=parts_line[1]
                    )
                )
                | "FilterMalformed" >> beam.Filter(lambda r: r is not None)
            )

            lines = parsed

            # -------- Valid output --------
            valid = (
                lines
                | "FilterValid" >> beam.Filter(is_valid)
                | "ProcessValid" >> beam.Map(process_valid)
                | "FormatValid" >> beam.Map(format_valid_output)
            )

            # Prepend header and write valid output inside pipeline
            valid_with_header = with_header(valid, "|".join(VALID_OUTPUT_HEADERS), name_suffix="Valid")
            _ = valid_with_header | "WriteValidOutput" >> beam.io.WriteToText(
                file_path_prefix=valid_file_prefix,
                file_name_suffix=".TXT",
                shard_name_template="",
            )

            # -------- Reject output --------
            rejects = (
                lines
                | "FilterReject" >> beam.Filter(is_reject)
                | "ProcessReject" >> beam.Map(process_reject)
                | "FormatReject" >> beam.Map(format_reject_output)
            )

            # Prepend header and write rejects inside pipeline
            rejects_with_header = with_header(rejects, "|".join(REJECT_OUTPUT_HEADERS), name_suffix="Reject")
            _ = rejects_with_header | "WriteRejectOutput" >> beam.io.WriteToText(
                file_path_prefix=reject_file_prefix,
                file_name_suffix=".TXT",
                shard_name_template="",
            )

            # Send email based on reject count (still inside pipeline)
            reject_count = rejects | "CountRejects" >> beam.combiners.Count.Globally()
            _ = reject_count | "Send Email" >> beam.ParDo(
                BuildAndSendEmail(smtp_creds, params, correlation_id, reject_file_full_path)
             )

            
            finalize_trigger = (
                (valid_with_header, rejects_with_header)
                | "MergeForFinalize" >> beam.Flatten()
                | "FinalizeCount" >> beam.combiners.Count.Globally()
            )

            # Archive input files (runs on worker)
            _ = finalize_trigger | "ArchiveInputFiles" >> beam.ParDo(
                ArchiveInputFilesDoFn(bucket, inbound_prefix, archive_prefix)
            )

            # Publish BEF success log (runs on worker)
            _ = finalize_trigger | "PublishSuccessBEF" >> beam.ParDo(PublishSuccessBEFDoFn(correlation_id))

        # Note: no local post-pipeline archive/publish — those are now inside Beam graph (as above)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        publish_bef_log(
            generate_bef_log(
                correlation_id=correlation_id,
                parent_correlation_id="",
                payload="",
                code=42,
                severity=3,
                flow_name="df_transactionaldata_clickdate_adhoc_cj_edw_daily",
                message=f"Pipeline error: {e}",
                application_component="Mule_MAO",
            )
        )
        raise


if __name__ == "__main__":
    run()
