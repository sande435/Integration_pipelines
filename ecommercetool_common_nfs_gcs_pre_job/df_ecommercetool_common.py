import os
import json
import base64
import logging
import posixpath
import stat
import fnmatch
import datetime
import paramiko
import requests
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage
from google.auth import default
from google.auth.transport.requests import Request


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
            help="Parameter Manager name containing config JSON.",
        )


# =========================
# Helpers: Parameter & Secret Loaders
# =========================
def _gcp_auth_headers():
    credentials, project_id = default()
    credentials.refresh(Request())
    return credentials, project_id, {"Authorization": f"Bearer {credentials.token}"}


def load_parameter_json(name: str):
    if not name:
        raise ValueError("Parameter name is empty or not provided.")
    _, project_id, headers = _gcp_auth_headers()

    base_url = (
        f"https://parametermanager.googleapis.com/v1/"
        f"projects/{project_id}/locations/global/parameters/{name}/versions"
    )
    resp = requests.get(base_url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if "parameterVersions" not in data or not data["parameterVersions"]:
        raise KeyError(f"No versions found for parameter: {name}")

    version_name = data["parameterVersions"][0]["name"]
    payload_resp = requests.get(
        f"https://parametermanager.googleapis.com/v1/{version_name}",
        headers=headers,
        timeout=60,
    )
    payload_resp.raise_for_status()
    payload = payload_resp.json()["payload"]["data"]
    return json.loads(base64.b64decode(payload).decode())


def load_secret(secret_id: str):
    if not secret_id:
        raise ValueError("Secret ID is empty or not provided.")
    _, project_id, headers = _gcp_auth_headers()

    url = (
        f"https://secretmanager.googleapis.com/v1/projects/{project_id}/"
        f"secrets/{secret_id}/versions/latest:access"
    )
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    payload_b64 = resp.json()["payload"]["data"]
    text = base64.b64decode(payload_b64).decode()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


# =========================
# DoFn: Transfer Files from NFS → GCS + Archive source
# =========================
class TransferFilesDoFn(beam.DoFn):
    def __init__(self, parameter_name_vp):
        self.parameter_name_vp = parameter_name_vp
        self.params = None
        self.secret = None
        self.ssh_client = None
        self.sftp_client = None
        self.gcs_client = None
        self.bucket = None

    def setup(self):
        param_name = self.parameter_name_vp.get() if self.parameter_name_vp else None
        if not param_name:
            raise RuntimeError("Missing --parameter_name at runtime.")
        self.params = load_parameter_json(param_name)

        # Expecting JSON secret with SSH_* fields
        self.secret = load_secret(self.params["PRE_NFS_SECRET_ID"])
        if not isinstance(self.secret, dict):
            raise ValueError("NFS secret must be JSON with SSH_HOST/PORT/USER/PASSWORD.")

        # GCS client for target project/bucket
        self.gcs_client = storage.Client(project=self.params["PRE_TARGET_GCS_PROJECT"])
        self.bucket = self.gcs_client.bucket(self.params["PRE_TARGET_GCS_BUCKET"])

    def process(self, _):
        files_transferred_count = 0

        try:
            # SSH/SFTP connect
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.secret["SSH_HOST"],
                port=int(self.secret["SSH_PORT"]),
                username=self.secret["SSH_USER"],
                password=self.secret["SSH_PASSWORD"],
                timeout=30,
            )
            self.sftp_client = self.ssh_client.open_sftp()

            inbound_prefix = self.params["PRE_NFS_INBOUND_PREFIX"]
            archive_prefix = self.params["PRE_NFS_INBOUND_ARCHIVE_PREFIX"]
            filename_pattern = self.params["PRE_NFS_INBOUND_FILE_NAME"]

            # List files in inbound dir
            directory_items = self.sftp_client.listdir_attr(inbound_prefix)

            for item_attr in directory_items:
                if not stat.S_ISREG(item_attr.st_mode):
                    continue
                if not fnmatch.fnmatch(item_attr.filename, filename_pattern):
                    continue

                remote_file_path = posixpath.join(inbound_prefix, item_attr.filename)
                gcs_outbound_blob = posixpath.join(
                    self.params["PRE_GCS_OUTBOUND_PREFIX"].strip("/"), item_attr.filename
                )

                # Upload to GCS
                with self.sftp_client.open(remote_file_path, "rb") as f:
                    self.bucket.blob(gcs_outbound_blob).upload_from_file(f, timeout=300)

                logging.info(
                    "Transferred %s → gs://%s/%s",
                    remote_file_path,
                    self.bucket.name,
                    gcs_outbound_blob,
                )

                # Move file to archive (this deletes from inbound automatically)
                archive_path = posixpath.join(archive_prefix, item_attr.filename)
                try:
                    self.sftp_client.rename(remote_file_path, archive_path)
                    logging.info("Archived source file to: %s", archive_path)
                except Exception as e:
                    logging.error("Failed to archive %s: %s", remote_file_path, e)
                    continue

                files_transferred_count += 1

        finally:
            try:
                if self.sftp_client:
                    self.sftp_client.close()
            finally:
                if self.ssh_client:
                    self.ssh_client.close()

        if files_transferred_count == 0:
            raise RuntimeError("No matching files found at NFS source.")

        yield f"Transferred {files_transferred_count} files"


# =========================
# Main Pipeline
# =========================
def run():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    logging.info("Starting pipeline at %s", timestamp)

    opts = PipelineOptions(save_main_session=True)
    opts.view_as(SetupOptions).save_main_session = True
    custom_opts = opts.view_as(CustomOptions)

    with beam.Pipeline(options=opts) as p:
        (
            p
            | "Seed" >> beam.Create([None])
            | "Transfer Files" >> beam.ParDo(TransferFilesDoFn(custom_opts.parameter_name))
        )


if __name__ == "__main__":
    run()
