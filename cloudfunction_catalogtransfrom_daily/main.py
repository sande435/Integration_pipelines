import os
import json
import base64
import logging
import requests
import uuid
from google.cloud import storage
from google.auth import default
from google.auth.transport.requests import Request
from google.api_core import retry
from bef_integration import generate_bef_log, publish_bef_log  

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ===== Load parameter from Parameter Manager =====
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

    version = data['parameterVersions'][0]['name']
    payload_resp = requests.get(f"https://parametermanager.googleapis.com/v1/{version}", headers=headers)
    payload_resp.raise_for_status()
    payload = payload_resp.json()['payload']['data']

    return json.loads(base64.b64decode(payload).decode())


# ===== Main file processing logic =====
def process_files(parameter_name: str):
    logging.info(f"Fetching parameters for: {parameter_name}")
    params = load_parameter_json(parameter_name)

    bucket_name = params['GCS_BUCKET']
    inbound_prefix = params['GCS_INBOUND_PREFIX'].rstrip('/') + '/'
    outbound_prefix = params['GCS_OUTBOUND_PREFIX'].rstrip('/')

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket_name, prefix=inbound_prefix))
    gz_files = [b for b in blobs if b.name.endswith(".gz")]

    if not gz_files:
        logging.info("No .gz files found.")
        return {"status": "no_files"}

    retry_policy = retry.Retry(deadline=300, maximum=5)

    # Generate IDs for BEF correlation
    corr_id = str(uuid.uuid4())
    parent_id = str(uuid.uuid4())

    try:
        for blob in gz_files:
            file_name = os.path.basename(blob.name)

            # Step 1: File pickup log
            pickup_log = generate_bef_log(
                correlation_id=corr_id,
                parent_correlation_id=parent_id,
                payload="",
                code=10,
                severity=2,
                flow_name="bef-catalogtransfrom-dwre-wunderkind-daily",
                message=f"File pickup: {file_name}, starting copy process.",
                application_component="Mule_MAO"
            )
            publish_bef_log(pickup_log)
            logging.info("BEF log (pickup) sent to Pub/Sub")

            # Perform the file copy
            target_path = f"{outbound_prefix}/{file_name}"
            logging.info(f"Copying {blob.name} → {target_path}")
            bucket.copy_blob(blob, bucket, target_path, retry=retry_policy)
            logging.info(f"✅ Copied: {blob.name} → {target_path}")

            # Step 3: File delivered log
            delivered_log = generate_bef_log(
                correlation_id=corr_id,
                parent_correlation_id=parent_id,
                payload="",
                code=42,
                severity=2,
                flow_name="bef-catalogtransfrom-dwre-wunderkind-daily",
                message=f"File delivered to Wunderkind: {file_name}",
                application_component="Mule_MAO"
            )
            publish_bef_log(delivered_log)
            logging.info("BEF log (delivery) sent to Pub/Sub")

        return {"status": "success", "files_copied": len(gz_files)}

    except Exception as e:
        # Step 4: Error log
        error_log = generate_bef_log(
            correlation_id=corr_id,
            parent_correlation_id=parent_id,
            payload="",
            code=42,
            severity=3,
            flow_name="bef-catalogtransfrom-dwre-wunderkind-daily",
            message=f"Error during processing: {e}",
            application_component="Mule_MAO"
        )
        publish_bef_log(error_log)
        logging.error("BEF log (error) sent to Pub/Sub")
        raise


# ===== Cloud Function entrypoint =====
def main_entry(request):
    """HTTP Cloud Function entry point."""
    request_json = request.get_json(silent=True)
    request_args = request.args

    # Parameter name can come from query param or JSON body
    parameter_name = None
    if request_json and 'parameter_name' in request_json:
        parameter_name = request_json['parameter_name']
    elif request_args and 'parameter_name' in request_args:
        parameter_name = request_args['parameter_name']

    if not parameter_name:
        return {"error": "Missing required parameter 'parameter_name'"}, 400

    try:
        result = process_files(parameter_name)
        return result, 200
    except Exception as e:
        logging.exception("Error processing files")
        return {"error": str(e)}, 500
