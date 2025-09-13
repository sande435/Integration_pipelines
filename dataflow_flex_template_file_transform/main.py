import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from datetime import datetime
import json
import base64
import requests
from google.auth import default
from google.auth.transport.requests import Request
import sys

# -------------------------
# Load Parameter JSON from GCP Parameter Manager
# -------------------------
def load_parameter_json(parameter_name):
    credentials, project_id = default()
    credentials.refresh(Request())

    url = f"https://parametermanager.googleapis.com/v1/projects/{project_id}/locations/global/parameters/{parameter_name}/versions"
    headers = {"Authorization": f"Bearer {credentials.token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    latest_version = response.json()['parameterVersions'][0]['name']
    param_response = requests.get(f"https://parametermanager.googleapis.com/v1/{latest_version}", headers=headers)
    param_response.raise_for_status()

    payload_data = param_response.json()['payload']['data']
    return json.loads(base64.b64decode(payload_data).decode('utf-8'))

# -------------------------
# CSV line parser
# -------------------------
def parse_csv(line):
    return line.split(',')

# -------------------------
# Transform fields
# -------------------------
def transform(fields):
    try:
        fields[3] = fields[3].replace('|', ' ')
    except IndexError:
        pass
    return ','.join(fields)

# -------------------------
# Custom options to support Flex Template
# -------------------------
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--parameter_name",
            required=True,
            help="The name of the parameter in Parameter Manager"
        )

# -------------------------
# Main Beam pipeline
# -------------------------


def run():
    # Pass sys.argv to retain CLI args like --parameter_name
    pipeline_options = PipelineOptions(sys.argv)
    custom_options = pipeline_options.view_as(CustomOptions)

    parameter_name = custom_options.parameter_name
    params = load_parameter_json(parameter_name)

    input_path = params["INBOUND_PATH"]
    output_path_base = params["OUTBOUND_PATH"]
    gcp_project = params.get("GCP_PROJECT")
    gcp_region = params.get("GCP_REGION")
    staging_location = params.get("STAGING_LOCATION")
    temp_location = params.get("TEMP_LOCATION")
    job_name = params.get("JOB_NAME", "beam-job")

    today = datetime.now().strftime('%Y%m%d')
    output_path = f"{output_path_base}_{today}"

    # Append pipeline arguments from JSON param config
    pipeline_args = [
        "--runner=DataflowRunner",
        f"--project={gcp_project}",
        f"--region={gcp_region}",
        f"--staging_location={staging_location}",
        f"--temp_location={temp_location}",
        f"--job_name={job_name}-{today}",
    ]

    # Combine CLI args with pipeline args
    options = PipelineOptions(sys.argv + pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read CSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Transform Fields" >> beam.Map(transform)
            | "Write CSV" >> beam.io.WriteToText(output_path, file_name_suffix=".csv")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
