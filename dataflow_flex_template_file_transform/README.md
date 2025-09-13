#  Dataflow Flex Template – CSV Field Transformer

> **Flex Template Pipeline** | **Apache Beam on Dataflow** | **GCS CSV → GCS CSV (Transformed)**

---

## Overview

This Apache Beam pipeline, built as a **Flex Template** using **Docker** and **Artifact Registry**, performs **daily CSV transformation**:

- ✅ Reads `.csv` files from a **GCS input path**
- ✅ Transforms content using Beam `Map` transforms
- ✅ Writes processed `.csv` files to a **GCS output path**
- ✅ Fully configurable via **GCP Parameter Manager**

> 💡 Ideal for use cases like **pipe-to-space replacements**, data prep, or format normalization across environments.

---

## Key Workflow

- 🧾 Reads CSV from GCS path defined in **Parameter Manager**
- 📊 Parses each line to split fields by comma
- 🔧 Replaces all `|` pipe characters in the **4th column** with a space
- 💾 Writes the updated CSV lines to a **dated output folder**
- 📝 All runtime config is dynamic – no hardcoded paths

---

## Configuration

### 📘 Parameter Manager JSON

This pipeline uses a **Parameter Manager key** (e.g., `csv_transform_config`) to pass runtime settings:

{
  "INBOUND_PATH": "gs://your-bucket/input/file.csv",
  "OUTBOUND_PATH": "gs://your-bucket/output/processed_file",
  "GCP_PROJECT": <PROJECT_ID>,
  "GCP_REGION": <REGION>,
  "STAGING_LOCATION": "gs://your-bucket/staging/",
  "TEMP_LOCATION": "gs://your-bucket/temp/",
  "JOB_NAME": "csv-transform-job"
}

#### create_Repository(optional create repo if not defined for your jobs.
#Create artifact repository if not already
#gcloud artifacts repositories create dataflow-flex-repo \
#  --repository-format=docker \
#  --location=us-east1 \
#  --description="Docker repo for Dataflow Flex Templates"


### build_and_push
# Set variables
export PROJECT_ID=<PROJECT_ID>
export REGION=<REGION>
export REPO=dataflow-flex-repo
export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/gcs-csv-transform-sm:latest"

# Authenticate Docker with Artifact Registry
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Build and push the image
docker build -t $IMAGE_NAME .         
docker push $IMAGE_NAME              

#gcloud container images list --repository=gcr.io/$PROJECT_ID

## deploy_template
gcloud dataflow flex-template build \
  gs://source_raw_edw_2/templates/gcs_csv_transform_artifact.json \
  --image "$IMAGE_NAME" \
  --sdk-language "PYTHON" \
  --metadata-file metadata.json
  
  
## trigger_job  
 gcloud dataflow flex-template run "gcs-csv-transform-job" \
  --project=<PROJECT_ID> \
  --region=<REGION> \
  --template-file-gcs-location=gs://source_raw_edw_2/templates/gcs_csv_transform_artifact.json \
  --parameters parameter_name=source-path-new1