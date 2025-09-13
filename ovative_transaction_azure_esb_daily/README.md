#  Dataflow Pipeline – Ovative Transaction Azure ESB Daily

> **Automated Daily Integration** | **Apache Beam on Google Dataflow** | **Azure → Snowflake → GCS**

---

## Overview

This Apache Beam pipeline (running on **Google Cloud Dataflow**) automates a **daily integration workflow** across:

- ✅ **Azure Blob Storage**
- ✅ **Snowflake**
- ✅ **Google Cloud Storage (GCS)**

### Key Workflow

- 🔍 Fetches `CURR_DAY` (processing date) dynamically from **Snowflake**
- 🧠 Computes Azure Blob path using the processing date
- 🔐 Downloads file from Azure using a **SAS token stored in Secret Manager**
- 🧾 Renames the file with a timestamp for traceability
- ☁️ Uploads the file to **GCS outbound and archive folders**
- 📝 Publishes **BEF logs** for job lifecycle: `Start`, `Success`, `Error`

> 💡 The pipeline is fully **parameterized** and **secret-managed** – no hardcoded credentials or paths.


## Architecture Diagram

flowchart LR
    Snowflake -->|CURR_DAY| ComputeBlobPath
    ComputeBlobPath --> AzureBlob[Azure Blob Storage]
    AzureBlob --> DownloadAzure[Download with SAS]
    DownloadAzure --> GCS[(Google Cloud Storage)]
    GCS --> BEFLogging[BEF Logs: Start / Success / Error].


## Organized paths and credentials:
{
  "AZURE_BLOB_NAME": "transaction_file",
  "AZURE_SECRET_ID": "azure-sas-secret",  ID from secret manager
  "SF_SECRET_ID": "snowflake-credentials", ID from secret manager
  "GCS_BUCKET": "ecomintegrations",
  "GCS_OUTBOUND_PREFIX": "outbound/",
  "GCS_ARCHIVE_PREFIX": "archive/",
  "BEF_FLOW_NAME": "df_ovative_transaction_azure_esb_daily"
}


## 🚀 How to Run

### Direct Job Run
python df_ovative_transaction_azure_esb_daily.py \
  --runner DataflowRunner \
  --project <PROJECT_ID> \
  --region <REGION> \
  --temp_location gs://<BUCKET_NAME>/temp/ \
  --staging_location gs://<BUCKET_NAME>/staging/ \
  --setup_file ./setup.py \
  --parameter_name <PARAMETER_NAME> \
  --service_account_email <SERVICE_ACCOUNT_EMAIL> \
  --network <VPC_NETWORK> \
  --subnetwork <SUBNETWORK_URL> \
  --max_num_workers 10 \
  --worker_machine_type n1-standard-4 \
  --no_use_public_ips

### Build Dataflow Template
python df_ovative_transaction_azure_esb_daily.py \
  --runner DataflowRunner \
  --project <PROJECT_ID> \
  --region <REGION> \
  --setup_file ./setup.py \
  --temp_location gs://<BUCKET_NAME>/temp/ \
  --staging_location gs://<BUCKET_NAME>/staging/ \
  --template_location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json


### Launch from Template:
gcloud dataflow jobs run "df_ovative_transaction_azure_esb_daily" \
  --project <PROJECT_ID> \
  --region <REGION> \
  --gcs-location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json \
  --parameters parameter_name=<PARAMETER_NAME> \
  --max-workers 20 \
  --num-workers 2 \
  --worker-machine-type n1-highmem-4 \
  --disable-public-ips
