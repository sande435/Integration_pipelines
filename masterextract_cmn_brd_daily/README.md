#  Dataflow Pipeline – BRD Master Extract (Oracle to GCS)

> **Daily Master Extract** | **Apache Beam on Google Dataflow** | **Oracle → GCS**

---

## Overview

This Apache Beam pipeline (running on **Google Cloud Dataflow**) automates a **daily master extract** of BRD data from **Oracle DB** and exports it as a **CSV file to GCS**.

- ✅ **Connects securely to Oracle** using credentials from Secret Manager
- ✅ **Executes SQL query** to fetch latest records
- ✅ **Converts Oracle rows to CSV**
- ✅ **Stores output to GCS** in structured folders
- ✅ **Archives processed files**
- ✅ **Publishes BEF logs** for job lifecycle: `Start`, `Success`, `Error`

> 💡 The pipeline is fully **parameterized** and **secret-managed** – no hardcoded credentials or paths.

---

## Key Workflow

- 🗂 Reads config from **Parameter Manager**
- 🔐 Loads Oracle DB credentials from **Secret Manager**
- 🧾 Runs SQL query to fetch data from `brd.brd_master`
- 🔄 Transforms data rows into CSV format
- ☁️ Writes result to:
  - `gs://<bucket>/outbound/master_extract_<timestamp>.csv`
- 📦 Archives processed files
- 📝 Publishes **BEF logs** for traceability and alerts

---

## Configuration

Pipeline is configured using **Google Parameter Manager** and secrets are loaded from **Secret Manager**.

### 📁 Example Parameter JSON

```json
{
  "GCS_BUCKET": "your-bucket-name",
  "GCS_OUTBOUND_PREFIX": "outbound/",
  "ARCHIVE_GCS_OUTBOUND_PREFIX": "archive/",
  "ORACLE_SECRET_ID": "oracle-db-creds"
}

## 🚀 How to Run

### Direct Job Run
python df_masterextract_cmn_brd_daily.py \
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
python df_masterextract_cmn_brd_daily.py \
  --runner DataflowRunner \
  --project <PROJECT_ID> \
  --region <REGION> \
  --setup_file ./setup.py \
  --temp_location gs://<BUCKET_NAME>/temp/ \
  --staging_location gs://<BUCKET_NAME>/staging/ \
  --template_location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json


### Launch from Template:
gcloud dataflow jobs run "df_masterextract_cmn_brd_daily" \
  --project <PROJECT_ID> \
  --region <REGION> \
  --gcs-location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json \
  --parameters parameter_name=<PARAMETER_NAME> \
  --max-workers 20 \
  --num-workers 2 \
  --worker-machine-type n1-highmem-4 \
  --disable-public-ips


