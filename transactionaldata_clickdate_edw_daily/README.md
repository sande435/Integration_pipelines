#  Dataflow Pipeline – CJ Transaction Click Date ETL

> **Automated Daily Validation** | **Apache Beam on Google Dataflow** | **GCS → Validate → Classify → Notify**

---

## Overview

This Apache Beam pipeline (running on **Google Cloud Dataflow**) automates a **daily ETL validation workflow** for CJ affiliate transaction data.

- ✅ **Reads files from Google Cloud Storage (GCS)**
- ✅ **Validates business rules** for each row
- ✅ **Classifies** rows into *valid* and *rejects*
- ✅ **Emails reject summary** to stakeholders
- ✅ **Archives** source files
- ✅ **Publishes BEF logs** for job lifecycle: `Start`, `Success`, `Error`

> 💡 The pipeline is fully **parameterized** and **secret-managed** – no hardcoded credentials or paths.

---

## Key Workflow

- 📂 Scans **GCS inbound folder** for CSV input files
- 📤 Reads and parses records line-by-line
- 🔍 Validates records using business rules:
  - Must have a valid `CLICK_DATE`
  - Excludes `ACTION_ID` in `[361305, 405252]`
- 🧼 Transforms valid rows (date normalization, pipe removal)
- ❌ Rejects invalid rows (with cleaned formatting)
- 📨 Sends email if any rejects are found
- 🪵 Publishes **BEF logs** for observability
- 📦 Archives input files after processing

---

## Configuration

Pipeline is configured using **Google Parameter Manager** and secrets are loaded from **Secret Manager**.

### 📁 Example Parameter JSON

{
  "GCS_BUCKET": "your-bucket-name",
  "GCS_INBOUND_PREFIX": "inbound/",
  "GCS_INBOUND_FILE": "*.csv",
  "GCS_OUTBOUND_PREFIX": "outbound/",
  "GCS_ARCHIVE_PREFIX": "archive/",
  "REJECT_PREFIX": "reject/",
  "EMAIL_FROM": "noreply@yourdomain.com",
  "EMAIL_TO": "team@yourdomain.com",
  "EMAIL_CC": "manager@yourdomain.com",
  "SMTP_SECRET_ID": "smtp-credentials-secret"
}

## 🚀 How to Run

### Direct Job Run
python df_transactionaldata_clickdate_edw_daily.py \
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
python df_transactionaldata_clickdate_edw_daily.py \
  --runner DataflowRunner \
  --project <PROJECT_ID> \
  --region <REGION> \
  --setup_file ./setup.py \
  --temp_location gs://<BUCKET_NAME>/temp/ \
  --staging_location gs://<BUCKET_NAME>/staging/ \
  --template_location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json


### Launch from Template:
gcloud dataflow jobs run "df_transactionaldata_clickdate_edw_daily" \
  --project <PROJECT_ID> \
  --region <REGION> \
  --gcs-location gs://<BUCKET_NAME>/templates/ovative_azure_esb_daily_template.json \
  --parameters parameter_name=<PARAMETER_NAME> \
  --max-workers 20 \
  --num-workers 2 \
  --worker-machine-type n1-highmem-4 \
  --disable-public-ips

