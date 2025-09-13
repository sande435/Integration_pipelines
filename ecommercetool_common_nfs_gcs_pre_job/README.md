#  Dataflow Pipeline – NFS to GCS File Transfer

> **Automated File Movement** | **Apache Beam on Google Dataflow** | **NFS → GCS Archive**
This template allows for the efficient and reliable transfer of hundreds of files across multiple interfaces. You simply need to provide the relevant parameters for each interface, and the system will automatically handle the file transfer to its corresponding GCS destination. This eliminates manual effort and ensures a consistent, repeatable process for all your file archiving needs.

---

## Overview

This Apache Beam pipeline (running on **Google Cloud Dataflow**) automates the **daily transfer of files from a remote NFS (via SFTP)** to **Google Cloud Storage (GCS)**.

- ✅ **Connects securely to NFS using SFTP**
- ✅ **Transfers files matching a pattern**
- ✅ **Uploads to GCS outbound folder**
- ✅ **Moves processed files to archive folder on NFS**
- ✅ **Logs progress and errors**

> 💡 The pipeline is fully **parameterized** and **secret-managed** – no hardcoded credentials or paths.

---

## Key Workflow

- 🔐 Loads SSH credentials from **Secret Manager**
- 📁 Connects to **remote NFS via SFTP (Paramiko)**
- 🔎 Scans NFS **inbound folder** for files matching a **glob pattern**
- ☁️ Transfers matched files to **GCS outbound path**
- 🗃 Archives files by moving them to a different NFS folder
- 📝 Logs each successful or failed file transfer

---

## Configuration

Pipeline is configured using **Google Parameter Manager** and **Secret Manager**.

### 📁 Example Parameter JSON

{
  "PRE_NFS_INBOUND_PREFIX": "/nfs/inbound",
  "PRE_NFS_INBOUND_ARCHIVE_PREFIX": "/nfs/archive",
  "PRE_NFS_INBOUND_FILE_NAME": "*.csv",
  "PRE_TARGET_GCS_PROJECT": "your-gcp-project-id",
  "PRE_TARGET_GCS_BUCKET": "your-bucket-name",
  "PRE_GCS_OUTBOUND_PREFIX": "outbound/nfs/",
  "PRE_NFS_SECRET_ID": "nfs-sftp-secret"
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