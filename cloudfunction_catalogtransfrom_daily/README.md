#  Cloud Function – GCS File Processor (Wunderkind)

> **Event-Driven GCS File Copier** | **Python Cloud Function** | **GCS → GCS + BEF Logs**
This cloud function template allows for the efficient and reliable transfer of hundreds of files across multiple interfaces. You simply need to provide the relevant parameters for each interface, and the system will automatically handle the file transfer to its corresponding GCS destination. This eliminates manual effort and ensures a consistent, repeatable process for all your file archiving needs.

---

## Overview

This Google Cloud Function automates the **detection and copying of `.gz` files** from one GCS prefix to another. It is designed for **integration with Wunderkind**, with complete logging via **BEF Pub/Sub**.


- ✅ **Triggers via HTTP**
- ✅ **Reads runtime parameters from Parameter Manager**
- ✅ **Copies files from inbound → outbound GCS folder**
- ✅ **Publishes BEF logs for pickup, delivery, and errors**

> 💡 No credentials or file paths are hardcoded. Everything is driven via **Parameter Manager** and **Secret Manager**.

---

## Key Workflow

- 📦 Lists `.gz` files in the **inbound GCS folder**
- 🔐 Loads pipeline configuration from **Parameter Manager**
- 📁 Copies each matched file to **outbound GCS folder**
- 📝 Sends **BEF logs**:
  - 📥 `File pickup`
  - ✅ `File delivered`
  - ❌ `Error during processing`

---

## Configuration

### 🧩 Parameter Manager JSON

{
  "GCS_BUCKET": "your-bucket-name",
  "GCS_INBOUND_PREFIX": "inbound/wunderkind/",
  "GCS_OUTBOUND_PREFIX": "outbound/wunderkind/"
}


## Define cloud function
gcloud functions deploy cf_gcs_gz_files_test \
    --project=<PROJECT_ID> \
    --gen2 \
    --runtime=python311 \
    --region=<REGION> \
    --source=. \
    --entry-point=main_entry \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=<SERVICE_ACCOUNT_EMAIL>
    
## Triggers via HTTP
curl -X POST "<your job cloud function url>" \
  -H "Content-Type: application/json" \
  -d '{"parameter_name": "catalogtransfrom-dwre-wunderkind-dailyjob1"}'

curl -X POST "<your job cloud function url>" \
  -H "Content-Type: application/json" \
  -d '{"parameter_name": "catalogencode-dwre-wunderkind-dailyjob2"}'

