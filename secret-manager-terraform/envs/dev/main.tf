module "secret_manager" {
  source     = "../../modules/secret-manager"
  project_id = var.project_id

  secrets = {
    "EDQ_CDW_EDQEMLRESULTS_TF"     = jsonencode({
      user      = "dwqetilt"
      password  = "xxxxxx"
      account   = "sf.east-us-2.azure"
      warehouse = "CDW_WH_QA"
      database  = "MARKETINGQA"
      schema    = "SYNCHRONYACC"
      table     = "EDQ_EML_RESULTS"
    })
  }

  # iam_member = "user:someone@example.com"  # Optional: include only if you want IAM binding!
}
