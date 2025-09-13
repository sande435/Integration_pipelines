variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "secrets" {
  type        = map(string)
  description = "Map of secret names and initial secret values"
}

resource "google_secret_manager_secret" "this" {
  for_each = var.secrets

  secret_id = each.key

  replication {
    auto {}
  }

}

resource "google_secret_manager_secret_version" "this" {
  for_each = var.secrets

  secret      = google_secret_manager_secret.this[each.key].id
  secret_data = each.value
}



