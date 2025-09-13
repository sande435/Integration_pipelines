output "secret_names" {
  value = keys(var.secrets)
}

output "secret_ids" {
  value = {
    for k, s in google_secret_manager_secret.this : k => s.id
  }
}