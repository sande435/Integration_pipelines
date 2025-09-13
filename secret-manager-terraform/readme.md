# GCP Secret Manager Terraform Module

## Purpose

Reusable module to create, rotate, and manage secrets (like DB passwords, API keys) in GCP Secret Manager.

## Structure

- `modules/secret-manager` → reusable core
- `envs/dev` → environment-specific usage
- `envs/prod` → production usage

## Usage

```bash
cd envs/dev
terraform init
terraform plan
terraform apply