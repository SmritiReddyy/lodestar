output "landing_bucket" {
  description = "Raw landing zone bucket name."
  value       = google_storage_bucket.landing.name
}

output "landing_uri" {
  description = "Value for LODESTAR_LANDING_URI."
  value       = "gs://${google_storage_bucket.landing.name}"
}

output "artifacts_bucket" {
  description = "Bucket holding dbt manifests and data quality reports."
  value       = google_storage_bucket.artifacts.name
}

output "datasets" {
  description = "BigQuery dataset ids, keyed by dbt layer."
  value       = { for k, v in google_bigquery_dataset.layers : k => v.dataset_id }
}

output "raw_dataset" {
  description = "Value for LODESTAR_RAW_DATASET."
  value       = google_bigquery_dataset.layers["raw"].dataset_id
}

output "observability_dataset" {
  description = "Dataset holding pipeline run metrics."
  value       = google_bigquery_dataset.observability.dataset_id
}

output "pipeline_service_account" {
  description = "Identity Airflow and dbt run as."
  value       = google_service_account.pipeline.email
}

output "bi_service_account" {
  description = "Read-only identity for the BI tool. Marts access only."
  value       = google_service_account.bi.email
}

output "composer_airflow_uri" {
  description = "Airflow web UI, when Composer is enabled."
  value       = var.enable_composer ? google_composer_environment.lodestar[0].config[0].airflow_uri : null
}

# Paste straight into a .env file or an Airflow connection.
output "pipeline_env" {
  description = "Environment variables the pipeline needs to target this stack."
  value = {
    LODESTAR_TARGET      = "gcp"
    LODESTAR_GCP_PROJECT = var.project_id
    LODESTAR_LANDING_URI = "gs://${google_storage_bucket.landing.name}"
    LODESTAR_RAW_DATASET = google_bigquery_dataset.layers["raw"].dataset_id
    LODESTAR_BQ_LOCATION = var.bq_location
    DBT_TARGET           = "prod"
  }
}

output "resource_count" {
  description = "Rough count of managed resources, for the README's IaC metric."
  value = (
    length(google_bigquery_dataset.layers)
    + 1 # observability dataset
    + 1 # pipeline_runs table
    + 2 # buckets
    + 2 # service accounts
    + length(google_bigquery_dataset_iam_member.pipeline_dataset_editor)
    + 6 # remaining IAM bindings
    + length(google_project_service.required)
    + (var.enable_composer ? 4 : 0)
  )
}
