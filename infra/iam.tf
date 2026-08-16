# Two identities with different jobs, rather than one that can do everything:
#
#   pipeline - writes the landing zone and builds the warehouse (Airflow, dbt)
#   bi       - reads the marts and nothing else (Metabase, Looker Studio)
#
# The split is the point. A compromised dashboard credential cannot drop a
# dataset, and a BI tool cannot accidentally query raw PII in the landing zone.

resource "google_service_account" "pipeline" {
  project      = var.project_id
  account_id   = "${local.name_prefix}-pipeline"
  display_name = "Lodestar pipeline (${var.environment})"
  description  = "Runs extraction, loading and dbt transformations."

  depends_on = [google_project_service.required]
}

resource "google_service_account" "bi" {
  project      = var.project_id
  account_id   = "${local.name_prefix}-bi"
  display_name = "Lodestar BI reader (${var.environment})"
  description  = "Read-only access to the marts layer for dashboards."

  depends_on = [google_project_service.required]
}

# --- pipeline: storage -----------------------------------------------------

# Scoped to the two buckets rather than granted project-wide.
resource "google_storage_bucket_iam_member" "pipeline_landing_admin" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_artifacts_admin" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# --- pipeline: BigQuery ----------------------------------------------------

# Running a query needs the job-user role at project level; there is no
# dataset-scoped equivalent. Data access itself stays dataset-scoped below.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_dataset_editor" {
  for_each = google_bigquery_dataset.layers

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_observability_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.observability.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

# --- BI: marts only --------------------------------------------------------

resource "google_project_iam_member" "bi_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.bi.email}"
}

resource "google_bigquery_dataset_iam_member" "bi_marts_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.layers["marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.bi.email}"
}

# Deliberately no binding for raw/staging/intermediate: the BI identity cannot
# reach them at all, so a dashboard cannot be pointed at an unmodelled table.
