# One dataset per dbt layer. Splitting them is what lets BI be granted read on
# `marts` alone, without exposing raw source data or half-built intermediates.
resource "google_bigquery_dataset" "layers" {
  for_each = local.datasets

  project     = var.project_id
  dataset_id  = each.value.id
  description = each.value.description
  location    = var.bq_location
  labels      = local.common_labels

  delete_contents_on_destroy = var.dataset_delete_contents_on_destroy

  # Raw and test-failure data expire on their own; modelled layers are rebuilt
  # by dbt on every run and need no default expiry.
  default_table_expiration_ms = contains(["test_failures"], each.key) ? 30 * 24 * 3600 * 1000 : null

  depends_on = [google_project_service.required]
}

# A dedicated dataset the pipeline writes its own run metrics into, so "how long
# did the DAG take last Tuesday" is a SQL question rather than a log search.
resource "google_bigquery_dataset" "observability" {
  project     = var.project_id
  dataset_id  = "lodestar_observability${local.dataset_suffix}"
  description = "Pipeline run metrics: durations, row counts, test outcomes."
  location    = var.bq_location
  labels      = local.common_labels

  delete_contents_on_destroy = var.dataset_delete_contents_on_destroy

  depends_on = [google_project_service.required]
}

resource "google_bigquery_table" "pipeline_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.observability.dataset_id
  table_id            = "pipeline_runs"
  description         = "One row per DAG task execution."
  deletion_protection = false
  labels              = local.common_labels

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  clustering = ["dag_id", "task_id"]

  schema = jsonencode([
    { name = "run_date", type = "DATE", mode = "REQUIRED", description = "Logical date of the run." },
    { name = "dag_id", type = "STRING", mode = "REQUIRED" },
    { name = "task_id", type = "STRING", mode = "REQUIRED" },
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "started_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "finished_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "duration_seconds", type = "FLOAT", mode = "NULLABLE" },
    { name = "status", type = "STRING", mode = "REQUIRED", description = "success | failed | skipped" },
    { name = "rows_processed", type = "INTEGER", mode = "NULLABLE" },
    { name = "details", type = "JSON", mode = "NULLABLE" },
  ])
}
