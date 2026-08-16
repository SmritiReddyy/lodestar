# Managed Airflow, off by default.
#
# A Composer 3 environment bills roughly USD 300+/month whether or not a DAG is
# running, which is the wrong default for a portfolio project. The identical DAG
# runs on the docker-compose stack in `airflow/` for nothing. Enable this only
# to demonstrate a managed deployment, then destroy it.

resource "google_project_service" "composer" {
  count = var.enable_composer ? 1 : 0

  project            = var.project_id
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}

# Composer's own agent needs permission to act as the environment's service
# account; without this the environment fails to create with an opaque error.
resource "google_service_account_iam_member" "composer_agent_sa_user" {
  count = var.enable_composer ? 1 : 0

  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:service-${data.google_project.this.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "composer_worker" {
  count = var.enable_composer ? 1 : 0

  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_composer_environment" "lodestar" {
  count = var.enable_composer ? 1 : 0

  project = var.project_id
  name    = "${local.name_prefix}-airflow"
  region  = var.region
  labels  = local.common_labels

  config {
    software_config {
      image_version = var.composer_image_version

      # The DAG reads its configuration from the environment, exactly as the
      # local docker-compose stack does, so the same file runs in both.
      env_variables = {
        LODESTAR_TARGET      = "gcp"
        LODESTAR_GCP_PROJECT = var.project_id
        LODESTAR_LANDING_URI = "gs://${google_storage_bucket.landing.name}"
        LODESTAR_RAW_DATASET = local.datasets.raw.id
        LODESTAR_BQ_LOCATION = var.bq_location
        DBT_TARGET           = "prod"
        DBT_SCHEMA           = var.environment == "prod" ? "lodestar" : "lodestar_${var.environment}"
      }

      pypi_packages = {
        "dbt-bigquery"       = ">=1.9.0"
        "great-expectations" = ">=1.3.0"
      }
    }

    node_config {
      service_account = google_service_account.pipeline.email
    }
  }

  depends_on = [
    google_project_service.composer,
    google_project_iam_member.composer_worker,
  ]
}
