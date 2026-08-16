provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  name_prefix = "lodestar-${var.environment}"

  # Dataset names carry the environment so a dev and a prod warehouse can share
  # one project without colliding. dbt's generate_schema_name macro produces the
  # same names from the other side.
  dataset_suffix = var.environment == "prod" ? "" : "_${replace(var.environment, "-", "_")}"

  datasets = {
    raw = {
      id          = "lodestar_raw${local.dataset_suffix}"
      description = "Landing tables written by the extract/load layer. No transformations."
    }
    staging = {
      id          = "staging${local.dataset_suffix}"
      description = "dbt staging layer: renamed, typed, deduplicated source data."
    }
    intermediate = {
      id          = "intermediate${local.dataset_suffix}"
      description = "dbt intermediate layer: joins and business logic."
    }
    marts = {
      id          = "marts${local.dataset_suffix}"
      description = "dbt marts: the dimensional model exposed to BI."
    }
    snapshots = {
      id          = "snapshots${local.dataset_suffix}"
      description = "dbt snapshots backing the Type 2 dimensions."
    }
    test_failures = {
      id          = "test_failures${local.dataset_suffix}"
      description = "Rows that failed a dbt test, stored so failures can be inspected."
    }
  }

  common_labels = merge(var.labels, {
    environment = var.environment
  })
}

# Enable exactly the APIs this stack needs. Doing it in Terraform means a fresh
# project can be brought up with a single apply, rather than a console tour.
resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  # Leave the APIs on when the stack is torn down; disabling them would break
  # anything else in the project that happens to use them.
  disable_on_destroy = false
}

resource "random_id" "suffix" {
  byte_length = 4
}
