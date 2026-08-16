locals {
  landing_bucket_name = coalesce(
    var.landing_bucket_name,
    "${local.name_prefix}-landing-${random_id.suffix.hex}"
  )
}

# The raw landing zone. Everything the warehouse holds can be rebuilt from the
# objects in this bucket, which is what makes a full replay possible.
resource "google_storage_bucket" "landing" {
  name     = local.landing_bucket_name
  project  = var.project_id
  location = var.region
  labels   = local.common_labels

  # Uniform access means permissions come from IAM only — no per-object ACLs to
  # drift out of sync with the service account bindings.
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Landing objects are immutable once written; a partition rewrite deletes and
  # replaces. Versioning keeps a short safety net against an accidental delete.
  versioning {
    enabled = true
  }

  dynamic "lifecycle_rule" {
    for_each = var.landing_nearline_days > 0 ? [1] : []
    content {
      condition {
        age = var.landing_nearline_days
      }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.landing_retention_days > 0 ? [1] : []
    content {
      condition {
        age = var.landing_retention_days
      }
      action {
        type = "Delete"
      }
    }
  }

  # Superseded object versions are pure cost after a week.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 7
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = var.dataset_delete_contents_on_destroy

  depends_on = [google_project_service.required]
}

# Artifacts a run produces about itself: dbt manifests, Great Expectations
# reports, run metrics. Kept apart from the data so a landing-zone lifecycle
# rule never quietly deletes the audit trail.
resource "google_storage_bucket" "artifacts" {
  name     = "${local.name_prefix}-artifacts-${random_id.suffix.hex}"
  project  = var.project_id
  location = var.region
  labels   = local.common_labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = var.dataset_delete_contents_on_destroy

  depends_on = [google_project_service.required]
}
