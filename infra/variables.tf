variable "project_id" {
  description = "GCP project that hosts the Lodestar warehouse and landing zone."
  type        = string
}

variable "region" {
  description = "Region for regional resources (bucket, Composer if enabled)."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = <<-EOT
    BigQuery dataset location. Must be the multi-region ("US", "EU") or region
    the datasets should live in. Cannot be changed after creation without
    recreating every dataset.
  EOT
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name; suffixes every resource so dev and prod can coexist in one project."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be lowercase alphanumeric with hyphens, 2-16 chars."
  }
}

variable "landing_bucket_name" {
  description = <<-EOT
    Name for the raw landing bucket. Leave empty to derive one from the project
    and environment plus a random suffix — bucket names are globally unique, so
    a fixed name will collide with someone else's sooner or later.
  EOT
  type        = string
  default     = ""
}

variable "landing_retention_days" {
  description = <<-EOT
    Days to keep raw landing objects before deletion. The landing zone is the
    replay source for the whole warehouse, so this is the real bound on how far
    back a rebuild can go. 0 disables the lifecycle rule entirely.
  EOT
  type        = number
  default     = 365
}

variable "landing_nearline_days" {
  description = "Days before landing objects move to Nearline storage. 0 disables."
  type        = number
  default     = 30
}

variable "dataset_delete_contents_on_destroy" {
  description = <<-EOT
    Whether `terraform destroy` may drop non-empty datasets. True is convenient
    for a portfolio/dev project; leave it false anywhere real.
  EOT
  type        = bool
  default     = true
}

variable "enable_composer" {
  description = <<-EOT
    Provision Cloud Composer (managed Airflow). Off by default: a Composer 3
    environment costs roughly USD 300+/month even when idle, which is not a
    sensible default for a portfolio project. The docker-compose stack under
    `airflow/` runs the same DAG for free. Turn this on only to demonstrate a
    managed deployment, and destroy it afterwards.
  EOT
  type        = bool
  default     = false
}

variable "composer_image_version" {
  description = "Composer image, used only when enable_composer is true."
  type        = string
  default     = "composer-3-airflow-2.10.5-build.3"
}

variable "alert_email" {
  description = "Address that receives pipeline failure notifications. Empty disables the alert resources."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Labels applied to every resource that supports them."
  type        = map(string)
  default = {
    project    = "lodestar"
    managed_by = "terraform"
  }
}
