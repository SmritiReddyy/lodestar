terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State lives in GCS so the environment is reproducible from any machine and
  # two people cannot apply at once. Bootstrap it with:
  #
  #   terraform init -backend-config="bucket=<your-tfstate-bucket>"
  #
  # The bucket itself is deliberately not managed here — a backend cannot
  # create the bucket that stores its own state.
  backend "gcs" {
    prefix = "lodestar/terraform/state"
  }
}
