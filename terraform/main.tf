terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }

  backend "gcs" {
    bucket = "accomodations-lux"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = "lux-immo-438316"
}

resource "google_storage_bucket" "main_gcs_bucket" {
  name = "accomodations-lux"
  location = "europe-west1"
  storage_class = "STANDARD"

  public_access_prevention = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 604800
  }
}