terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }

    local = {
      source = "hashicorp/local"
      version = "~> 2.5"
    }
  }

  backend "gcs" {
    bucket = "accomodations-lux"
    prefix = "terraform/state"
    credentials = "service_account_credentials.json"
  }
}

provider "google" {
  project = "lux-immo-438316"
  credentials = file("service_account_credentials.json")
}

data "google_project" "project" {

}

resource "google_service_account" "cloud_storage_access" {
  account_id = "cloud-storage-access"
  display_name = "cloud-storage-access"
  project = data.google_project.project.project_id
}

resource "google_project_iam_member" "cloud_storage_access_role" {
  project = data.google_project.project.project_id
  role="roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.cloud_storage_access.email}"
}

resource "google_service_account_key" "cloud_storage_access_key" {
  service_account_id = google_service_account.cloud_storage_access.name
}

#Store the GCS service account private key locally to be able to access GCS from the Airflow DAG
resource "local_file" "gcs_service_account_credentials_file" {
  content = base64encode(google_service_account_key.cloud_storage_access_key.private_key)
  filename="../${path.module}/include/gcs_service_account_credentials.json"
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

#GKE Configuration

resource "google_service_account" "gke_access" {
  account_id = "gke-access"
  display_name = "gke-access"
  project = data.google_project.project.project_id
}

resource "google_project_iam_member" "default_node_service_account_role" {
  project = data.google_project.project.project_id
  role = "roles/container.defaultNodeServiceAccount"
  member = "serviceAccount:${google_service_account.gke_access.email}"
}

resource "google_container_cluster" "gke_cluster" {
  name = "main-cluster"
  location = "europe-west1"
  enable_autopilot = true

  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.gke_access.email
    }
  }
}