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

resource "google_service_account" "airflow" {
  account_id = "airflow"
  display_name = "airflow"
  project = data.google_project.project.project_id
}

resource "google_project_iam_member" "cloud_storage_access_role" {
  project = data.google_project.project.project_id
  role="roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "gke_manager_role" {
  project = data.google_project.project.project_id
  role = "roles/container.admin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "service_account_user_role" {
  project = data.google_project.project.project_id
  role = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
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

#To be able to pull custom docker images from Artifact Registry
resource "google_project_iam_member" "artifact_registry_reader_role" {
  project = data.google_project.project.project_id
  role = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.gke_access.email}"
}

resource "google_artifact_registry_repository" "docker_images_repository" {
  location = "europe-west1"
  repository_id = "docker-images"
  description = "Docker images for the immo_lux project"
  format = "DOCKER"
  mode = "STANDARD_REPOSITORY"

  cleanup_policies {
    id = "delete-old-images"
    action = "DELETE"
    condition {
      tag_state = "UNTAGGED"
      older_than = "3600s"
    }
  }
}

#To be able to push custom docker images to Artifact Registry

resource "google_service_account" "artifact_registry_writer" {
  account_id = "artifact-registry-writer"
  display_name = "artifact-registry-writer"
  project = data.google_project.project.project_id
}

resource "google_project_iam_member" "artifact_registry_writer_role" {
  project = data.google_project.project.project_id
  role = "roles/artifactregistry.writer"
  member = "serviceAccount:${google_service_account.artifact_registry_writer.email}"
}