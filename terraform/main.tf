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

    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "~> 3.2.1"
    }
  }

  backend "gcs" {
    bucket = "accomodations-lux"
    prefix = "terraform/state"
    credentials = "service_account_credentials.json"
  }
}

provider "google" {
  project = var.project_id
  credentials = file("service_account_credentials.json")
  # impersonate_service_account = var.terraform_service_account_email
}

resource "google_service_account" "airflow" {
  account_id = "airflow"
  display_name = "airflow"
  project = var.project_id
}

resource "google_project_iam_member" "cloud_storage_access_role" {
  project = var.project_id
  role="roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "gke_cluster_edition_role" {
  project = var.project_id
  role = "roles/container.developer"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "service_account_user_role" {
  project = var.project_id
  role = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

#The GCS bucket used to create the terraform state file and to store the data extracted from the websites
resource "google_storage_bucket" "main_gcs_bucket" {
  name = "accomodations-lux"
  location = var.region
  storage_class = "STANDARD"

  public_access_prevention = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 604800
  }
}

#GKE Nodes Service account setup

resource "google_service_account" "gke_access" {
  account_id = "gke-access"
  display_name = "gke-access"
  project = var.project_id
}

resource "google_project_iam_member" "default_node_service_account_role" {
  project = var.project_id
  role = "roles/container.defaultNodeServiceAccount"
  member = "serviceAccount:${google_service_account.gke_access.email}"
}

#GKE Cluster creation

resource "google_container_cluster" "immo-dag-cluster" {
  name = "immo-dag-cluster"
  location = var.region
  
  enable_autopilot = true

  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.gke_access.email
      oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }

  logging_config {
    enable_components = [ "SYSTEM_COMPONENTS", "WORKLOADS" ]
  }
}

#To be able to log in to the GKE cluster previously created and execute kubernetes commands (kubectl)
provider "kubernetes" {
  host                   = "https://${google_container_cluster.immo-dag-cluster.endpoint}"
  cluster_ca_certificate = base64decode(google_container_cluster.immo-dag-cluster.master_auth[0].cluster_ca_certificate)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "gke-gcloud-auth-plugin"
  }
}

resource "google_service_account" "scraper" {
  account_id = "scraper"
  display_name = "scraper"
  project = var.project_id
}

resource "google_project_iam_member" "scraper_role" {
  project = var.project_id
  role = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.scraper.email}"
}

resource "kubernetes_service_account_v1" "scraping-ksa" {
  metadata {
    name = "scraping-ksa"
    namespace = "default"
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.scraper.email
    }
  }
}

resource "google_service_account_iam_member" "allow_ksa_impersonation" {
  service_account_id = google_service_account.scraper.name
  role = "roles/iam.workloadIdentityUser"
  member = "serviceAccount:${var.project_id}.svc.id.goog[default/scraping-ksa]"
}

#To be able to pull custom docker images from Artifact Registry
resource "google_project_iam_member" "artifact_registry_reader_role" {
  project = var.project_id
  role = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.gke_access.email}"
}

resource "google_artifact_registry_repository" "docker_images_repository" {
  location = var.region
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
  project = var.project_id
}

resource "google_project_iam_member" "artifact_registry_writer_role" {
  project = var.project_id
  role = "roles/artifactregistry.writer"
  member = "serviceAccount:${google_service_account.artifact_registry_writer.email}"
}