terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${var.gcs_bucket_name}_${var.project}"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Data Warehouse Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.bq_dataset_name
  location                    = var.location
  delete_contents_on_destroy  = true
}

# External Table for Raw Data
resource "google_bigquery_table" "raw_market_data" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "raw_market_data"

  external_data_configuration {
    autodetect    = true
    source_format = "NEWLINE_DELIMITED_JSON"
    source_uris   = ["gs://${google_storage_bucket.data-lake-bucket.name}/raw/*.json"]
  }

  deletion_protection = false
}
