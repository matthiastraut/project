variable "project" {
  description = "Project"
  default     = "zoomcamp-de-project-495316"
}

variable "region" {
  description = "Region"
  default     = "europe-west2"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "jane_street_market_data"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "jane_street_market_data_lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
