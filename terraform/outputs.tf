output "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  value       = google_storage_bucket.data-lake-bucket.name
}

output "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  value       = google_bigquery_dataset.dataset.dataset_id
}
