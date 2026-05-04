import os
from google.cloud import storage

def sync_to_gcs(local_dir, bucket_name, gcs_path):
    print(f"Syncing {local_dir} to gs://{bucket_name}/{gcs_path}...")
    client = storage.Client.from_service_account_json("gcp.json")
    bucket = client.bucket(bucket_name)

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_file = os.path.join(root, file)
            # Create a relative path for the blob name
            rel_path = os.path.relpath(local_file, local_dir)
            blob_name = os.path.join(gcs_path, rel_path)
            
            blob = bucket.blob(blob_name)
            if not blob.exists():
                print(f"Uploading {local_file} -> {blob_name}")
                blob.upload_from_filename(local_file)

if __name__ == "__main__":
    BUCKET = "jane_street_market_data_lake_zoomcamp-de-project-495316"
    sync_to_gcs("data/raw", BUCKET, "raw")
    print("Sync complete!")
