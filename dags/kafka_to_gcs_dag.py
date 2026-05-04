import json
import io
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from google.cloud import storage

# Configuration
BUCKET_NAME = "jane_street_market_data_lake_zoomcamp-de-project-495316"
KAFKA_TOPIC = "market-data-raw"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" # Inside Docker, use 'kafka:9092'
GCS_DEST_PREFIX = "raw"
SERVICE_ACCOUNT_PATH = "/opt/airflow/gcp.json"

def kafka_to_gcs_in_memory():
    """Consumes messages from Kafka and uploads them directly to GCS without saving to disk."""
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}...")
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'airflow-gcs-sync-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    records = []
    max_messages = 10000 # Batch size per DAG run
    timeout = 10.0 # Stop if no messages for 10 seconds
    
    print(f"Consuming up to {max_messages} messages...")
    
    try:
        count = 0
        while count < max_messages:
            msg = consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            
            # Add record to our in-memory list
            records.append(json.loads(msg.value().decode('utf-8')))
            count += 1
            
        if not records:
            print("No new messages found. Skipping upload.")
            return

        # Create a JSON-lines string in memory
        output = io.StringIO()
        for record in records:
            output.write(json.dumps(record) + '\n')
        
        # Upload to GCS
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        blob_name = f"{GCS_DEST_PREFIX}/market_data_{timestamp}.json"
        
        print(f"Uploading {len(records)} records to gs://{BUCKET_NAME}/{blob_name}...")
        
        client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_string(output.getvalue(), content_type='application/json')
        print("Upload successful!")

    finally:
        consumer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'kafka_to_gcs_direct',
    default_args=default_args,
    description='Streams Kafka data directly to GCS in-memory',
    schedule_interval=timedelta(minutes=5),  # Adjust frequency as needed
    catchup=False,
    tags=['project', 'memory_sync'],
) as dag:

    sync_task = PythonOperator(
        task_id='kafka_to_gcs_direct_task',
        python_callable=kafka_to_gcs_in_memory,
    )

    sync_task
