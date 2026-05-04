import json
import time
import argparse
import pyarrow.dataset as ds
from confluent_kafka import Producer
import os

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "market-data-raw"
DATASET_PATH = "../../jane-street-real-time-market-data-forecasting/train.parquet"

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def main(limit=None):
    # Configure Kafka Producer
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'jane-street-producer',
        'linger.ms': 10,
        'batch.size': 16384 * 2
    }
    producer = Producer(producer_conf)

    print(f"Loading dataset from {DATASET_PATH}...")
    dataset = ds.dataset(DATASET_PATH, format="parquet", partitioning="hive")
    
    count = 0
    start_time = time.time()
    
    # Read the dataset sequentially
    for batch in dataset.to_batches():
        df = batch.to_pandas()
        
        # We need to serialize columns properly, e.g., dropping NaNs or filling them
        # Convert to dictionary orient="records"
        records = df.to_dict(orient="records")
        
        for record in records:
            # Clean up NaN values for JSON serialization
            clean_record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            # Using symbol_id as the key to ensure order per symbol
            key = str(clean_record.get('symbol_id', 'unknown'))
            value = json.dumps(clean_record)
            
            producer.produce(
                TOPIC_NAME, 
                key=key.encode('utf-8'), 
                value=value.encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0)
            count += 1
            
            # Throttle to avoid overwhelming local Kafka
            if count % 100 == 0:
                time.sleep(0.01)
            
            if limit and count >= limit:
                break
                
        producer.flush()
        if limit and count >= limit:
            break
            
        print(f"Produced {count} records...")

    producer.flush()
    elapsed = time.time() - start_time
    print(f"Finished producing {count} records in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    import pandas as pd
    parser = argparse.ArgumentParser(description="Produce Jane Street market data to Kafka.")
    parser.add_argument("--limit", type=int, help="Maximum number of records to produce", default=None)
    args = parser.parse_args()
    
    # Fix path assuming script is run from project root (where Makefile is)
    DATASET_PATH = "jane-street-real-time-market-data-forecasting/train.parquet"
    if not os.path.exists(DATASET_PATH):
        print(f"Error: Dataset not found at {DATASET_PATH}")
        print("Please ensure you are running from the project root.")
    else:
        main(args.limit)
