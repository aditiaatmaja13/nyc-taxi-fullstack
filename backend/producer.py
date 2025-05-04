import time
import json
import logging
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic": "taxi-trips",
        "max_retries": 3,
        "retry_delay": 1  # seconds
    },
    "data": {
        "parquet_path": "/Users/aadti/nyc-taxi-fullstack/backend/data/yellow_tripdata_2025.parquet",
        "zones_csv": "/Users/aadti/nyc-taxi-fullstack/backend/data/taxi_zone_lookup.csv",
        "chunk_size": 5000,
        "max_speed": 95,  # 95th percentile speed cap
        "delay": 0.001  # seconds between chunks
    }
}

def load_valid_zones():
    """Load and return valid taxi zone IDs"""
    try:
        zones = pd.read_csv(CONFIG["data"]["zones_csv"])
        return set(zones["LocationID"].astype(int))
    except Exception as e:
        logger.error(f"Error loading zones: {e}")
        exit(1)

def process_chunk(chunk):
    """Process a chunk of data and return valid records"""
    valid_zones = load_valid_zones()
    records = []
    
    for _, row in chunk.iterrows():
        try:
            # Validate zones
            pu_id = int(row["PULocationID"])
            do_id = int(row["DOLocationID"])
            if pu_id not in valid_zones or do_id not in valid_zones:
                continue

            # Calculate speed
            pickup = pd.to_datetime(row["tpep_pickup_datetime"])
            dropoff = pd.to_datetime(row["tpep_dropoff_datetime"])
            duration = (dropoff - pickup).total_seconds()
            
            if duration <= 10 or row["trip_distance"] <= 0:  # Minimum 10 sec duration
                continue
                
            speed = row["trip_distance"] / (duration / 3600)
            if speed > CONFIG["data"]["max_speed"]:
                continue

            records.append({
                "timestamp": pickup.isoformat(),
                "pickup_zone": pu_id,
                "dropoff_zone": do_id,
                "speed_mph": round(speed, 2)
            })
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Skipping invalid row: {e}")
    
    return records

def main():
    """Main producer execution"""
    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=CONFIG["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=CONFIG["kafka"]["max_retries"]
        )
    except KafkaError as e:
        logger.error(f"Kafka connection failed: {e}")
        exit(1)

    # Load data
    try:
        df = pd.read_parquet(
            CONFIG["data"]["parquet_path"],
            engine="pyarrow"
        )
        logger.info(f"Loaded {len(df):,} records from Parquet file")
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        exit(1)

    # Process in chunks
    total_sent = 0
    chunks = np.array_split(df, len(df) // CONFIG["data"]["chunk_size"] + 1)
    
    try:
        for i, chunk in enumerate(chunks):
            records = process_chunk(chunk)
            
            # Send records with error handling
            for record in records:
                future = producer.send(CONFIG["kafka"]["topic"], record)
                future.add_errback(
                    lambda e: logger.error(f"Failed to send record: {e}")
                )
            
            total_sent += len(records)
            logger.info(f"Chunk {i+1}/{len(chunks)}: Sent {len(records)} records "
                       f"(Total: {total_sent:,})")
            
            time.sleep(CONFIG["data"]["delay"])
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Cleanup
        producer.flush(timeout=30)
        producer.close()
        logger.info(f"Producer shutdown. Total records sent: {total_sent:,}")

if __name__ == "__main__":
    main()
