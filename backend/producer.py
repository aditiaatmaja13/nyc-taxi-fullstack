import time
import json
import pandas as pd
from kafka import KafkaProducer

# Initialize producer with error handling
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"  # Wait for all replicas to acknowledge
    )
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit(1)

# Load data with error handling
try:
    df = pd.read_parquet("data/yellow_tripdata_2025.parquet", engine="pyarrow")
    print(f"Loaded {len(df)} records from Parquet file")
except FileNotFoundError:
    print("Error: Parquet file not found. Check the path.")
    exit(1)
except Exception as e:
    print(f"Error loading Parquet file: {e}")
    exit(1)

# Process and send records
records_sent = 0
try:
    for _, row in df.iterrows():
        pickup = pd.to_datetime(row["tpep_pickup_datetime"])
        dropoff = pd.to_datetime(row["tpep_dropoff_datetime"])
        duration = (dropoff - pickup).total_seconds()
        
        # Add validation for speed calculation
        if duration <= 0 or row["trip_distance"] < 0:
            speed = 0
        else:
            speed = row["trip_distance"] / (duration / 3600)
            # Cap unreasonable speeds (e.g., data errors showing 200+ mph)
            if speed > 100:
                speed = 100
                
        record = {
            "timestamp": pickup.isoformat(),
            "pickup_zone": int(row["PULocationID"]),
            "dropoff_zone": int(row["DOLocationID"]),
            "speed_mph": speed
        }
        
        # Send to Kafka and handle errors
        future = producer.send("taxi-trips", record)
        records_sent += 1
        
        # Print progress every 100 records
        if records_sent % 100 == 0:
            print(f"Sent {records_sent} records to Kafka")
            
        time.sleep(0.01)  
        
except KeyboardInterrupt:
    print("Producer stopped by user")
except Exception as e:
    print(f"Error in producer: {e}")
finally:
    # Always flush and close the producer
    producer.flush()
    producer.close()
    print(f"Producer finished. Total records sent: {records_sent}")
