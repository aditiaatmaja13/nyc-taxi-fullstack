from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, round as round_, 
    when, to_timestamp, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
import os
import shutil

# Configuration constants
KAFKA_SERVERS = "localhost:9092"
MONGO_URI = "mongodb://127.0.0.1:27017/nyc_congestion"
CHECKPOINT_DIR = "chkpt"
WATERMARK_DELAY = "10 minutes"
PROCESSING_INTERVAL = "1 minute"
WINDOW_DURATION = "15 minutes"
MAX_OFFSETS_PER_TRIGGER = 10000
DEFAULT_SPEED = 1.8  

def write_to_mongo(batch_df, batch_id):
    """Optimized MongoDB writer with error handling"""
    try:
        if not batch_df.isEmpty():
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("uri", MONGO_URI) \
                .option("collection", "traffic_congestion") \
                .save()
            print(f"Batch {batch_id} wrote {batch_df.count()} records")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {str(e)}")

if __name__ == "__main__":
    # Clean previous state
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # Initialize Spark with optimized config
    spark = (SparkSession.builder
             .appName("NYCTrafficStreaming")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")

    # Load taxi zones
    taxi_zones = (spark.read
                  .option("header", "true")
                  .csv("/Users/aadti/nyc-taxi-fullstack/backend/data/taxi_zone_lookup.csv")
                  .select(
                      col("LocationID").cast("int").alias("zone_id"),
                      col("Borough").alias("borough"),
                      col("Zone").alias("zone_name")
                  ))
    broadcast_zones = broadcast(taxi_zones)

    # Define schema for Kafka messages
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("pickup_zone", IntegerType()),
        StructField("dropoff_zone", IntegerType()),
        StructField("speed_mph", DoubleType()),
    ])

    # Stream processing pipeline
    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                  .option("subscribe", "taxi-trips")
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
                  .load())

    processed_stream = (raw_stream
                        .select(from_json(col("value").cast("string"), schema).alias("data"))
                        .select("data.*")
                        .withColumn("timestamp", to_timestamp(col("timestamp")))
                        # Impute low/zero speeds instead of filtering
                        .withColumn("speed_mph", 
                                   when((col("speed_mph") <= 0) | (col("speed_mph").isNull()), DEFAULT_SPEED)
                                   .otherwise(col("speed_mph")))
                        .filter(col("pickup_zone").isNotNull()))

    enriched_stream = (processed_stream
                       .join(broadcast_zones, 
                             col("pickup_zone") == col("zone_id"),
                             "left")
                       .drop("zone_id"))

    agg_stream = (enriched_stream
                  .withWatermark("timestamp", WATERMARK_DELAY)
                  .groupBy(
                      window(col("timestamp"), WINDOW_DURATION),
                      col("pickup_zone"),
                      col("borough"),
                      col("zone_name")
                  )
                  .agg(
                      avg("speed_mph").alias("raw_avg_speed")
                  )
                  .withColumn("avg_speed_mph", round_(col("raw_avg_speed"), 2))
                  .withColumn("congestion_level",
                             when(col("raw_avg_speed") < 7, "heavy")
                             .when(col("raw_avg_speed") < 15, "moderate")
                             .otherwise("smooth"))
                  .select(
                      col("window.start").alias("timestamp"),
                      col("pickup_zone"),
                      col("borough"),
                      col("zone_name"),
                      col("avg_speed_mph"),
                      col("congestion_level")
                  ))

    # Start streaming query
    query = (agg_stream.writeStream
             .outputMode("update")
             .foreachBatch(write_to_mongo)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime=PROCESSING_INTERVAL)
             .start())

    
    query.awaitTermination()
