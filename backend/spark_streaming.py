from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, round as round_, when, to_timestamp, broadcast,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
import os
import shutil
from datetime import datetime

def write_to_mongo(batch_df, batch_id):
    """Write the batch dataframe to MongoDB with enhanced error handling"""
    try:
        count = batch_df.count()
        print(f"Processing batch {batch_id} with {count} records")
        
        if count == 0:
            print("Skipping empty batch")
            return
            
        # Add processing metadata to each record
        batch_with_metadata = (batch_df
            .withColumn("processed_at", current_timestamp())
            .withColumn("batch_id", lit(str(batch_id)))
        )

        # Write to MongoDB with additional options for better performance
        (batch_with_metadata
            .write
            .format("mongodb")
            .mode("append")
            .option("uri", "mongodb://127.0.0.1:27017/nyc_congestion")
            .option("collection", "traffic_congestion")
            .option("replaceDocument", "false")
            .option("ordered", "true")
            .option("writeConcern.w", "majority")
            .save()
        )
        print(f"Successfully wrote batch {batch_id} to MongoDB at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"Critical error writing to MongoDB: {str(e)}")

if __name__ == "__main__":
    # Clean checkpoint directory if exists
    if os.path.exists("chkpt"):
        shutil.rmtree("chkpt")
    os.makedirs("chkpt", exist_ok=True)

    spark = (
        SparkSession.builder
            .appName("NYCTrafficStreaming")
            .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/nyc_congestion")
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
            .config("spark.mongodb.operation.sampleSize", "100")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load taxi zone lookup data
    taxi_zones = (
        spark.read
            .option("header", "true")
            .csv("/Users/aadti/nyc-traffic-project/data/taxi_zone_lookup.csv")
            .select(
                col("LocationID").cast("int").alias("zone_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone_name")
            )
    )
    broadcast_zones = broadcast(taxi_zones)

    # Schema definition for Kafka messages
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("pickup_zone", IntegerType(), True),
        StructField("dropoff_zone", IntegerType(), True),
        StructField("speed_mph", DoubleType(), True),
    ])

    # Kafka source configuration
    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "taxi-trips")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
    )

    # Parse JSON data
    parsed = (
        raw.select(
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    )

    # Enrich with zone information
    enriched = (
        parsed
        .join(
            broadcast_zones,
            parsed.pickup_zone == broadcast_zones.zone_id,
            "left"
        )
        .drop("zone_id")
    )

    # Console output for debugging
    debug_query = (
        enriched.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
            .start()
    )

    # Streaming aggregation with watermark
    agg = (
        enriched
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "15 minutes"),
            col("pickup_zone"),
            col("borough"),
            col("zone_name")
        )
        .agg(
            round_(avg("speed_mph"), 2).alias("avg_speed_mph")
        )
        .withColumn(
            "congestion_level",
            when(col("avg_speed_mph") < 7, "heavy")
            .when(col("avg_speed_mph") < 15, "moderate")
            .otherwise("smooth")
        )
        .select(
            col("window.start").alias("timestamp"),
            col("pickup_zone"),
            col("borough"),
            col("zone_name"),
            col("avg_speed_mph"),
            col("congestion_level")
        )
    )

    # Write to MongoDB 
    mongo_query = (
        agg.writeStream
            .outputMode("update")
            .foreachBatch(write_to_mongo)
            .option("checkpointLocation", "chkpt")
            .trigger(processingTime="1 minute")
            .start()
    )

    print("Streaming system operational. Monitoring traffic congestion by neighborhood...")
    spark.streams.awaitAnyTermination()
