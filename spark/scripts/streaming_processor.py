# This follows kafka-pyspark-minio (raw layer) architecture
# This is for raw layer, which is responsible for capturing the raw data from the Kafka topics and storing it in MinIO

# docker exec spark /opt/bitnami/spark/scripts/jobs.sh streaming

import os
import time
import json

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, when, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, BinaryType
)

# We are using AWS/Hadoop because MinIO requires it - it's an S3-compatible storage
def create_spark_session():
    """Create a Spark session with appropriate configurations."""
    return (SparkSession.builder
            .appName("CDC to MinIO Processor")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.595")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            # Path style access is required for MinIO
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Explicitly set the credentials provider to avoid warnings
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())

def define_schemas():
    """Define schemas for the different entities we're capturing via CDC."""
    # Customer table schema
    customer_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("registration_date", LongType(), True),
        StructField("last_update", LongType(), True),
        
        # Additional metadata fields from Debezium
        StructField("__deleted", StringType(), True),
        StructField("__op", StringType(), True),
        StructField("__table", StringType(), True),
        StructField("__lsn", LongType(), True),
        StructField("__source_ts_ms", LongType(), True)
    ])
    
    # Order table schema
    order_schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", LongType(), True),
        StructField("status", StringType(), True),
        
        # Postgres sends Decimal format as binary
        StructField("total_amount", BinaryType()),
        
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("shipping_city", StringType(), True),
        StructField("shipping_state", StringType(), True),
        StructField("shipping_zip", StringType(), True),
        StructField("last_update", LongType(), True),

        # Additional metadata fields from Debezium
        StructField("__deleted", StringType(), True),
        StructField("__op", StringType(), True),
        StructField("__table", StringType(), True),
        StructField("__lsn", LongType(), True),
        StructField("__source_ts_ms", LongType(), True)
    ])
    
    # Order item schema
    order_item_schema = StructType([
        StructField("item_id", IntegerType()),
        StructField("order_id", IntegerType(), True),
        StructField("product_name", StringType()),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType()),
        
        # Postgres sends Decimal format as binary
        StructField("unit_price", BinaryType()),
        StructField("discount", BinaryType(), True),

        StructField("last_update", LongType(), True),

        # Additional metadata fields from Debezium
        StructField("__deleted", StringType(), True),
        StructField("__op", StringType(), True),
        StructField("__table", StringType(), True),
        StructField("__lsn", LongType(), True),
        StructField("__source_ts_ms", LongType(), True)
    ])
    
    return {
        "customers": customer_schema,
        "orders": order_schema,
        "order_items": order_item_schema
    }

def read_kafka_topic(spark, topic, schema):
    """Read data from a Kafka topic with the specified schema."""
    # Check if checkpoint exists and choose offset accordingly
    checkpoint_path = f"/opt/bitnami/spark/checkpoints/{topic.replace('.', '_')}"
    starting_offset = "latest" if os.path.exists(checkpoint_path) else "earliest"
    
    print(f"Starting offset for {topic}: {starting_offset} (checkpoint exists: {os.path.exists(checkpoint_path)})")
    
    return (spark
            .readStream
            .format("kafka")
            # Kafka broker address
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", topic)
            # Use dynamic starting offset based on checkpoint existence
            .option("startingOffsets", starting_offset)
            .load()

            # Read raw message from Kafka as JSON
            .selectExpr("CAST(value AS STRING) as json", "timestamp")
            
            # Extract the payload portion of the JSON
            .selectExpr("get_json_object(json, '$.payload') as payload_json", "timestamp")
            
            # Parse the payload to a struct using the specified schema
            .select(from_json("payload_json", schema).alias("data"), "timestamp")
            .select("data.*", "timestamp"))

def process_and_save_to_minio(df, entity_name):
    """Process the stream and save to MinIO data lake."""
    
    # Add processing timestamp
    processed_df = df.withColumn("processing_time", current_timestamp())
    
    # Add a flag for deleted records based on the __deleted field
    processed_df = processed_df.withColumn(
        "is_deleted", 
        when(col("__deleted") == "true", lit(True)).otherwise(lit(False))
    )
    
    # Convert CDC timestamp to actual timestamp
    processed_df = processed_df.withColumn(
        "cdc_timestamp", 
        to_timestamp(col("__source_ts_ms") / 1000)
    )
    
    # Define the MinIO output path
    s3_path = f"s3a://datalake/raw/{entity_name}"
    
    # Use a UNIQUE checkpoint path for each entity
    # Include timestamp to ensure uniqueness if we restart the application
    checkpoint_timestamp = int(datetime.now().timestamp())
    checkpoint_path = f"/opt/bitnami/spark/checkpoints/{entity_name}_{checkpoint_timestamp}"
    
    # Create a tracking file to monitor data written to data lake
    tracking_dir = "/opt/bitnami/spark/tracking"
    os.makedirs(tracking_dir, exist_ok=True)
    tracking_file = f"{tracking_dir}/{entity_name}_write_tracker.json"
    
    # Initialize tracking file if it doesn't exist
    if not os.path.exists(tracking_file):
        with open(tracking_file, 'w') as f:
            json.dump({"last_batch_id": -1, "total_records_written": 0, "processed_lsns": []}, f)
    
    # Add a foreach batch to monitor progress and track writes to data lake
    def process_batch(batch_df, batch_id):
        # Read current tracking info
        tracking_info = {"last_batch_id": -1, "total_records_written": 0, "processed_lsns": []}
        if os.path.exists(tracking_file):
            with open(tracking_file, 'r') as f:
                try:
                    tracking_info = json.load(f)
                    if "processed_lsns" not in tracking_info:  # Handle backward compatibility
                        tracking_info["processed_lsns"] = []
                except:
                    pass
        
        # Collect LSNs from this batch to check for duplicates
        # LSN (Log Sequence Number) is a unique identifier for each CDC event
        batch_lsns = [row["__lsn"] for row in batch_df.select("__lsn").collect()]
        
        # Enhanced debugging - display more info about the batch and LSNs
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] Batch {batch_id} contains {len(batch_lsns)} records with LSNs: {batch_lsns[:5]}...")
        print(f"[{timestamp}] Already processed LSNs (sample): {tracking_info['processed_lsns'][:5]}...")
        
        # Filter out records we've already processed
        new_lsns = [lsn for lsn in batch_lsns if lsn not in tracking_info["processed_lsns"]]
        print(f"[{timestamp}] New LSNs to process: {new_lsns[:5]}...")
        
        # If we have new records to process
        if new_lsns:
            # Filter the DataFrame to only include new records
            filtered_df = batch_df.filter(col("__lsn").isin(new_lsns))
            row_count = filtered_df.count()
            
            if row_count > 0:
                # Actually write the data to MinIO
                # Partition by operation type
                (filtered_df
                 .write
                 .partitionBy("__op")
                 .mode("append")
                 .parquet(s3_path))
                
                # Update total count
                tracking_info["last_batch_id"] = batch_id
                tracking_info["total_records_written"] += row_count
                
                # Add the processed LSNs to our tracking
                tracking_info["processed_lsns"].extend(new_lsns)
                
                # Sort LSNs numerically before truncating to ensure we keep the highest ones
                if len(tracking_info["processed_lsns"]) > 10000:
                    tracking_info["processed_lsns"] = sorted(tracking_info["processed_lsns"])[-10000:]
                
                # Save tracking info
                with open(tracking_file, 'w') as f:
                    json.dump(tracking_info, f)
                
                # Log the write operation
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] WROTE TO DATALAKE: {row_count} {entity_name} records (batch #{batch_id})")
                print(f"[{timestamp}] Total {entity_name} records in data lake: {tracking_info['total_records_written']}")
                print(f"[{timestamp}] Data location: {s3_path}/__op=*")
        else:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] Skipped {len(batch_lsns)} duplicate {entity_name} records (batch #{batch_id})")
    
    # Write to MinIO in Parquet format using foreachBatch
    query = (processed_df
             .writeStream
             .outputMode("append")
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpoint_path)
             .start())
    
    return query

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n" + "="*80)
    print("PySpark MinIO Data Processor started!")
    print("="*80 + "\n")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Initializing streaming jobs...")
    
    # Define schemas
    schemas = define_schemas()
    
    # Define topics
    topics = {
        "customers": "postgres_server.public.customers",
        "orders": "postgres_server.public.orders",
        "order_items": "postgres_server.public.order_items"
    }
    
    # Create streaming queries for each entity
    queries = []
    for entity, topic in topics.items():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Setting up streaming for {entity} from topic {topic}")
        df = read_kafka_topic(spark, topic, schemas[entity])
        query = process_and_save_to_minio(df, entity)
        queries.append(query)
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] All streaming jobs initialized successfully")
    print("\nMonitoring for data processing (press Ctrl+C to stop)...\n")
    
    # Wait for streaming queries to terminate while displaying simplified status
    try:
        while True:
            all_active = True
            active_streams = []
            
            for i, query in enumerate(queries):
                entity = list(topics.keys())[i]
                
                if query.isActive:
                    active_streams.append(entity)
                else:
                    all_active = False
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Stream for {entity} has terminated")
            
            if not all_active:
                print("One or more streams have terminated. Exiting...")
                break
            
            # Print a simple heartbeat
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] Heartbeat: All streams active, waiting for data... ({', '.join(active_streams)})")
                
            # Sleep for 15 seconds before checking again (reduced from 30)
            time.sleep(15)
            
    except KeyboardInterrupt:
        for query in queries:
            query.stop()
    
    # In case we don't catch through the loop
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()