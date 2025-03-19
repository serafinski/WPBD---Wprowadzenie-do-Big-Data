# This follows kafka-pyspark-minio (raw layer) architecture
# This is for raw layer, which is responsible for capturing the raw data from the Kafka topics and storing it in MinIO

# docker exec spark /opt/bitnami/spark/scripts/jobs.sh streaming

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
            .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/checkpoints")
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
    return (spark
            .readStream
            .format("kafka")
            # Kafka broker address
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", topic)
            # Start from the earliest available messages in the topic
            .option("startingOffsets", "earliest")
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
    checkpoint_path = f"/opt/bitnami/spark/checkpoints/{entity_name}"
    
    # Write to MinIO in Parquet format, partitioned by operation type
    query = (processed_df
             .writeStream
             .outputMode("append")
             .format("parquet")
             .partitionBy("__op")  # Partition by operation type
             .option("path", s3_path)
             .option("checkpointLocation", checkpoint_path)
             .start())
    
    return query

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("PySpark MinIO Data Processor started!")
    
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
        print(f"Setting up streaming for {entity} from topic {topic}")
        df = read_kafka_topic(spark, topic, schemas[entity])
        query = process_and_save_to_minio(df, entity)
        queries.append(query)
    
    # Wait for any of the queries to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()