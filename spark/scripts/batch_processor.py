# This follows minio-pyspark-minio architecture

# This is for batch layer, which is responsible for processing the data in the data lake and generating analytics

# docker exec spark /opt/bitnami/spark/scripts/jobs.sh batch

import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import (
    col, sum, count, avg, expr, to_date, year, 
    datediff, when, lit, max as spark_max,
    to_timestamp, current_timestamp, countDistinct, udf,
    row_number, desc
)

# We are using AWS/Hadoop because MinIO requires it - it's an S3-compatible storage
def create_spark_session():
    """Create a Spark session with appropriate configurations."""
    return (SparkSession.builder
            .appName("Batch Analytics")
            .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/checkpoints")
            # No Kafka dependencies - since not needed for batch processing
            .config("spark.jars.packages", 
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

def load_data_from_datalake(spark, entity_name, data_lake_path):
    """
    Load the latest data from the data lake.
    This enhanced version properly handles CDC data with improved debugging.
    """
    full_path = f"{data_lake_path}/{entity_name}"
    
    try:
        # Read all the processed data
        all_data = spark.read.format("parquet").load(full_path)
        initial_count = all_data.count()
        print(f"DEBUG: Initial {entity_name} record count from data lake: {initial_count}")
        
        # Identify primary key field name
        # For CDC operations, each entity has a specific ID field
        primary_key_field = None
        if entity_name == "customers":
            primary_key_field = "customer_id"
        elif entity_name == "orders":
            primary_key_field = "order_id"
        elif entity_name == "order_items":
            # For order items, we need both fields for a composite key
            order_items_count = all_data.select("order_id", "product_id").distinct().count()
            print(f"DEBUG: Distinct order_items by composite key: {order_items_count}")
            
            # Define window for order_items based on composite key
            window_spec = Window.partitionBy("order_id", "product_id").orderBy(desc("__source_ts_ms"))
            
            # First get just the latest version of each order_item
            latest_records = all_data.withColumn("row_num", row_number().over(window_spec))
            latest_records = latest_records.filter(col("row_num") == 1).drop("row_num")
            
            # Filter out deleted records
            active_records = latest_records.filter(col("is_deleted") == False)
            
            active_count = active_records.count()
            print(f"Loaded {active_count} active {entity_name} records (after filtering duplicates and deleted records)")
            
            return active_records
        
        if not primary_key_field:
            raise ValueError(f"Unsupported entity type: {entity_name}")
        
        print(f"DEBUG: Using primary key field: {primary_key_field} for {entity_name}")
        
        # Show distinct primary key count
        distinct_keys = all_data.select(primary_key_field).distinct().count()
        print(f"DEBUG: Distinct {primary_key_field} count: {distinct_keys}")
        
        # Define a window to get the latest version of each record
        window_spec = Window.partitionBy(primary_key_field).orderBy(desc("__source_ts_ms"))
        
        # Add row number within each partition
        with_row_nums = all_data.withColumn("row_num", row_number().over(window_spec))
        
        # Check row numbers distribution
        row_num_counts = with_row_nums.groupBy("row_num").count().orderBy("row_num")
        print("DEBUG: Row number distribution:")
        row_num_counts.show(10, truncate=False)
        
        # Filter to only get the most recent version of each record
        latest_records = with_row_nums.filter(col("row_num") == 1).drop("row_num")
        latest_count = latest_records.count()
        print(f"DEBUG: Latest records count (one per {primary_key_field}): {latest_count}")
        
        # Filter out deleted records
        active_records = latest_records.filter(col("is_deleted") == False)
        active_count = active_records.count()
        print(f"Loaded {active_count} active {entity_name} records (after filtering duplicates and deleted records)")
        
        return active_records
    except Exception as e:
        print(f"Error loading latest data for {entity_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def load_or_create_checkpoint(spark, checkpoint_path, entity_name):
    """
    Load the latest checkpoint information or create a new one if it doesn't exist.
    Returns the latest processed timestamp.
    """
    checkpoint_file = f"{checkpoint_path}/{entity_name}_checkpoint.parquet"
    
    try:
        # Try to read the checkpoint
        checkpoint_df = spark.read.parquet(checkpoint_file)
        last_processed_ts = checkpoint_df.select("last_processed_timestamp").first()[0]
        print(f"Loaded checkpoint for {entity_name}: Last processed timestamp = {last_processed_ts}")
        return last_processed_ts
    except:
        print(f"No checkpoint found for {entity_name}, creating a new one with epoch start")
        # Return timestamp 0 (epoch start) if no checkpoint exists
        return 0

def save_checkpoint(spark, checkpoint_path, entity_name, last_processed_ts):
    """Save the checkpoint information with the latest processed timestamp."""
    checkpoint_file = f"{checkpoint_path}/{entity_name}_checkpoint.parquet"
    
    # Create dataframe with checkpoint info and explicit schema
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
    import datetime
    
    # Get current time as Python datetime object
    current_time = datetime.datetime.now()
    
    schema = StructType([
        StructField("entity_name", StringType(), False),
        StructField("last_processed_timestamp", LongType(), False),
        StructField("checkpoint_created_at", TimestampType(), False)
    ])
    
    checkpoint_df = spark.createDataFrame(
        [(entity_name, last_processed_ts, current_time)], 
        schema
    )
    
    # Save the checkpoint
    checkpoint_df.write.mode("overwrite").parquet(checkpoint_file)
    print(f"Saved checkpoint for {entity_name}: Last processed timestamp = {last_processed_ts}")

def process_raw_data(spark, entity_name, raw_path, processed_path, checkpoint_path=None):
    """
    Process the raw CDC data into a cleaned format for analytics.
    With direct handling for PostgreSQL binary numeric types using a UDF.
    
    If checkpoint_path is provided, only process data newer than the last checkpoint.
    
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    
    # Determine if we're doing incremental processing
    incremental = checkpoint_path is not None
    
    if incremental:
        # Get the last processed timestamp
        last_processed_ts = load_or_create_checkpoint(spark, checkpoint_path, entity_name)
    else:
        last_processed_ts = None

    # Create a UDF (User-Defined Function) to directly convert PostgreSQL binary to numeric values
    @udf(returnType=DoubleType())
    def pg_numeric_to_double(binary_data):
        if binary_data is None:
            return None
        
        try:
            # Convert binary data to integer based on number of bytes
            value = int.from_bytes(binary_data, byteorder='big')
            
            # Apply fixed scaling factor for 2 decimal places
            return value / 100.0
        except Exception as e:
            print(f"Error converting numeric: {e}")
            return None
    
    full_path = f"{raw_path}/{entity_name}"
    output_path = f"{processed_path}/{entity_name}"
    
    if incremental:
        print(f"Processing {entity_name} data from {full_path} with timestamp > {last_processed_ts}...")
    else:
        print(f"Processing all {entity_name} data from {full_path}...")
    
    try:
        # Read the raw data from MinIO
        try:
            raw_df = spark.read.format("parquet").load(full_path)
        except Exception as e:
            print(f"Error reading data from {full_path}: {str(e)}")
            return False
        
        # Filter only new data if doing incremental processing
        if incremental:
            df = raw_df.filter(col("__source_ts_ms") > last_processed_ts)
            
            # Check if there's any new data to process
            count = df.count()
            if count == 0:
                print(f"No new data to process for {entity_name} since last checkpoint")
                return True  # Return success as this is not an error
            print(f"Processing {count} new records for {entity_name}")
        else:
            df = raw_df
        
        # Add processing timestamp
        processed_df = df.withColumn("processing_time", current_timestamp())
        
        # Replace deleted flag with proper boolean
        processed_df = processed_df.withColumn(
            "is_deleted", 
            when(col("__deleted") == "true", lit(True)).otherwise(lit(False))
        )
        
        # Convert CDC timestamp to actual timestamp
        processed_df = processed_df.withColumn(
            "cdc_timestamp", 
            to_timestamp(col("__source_ts_ms") / 1000)
        )
        
        # Convert timestamps fields if present
        if "registration_date" in df.columns:
            processed_df = processed_df.withColumn(
                "registration_date", 
                to_timestamp(col("registration_date") / 1000000)
            )
        
        if "last_update" in df.columns:
            processed_df = processed_df.withColumn(
                "last_update", 
                to_timestamp(col("last_update") / 1000000)
            )
        
        if "order_date" in df.columns:
            processed_df = processed_df.withColumn(
                "order_date", 
                to_timestamp(col("order_date") / 1000000)
            )
        
        # Handle binary fields with previously defined UDF
        if entity_name == "orders" and "total_amount" in df.columns:
            processed_df = processed_df.withColumn(
                "total_amount",
                pg_numeric_to_double(col("total_amount"))
            )
        
        if entity_name == "order_items":
            if "unit_price" in df.columns:
                processed_df = processed_df.withColumn(
                    "unit_price",
                    pg_numeric_to_double(col("unit_price"))
                )
            
            if "discount" in df.columns:
                processed_df = processed_df.withColumn(
                    "discount",
                    pg_numeric_to_double(col("discount"))
                )
        
        # Write mode depends on whether we're doing incremental processing
        write_mode = "append" if incremental else "overwrite"
        
        # Write to MinIO in Parquet format
        (processed_df
         .write
         .mode(write_mode)
         .format("parquet")
         .partitionBy("__op")
         .save(output_path))
        
        # Update checkpoint if doing incremental processing
        if incremental:
            max_ts = df.select(spark_max("__source_ts_ms")).first()[0]
            save_checkpoint(spark, checkpoint_path, entity_name, max_ts)
            
        print(f"Successfully processed and saved {entity_name} data to {output_path}")
        return True
    
    except Exception as e:
        print(f"Error processing {entity_name} data: {str(e)}")
        return False

# price x quantity x (1 - discount)
# This is a simple calculation to get the total line item price
def analyze_sales_by_period(orders_df, order_items_df):
    """Analyze sales metrics by different time periods."""
    # Join orders with order items
    sales_data = (orders_df
                  .join(order_items_df, "order_id")
                  .withColumn("line_total", 
                             col("quantity") * col("unit_price") * (1 - col("discount"))))
    
    # Daily sales
    daily_sales = (sales_data
                   .withColumn("order_date", to_date("order_date"))
                   .groupBy("order_date")
                   .agg(
                       sum("line_total").alias("total_sales"),
                       countDistinct("order_id").alias("order_count"),
                       avg("line_total").alias("avg_order_value")
                   )
                   .orderBy("order_date"))
    
    # Monthly sales
    monthly_sales = (sales_data
                    .withColumn("year_month", expr("date_format(order_date, 'yyyy-MM')"))
                    .groupBy("year_month")
                    .agg(
                        sum("line_total").alias("total_sales"),
                        countDistinct("order_id").alias("order_count"),
                        avg("line_total").alias("avg_order_value")
                    )
                    .orderBy("year_month"))
    
    # Weekly sales (new)
    weekly_sales = (sales_data
                   .withColumn("week", expr("weekofyear(order_date)"))
                   .withColumn("year", year("order_date"))
                   .groupBy("year", "week")
                   .agg(
                       sum("line_total").alias("total_sales"),
                       countDistinct("order_id").alias("order_count"),
                       avg("line_total").alias("avg_order_value")
                   )
                   .orderBy("year", "week"))
    
    return {
        "daily": daily_sales,
        "weekly": weekly_sales,
        "monthly": monthly_sales
    }

# Segment customers based on registration date and order activity
def analyze_customer_cohorts(customers_df, orders_df):
    """Analyze customer cohorts based on registration date."""
    # Create cohort based on registration month
    cohorted_customers = (customers_df
                         .withColumn("registration_cohort", 
                                    expr("date_format(registration_date, 'yyyy-MM')"))
                         .select("customer_id", "registration_cohort"))
    
    # Join with orders
    # Used to analyze how different cohorts behave over time
    # This counts the number of active customers and total spent per cohort per month
    cohort_activity = (orders_df
                      .join(cohorted_customers, "customer_id")
                      .withColumn("order_month", 
                                 expr("date_format(order_date, 'yyyy-MM')"))
                      .groupBy("registration_cohort", "order_month")
                      .agg(
                          countDistinct("customer_id").alias("active_customers"),
                          sum("total_amount").alias("total_spent")
                      ))
    
    return cohort_activity

# Calculate metrics for each product - total units sold, total revenue, avg discount, number of orders containing the product
# Order by total revenue descending
def analyze_product_performance(order_items_df):
    """Analyze product performance metrics."""
    return (order_items_df
            .groupBy("product_id", "product_name")
            .agg(
                sum("quantity").alias("total_units_sold"),
                sum(col("quantity") * col("unit_price") * (1 - col("discount"))).alias("total_revenue"),
                avg("discount").alias("avg_discount"),
                countDistinct("order_id").alias("order_count")
            )
            .orderBy(col("total_revenue").desc()))

# Calculate RFM (Recency, Frequency, Monetary) metrics for customer segmentation
# Recency: Days since last order
# Frequency: Number of orders
# Monetary: Total amount spent
# Assign scores (1-3) based on R, F, M dimensions based on tresholds
# Combine scores to create RFM score - based on which customers are segmented
def calculate_customer_recency_frequency_monetary(customers_df, orders_df):
    """
    Calculate RFM (Recency, Frequency, Monetary) metrics for customer segmentation.
    
    This is a more advanced analysis to segment customers based on their
    purchasing behavior.
    """
    # Get the most recent date in the orders dataset - used as a reference point
    max_date = orders_df.select(spark_max("order_date")).first()[0]
    
    # Calculate RFM metrics per customer
    rfm = (orders_df
           .filter(col("status") != "cancelled")  # Exclude cancelled orders
           .groupBy("customer_id")
           .agg(
               datediff(lit(max_date), spark_max("order_date")).alias("recency"),
               count("order_id").alias("frequency"),
               sum("total_amount").alias("monetary")
           ))
    
    # Create RFM segments
    rfm_segmented = (rfm
        .withColumn("r_score", 
                   when(col("recency") <= 30, 3)
                   .when(col("recency") <= 90, 2)
                   .otherwise(1))
        .withColumn("f_score",
                   when(col("frequency") >= 5, 3)
                   .when(col("frequency") >= 2, 2)
                   .otherwise(1))
        .withColumn("m_score",
                   when(col("monetary") >= 500, 3)
                   .when(col("monetary") >= 100, 2)
                   .otherwise(1))
        .withColumn("rfm_score", 
                    col("r_score") * 100 + col("f_score") * 10 + col("m_score"))
        .withColumn("segment",
                   when(col("rfm_score") >= 321, "Champions")
                   .when(col("rfm_score") >= 311, "Loyal Customers")
                   .when(col("rfm_score") >= 131, "Potential Loyalists")
                   .when(col("rfm_score") >= 121, "New Customers")
                   .when(col("rfm_score") >= 221, "At Risk Customers")
                   .when(col("rfm_score") >= 211, "Can't Lose Them")
                   .otherwise("Lost Customers")))
    
    # Join with customer data
    return rfm_segmented.join(
        customers_df.select("customer_id", "first_name", "last_name", "email"),
        "customer_id"
    )

# Identify frequently co-purchased products
def analyze_product_affinity(order_items_df):
    """
    Perform product affinity analysis to find frequently co-purchased products.
    
    This helps identify products that are often bought together.
    """
    # Self-join to find products purchased in the same order
    product_pairs = (order_items_df
                    .select("order_id", "product_id", "product_name")
                    .alias("a")
                    .join(
                        order_items_df.select("order_id", "product_id", "product_name").alias("b"),
                        (col("a.order_id") == col("b.order_id")) & 
                        # Ensure pair is only counted once
                        (col("a.product_id") < col("b.product_id"))
                    )
                    .select(
                        col("a.product_id").alias("product_a_id"),
                        col("a.product_name").alias("product_a_name"),
                        col("b.product_id").alias("product_b_id"),
                        col("b.product_name").alias("product_b_name")
                    ))
    
    # Count frequency of each product pair
    return (product_pairs
           .groupBy("product_a_id", "product_a_name", "product_b_id", "product_b_name")
           .count()
           .orderBy(col("count").desc()))

def save_analytics_results(df, name, output_path):
    """Save analytics results to the specified path."""
    # Save to MinIO in Parquet format
    (df.write
     .mode("overwrite")
     .format("parquet")
     .save(f"{output_path}/analytics/{name}"))
    
    # Also save as CSV for easier viewing
    (df.coalesce(1)  # Combine to single file
     .write
     .mode("overwrite")
     .option("header", "true")
     .csv(f"{output_path}/analytics/csv/{name}"))
    
    print(f"Saved analytics results for '{name}' to {output_path}")

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Paths - update to use MinIO S3A
    data_lake_raw_path = "s3a://datalake/raw"
    data_lake_processed_path = "s3a://datalake/processed"
    analytics_output_path = "s3a://datalake"
    checkpoint_path = "s3a://datalake/checkpoints"
    
    # Enable incremental processing
    incremental_processing = True
    
    # Display S3 configuration for debugging
    print("S3/MinIO Configuration:")
    for conf in spark.sparkContext.getConf().getAll():
        if "s3" in conf[0]:
            print(f"  {conf[0]}: {conf[1]}")
    
    # Load the data
    print("Loading data from data lake...")
    
    try:
        # First process raw data to processed format
        print("Processing raw data to processed format...")
        entities = ["customers", "orders", "order_items"]
        processed_entities = []
        all_successful = True
        
        for entity in entities:
            # Process raw data, using incremental if enabled
            checkpoint_path_arg = checkpoint_path if incremental_processing else None
            success = process_raw_data(spark, entity, data_lake_raw_path, 
                                     data_lake_processed_path, checkpoint_path_arg)
            if success:
                processed_entities.append(entity)
            else:
                print(f"ERROR: Failed to process {entity}.")
                all_successful = False

        # Check if all required entities were processed successfully
        if not all_successful:
            print("ERROR: Not all required entities were processed successfully.")
            sys.exit(1)
        
        # Debug: Before loading data, check files in processed path
        print("\nDEBUG: Checking processed data files:")
        for entity in processed_entities:
            try:
                # Count partitions and files
                path = f"{data_lake_processed_path}/{entity}"
                df = spark.read.format("parquet").load(path)
                total_count = df.count()
                partition_count = df.select("__op").distinct().count()
                
                print(f"Entity: {entity}")
                print(f"  Total records: {total_count}")
                print(f"  Distinct operations: {partition_count}")
                
                # Show operation types and counts
                print("  Operation counts:")
                df.groupBy("__op").count().show(truncate=False)
                
                # Check 'is_deleted' distribution
                print("  Deleted status counts:")
                df.groupBy("is_deleted").count().show(truncate=False)
            except Exception as e:
                print(f"  Error examining {entity} files: {str(e)}")
        
        # Now load the processed data for analytics
        print("\nLoading processed data for analytics:")
        dataframes = {}
        
        for entity in processed_entities:
            try:
                print(f"\nLoading {entity} data...")
                dataframes[entity] = load_data_from_datalake(spark, entity, data_lake_processed_path)
                print(f"Successfully loaded {entity} data with {dataframes[entity].count()} records")
            except Exception as e:
                print(f"ERROR: Failed to load {entity} data: {str(e)}")
                print("Cannot proceed with analytics due to data loading failure.")
                sys.exit(1)
        
        # Sales analytics (requires orders and order_items)
        if "orders" in dataframes and dataframes["orders"] is not None and "order_items" in dataframes and dataframes["order_items"] is not None:
            print("Analyzing sales by period...")
            sales_by_period = analyze_sales_by_period(dataframes["orders"], dataframes["order_items"])
            
            print("Saving sales analytics results...")
            save_analytics_results(sales_by_period["daily"], "daily_sales", analytics_output_path)
            save_analytics_results(sales_by_period["weekly"], "weekly_sales", analytics_output_path)
            save_analytics_results(sales_by_period["monthly"], "monthly_sales", analytics_output_path)
            
            # Total sales by status
            print("Generating sales by status metrics...")
            sales_by_status = (dataframes["orders"]
                              .groupBy("status")
                              .agg(
                                  count("order_id").alias("order_count"),
                                  sum("total_amount").alias("total_amount")
                              )
                              .orderBy("status"))
            
            save_analytics_results(sales_by_status, "sales_by_status", analytics_output_path)
        else:
            print("ERROR: Failed to generate sales analytics due to missing data")
            sys.exit(1)
            
        # Customer analytics (requires customers and orders)
        if "customers" in dataframes and dataframes["customers"] is not None and "orders" in dataframes and dataframes["orders"] is not None:
            print("Analyzing customer cohorts...")
            customer_cohorts = analyze_customer_cohorts(dataframes["customers"], dataframes["orders"])
            save_analytics_results(customer_cohorts, "customer_cohorts", analytics_output_path)
            
            print("Calculating customer RFM segments...")
            customer_rfm = calculate_customer_recency_frequency_monetary(dataframes["customers"], dataframes["orders"])
            save_analytics_results(customer_rfm, "customer_rfm", analytics_output_path)
            
            # RFM segment distribution
            print("Generating customer segment distribution...")
            segment_distribution = (customer_rfm
                                   .groupBy("segment")
                                   .agg(count("*").alias("customer_count"))
                                   .orderBy(col("customer_count").desc()))
            
            save_analytics_results(segment_distribution, "customer_segments", analytics_output_path)
        else:
            print("ERROR: Failed to generate customer analytics due to missing data")
            sys.exit(1)
            
        # Product analytics (requires order_items)
        if "order_items" in dataframes and dataframes["order_items"] is not None:
            print("Analyzing product performance...")
            product_performance = analyze_product_performance(dataframes["order_items"])
            save_analytics_results(product_performance, "product_performance", analytics_output_path)
            
            print("Analyzing product affinity...")
            product_affinity = analyze_product_affinity(dataframes["order_items"])
            save_analytics_results(product_affinity, "product_affinity", analytics_output_path)
        else:
            print("ERROR: Failed to generate product analytics due to missing data")
            sys.exit(1)
        
        print("Analytics processing complete!")
        
    except Exception as e:
        print(f"Error during analytics processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()