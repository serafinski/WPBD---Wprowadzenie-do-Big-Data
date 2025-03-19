# This is for batch layer, which is responsible for processing the data in the data lake and generating analytics

# docker exec spark /opt/bitnami/spark/scripts/jobs.sh batch

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, expr, to_date, year, 
    datediff, when, lit, max as spark_max,
    to_timestamp, current_timestamp, countDistinct
)

def create_spark_session():
    """Create a Spark session with appropriate configurations."""
    return (SparkSession.builder
            .appName("E-commerce Batch Analytics")
            .config("spark.sql.streaming.checkpointLocation", "/opt/bitnami/spark/checkpoints")
            # Configure MinIO S3 connectivity - updated to match streaming processor
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.595")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Add these configs to avoid S3A FileSystem problems
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())

def load_data_from_datalake(spark, entity_name, data_lake_path):
    """Load the latest data from the data lake."""
    full_path = f"{data_lake_path}/{entity_name}"
    
    # Read the data and filter out deleted records
    df = (spark.read
          .format("parquet")
          .load(full_path)
          .filter(col("is_deleted") == False))
    
    return df

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

def analyze_customer_cohorts(customers_df, orders_df):
    """Analyze customer cohorts based on registration date."""
    # Create cohort based on registration month
    cohorted_customers = (customers_df
                         .withColumn("registration_cohort", 
                                    expr("date_format(registration_date, 'yyyy-MM')"))
                         .select("customer_id", "registration_cohort"))
    
    # Join with orders
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

def calculate_customer_recency_frequency_monetary(customers_df, orders_df):
    """
    Calculate RFM (Recency, Frequency, Monetary) metrics for customer segmentation.
    
    This is a more advanced analysis to segment customers based on their
    purchasing behavior.
    """
    # Get the most recent date in the orders dataset
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

def process_raw_data(spark, entity_name, raw_path, processed_path):
    """
    Process the raw CDC data into a cleaned format for analytics.
    With direct handling for PostgreSQL binary numeric types using a UDF.
    
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    from pyspark.sql.functions import expr, udf
    from pyspark.sql.types import DoubleType
    import binascii
    
    # Create a UDF to directly convert PostgreSQL binary to numeric values
    @udf(returnType=DoubleType())
    def pg_numeric_to_double(binary_data):
        if binary_data is None:
            return None
        
        try:
            # Convert to hex for processing
            hex_str = binascii.hexlify(binary_data).decode('utf-8')
            
            # Handle different hex lengths
            if len(hex_str) == 6:  # 3 bytes: xxxxxx
                high = int(hex_str[0:2], 16)
                mid = int(hex_str[2:4], 16)
                low = int(hex_str[4:6], 16)
                value = (high * 65536 + mid * 256 + low) / 100.0
                return value
            elif len(hex_str) == 4:  # 2 bytes: xxxx
                high = int(hex_str[0:2], 16)
                low = int(hex_str[2:4], 16)
                value = (high * 256 + low) / 100.0
                return value
            elif len(hex_str) == 2:  # 1 byte: xx
                value = int(hex_str, 16) / 100.0
                return value
            else:
                return None
        
        except Exception:
            return None
    
    full_path = f"{raw_path}/{entity_name}"
    output_path = f"{processed_path}/{entity_name}"
    
    print(f"Processing {entity_name} data from {full_path}...")
    
    try:
        # Read the raw data
        try:
            df = spark.read.format("parquet").load(full_path)
            if df.count() == 0:
                print(f"No data found in {full_path}. Skipping processing.")
                return False
        except Exception as e:
            print(f"Error reading data from {full_path}: {str(e)}")
            return False
        
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
        
        # Convert timestamp fields if present
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
        
        # Handle binary fields with direct UDF conversion
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
        
        # Write to MinIO in Parquet format
        (processed_df
         .write
         .mode("overwrite")
         .format("parquet")
         .partitionBy("__op")
         .save(output_path))
        
        print(f"Successfully processed and saved {entity_name} data to {output_path}")
        return True
    
    except Exception as e:
        print(f"Error processing {entity_name} data: {str(e)}")
        return False

def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Paths - update to use MinIO S3A
    data_lake_raw_path = "s3a://datalake/raw"
    data_lake_processed_path = "s3a://datalake/processed"
    analytics_output_path = "s3a://datalake"
    
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
        
        for entity in entities:
            success = process_raw_data(spark, entity, data_lake_raw_path, data_lake_processed_path)
            if success:
                processed_entities.append(entity)
            else:
                print(f"WARNING: Failed to process {entity}. Skipping it in analytics.")
        
        # Now load the processed data for analytics
        dataframes = {}
        
        for entity in processed_entities:
            try:
                dataframes[entity] = load_data_from_datalake(spark, entity, data_lake_processed_path)
                print(f"Successfully loaded {entity} data with {dataframes[entity].count()} records")
            except Exception as e:
                print(f"ERROR: Failed to load {entity} data: {str(e)}")
                dataframes[entity] = None
        
        # Check if we have all required dataframes
        if "customers" not in dataframes or dataframes["customers"] is None:
            print("ERROR: Customers data is missing. Cannot proceed with customer analytics.")
        
        if "orders" not in dataframes or dataframes["orders"] is None:
            print("ERROR: Orders data is missing. Cannot proceed with order analytics.")
        
        if "order_items" not in dataframes or dataframes["order_items"] is None:
            print("ERROR: Order items data is missing. Cannot proceed with product analytics.")
        
        # Perform analytics only if we have the necessary data
        
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
            print("Skipping sales analytics due to missing data")
            
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
            print("Skipping customer analytics due to missing data")
            
        # Product analytics (requires order_items)
        if "order_items" in dataframes and dataframes["order_items"] is not None:
            print("Analyzing product performance...")
            product_performance = analyze_product_performance(dataframes["order_items"])
            save_analytics_results(product_performance, "product_performance", analytics_output_path)
            
            print("Analyzing product affinity...")
            product_affinity = analyze_product_affinity(dataframes["order_items"])
            save_analytics_results(product_affinity, "product_affinity", analytics_output_path)
        else:
            print("Skipping product analytics due to missing data")
        
        print("Analytics processing complete!")
        
    except Exception as e:
        print(f"Error during analytics processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()