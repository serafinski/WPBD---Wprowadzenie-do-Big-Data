#!/bin/bash

# docker exec spark chmod +x /opt/bitnami/spark/scripts/jobs.sh

# This script submits PySpark jobs to process data and store it in MinIO

# Set environment variables
SPARK_HOME=/opt/bitnami/spark
SCRIPTS_DIR=/opt/bitnami/spark/scripts

# Submit the CDC to MinIO streaming job (runs continuously)
submit_streaming_job() {
    echo "Submitting CDC to MinIO streaming job..."
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.595 \
        --conf "spark.executor.memory=2g" \
        --conf "spark.driver.memory=2g" \
        ${SCRIPTS_DIR}/streaming_processor.py
}

# Submit the batch processor job (runs on schedule)
submit_batch_job() {
    echo "Submitting batch processing job..."
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.595 \
        --conf "spark.executor.memory=2g" \
        --conf "spark.driver.memory=2g" \
        ${SCRIPTS_DIR}/batch_processor.py
}

# Check if a specific job was requested
if [ "$1" == "streaming" ]; then
    submit_streaming_job
elif [ "$1" == "batch" ]; then
    submit_batch_job
else
    echo "Available commands:"
    echo "  streaming - Start the streaming job from Kafka to MinIO"
    echo "  batch     - Run the batch processing job"
    echo ""
    echo "Example: ./run_spark_minio_jobs.sh streaming"
fi