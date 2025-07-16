# WPBD - Wprowadzenie do Big Data (Introduction to Big Data)

This repository contains a comprehensive Big Data project demonstrating an end-to-end data pipeline. It covers Change Data Capture (CDC) from a PostgreSQL database using Debezium and Kafka, data ingestion into a MinIO data lake, and subsequent batch and streaming processing using Apache Spark.

## 🚀 Project Overview

This project simulates an e-commerce data pipeline, capturing real-time changes from a transactional PostgreSQL database and processing them for analytical purposes. It showcases the integration of several key Big Data technologies to build a robust and scalable data platform.

## ✨ Features

*   **Change Data Capture (CDC)**: Real-time capture of database changes (inserts, updates, deletes) from PostgreSQL using Debezium.
*   **Kafka Messaging**: Efficient and reliable data streaming using Apache Kafka.
*   **Data Lake Storage**: Raw and processed data stored in a MinIO (S3-compatible) data lake.
*   **Apache Spark Processing**:
    *   **Streaming Layer**: Ingests raw CDC events from Kafka into the MinIO data lake.
    *   **Batch Layer**: Performs advanced analytics (sales, customer cohorts, product performance, RFM analysis, product affinity) on processed data in the data lake.
*   **Dockerized Environment**: All components are containerized using Docker Compose for easy setup and deployment.
*   **Data Generation**: Script to generate sample customer, order, and order item data for testing and demonstration.

## 🛠️ Technologies Used

*   **PostgreSQL**: Relational database for transactional data.
*   **Debezium**: Distributed CDC platform for capturing database changes.
*   **Apache Kafka**: Distributed streaming platform for publishing and subscribing to data streams.
*   **Kafka UI**: Web interface for monitoring and managing Kafka clusters.
*   **MinIO**: High-performance, S3 compatible object storage for the data lake.
*   **Apache Spark**: Unified analytics engine for large-scale data processing.
*   **Python**: For data generation scripts, database interactions, and Spark jobs.
*   **Docker & Docker Compose**: For containerization and orchestration of services.

## 🌐 Exposed Ports

The following ports are exposed by the Docker services:

*   **PostgreSQL**: `5432`
*   **Kafka**: `9092` (internal), `29092` (external for `localhost`)
*   **Kafka UI**: `8080`
*   **Debezium Kafka Connect**: `8083`
*   **MinIO**: `9000` (API), `9001` (Console)
*   **Spark Master UI**: `8090` (mapped from container's `8080`)
*   **Spark Master (internal)**: `7077`

## ⚙️ Setup and Installation

To get this project up and running, ensure you have Docker and Docker Compose installed on your system.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/WPBD---Wprowadzenie-do-Big-Data.git
    cd WPBD---Wprowadzenie-do-Big-Data
    ```

2.  **Start the Docker services:**
    This command will build the necessary images and start all services (PostgreSQL, Kafka, Debezium, MinIO, Spark, Kafka UI, and setup containers).
    ```bash
    docker-compose up --build -d
    ```
    *   **Note**: The `connector-setup` service will automatically create the Debezium PostgreSQL connector once Debezium and PostgreSQL are healthy. The `mc` service will create the `datalake` bucket in MinIO.

3.  **Verify services are running:**
    ```bash
    docker-compose ps
    ```
    You should see all services in a `running` state.

## 🚀 Usage

### 1. Generate Sample Data

Once all services are up, you can generate sample data in the PostgreSQL database. This data will be captured by Debezium and streamed to Kafka.

```bash
docker exec -it connector-setup python /app/generate_data.py --customers 100 --max-orders 5 --max-items 10
```
*   You can adjust the number of customers, maximum orders per customer, and maximum items per order using the arguments.
*   `--customers-only`: Generate only customer data.
*   `--orders-only`: Generate only orders and items for existing customers.

### 2. Monitor Kafka Topics (Optional)

You can access Kafka UI to monitor the Kafka topics where Debezium publishes CDC events:
*   **Kafka UI**: `http://localhost:8080`

You should see topics like `postgres_server.public.customers`, `postgres_server.public.orders`, and `postgres_server.public.order_items` being populated with data.

### 3. Run Spark Streaming Job

The Spark streaming job continuously reads CDC events from Kafka and writes the raw data to the MinIO data lake.

```bash
docker exec spark /opt/bitnami/spark/scripts/jobs.sh streaming
```
This command will keep running, processing new data as it arrives in Kafka. You can stop it with `Ctrl+C`.

### 4. Run Spark Batch Job

The Spark batch job processes the raw data in the MinIO data lake, cleans it, and performs various analytical calculations, saving the results back to MinIO.

```bash
docker exec spark /opt/bitnami/spark/scripts/jobs.sh batch
```
This job will run to completion. The analytical results will be saved in `s3a://datalake/processed/analytics/` in Parquet and CSV formats.

### 5. Access MinIO (Optional)

You can access the MinIO console to view the raw and processed data:
*   **MinIO Console**: `http://localhost:9001`
*   **Access Key**: `minioadmin`
*   **Secret Key**: `minioadmin`

Look for the `datalake` bucket, which will contain `raw/` and `processed/` directories.

## 📂 Project Structure

```
.
├── .gitignore
├── .python-version
├── cleanup.sh                  # Script to clean up Docker volumes and data
├── docker-compose.yaml         # Defines all Docker services and their configurations
├── generate_data.py            # Script to generate sample data for PostgreSQL
├── pyproject.toml              # Poetry configuration for Python dependencies
├── README.md                   # This file
├── uv.lock                     # Poetry lock file
├── connector-setup/            # Contains scripts and configuration for Debezium connector
│   ├── create_connector.py     # Python script to create the Debezium connector
│   ├── Dockerfile              # Dockerfile for the connector setup service
│   └── config/
│       └── connector_config.json # Debezium connector configuration
├── database/                   # Database management and models
│   ├── db_manager.py           # Handles database connections and sessions
│   ├── models.py               # SQLAlchemy ORM models for database tables
│   └── dbo/                    # Data Access Objects (DAOs) for database operations
│       ├── customers.py
│       ├── orders_items.py
│       └── orders.py
└── spark/                      # Apache Spark related scripts
    └── scripts/
        ├── batch_processor.py      # Spark script for batch data processing and analytics
        ├── jobs.sh                 # Helper script to run Spark jobs (streaming/batch)
        └── streaming_processor.py  # Spark script for streaming data ingestion from Kafka to MinIO
```

## 🤝 Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.