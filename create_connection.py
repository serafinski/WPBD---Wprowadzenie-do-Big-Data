import json
import requests
import time
import socket
import argparse


def wait_for_service(host, port, timeout=180):
    """Wait for a service to be available at host:port."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"Service at {host}:{port} is available")
                return True
        except (socket.timeout, ConnectionRefusedError) as e:
            if time.time() - start_time > timeout:
                print(f"Timeout waiting for {host}:{port}")
                return False
            print(f"Service at {host}:{port} not yet available, retrying...")
            time.sleep(5)


def check_postgres(host, port, timeout=180):
    """Check if PostgreSQL is running by attempting to connect to it."""
    return wait_for_service(host, port, timeout)


def check_kafka_connect(url, timeout=180):
    """Check if Kafka Connect REST API is available."""
    start_time = time.time()
    while True:
        try:
            response = requests.get(f"{url}/connectors", timeout=5)
            if response.status_code == 200:
                print(f"Kafka Connect API at {url} is available")
                return True
        except requests.exceptions.RequestException:
            pass

        if time.time() - start_time > timeout:
            print(f"Timeout waiting for Kafka Connect API at {url}")
            return False

        print(f"Kafka Connect API at {url} not yet available, retrying...")
        time.sleep(5)


def get_existing_connectors(connect_url):
    """Get list of existing connectors."""
    try:
        response = requests.get(f"{connect_url}/connectors")
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []


def create_connector(connect_url, connector_config):
    """Create a new connector using the Kafka Connect REST API."""
    # Check if connector already exists
    existing_connectors = get_existing_connectors(connect_url)
    connector_name = connector_config['name']

    if connector_name in existing_connectors:
        print(f"Connector '{connector_name}' already exists")

        # Check connector status
        response = requests.get(f"{connect_url}/connectors/{connector_name}/status")
        if response.status_code == 200:
            status = response.json()
            if 'state' in status.get('connector', {}):
                print(f"Current state: {status['connector']['state']}")

            # Option to delete existing connector
            if input(f"Do you want to delete the existing connector '{connector_name}'? (y/n): ").lower() == 'y':
                print(f"Deleting connector '{connector_name}'...")
                delete_response = requests.delete(f"{connect_url}/connectors/{connector_name}")
                if delete_response.status_code in (204, 200):
                    print(f"Connector '{connector_name}' deleted successfully")
                else:
                    print(f"Failed to delete connector: {delete_response.text}")
                    return False
            else:
                return False
        else:
            print(f"Failed to get connector status: {response.text}")
            return False

    # Create the connector
    print(f"Creating connector '{connector_name}'...")
    headers = {'Content-Type': 'application/json'}
    response = requests.post(
        f"{connect_url}/connectors",
        data=json.dumps(connector_config),
        headers=headers
    )

    if response.status_code == 201:
        print(f"Connector '{connector_name}' created successfully")
        return True
    else:
        print(f"Failed to create connector: {response.text}")
        return False


def get_connector_status(connect_url, connector_name):
    """Get the status of a specific connector."""
    try:
        response = requests.get(f"{connect_url}/connectors/{connector_name}/status")
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None


def main():
    parser = argparse.ArgumentParser(description='Create a Debezium PostgreSQL connector.')
    parser.add_argument('--connect-host', default='localhost', help='Kafka Connect host')
    parser.add_argument('--connect-port', default=8083, type=int, help='Kafka Connect port')
    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--pg-port', default=5432, type=int, help='PostgreSQL port')
    parser.add_argument('--config-file', default=None, help='JSON file with connector configuration')
    args = parser.parse_args()

    # Default connector configuration
    default_config = {
        "name": "postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": "postgres_db",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "password",
            "database.dbname": "postgres",
            "database.server.name": "postgres_server",
            "topic.prefix": "postgres_server",
            "schema.include.list": "public",
            "table.include.list": "public.customers,public.orders,public.order_items",
            "plugin.name": "decoderbufs",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,table,lsn,source.ts_ms"
        }
    }

    # Load config from file if provided
    if args.config_file:
        try:
            with open(args.config_file, 'r') as f:
                connector_config = json.load(f)
            print(f"Loaded configuration from {args.config_file}")
        except Exception as e:
            print(f"Error loading config file: {e}")
            connector_config = default_config
    else:
        connector_config = default_config

    connect_url = f"http://{args.connect_host}:{args.connect_port}"

    # Wait for services to be ready
    print(f"Checking if PostgreSQL is available at {args.pg_host}:{args.pg_port}...")
    if not check_postgres(args.pg_host, args.pg_port):
        print("PostgreSQL is not available. Exiting.")
        return False

    print(f"Checking if Kafka Connect is available at {connect_url}...")
    if not check_kafka_connect(connect_url):
        print("Kafka Connect is not available. Exiting.")
        return False

    # Create the connector
    if create_connector(connect_url, connector_config):
        print("Connector creation initiated")

        # Wait a bit for the connector to start
        print("Waiting for connector to initialize...")
        time.sleep(5)

        # Check connector status
        status = get_connector_status(connect_url, connector_config['name'])
        if status:
            if 'state' in status.get('connector', {}):
                print(f"Connector state: {status['connector']['state']}")

            # Check tasks status
            tasks = status.get('tasks', [])
            if tasks:
                for i, task in enumerate(tasks):
                    if 'state' in task:
                        print(f"Task {i} state: {task['state']}")
                    if 'trace' in task:
                        print(f"Task {i} error: {task['trace']}")

        print("\nConnector setup complete. Monitor Kafka topics to see data changes.")
        return True
    else:
        print("Failed to create connector")
        return False


if __name__ == "__main__":
    main()