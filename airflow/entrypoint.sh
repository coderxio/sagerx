#!/bin/bash

# Initialize the Airflow database
echo "Initializing Airflow database..."
airflow db init

# Create admin user if it doesn't exist
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin \
    2>/dev/null || true

# Start the webserver
echo "Starting Airflow webserver..."
exec "$@"
