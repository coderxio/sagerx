
FROM apache/airflow:2.5.1

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy airflow config
COPY config/airflow.cfg /opt/airflow/airflow.cfg

# Copy DAGs
COPY dags/ /opt/airflow/dags/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["airflow", "webserver"]
