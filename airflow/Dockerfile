FROM apache/airflow:2.5.1

USER root

# google-cloud-bigquery pulls in Shapely; wheels still expect libgeos_c at runtime.
RUN apt-get update \
  && apt-get install -y --no-install-recommends libgeos-c1v5 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root requirements.txt .

# Postgres 10+ often uses SCRAM-SHA-256; the stock Airflow 2.5 image can ship
# psycopg2 linked against libpq < 10. Remove the base wheel first so pip does one
# clean install (avoids a force-reinstall temp tree that spikes disk use).
RUN pip uninstall -y psycopg2 psycopg2-binary 2>/dev/null || true \
  && pip install --no-cache-dir -r requirements.txt
