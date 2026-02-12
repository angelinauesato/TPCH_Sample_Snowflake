# Use the official Apache Airflow image (Airflow 2.10)
FROM apache/airflow:2.10.2

# Switch to root to install system-level dependencies (if needed)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

RUN pip install --no-cache-dir "astronomer-cosmos"
RUN pip install --no-cache-dir "dbt-snowflake>=1.8.0,<1.9.0"