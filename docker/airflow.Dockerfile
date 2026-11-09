# Image Airflow (phase 2) : Airflow 2.10 + Java (pour Spark) + un Python séparé pour les jobs.
FROM apache/airflow:2.10.5-python3.11

USER root
# Java est nécessaire à PySpark. (mkdir : bug connu de Java sur les images Debian "slim")
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Environnement Python des jobs (PySpark, pandas, XGBoost, MLflow), séparé d'Airflow
COPY docker/jobs-requirements.txt /tmp/jobs-requirements.txt
RUN python -m venv /opt/jobs-venv \
    && /opt/jobs-venv/bin/pip install --no-cache-dir -r /tmp/jobs-requirements.txt \
    && chown -R airflow:root /opt/jobs-venv

# Phase 3 : environnement dbt (DuckDB + Snowflake), séparé lui aussi (ses versions
# de protobuf / jinja ne doivent pas casser MLflow ni Airflow)
COPY crimewatch_dbt/requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt "dbt-snowflake>=1.9,<2" \
    && chown -R airflow:root /opt/dbt-venv

USER airflow