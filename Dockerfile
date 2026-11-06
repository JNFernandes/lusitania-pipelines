# Our own Airflow image = the official Airflow image + what the Lusitânia pipeline needs.
#
#   already in the official image:  Airflow 3.3.2, Python, the SQL Server ODBC driver (msodbcsql18)
#   added here:                     Java (for Spark) + a SEPARATE Python environment for the pipeline
#
# Built automatically by `docker compose up -d --build` (see "build: ." in docker-compose.yaml).

# Python 3.11, the same version as the Project 2 venv on the laptop
FROM apache/airflow:3.3.2-python3.11

# --- 1. System packages: need root ---------------------------------------------------
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# --- 2. The pipeline's own Python environment ----------------------------------------
# Why a separate venv: Airflow depends on hundreds of pinned packages, and dbt and Spark on
# others. Installed together they clash. So Airflow keeps its environment, and the pipeline
# gets /opt/lusitania-venv. Airflow tasks run the pipeline with THAT environment's python.
RUN python -m venv /opt/lusitania-venv && chown -R airflow:0 /opt/lusitania-venv
USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN /opt/lusitania-venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt

# --- 3. Download the Spark jars now, at build time ------------------------------------
# Delta Lake + the KAFKA CONNECTOR (for the streaming consumer; its version must match Spark's).
# Spark fetches them from the internet the first time a session starts. Doing it here bakes
# them into the image, so pipeline runs do not depend on that download.
RUN /opt/lusitania-venv/bin/python -c "import pyspark; from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; kafka = 'org.apache.spark:spark-sql-kafka-0-10_2.12:' + pyspark.__version__; configure_spark_with_delta_pip(SparkSession.builder.master('local[1]'), extra_packages=[kafka]).getOrCreate().stop()"
