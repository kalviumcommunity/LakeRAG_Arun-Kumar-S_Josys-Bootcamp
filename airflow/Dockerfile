FROM apache/airflow:2.8.1-python3.10

USER root

# Install Java & Spark dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless wget curl && \
    apt-get clean

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -P /tmp && \
    tar -xzf /tmp/spark-3.5.0-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin:/opt/spark/sbin"

USER airflow

# Spark provider (correct constraint for Airflow 2.8.1 Py3.10)
RUN pip install \
  "apache-airflow-providers-apache-spark" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
