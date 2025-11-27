from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow"}

with DAG(
    dag_id="scala_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
):

    raw_ingestion = SparkSubmitOperator(
        task_id="raw_ingestion",
        application="/opt/etl/lakerag-etl.jar",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark:7077"},
        java_class="jobs.RawIngestionJob",
        packages="io.delta:delta-spark_2.12:3.2.0",
    )

    raw_to_silver = SparkSubmitOperator(
        task_id="raw_to_silver",
        application="/opt/etl/lakerag-etl.jar",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark:7077"},
        java_class="jobs.RawToSilverJob",
        packages="io.delta:delta-spark_2.12:3.2.0",
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/etl/lakerag-etl.jar",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark:7077"},
        java_class="jobs.SilverToGoldJob",
        packages="io.delta:delta-spark_2.12:3.2.0",
    )

    raw_ingestion >> raw_to_silver >> silver_to_gold
