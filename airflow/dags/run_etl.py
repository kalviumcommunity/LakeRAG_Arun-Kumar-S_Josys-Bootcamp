from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow"}

with DAG(
    dag_id="scala_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
):

    raw_ingestion = BashOperator(
        task_id="raw_ingestion",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "/opt/etl/lakerag-etl.jar jobs.RawIngestionJob"
        ),
    )

    raw_to_silver = BashOperator(
        task_id="raw_to_silver",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "/opt/etl/lakerag-etl.jar jobs.RawToSilverJob"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "/opt/etl/lakerag-etl.jar jobs.SilverToGoldJob"
        ),
    )

    raw_ingestion >> raw_to_silver >> silver_to_gold
