from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
import os

default_args = {"owner": "airflow", "retries": 1}

# Get AWS credentials from environment (should be set in docker-compose.yml)
def get_aws_env():
    return {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_REGION": os.environ.get("AWS_REGION", "ap-south-1"),
        "PYTHONUNBUFFERED": "1",
    }

with DAG(
    dag_id="test_embeddings_only",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Test only embeddings + FAISS (skip Scala jobs)",
    tags=["test", "embeddings"],
):

    generate_embeddings = DockerOperator(
        task_id="generate_embeddings",
        image="lakerag_arun-kumar-s_josys-bootcamp_embeddings:latest",
        container_name="test_embeddings_{{ ts_nodash }}",
        api_version="auto",
        auto_remove="success",
        force_pull=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",  # Use host network for internet access
        working_dir="/app/embeddings",
        command="python generate_embeddings.py",
        mounts=[
            {"source": "/home/sak23/Desktop/Projects/Josys-Bootcamp/LakeRAG_Arun-Kumar-S_Josys-Bootcamp/local_data", "target": "/app/local_data", "type": "bind"},
        ],
        environment=get_aws_env(),
        mount_tmp_dir=False,
    )

    build_faiss_index = DockerOperator(
        task_id="build_faiss_index",
        image="lakerag_arun-kumar-s_josys-bootcamp_embeddings:latest",
        container_name="test_faiss_{{ ts_nodash }}",
        api_version="auto",
        auto_remove="success",
        force_pull=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",  # Use host network for internet access
        working_dir="/app/embeddings",
        command="python build_faiss_index.py",
        mounts=[
            {"source": "/home/sak23/Desktop/Projects/Josys-Bootcamp/LakeRAG_Arun-Kumar-S_Josys-Bootcamp/local_data", "target": "/app/local_data", "type": "bind"},
        ],
        environment=get_aws_env(),
        mount_tmp_dir=False,
    )

    generate_embeddings >> build_faiss_index