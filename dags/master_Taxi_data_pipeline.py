import os
import requests
import boto3
import pendulum
import logging
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from gateway.s3_gateway import S3GateWay
from gateway.data_gateway import TaxiDataGateway
from gateway.postgres_gateway import PostgresGateway
from scripts.transform_data import DataTransformer
from utils.loader import DataLoader
import constants


# Setup logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# --- Re-using your functions from previous DAGs ---

def s3_bucket():
    """Task to create the S3 bucket if it does not exist."""
    s3_gateway = S3GateWay()
    s3_gateway.create_bucket(start_up=True)


def _download_data(url: str, local_path: str):
    """Task to download the raw data file from a URL."""
    downloader = TaxiDataGateway(url=url, local_path=local_path)
    downloader.download_file()
    return local_path


def _upload_data(ti):
    """Task to upload the raw data to the S3 bucket."""
    local_path = ti.xcom_pull(task_ids="download_data_from_url")
    if not local_path or not os.path.exists(local_path):
        raise AirflowFailException("Downloaded file not found or path is missing.")
    
    file_names = [f for f in os.listdir(local_path)]
    uploader = S3GateWay()
    for file in file_names:
        file_path = os.path.join(local_path, file)
        uploader.upload_file(
            file_path=file_path,
            upload_to=constants.S3_RAW_FOLDER_NAME + file,
        )


def _transform_and_upload():
    """Task to transform the data and upload the cleaned version back to S3."""
    try:
        transformer = DataTransformer()
        transformer.transform_and_upload()
    except Exception as e:
        raise AirflowFailException(f"Transformation Failed {e}")


def _create_postgres_table():
    """Task to create the table schema in PostgreSQL if it doesn't exist."""
    postgres_gateway = PostgresGateway()
    postgres_gateway.get_or_create_table_schema(
        table_name=constants.POSTGRES_TABLE_NAME, 
        default_schema=constants.TABLE_SCHEMA
    )


def _load_data_to_postgres():
    """Task to load the cleaned data from S3 to PostgreSQL."""
    loader = DataLoader()
    loader.load_data_from_s3_to_postgres()


# --- Main DAG Definition ---

with DAG(
    dag_id="master_nyc_taxi_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule= "*/30 * * * *",  # Runs daily at every 30 mins UTC
    catchup=False,
    tags=["etl", "production-ready"],
) as dag:
    LOGGER.info("Starting the master DAG")

    # Phase 1 & 2: Data Ingestion (Extract & Load)
    bucket_create_task = PythonOperator(
        task_id="s3_bucket_create", 
        python_callable=s3_bucket
    )
    
    download_task = PythonOperator(
        task_id="download_data_from_url",
        python_callable=_download_data,
        op_kwargs={
            "url": f"{constants.URL_PREFIX}/{constants.FILE_NAME}",
            "local_path": constants.RAW_FILE_DOWNLOAD_PATH,
        },
    )

    upload_task = PythonOperator(
        task_id="upload_data_to_s3",
        python_callable=_upload_data,
    )

    # Phase 3: Data Transformation
    transform_task = PythonOperator(
        task_id="transform_and_load", 
        python_callable=_transform_and_upload
    )

    # Phase 4: Data Warehousing & Analysis
    create_table_task = PythonOperator(
        task_id="create_postgres_table",
        python_callable=_create_postgres_table
    )

    load_data_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres
    )

    # Define the sequential dependencies
    bucket_create_task >> download_task >> upload_task >> transform_task >> create_table_task >> load_data_task