import os
import requests
import boto3
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from gateway.data_gateway import TaxiDataGateway
from gateway.s3_gateway import S3GateWay
import constants


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def _download_data(url: str, local_path: str):
    """Wrapper function"""

    downloader = TaxiDataGateway(url=url, local_path=local_path)
    downloader.download_file()
    return local_path


def _upload_data(ti):
    """Wrapper function"""
    # Pull the local path from the download task's XCom
    local_path = ti.xcom_pull(task_ids="download_data_from_url")
    if not local_path or not os.path.exists(local_path):
        raise AirflowFailException(
            "Downloaded file not found or path is missing."
        )

    # Extract the file name from the local path
    file_name = os.path.basename(local_path)
    # Construct the S3 key dynamically using the prefix and file name

    uploader = S3GateWay()
    uploader.upload_file(
        file_path=local_path,
        folder=constants.S3_RAW_FOLDER_NAME,
        key=file_name,
    )


with DAG(
    dag_id="nyc_taxi_data_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=True,
    tags=["etl", "taxt"],
) as dag:
    LOGGER.info("Starting the DAG")

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

    # Set the sequential task dependency
    download_task >> upload_task
