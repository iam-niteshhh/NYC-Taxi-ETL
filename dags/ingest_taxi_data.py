import os
import requests
import boto
import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from gateway.download import DownloadTaxiDataGateway
from gateway.storage import S3EventStorageGateWay
import constants

def _download_data(url:str, local_path:str):
    """Wrapper function"""

    downloader = DownloadTaxiDataGateway(url=url)
    response = downloader.download_file()
    return response

def _upload_data(ti, bucket:str, key_prefix:str):
    """Wrapper function"""
 # Pull the local path from the download task's XCom
    local_path = ti.xcom_pull(task_ids='download_data_from_url')
    if not local_path or not os.path.exists(local_path):
        raise AirflowFailException("Downloaded file not found or path is missing.")

    uploader = S3EventStorageGateWay(bucket=bucket)
    uploader.upload_file(file_path=local_path, key=f'{constants.TAXI_TYPE}/{constants.FILE_NAME}')

with DAG(
    dag_id = 'nyc_taxi_data_ingestion',
    start_date = pendulum.datetime(2023,1,1, tz='UTC'),
    schedule = None,
    catchup = False,
    tags = ['etl', 'taxt']
) as dag:
    
    download_task = PythonOperator(
        task_id='download_data_from_url',
        python_callable=_download_data,
        op_kwargs={
            'url': f'{constants.URL_PREFIX}/{constants.FILE_NAME}',
            'local_path': constants.DOWNLOAD_PATH,
        },
    )

    upload_task = PythonOperator(
        task_id='upload_data_to_s3',
        python_callable=_upload_data,
        op_kwargs={
            'bucket': constants.RAW_FILE_BUCKET,
            'key_prefix': constants.RAW_KEY_PREFIX,
        },
    )

    # Set the sequential task dependency
    download_task >> upload_task