import pendulum
import os
import pandas as pd
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from gateway.s3_gateway import S3GateWay
from scripts.transform_data import DataTransformer
import constants


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def _transform_and_upload():
    try:
        transformer = DataTransformer()
        transformer.transform_and_upload()
    except Exception as e:
        raise AirflowFailException(f"Transformation Failed {e}")


with DAG(
    dag_id="nyc_taxi_data_transformation",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "transform"],
) as dag:
    transform_task = PythonOperator(
        task_id="transform_and_load", python_callable=_transform_and_upload
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_next_dag",
        trigger_dag_id="load_to_postgres",
        dag=dag,
    )

    transform_task >> trigger_next_dag