import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from gateway.postgres_gateway import PostgresGateway
from utils.loader import DataLoader
import constants

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

table_schema = constants.TABLE_SCHEMA


def create_postgres_table():
    redshift_gateway = PostgresGateway()
    redshift_gateway.get_or_create_table_schema(table_name=constants.POSTGRES_TABLE_NAME, default_schema=table_schema)

def load_data_wrapper():
    loader = DataLoader()
    loader.load_data_from_s3_to_postgres()


with DAG(
    dag_id='load_to_postgres',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    schedule=None,
    catchup=False,
    tags=['data-warehousing', 'redshift', 'etl'],
) as dag:
    
    create_table_task = PythonOperator(
        task_id = 'create_postgres_table',
        python_callable=create_postgres_table
    )

    load_data_task = PythonOperator(
        task_id = 'load_data_to_postgres',
        python_callable=load_data_wrapper
    )

    create_table_task >> load_data_task