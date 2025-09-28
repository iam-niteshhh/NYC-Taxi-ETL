import os
import yaml

CONFIG_FILE_PATH = os.environ.get("CONFIG_FILE", None)
if not CONFIG_FILE_PATH:
    raise Exception("Environment Not loaded")
with open(CONFIG_FILE_PATH, "r") as file:
    CONFIG_FILE = yaml.safe_load(file)

STAGE = CONFIG_FILE.get("stage")
REGION_NAME = CONFIG_FILE.get("region")
S3_EVENT_BUCKET = CONFIG_FILE.get("s3EventBucketName")
S3_EVENT_ENDPOINT = CONFIG_FILE.get("s3EventEndpoint")
RAW_FILE_DOWNLOAD_PATH = "./data/raw/"  # local path to download from source
PROCESSED_FILE_SAVE_PATH = "./data/processed/"
S3_RAW_FOLDER_NAME = "raw/"
S3_PROCESSED_FOLDER_NAME = "processed/"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow_tripdata"
FILE_MONTH = "2023-01"  # Example: January 2023
FILE_NAME = (
    f"{TAXI_TYPE}_{FILE_MONTH}.parquet"  # We'll use Parquet for better performance
)
POSTGRES_TABLE_NAME = "clean_taxi_trips"
# Define the table schema within the DAG for clarity
TABLE_SCHEMA = [
    {'name': 'trip_id', 'type': 'SERIAL', 'constraint': 'PRIMARY KEY'},
    {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP'},
    {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP'},
    {'name': 'passenger_count', 'type': 'INT'},
    {'name': 'trip_distance', 'type': 'FLOAT'},
    {'name': 'trip_duration_minutes', 'type': 'FLOAT'},
    {'name': 'store_and_fwd_flag', 'type': 'VARCHAR(10)'},
]