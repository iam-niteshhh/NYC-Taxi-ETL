import os

CONFIG_FILE = os.environ.get("CONFIG_FILE", None)
if not CONFIG_FILE:
    raise Exception("Environment Not loaded")

REGION_NAME = CONFIG_FILE.get("region")
S3_EVENT_BUCKET = CONFIG_FILE.get("s3EventBucketName")
S3_EVENT_ENDPOINT = CONFIG_FILE.get("s3EventEndpoint")
RAW_FILE_DOWNLOAD_PATH = "./data/raw/"  # local path to download from source
S3_RAW_FOLDER_NAME = "raw"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPE = "yellow_tripdata"
FILE_MONTH = "2023-01"  # Example: January 2023
FILE_NAME = f"{TAXI_TYPE}_{FILE_MONTH}.parquet"  # We'll use Parquet for better performance
