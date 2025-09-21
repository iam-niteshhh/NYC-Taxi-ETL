import pandas as pd
import logging

import constants
from gateway.s3_gateway import S3GateWay

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class DataTransformer(object):
    def __init__(self):
        self.s3_gateway = S3GateWay()

    def transform_data(self):
        files = self.s3_gateway.read_files(folder=constants.RAW_FILE_FOLDER)

        if not files:
            LOGGER.info("No New files Present")
            return

        for file_name, file_content in files.items():
            LOGGER.info(f"Processing file: {file_name}")
            df = pd.read_parquet(file_content)
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
            df = df[(df["passenger_count"] > 0) & (df["trip_distance"] > 0)]
            df["trip_duration_minutes"] = (
                df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60

            df.to_csv("Name", index=False)  # give file name
            self.s3_gateway.upload_file(file_path="hk", key="processed")
