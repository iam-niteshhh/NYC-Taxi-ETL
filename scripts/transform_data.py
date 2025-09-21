import pandas as pd
import logging
from io import BytesIO
import os

import constants
from gateway.s3_gateway import S3GateWay

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class DataTransformer(object):
    def __init__(self):
        self.s3_gateway = S3GateWay()

    def transform_and_upload(self):
        files = self.s3_gateway.read_files(folder=constants.S3_RAW_FOLDER_NAME)

        if not files:
            LOGGER.info("No New files Present")
            return

        for file_name, file_content in files.items():
            LOGGER.info(f"Processing file: {file_name}")
            print(file_content.get("Body"))
            df = pd.read_parquet(BytesIO(file_content.get("Body").read()))

            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
            df = df[(df["passenger_count"] > 0) & (df["trip_distance"] > 0)]
            df["trip_duration_minutes"] = (
                df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60

            LOGGER.info(f"Cleaned records: {len(df)}")
            # 4. Save the transformed data to a Parquet buffer
            os.makedirs(constants.PROCESSED_FILE_SAVE_PATH, exist_ok=True)
            df.to_parquet(
                constants.PROCESSED_FILE_SAVE_PATH + file_name.split("/")[1],
                engine="pyarrow",
            )

            self.s3_gateway.upload_file(
                file_path=constants.PROCESSED_FILE_SAVE_PATH + file_name.split("/")[1],
                upload_to=constants.S3_PROCESSED_FOLDER_NAME + file_name.split("/")[1],
            )
