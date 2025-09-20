import boto3
import json
import uuid
import decimal
import logging

from utils.encoder import DecimalEncoder
import constants

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class S3GateWay(object):
    def __init__(self, bucket=None):
        self.region = constants.REGION_NAME
        self.s3_event_endpoint = constants.S3_EVENT_ENDPOINT
        self.bucket = bucket or constants.S3_EVENT_BUCKET
        self.s3 = boto3.client(
            "s3", region_name=self.region, endpoint_url=self.s3_event_endpoint
        )

    def store_event(self, event: dict, key=None):
        key = key or str(uuid.uuid4())
        self.s3.put_object(
            Key=key,
            Bucket=self.bucket,
            Body=json.dumps(event, cls=DecimalEncoder),
        )
        return key

    def upload_file(self, file_path: str, folder: str, key: str):
        """
        Uploads a file from a local file path to S3.
        """
        try:
            self.s3.upload_file(file_path, self.bucket, key)
        except Exception as e:
            raise  # Re-raise the exception to fail the Airflow task

    def read_files(self, folder, failed_files=[]):
        """
        :param folder: path to files
        :param: failed_files: list to hold files failed to get
        :return: dict which contains inventory file name as key and value as inventory file data
        """
        files = {}
        objs = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=folder + "/"
        ).get("Contents", [])

        if not objs:
            LOGGER.info("order status file not present")
            return files

        last_added_files = [
            obj["Key"]
            for obj in objs
            if obj.get("Key") and obj.get("Size") > 0
        ]

        LOGGER.info("last_added_files - %s", last_added_files)

        if not last_added_files:
            return files

        for file_name in last_added_files:
            try:
                s3_response_object = self.s3.get_object(
                    Bucket=self.bucket, Key=file_name
                )
                files[file_name] = s3_response_object
            except Exception as exception:
                LOGGER.error("Error - %s ", exception)
                failed_files.append(file_name)
        return files

    def move_file(self, source_folder, dest_folder):
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=source_folder
        )

        if "Contents" not in response:
            LOGGER.info(
                f"No files found in the source folder: {source_folder}"
            )
            return

        for obj in response["Contents"]:
            source_key = obj["Key"]
            if source_key.endswith("/"):
                continue

            # Define destination key
            dest_key = source_key.replace(source_folder, dest_folder, 1)

            try:
                # Copy the file to the destination folder
                self.s3.copy_object(
                    Bucket=self.bucket,
                    CopySource={"Bucket": self.bucket, "Key": source_key},
                    Key=dest_key,
                )
                # Delete the file from the source folder after copying
                self.s3.delete_object(Bucket=self.bucket, Key=source_key)
                LOGGER.info(f"Moved file {source_key} to {dest_key}")
            except Exception as e:
                LOGGER.error(
                    f"Error moving {source_key} to {dest_key}: {str(e)}"
                )
