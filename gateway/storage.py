import boto3
import json
import uuid
import decimal

import constants

class S3EventStorageGateWay(object):
    def __init__(self, bucket=None):
        self.region = constants.REGION_NAME
        self.s3_event_endpoint = constants.S3_EVENT_ENDPOINT
        self.bucket = bucket or constants.S3_EVENT_BUCKET
        self.s3 = boto3.client(
            's3',
            region_name=self.region,
            endpoint_url=self.s3_event_endpoint
        )
    
    def store_event(self, event:dict, key=None):
        key = key or str(uuid.uuid4())
        self.s3.put_object(
            Key=key,
            Bucket=self.bucket,
            Body = json.dumps(event, cls=DecimalEncoder)
        )
        return key

    def upload_file(self, file_path: str, key: str):
        """
        Uploads a file from a local file path to S3.
        """
        try:
            self.s3.upload_file(file_path, self.bucket, key)
        except Exception as e:
            raise # Re-raise the exception to fail the Airflow task

class DecimalEncoder(json.JSONEncoder):
    """
    DecimalEncoder class
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            return float(o)
        return super(DecimalEncoder, self).default(o)