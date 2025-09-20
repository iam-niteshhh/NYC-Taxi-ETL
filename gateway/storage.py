import boto3
import json
import uuid
import decimal

import constants

class S3EventStorageGateWay(object):
    def __init__(self):
        self.region = constants.REGION_NAME
        self.s3_event_endpoint = constants.S3_EVENT_ENDPOINT
        self.bucket = constants.S3_EVENT_BUCKET
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