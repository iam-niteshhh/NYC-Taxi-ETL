import requests
import logging
import os
import datetime
from airflow.exceptions import AirflowFailException


# Set up logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class TaxiDataGateway(object):
    def __init__(self, url, local_path):
        self.url = url
        self.local_path = local_path
        self.file_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.full_file_path = self.local_path + self.file_name

    def download_file(self):
        try:
            LOGGER.info(f"Starting download from {self.url} to {self.local_path}")
            os.makedirs(os.path.dirname(self.local_path), exist_ok=True)

            response = requests.get(self.url, stream=True)
            response.raise_for_status()

            with open(self.full_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            LOGGER.info(f"File downloaded successfully to {self.local_path}")

        except requests.exceptions.HTTPError as http_err:
            LOGGER.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"Download failed: {http_err}")

        except requests.exceptions.RequestException as req_err:
            LOGGER.error(f"Error occurred: {req_err}")
            raise AirflowFailException(f"Download failed: {req_err}")

        except Exception as err:
            LOGGER.error(f"An unexpected error occurred: {err}")
            raise AirflowFailException(f"Download failed: {err}")
