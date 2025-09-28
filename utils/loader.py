import logging
import pandas as pd
from io import BytesIO
import constants
from gateway.s3_gateway import S3GateWay
from gateway.postgres_gateway import PostgresGateway
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

class DataLoader(object):
    def __init__(self):
        self.s3_gateway = S3GateWay()
        self.postgres_gateway = PostgresGateway()
    
    def load_data_from_s3_to_postgres(self):
        files = self.s3_gateway.read_files(folder=constants.S3_PROCESSED_FOLDER_NAME)
        
        if not files:
            LOGGER.info("No New files Present")
            return
        
        for file_name, file_content in files.items():
            LOGGER.info(f"Loading data from {file_name} to PostgreSQL table '{constants.POSTGRES_TABLE_NAME}'")
            try:
                df = pd.read_parquet(BytesIO(file_content.get("Body").read()))
            except Exception as e:
                LOGGER.error(f"Error reading file {file_name}: {e}")
                continue
            self.insert_data_into_postgres(df)
    
    
    def insert_data_into_postgres(self, df):
        """Insert data from the DataFrame into PostgreSQL."""
        # Get the column names from the constants file, excluding the primary key as it's auto-generated.
        target_columns = [col['name'] for col in constants.TABLE_SCHEMA if col['name'] != 'trip_id']

        # Create a new DataFrame with only the columns to be inserted
        df_to_insert = df[target_columns]

        # Convert DataFrame rows to tuples
        data = [tuple(row) for row in df_to_insert.values]
        
        # Insert data into the Postgres table
        try:
            # Pass the data and the filtered list of column names
            self.postgres_gateway.insert_data(constants.POSTGRES_TABLE_NAME, data, target_columns)
            LOGGER.info(f"Data loaded successfully into PostgreSQL table {constants.POSTGRES_TABLE_NAME}")
        except Exception as e:
            LOGGER.error(f"Failed to insert data into PostgreSQL: {e}")
            raise