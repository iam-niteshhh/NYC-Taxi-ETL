import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import constants
from time import sleep

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

class PostgresGateway(object):
    def __init__(self, conn_id='postgres_default', retries=3, delay=5):
        """Initialize PostgresGateway with connection ID."""
        self.conn_id = conn_id
        self.retries = retries
        self.delay = delay
        
        try:
            self.hook = PostgresHook(postgres_conn_id=self.conn_id)
            self.validate_connection()
        except Exception as e:
            LOGGER.error(f"Failed to create Postgres hook with connection id '{self.conn_id}'")
            raise AirflowException(f'Postgres connection failed: {e}')
    
    def validate_connection(self):
        """Validate Postgres connection with retry logic."""
        for attempt in range(self.retries):
            try:
                self.hook.get_conn()
                LOGGER.info("Successfully connected to Postgres.")
                return
            except Exception as e:
                if attempt < self.retries - 1:
                    LOGGER.warning(f"Attempt {attempt + 1} failed. Retrying in {self.delay} seconds...")
                    sleep(self.delay)
                else:
                    LOGGER.error(f"Failed to connect to Postgres after {self.retries} attempts.")
                    raise AirflowException(f"Postgres connection validation failed: {e}")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        sql_check = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = '{table_name}'
            );
        """
        try:
            result = self.hook.get_first(sql_check)
            return result[0]
        except Exception as e:
            LOGGER.error(f"Error checking for table existence: {e}")
            return False

    def get_table_schema(self, table_name: str) -> list or None:
        """Get a table's schema as a list of dictionaries if it exists."""
        if not self.table_exists(table_name):
            LOGGER.info(f"Table '{table_name}' does not exist.")
            return None

        sql_query = f"""
            SELECT
                column_name,
                data_type
            FROM
                information_schema.columns
            WHERE
                table_schema = 'public' AND table_name = '{table_name}'
            ORDER BY
                ordinal_position;
        """
        try:
            results = self.hook.get_records(sql_query)
            schema = [{'name': row[0], 'type': row[1]} for row in results]
            LOGGER.info(f"Retrieved schema for table '{table_name}': {schema}")
            return schema
        except Exception as e:
            LOGGER.error(f"Failed to retrieve schema for table '{table_name}': {e}")
            return None

    def get_or_create_table_schema(self, table_name: str, default_schema: list) -> list:
        """
        Check if table exists. If so, return its schema.
        If not, create the table with the provided default schema and return it.
        """
        if self.table_exists(table_name):
            return self.get_table_schema(table_name)
        else:
            self.create_table(table_name, default_schema)
            # Return the schema of the newly created table
            return self.get_table_schema(table_name)
    
    def create_table(self, table_name: str, schema: list):
        """Create a table in Postgres using the provided schema."""
        schema_sql = ", ".join([f"{col['name']} {col['type']}" + (' ' + col.get('constraint', '') if col.get('constraint') else '') for col in schema])
        
        sql_statement = f"""
            CREATE TABLE {table_name} (
                {schema_sql}
            );
        """
        LOGGER.info(f"Attempting to create Postgres table: '{table_name}'")
        
        try:
            LOGGER.info(f"SQL Statement: {sql_statement}")
            self.hook.run(sql_statement)
            LOGGER.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            LOGGER.error(f"Error creating table '{table_name}': {e}")
            raise AirflowException(f"Postgres table creation failed: {e}")

    def insert_data(self, table_name: str, data: list, target_fields: list):
        """Insert data into a Postgres table."""
        if not data:
            LOGGER.warning("No data to insert.")
            return

        try:
            LOGGER.info(f"Inserting data into table {table_name}: {data[:5]}...")  # Preview of first 5 rows
            # Disable upsert and just insert rows
            self.hook.insert_rows(
                table=table_name,  # Pass the table name here
                rows=data,  # The list of tuples to be inserted
                target_fields=target_fields,  # Columns where data will be inserted
                replace=False  # Set to False to avoid the conflict resolution
            )
            LOGGER.info(f"Data inserted into table {table_name} successfully.")
        except Exception as e:
            LOGGER.error(f"Error inserting data into table '{table_name}': {e}")
            raise AirflowException(f"Postgres data insertion failed: {e}")


