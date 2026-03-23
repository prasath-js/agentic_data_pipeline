import logging
import os
import pandas as pd
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SourceConnector:
    """
    A class to connect to various data sources and retrieve data.

    Supports CSV, PostgreSQL, Azure Blob Storage, Snowflake, and MySQL.
    """

    def __init__(self, source_config: Dict[str, Any]):
        """
        Initializes the SourceConnector with the given configuration.

        Args:
            source_config (Dict[str, Any]): A dictionary containing source configuration.
                                            Expected keys: 'type', 'path' (for CSV/Azure),
                                            or database connection details.
        """
        self.source_config = source_config
        self.source_type = source_config.get('type')
        logger.info(f"Initializing SourceConnector for source type: {self.source_type}")

    def _read_csv(self, file_path: str) -> pd.DataFrame:
        """
        Reads data from a CSV file.

        Args:
            file_path (str): The path to the CSV file.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            pd.errors.EmptyDataError: If the CSV file is empty.
            Exception: For other errors during CSV reading.
        """
        logger.info(f"Attempting to read CSV from: {file_path}")
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read {len(df)} rows from CSV: {file_path}")
            return df
        except FileNotFoundError as e:
            logger.error(f"CSV file error: {e}")
            raise
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV file is empty: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading CSV from {file_path}: {e}")
            raise

    def _read_postgres(self, db_config: Dict[str, Any], query: str) -> pd.DataFrame:
        """
        Reads data from a PostgreSQL database.

        Args:
            db_config (Dict[str, Any]): Database connection configuration.
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data.

        Raises:
            Exception: For errors during database connection or query execution.
        """
        from sqlalchemy import create_engine
        from sqlalchemy.exc import SQLAlchemyError
        engine = None
        conn = None
        try:
            db_user = os.getenv(db_config.get('user_env', 'PG_USER'))
            db_password = os.getenv(db_config.get('password_env', 'PG_PASSWORD'))
            db_host = os.getenv(db_config.get('host_env', 'PG_HOST'))
            db_port = os.getenv(db_config.get('port_env', 'PG_PORT'), '5432')
            db_name = os.getenv(db_config.get('database_env', 'PG_DATABASE'))

            if not all([db_user, db_password, db_host, db_port, db_name]):
                raise ValueError("Missing one or more PostgreSQL environment variables.")

            conn_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(conn_string)
            conn = engine.connect()
            logger.info(f"Connected to PostgreSQL database: {db_name}@{db_host}")
            df = pd.read_sql(query, conn)
            logger.info(f"Successfully read {len(df)} rows from PostgreSQL.")
            return df
        except (SQLAlchemyError, ValueError) as e:
            logger.error(f"Error reading from PostgreSQL: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("PostgreSQL connection closed.")
            if engine:
                engine.dispose()
                logger.debug("PostgreSQL engine disposed.")

    def _read_mysql(self, db_config: Dict[str, Any], query: str) -> pd.DataFrame:
        """
        Reads data from a MySQL database.

        Args:
            db_config (Dict[str, Any]): Database connection configuration.
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data.

        Raises:
            Exception: For errors during database connection or query execution.
        """
        from sqlalchemy import create_engine
        from sqlalchemy.exc import SQLAlchemyError
        engine = None
        conn = None
        try:
            db_user = os.getenv(db_config.get('user_env', 'MYSQL_USER'))
            db_password = os.getenv(db_config.get('password_env', 'MYSQL_PASSWORD'))
            db_host = os.getenv(db_config.get('host_env', 'MYSQL_HOST'))
            db_port = os.getenv(db_config.get('port_env', 'MYSQL_PORT'), '3306')
            db_name = os.getenv(db_config.get('database_env', 'MYSQL_DATABASE'))

            if not all([db_user, db_password, db_host, db_port, db_name]):
                raise ValueError("Missing one or more MySQL environment variables.")

            conn_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(conn_string)
            conn = engine.connect()
            logger.info(f"Connected to MySQL database: {db_name}@{db_host}")
            df = pd.read_sql(query, conn)
            logger.info(f"Successfully read {len(df)} rows from MySQL.")
            return df
        except (SQLAlchemyError, ValueError) as e:
            logger.error(f"Error reading from MySQL: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("MySQL connection closed.")
            if engine:
                engine.dispose()
                logger.debug("MySQL engine disposed.")

    def _read_azure_blob(self, container_name: str, blob_name: str) -> pd.DataFrame:
        """
        Reads data from an Azure Blob Storage CSV file.

        Args:
            container_name (str): The name of the Azure Blob Storage container.
            blob_name (str): The name of the blob (file) within the container.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data.

        Raises:
            ImportError: If azure-storage-blob is not installed.
            Exception: For other errors during Azure Blob reading.
        """
        try:
            from azure.storage.blob import BlobServiceClient
            from azure.core.exceptions import AzureError
        except ImportError:
            logger.error("azure-storage-blob package not found. Please install it to use Azure Blob storage.")
            raise ImportError("azure-storage-blob package not found.")

        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set.")

        blob_service_client = None
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            logger.info(f"Attempting to download blob '{blob_name}' from container '{container_name}'...")
            download_stream = blob_client.download_blob()
            data = download_stream.readall()

            # Assuming the blob content is CSV
            from io import StringIO
            df = pd.read_csv(StringIO(data.decode('utf-8')))
            logger.info(f"Successfully read {len(df)} rows from Azure Blob: {container_name}/{blob_name}")
            return df
        except (AzureError, ValueError) as e:
            logger.error(f"Error reading from Azure Blob Storage: {e}")
            raise
        finally:
            # BlobServiceClient doesn't have an explicit close method,
            # but connections are managed internally.
            pass

    def _read_snowflake(self, db_config: Dict[str, Any], query: str) -> pd.DataFrame:
        """
        Reads data from a Snowflake database.

        Args:
            db_config (Dict[str, Any]): Database connection configuration.
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the data.

        Raises:
            ImportError: If snowflake-connector-python is not installed.
            Exception: For errors during database connection or query execution.
        """
        try:
            import snowflake.connector
            from snowflake.connector.pandas import read_snowflake
        except ImportError:
            logger.error("snowflake-connector-python package not found. Please install it to use Snowflake.")
            raise ImportError("snowflake-connector-python package not found.")

        conn = None
        try:
            sf_user = os.getenv(db_config.get('user_env', 'SF_USER'))
            sf_password = os.getenv(db_config.get('password_env', 'SF_PASSWORD'))
            sf_account = os.getenv(db_config.get('account_env', 'SF_ACCOUNT'))
            sf_warehouse = os.getenv(db_config.get('warehouse_env', 'SF_WAREHOUSE'))
            sf_database = os.getenv(db_config.get('database_env', 'SF_DATABASE'))
            sf_schema = os.getenv(db_config.get('schema_env', 'SF_SCHEMA'))
            sf_role = os.getenv(db_config.get('role_env', 'SF_ROLE'))

            if not all([sf_user, sf_password, sf_account, sf_warehouse, sf_database, sf_schema]):
                raise ValueError("Missing one or more Snowflake environment variables.")

            conn = snowflake.connector.connect(
                user=sf_user,
                password=sf_password,
                account=sf_account,
                warehouse=sf_warehouse,
                database=sf_database,
                schema=sf_schema,
                role=sf_role,
            )
            logger.info(f"Connected to Snowflake account: {sf_account}, database: {sf_database}, schema: {sf_schema}")
            df = read_snowflake(conn, query)
            logger.info(f"Successfully read {len(df)} rows from Snowflake.")
            return df
        except (snowflake.connector.errors.DatabaseError, ValueError) as e:
            logger.error(f"Error reading from Snowflake: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Snowflake connection closed.")

    def read_data(self) -> Optional[pd.DataFrame]:
        """
        Reads data from the configured source.

        Returns:
            Optional[pd.DataFrame]: A pandas DataFrame containing the data, or None if an error occurs.
        """
        df: Optional[pd.DataFrame] = None
        try:
            if self.source_type == 'csv':
                file_path = self.source_config.get('path')
                if not file_path:
                    raise ValueError("CSV source type requires 'path' in config.")
                df = self._read_csv(file_path)
            elif self.source_type == 'postgres':
                query = self.source_config.get('query')
                if not query:
                    raise ValueError("PostgreSQL source type requires 'query' in config.")
                df = self._read_postgres(self.source_config, query)
            elif self.source_type == 'mysql':
                query = self.source_config.get('query')
                if not query:
                    raise ValueError("MySQL source type requires 'query' in config.")
                df = self._read_mysql(self.source_config, query)
            elif self.source_type == 'azure_blob':
                container_name = self.source_config.get('container_name')
                blob_name = self.source_config.get('blob_name')
                if not container_name or not blob_name:
                    raise ValueError("Azure Blob source type requires 'container_name' and 'blob_name' in config.")
                df = self._read_azure_blob(container_name, blob_name)
            elif self.source_type == 'snowflake':
                query = self.source_config.get('query')
                if not query:
                    raise ValueError("Snowflake source type requires 'query' in config.")
                df = self._read_snowflake(self.source_config, query)
            else:
                logger.error(f"Unsupported source type: {self.source_type}")
                raise ValueError(f"Unsupported source type: {self.source_type}")

            logger.info(f"Data successfully read from {self.source_type}. Rows: {len(df) if df is not None else 0}")
            return df

        except Exception as e:
            logger.error(f"Failed to read data from source '{self.source_config.get('name', 'unknown')}': {e}", exc_info=True)
            return None

def main() -> None:
    """
    Main function to demonstrate the SourceConnector.
    Reads sales data from a CSV file.
    """
    logger.info("Starting source connector demonstration for sales_pipeline.")

    # Example configuration for a CSV source 'sales'
    sales_source_config = {
        'type': 'csv',
        'name': 'sales',
        'path': os.path.join(os.path.dirname(__file__), '../../data/raw/sales_data.csv'), # Relative path for demonstration
        'mode': 'full'
    }

    # Create a dummy CSV file for demonstration if it doesn't exist
    dummy_csv_path = sales_source_config['path']
    os.makedirs(os.path.dirname(dummy_csv_path), exist_ok=True)
    if not os.path.exists(dummy_csv_path):
        logger.info(f"Creating a dummy CSV file at {dummy_csv_path} for demonstration.")
        dummy_data = {
            'order_id': [1, 2, 3, 4, 5],
            'customer_id': [101, 102, 103, 104, 105],
            'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com', 'eve@example.com'],
            'product_id': [1001, 1002, 1001, 1003, 1002],
            'product_name': ['Laptop', 'Mouse', 'Laptop', 'Keyboard', 'Mouse'],
            'quantity': [1, 2, 1, 1, 3],
            'unit_price': [1200.00, 25.00, 1200.00, 75.00, 25.00],
            'total_amount': [1200.00, 50.00, 1200.00, 75.00, 75.00],
            'order_date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03'],
            'region': ['North', 'South', 'East', 'West', 'North'],
            'status': ['Completed', 'Completed', 'Pending', 'Completed', 'Completed']
        }
        dummy_df = pd.DataFrame(dummy_data)
        dummy_df.to_csv(dummy_csv_path, index=False)
        logger.info("Dummy CSV file created.")

    connector = SourceConnector(sales_source_config)
    sales_df = connector.read_data()

    if sales_df is not None:
        logger.info("Sales data successfully loaded:")
        logger.info(sales_df.head())
        logger.info(f"Total rows: {len(sales_df)}")
    else:
        logger.error("Failed to load sales data.")

    logger.info("Source connector demonstration finished.")

if __name__ == "__main__":
    main()
