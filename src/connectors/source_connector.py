import logging
import os
import pandas as pd
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SourceConnector:
    """
    A connector to read data from various sources (CSV, PostgreSQL, Azure Blob, Snowflake, MySQL).
    This class handles the logic for connecting to a source and retrieving data based on its type.
    """

    def __init__(self, source_config: Dict[str, Any]):
        """
        Initializes the SourceConnector with the given source configuration.

        Args:
            source_config (Dict[str, Any]): A dictionary containing the configuration for the source.
                                            Expected keys: 'type', and source-specific keys like 'path' for CSV.
        """
        self.source_config = source_config
        self.source_type = source_config.get('type')
        if not self.source_type:
            raise ValueError("Source configuration must specify a 'type'.")

        logger.info(f"Initialized SourceConnector for type: {self.source_type}")

    def _read_csv(self) -> pd.DataFrame:
        """
        Reads data from a CSV file.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the CSV file.
        Raises:
            FileNotFoundError: If the CSV file does not exist.
            Exception: For other errors during CSV reading.
        """
        file_path = self.source_config.get('path')
        if not file_path:
            raise ValueError("CSV source configuration must specify a 'path'.")
        
        logger.info(f"Attempting to read CSV from: {file_path}")
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read {len(df)} rows from CSV: {file_path}")
            return df
        except FileNotFoundError as e:
            logger.error(f"CSV file not found at {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            raise

    def read_data(self) -> pd.DataFrame:
        """
        Reads data from the configured source based on its type.

        Returns:
            pd.DataFrame: A DataFrame containing the data read from the source.
        Raises:
            ValueError: If the source type is unsupported or configuration is incomplete.
        """
        logger.info(f"Reading data from source type: {self.source_type}")
        if self.source_type == 'csv':
            return self._read_csv()
        elif self.source_type == 'postgres':
            # This pipeline definition implies only CSV for 'sales' source.
            # However, for a production-ready source_connector, other types are often included.
            # Placeholder for future expansion:
            # from sqlalchemy import create_engine
            # db_url = os.getenv('POSTGRES_CONNECTION_STRING')
            # if not db_url:
            #     raise ValueError("POSTGRES_CONNECTION_STRING environment variable not set.")
            # engine = create_engine(db_url)
            # table_name = self.source_config.get('table')
            # if not table_name:
            #     raise ValueError("Postgres source configuration must specify a 'table'.")
            # try:
            #     with engine.connect() as connection:
            #         df = pd.read_sql_table(table_name, connection)
            #     logger.info(f"Successfully read {len(df)} rows from PostgreSQL table: {table_name}")
            #     return df
            # except Exception as e:
            #     logger.error(f"Error reading from PostgreSQL table {table_name}: {e}")
            #     raise
            raise NotImplementedError(f"Source type '{self.source_type}' not implemented for this pipeline configuration.")
        elif self.source_type == 'azure_blob':
            raise NotImplementedError(f"Source type '{self.source_type}' not implemented for this pipeline configuration.")
        elif self.source_type == 'snowflake':
            raise NotImplementedError(f"Source type '{self.source_type}' not implemented for this pipeline configuration.")
        elif self.source_type == 'mysql':
            raise NotImplementedError(f"Source type '{self.source_type}' not implemented for this pipeline configuration.")
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

def main() -> None:
    """
    Main function to demonstrate the SourceConnector.
    It reads 'sales' data from a CSV file.
    """
    logger.info("Starting source connector demonstration.")

    # Example configuration for the 'sales' source
    # For a real pipeline, this would come from a config file or orchestrator
    sales_source_config = {
        'name': 'sales',
        'type': 'csv',
        # Assuming the CSV file is in a 'data' directory relative to where the script is run
        # In a production environment, this path would be managed by the deployment
        'path': os.path.join(os.path.dirname(__file__), '../../data/sales/sales.csv'),
        'mode': 'full'
    }

    # Create dummy data for demonstration if the file does not exist
    dummy_sales_data_path = sales_source_config['path']
    if not os.path.exists(os.path.dirname(dummy_sales_data_path)):
        os.makedirs(os.path.dirname(dummy_sales_data_path))
    if not os.path.exists(dummy_sales_data_path):
        logger.info(f"Creating dummy sales CSV data at {dummy_sales_data_path} for demonstration.")
        dummy_data = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5],
            'customer_id': [101, 102, 103, 101, 104],
            'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Alice Smith', 'Diana Prince'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'alice@example.com', 'diana@example.com'],
            'product_id': [1001, 1002, 1003, 1001, 1004],
            'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Laptop', 'Monitor'],
            'quantity': [1, 2, 1, 1, 1],
            'unit_price': [1200.00, 25.00, 75.00, 1200.00, 300.00],
            'total_amount': [1200.00, 50.00, 75.00, 1200.00, 300.00],
            'order_date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03'],
            'region': ['East', 'West', 'North', 'East', 'South'],
            'status': ['Completed', 'Pending', 'Completed', 'Completed', 'Pending']
        })
        dummy_data.to_csv(dummy_sales_data_path, index=False)
        logger.info("Dummy data created successfully.")

    try:
        sales_connector = SourceConnector(sales_source_config)
        sales_df = sales_connector.read_data()
        logger.info("Successfully read sales data. First 5 rows:")
        logger.info(f"\n{sales_df.head().to_string()}")
        logger.info("Sales data schema:")
        sales_df.info()

    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
    except FileNotFoundError as e:
        logger.error(f"File Error: {e}")
    except NotImplementedError as e:
        logger.error(f"Feature Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    logger.info("Source connector demonstration finished.")

if __name__ == "__main__":
    main()
