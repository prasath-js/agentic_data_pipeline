import logging
import os
import pandas as pd
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TargetConnector:
    """
    Handles writing final processed data (Gold layer) to the configured output destination.
    This connector is specifically designed for writing to local files.
    """

    def __init__(self, output_base_path: str) -> None:
        """
        Initializes the TargetConnector with the base path for output files.

        Args:
            output_base_path (str): The base directory where output files will be saved.
        """
        if not output_base_path:
            raise ValueError("Output base path cannot be empty.")
        self.output_base_path = output_base_path
        logger.info(f"TargetConnector initialized with output base path: {self.output_base_path}")
        self._ensure_output_directory_exists()

    def _ensure_output_directory_exists(self) -> None:
        """
        Ensures that the output directory exists, creating it if necessary.
        """
        try:
            os.makedirs(self.output_base_path, exist_ok=True)
            logger.info(f"Ensured output directory exists: {self.output_base_path}")
        except OSError as e:
            logger.error(f"Error creating output directory {self.output_base_path}: {e}")
            raise

    def write_dataframe_to_local_parquet(self, dataframe: pd.DataFrame, file_name: str, **kwargs: Any) -> None:
        """
        Writes a pandas DataFrame to a local Parquet file.

        Args:
            dataframe (pd.DataFrame): The DataFrame to write.
            file_name (str): The name of the file (e.g., "sales_daily_summary.parquet").
            **kwargs (Any): Additional keyword arguments to pass to pandas.DataFrame.to_parquet.
        
        Raises:
            IOError: If there's an issue writing the file.
        """
        if dataframe.empty:
            logger.warning(f"Attempted to write an empty DataFrame to {file_name}. No file will be created.")
            return

        file_path = os.path.join(self.output_base_path, file_name)
        try:
            logger.info(f"Attempting to write DataFrame to local Parquet file: {file_path}")
            dataframe.to_parquet(file_path, index=False, **kwargs)
            logger.info(f"Successfully wrote DataFrame to {file_path}.")
            logger.info(f"Rows written: {len(dataframe)}")
        except IOError as e:
            logger.error(f"Failed to write DataFrame to {file_path}: {e}")
            raise IOError(f"Failed to write DataFrame to {file_path}") from e

def main() -> None:
    """
    Main function to demonstrate the TargetConnector's capabilities.
    In a real pipeline, this would receive the Gold layer DataFrame.
    """
    logger.info("Starting TargetConnector demonstration.")

    # Configuration for the output path
    # Example: SALES_PIPELINE_GOLD_OUTPUT_PATH=/app/data/gold
    output_base_path = os.getenv("SALES_PIPELINE_GOLD_OUTPUT_PATH", "data/gold")

    if not output_base_path:
        logger.error("SALES_PIPELINE_GOLD_OUTPUT_PATH environment variable not set. Exiting.")
        return

    try:
        # Initialize the connector
        connector = TargetConnector(output_base_path=output_base_path)

        # Simulate a Gold layer DataFrame
        gold_data = {
            'order_date': pd.to_datetime(['2023-01-01', '2023-01-01', '2023-01-02']),
            'region': ['North', 'South', 'North'],
            'total_sales': [1500.50, 2300.75, 1200.00],
            'total_quantity': [10, 15, 8]
        }
        gold_df = pd.DataFrame(gold_data)
        logger.info(f"Simulated Gold DataFrame created with {len(gold_df)} rows.")

        # Define the output file name
        output_file_name = "sales_daily_summary_20230101.parquet"

        # Write the DataFrame
        connector.write_dataframe_to_local_parquet(gold_df, output_file_name)

        # Demonstrate writing an empty DataFrame (should log a warning and not create a file)
        empty_df = pd.DataFrame()
        connector.write_dataframe_to_local_parquet(empty_df, "empty_data_test.parquet")

    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
    except IOError as ioe:
        logger.error(f"File operation error: {ioe}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during demonstration: {e}", exc_info=True)

    logger.info("TargetConnector demonstration finished.")

if __name__ == "__main__":
    main()
