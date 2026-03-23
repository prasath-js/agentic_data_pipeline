import logging
import os
import pandas as pd
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TargetConnector:
    """
    Manages writing data to the configured output destination.
    Supports writing to local files.
    """

    def __init__(self, output_config: Dict[str, Any]):
        """
        Initializes the TargetConnector with output configuration.

        Args:
            output_config (Dict[str, Any]): A dictionary containing output destination configuration,
                                             e.g., {"type": "local_file", "path": "/path/to/output"}.
        """
        self.output_config = output_config
        self.output_type = output_config.get("type")
        logger.info(f"Initialized TargetConnector with output type: {self.output_type}")

        if self.output_type == "local_file":
            # For local_file, we expect a base directory, the actual file path will be constructed later
            self.base_output_path = output_config.get("path", os.getenv("SALES_PIPELINE_OUTPUT_PATH", "data/gold"))
            if not os.path.exists(self.base_output_path):
                logger.info(f"Creating output directory: {self.base_output_path}")
                os.makedirs(self.base_output_path)
        else:
            raise ValueError(f"Unsupported output type: {self.output_type}")

    def write_data(self, dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Writes the given DataFrame to the configured output destination.

        Args:
            dataframe (pd.DataFrame): The DataFrame to write.
            file_name (str): The name of the file to write (e.g., "aggregated_sales.parquet").
        """
        logger.info(f"Attempting to write data to target: {self.output_type}")

        if self.output_type == "local_file":
            self._write_to_local_file(dataframe, file_name)
        else:
            # This case should ideally be caught in __init__
            raise ValueError(f"Unsupported output type: {self.output_type}")

    def _write_to_local_file(self, dataframe: pd.DataFrame, file_name: str) -> None:
        """
        Writes the DataFrame to a local file.

        Args:
            dataframe (pd.DataFrame): The DataFrame to write.
            file_name (str): The name of the file (e.g., "aggregated_sales.parquet").
        """
        output_file_path = os.path.join(self.base_output_path, file_name)
        file_extension = os.path.splitext(file_name)[1].lower()

        try:
            if file_extension == ".csv":
                dataframe.to_csv(output_file_path, index=False)
                logger.info(f"Successfully wrote {len(dataframe)} rows to local CSV file: {output_file_path}")
            elif file_extension == ".parquet":
                dataframe.to_parquet(output_file_path, index=False)
                logger.info(f"Successfully wrote {len(dataframe)} rows to local Parquet file: {output_file_path}")
            else:
                raise ValueError(f"Unsupported file format for local_file output: {file_extension}")
        except Exception as e:
            logger.error(f"Error writing data to local file {output_file_path}: {e}")
            raise
        finally:
            logger.debug("Local file write operation attempted.")

def main() -> None:
    """
    Main function to demonstrate the TargetConnector.
    This will typically be called from the Gold layer to write final output.
    """
    logger.info("Starting TargetConnector demonstration.")

    # Example configuration for local file output
    output_config = {
        "type": "local_file",
        "path": os.getenv("SALES_PIPELINE_OUTPUT_PATH", "data/gold")
    }

    # Create a dummy DataFrame
    data = {
        'order_date': pd.to_datetime(['2023-01-01', '2023-01-01', '2023-01-02']),
        'region': ['East', 'West', 'East'],
        'total_sales': [100.50, 200.75, 150.20],
        'total_quantity': [2, 3, 1]
    }
    df = pd.DataFrame(data)

    try:
        connector = TargetConnector(output_config)
        # Write to a Parquet file
        connector.write_data(df, "sales_pipeline_gold_output.parquet")
        # Write to a CSV file (demonstration of different format)
        connector.write_data(df, "sales_pipeline_gold_output.csv")
    except Exception as e:
        logger.error(f"TargetConnector demonstration failed: {e}")

    logger.info("TargetConnector demonstration finished.")

if __name__ == "__main__":
    main()
