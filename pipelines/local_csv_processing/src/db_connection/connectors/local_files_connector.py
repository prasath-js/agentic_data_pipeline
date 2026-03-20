import pandas as pd
import os
import logging
from typing import Union, Literal
from src.db_connection.base import BaseConnector

# Configure logger for this module
logger = logging.getLogger(__name__)

class LocalFilesConnector(BaseConnector):
    """
    A connector for reading from and writing to local file systems.
    Supports CSV, Parquet, and JSON file formats.
    """

    def read(self, file_path: str, file_format: Literal["csv", "parquet", "json"]) -> pd.DataFrame:
        """
        Reads data from a local file into a Pandas DataFrame.

        Args:
            file_path (str): The path to the local file.
            file_format (Literal["csv", "parquet", "json"]): The format of the file.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the data from the file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            ValueError: If an unsupported file format is provided.
            Exception: For other errors during file reading.
        """
        logger.info(f"Attempting to read data from {file_path} with format {file_format}.")
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found at path: {file_path}")

            if file_format == "csv":
                df = pd.read_csv(file_path)
            elif file_format == "parquet":
                df = pd.read_parquet(file_path)
            elif file_format == "json":
                df = pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'csv', 'parquet', 'json'.")

            logger.info(f"Successfully read {len(df)} rows from {file_path}.")
            return df
        except FileNotFoundError as e:
            logger.error(f"File not found error when reading from {file_path}: {e}")
            raise
        except ValueError as e:
            logger.error(f"Configuration error when reading from {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading from {file_path}: {e}")
            raise

    def write(self, df: pd.DataFrame, file_path: str, file_format: Literal["csv", "parquet", "json"]) -> None:
        """
        Writes a Pandas DataFrame to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): The path where the file will be written.
            file_format (Literal["csv", "parquet", "json"]): The format to write the file in.

        Raises:
            ValueError: If an unsupported file format is provided.
            Exception: For other errors during file writing.
        """
        logger.info(f"Attempting to write {len(df)} rows to {file_path} with format {file_format}.")
        try:
            # Ensure the directory exists
            output_dir = os.path.dirname(file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.debug(f"Created directory: {output_dir}")

            if file_format == "csv":
                df.to_csv(file_path, index=False)
            elif file_format == "parquet":
                df.to_parquet(file_path, index=False)
            elif file_format == "json":
                df.to_json(file_path, orient="records", indent=4)
            else:
                raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'csv', 'parquet', 'json'.")

            logger.info(f"Successfully wrote data to {file_path}.")
        except ValueError as e:
            logger.error(f"Configuration error when writing to {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while writing to {file_path}: {e}")
            raise