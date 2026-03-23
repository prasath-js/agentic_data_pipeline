import os
import logging
import pandas as pd
from typing import Optional, Dict, Any

from ..base import BaseConnector

# Configure logging for this module
logger = logging.getLogger(__name__)

class LocalFilesConnector(BaseConnector):
    """
    A connector for interacting with local files (CSV, Parquet, JSON).

    This class provides methods to read from and write to various local file formats,
    extending the BaseConnector for a unified interface.
    """

    def __init__(self) -> None:
        """
        Initializes the LocalFilesConnector.
        No specific connection parameters are needed for local file operations,
        but BaseConnector requires an __init__.
        """
        super().__init__()
        logger.info("LocalFilesConnector initialized.")

    def _get_read_function(self, file_format: str):
        """Internal helper to map file formats to pandas read functions."""
        read_functions = {
            "csv": pd.read_csv,
            "parquet": pd.read_parquet,
            "json": pd.read_json,
        }
        if file_format not in read_functions:
            raise ValueError(f"Unsupported read file format: {file_format}. Supported formats are {list(read_functions.keys())}")
        return read_functions[file_format]

    def _get_write_function(self, file_format: str):
        """Internal helper to map file formats to pandas write functions."""
        write_functions = {
            "csv": lambda df, path, **kwargs: df.to_csv(path, index=False, **kwargs),
            "parquet": lambda df, path, **kwargs: df.to_parquet(path, index=False, **kwargs),
            "json": lambda df, path, **kwargs: df.to_json(path, orient="records", lines=True, **kwargs),
        }
        if file_format not in write_functions:
            raise ValueError(f"Unsupported write file format: {file_format}. Supported formats are {list(write_functions.keys())}")
        return write_functions[file_format]

    def read(self, file_path: str, file_format: str, **kwargs: Any) -> pd.DataFrame:
        """
        Reads data from a local file into a Pandas DataFrame.

        Args:
            file_path (str): The full path to the local file.
            file_format (str): The format of the file ('csv', 'parquet', 'json').
            **kwargs (Any): Additional keyword arguments to pass to the pandas read function.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            ValueError: If an unsupported file format is provided or other read errors occur.
        """
        logger.info(f"Attempting to read data from local file: {file_path} (format: {file_format})")
        try:
            read_func = self._get_read_function(file_format.lower())
            df = read_func(file_path, **kwargs)
            logger.info(f"Successfully read {len(df)} rows from {file_path}.")
            return df
        except FileNotFoundError as e:
            logger.error(f"File not found at {file_path}: {e}")
            raise
        except ValueError as e:
            logger.error(f"Error with file format or arguments for {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading from {file_path}: {e}")
            raise

    def write(self, df: pd.DataFrame, file_path: str, file_format: str, **kwargs: Any) -> None:
        """
        Writes a Pandas DataFrame to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): The full path where the file should be written.
            file_format (str): The desired format for the file ('csv', 'parquet', 'json').
            **kwargs (Any): Additional keyword arguments to pass to the pandas write function.

        Raises:
            IOError: If there's an issue writing the file.
            ValueError: If an unsupported file format is provided or other write errors occur.
        """
        logger.info(f"Attempting to write {len(df)} rows to local file: {file_path} (format: {file_format})")
        try:
            # Ensure the directory exists
            output_dir = os.path.dirname(file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.debug(f"Created directory: {output_dir}")

            write_func = self._get_write_function(file_format.lower())
            write_func(df, file_path, **kwargs)
            logger.info(f"Successfully wrote data to {file_path}.")
        except ValueError as e:
            logger.error(f"Error with file format or arguments for {file_path}: {e}")
            raise
        except IOError as e:
            logger.error(f"IO Error writing to {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while writing to {file_path}: {e}")
            raise

    def connect(self) -> Optional[Any]:
        """
        Establishes a connection (if applicable) for the connector.
        For local files, this operation is generally a no-op as files are opened/closed per access.
        """
        logger.debug("LocalFilesConnector: No explicit connection established for local file operations.")
        return None

    def disconnect(self) -> None:
        """
        Closes any open connection (if applicable) for the connector.
        For local files, this operation is generally a no-op.
        """
        logger.debug("LocalFilesConnector: No explicit disconnection needed for local file operations.")