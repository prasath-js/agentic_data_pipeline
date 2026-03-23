import pandas as pd
import logging
from typing import Optional, Any
from pathlib import Path

# Assuming BaseConnector exists in ../base.py and provides a common interface
from src.db_connection.base import BaseConnector

logger = logging.getLogger(__name__)

class LocalFilesConnector(BaseConnector):
    """
    A connector for reading from and writing to local files (CSV, Parquet, JSON).
    """

    def connect(self) -> Any:
        """
        Establishes a connection to the local file system.
        For local files, this method primarily ensures that the necessary
        libraries are available and logging is configured.
        """
        logger.info("LocalFilesConnector: Initializing connection to local file system.")
        # No explicit connection object for local files, but we can return self
        # or a dummy object if a non-None return is expected by BaseConnector.
        return self

    def disconnect(self, connection: Any) -> None:
        """
        Closes the connection to the local file system.
        For local files, this is generally a no-op as there's no open connection
        resource to close.
        """
        logger.info("LocalFilesConnector: Disconnecting from local file system (no-op).")
        pass

    def read(self, file_path: str, file_format: str, **kwargs: Any) -> pd.DataFrame:
        """
        Reads data from a local file into a Pandas DataFrame.

        Args:
            file_path (str): The path to the local file.
            file_format (str): The format of the file ('csv', 'parquet', 'json').
            **kwargs: Additional keyword arguments to pass to the pandas read function.

        Returns:
            pd.DataFrame: The data read from the file.

        Raises:
            ValueError: If the specified file_format is not supported.
            FileNotFoundError: If the file does not exist.
            Exception: For other read-related errors.
        """
        logger.info(f"LocalFilesConnector: Attempting to read from {file_path} (format: {file_format}).")
        try:
            if file_format.lower() == 'csv':
                df = pd.read_csv(file_path, **kwargs)
            elif file_format.lower() == 'parquet':
                df = pd.read_parquet(file_path, **kwargs)
            elif file_format.lower() == 'json':
                df = pd.read_json(file_path, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'csv', 'parquet', 'json'.")
            
            logger.info(f"LocalFilesConnector: Successfully read {len(df)} rows from {file_path}.")
            return df
        except FileNotFoundError:
            logger.error(f"LocalFilesConnector: File not found at {file_path}.")
            raise
        except ValueError as ve:
            logger.error(f"LocalFilesConnector: Error reading file due to unsupported format or invalid arguments: {ve}")
            raise
        except Exception as e:
            logger.error(f"LocalFilesConnector: An unexpected error occurred while reading from {file_path}: {e}")
            raise

    def write(self, df: pd.DataFrame, file_path: str, file_format: str, index: bool = False, **kwargs: Any) -> None:
        """
        Writes a Pandas DataFrame to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): The path where the file will be saved.
            file_format (str): The format of the file ('csv', 'parquet', 'json').
            index (bool): Whether to write the DataFrame index (default: False for CSV).
            **kwargs: Additional keyword arguments to pass to the pandas write function.

        Returns:
            None

        Raises:
            ValueError: If the specified file_format is not supported.
            Exception: For other write-related errors.
        """
        logger.info(f"LocalFilesConnector: Attempting to write {len(df)} rows to {file_path} (format: {file_format}).")
        try:
            output_dir = Path(file_path).parent
            output_dir.mkdir(parents=True, exist_ok=True) # Ensure directory exists

            if file_format.lower() == 'csv':
                df.to_csv(file_path, index=index, **kwargs)
            elif file_format.lower() == 'parquet':
                df.to_parquet(file_path, index=index, **kwargs)
            elif file_format.lower() == 'json':
                df.to_json(file_path, orient='records', lines=True, **kwargs) # Assuming common line-delimited JSON
            else:
                raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'csv', 'parquet', 'json'.")
            
            logger.info(f"LocalFilesConnector: Successfully wrote data to {file_path}.")
        except ValueError as ve:
            logger.error(f"LocalFilesConnector: Error writing file due to unsupported format or invalid arguments: {ve}")
            raise
        except Exception as e:
            logger.error(f"LocalFilesConnector: An unexpected error occurred while writing to {file_path}: {e}")
            raise

    def execute(self, query: str, **kwargs: Any) -> Optional[pd.DataFrame]:
        """
        Executes a 'query' (not applicable for local files in the traditional sense).
        This method is required by BaseConnector but is not directly used for
        standard local file operations.
        """
        logger.warning("LocalFilesConnector: execute method called, but queries are not applicable for direct local file operations.")
        return None
