import os
import logging
import pandas as pd
from typing import Any, Optional
from src.db_connection.base import BaseConnector

logger = logging.getLogger(__name__)

class LocalFilesConnector(BaseConnector):
    """
    Connector implementation for local files.
    Supports reading and writing CSV, Parquet, and JSON formats.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize the LocalFilesConnector.
        """
        super().__init__(**kwargs)
        self.is_connected = False
        # Fetch an optional base directory from environment variables, defaults to root
        self.base_dir = os.getenv("LOCAL_DATA_DIR", "")

    def connect(self) -> None:
        """
        Establish connection or verify file system access.
        For local files, this sets the connection state to True.
        """
        logger.info("Initializing local file system connector.")
        self.is_connected = True

    def disconnect(self) -> None:
        """
        Close any resources. For local files, this is a no-op.
        """
        logger.info("Closing local file system connector.")
        self.is_connected = False

    def _resolve_path(self, file_path: str) -> str:
        """
        Resolve the absolute path using the base directory if provided.
        
        Args:
            file_path (str): The relative or absolute file path.
            
        Returns:
            str: The resolved file path.
        """
        if self.base_dir and not os.path.isabs(file_path):
            return os.path.join(self.base_dir, file_path)
        return file_path

    def read(self, file_path: str, file_format: str = "csv", **kwargs: Any) -> pd.DataFrame:
        """
        Read data from a local file into a Pandas DataFrame.

        Args:
            file_path (str): The path to the file.
            file_format (str): The format of the file ('csv', 'parquet', 'json').
            **kwargs: Additional arguments to pass to pandas read functions.

        Returns:
            pd.DataFrame: The loaded data.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file format is unsupported.
        """
        full_path = self._resolve_path(file_path)
        
        if not os.path.exists(full_path):
            logger.error("File not found: %s", full_path)
            raise FileNotFoundError(f"File not found: {full_path}")

        logger.info("Reading %s file from %s", file_format, full_path)
        
        try:
            if file_format.lower() == "csv":
                df = pd.read_csv(full_path, **kwargs)
            elif file_format.lower() == "parquet":
                df = pd.read_parquet(full_path, **kwargs)
            elif file_format.lower() == "json":
                df = pd.read_json(full_path, **kwargs)
            else:
                logger.error("Unsupported file format for reading: %s", file_format)
                raise ValueError(f"Unsupported file format: {file_format}")
            
            logger.info("Successfully read %d rows from %s", len(df), full_path)
            return df
            
        except Exception as e:
            logger.error("Failed to read file %s: %s", full_path, str(e))
            raise

    def write(self, df: pd.DataFrame, file_path: str, file_format: str = "csv", **kwargs: Any) -> None:
        """
        Write a Pandas DataFrame to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): The destination file path.
            file_format (str): The format to save as ('csv', 'parquet', 'json').
            **kwargs: Additional arguments to pass to pandas write functions.

        Raises:
            ValueError: If the file format is unsupported.
        """
        full_path = self._resolve_path(file_path)
        logger.info("Writing dataframe of %d rows to %s as %s", len(df), full_path, file_format)
        
        # Ensure target directory exists
        directory = os.path.dirname(full_path)
        if directory and not os.path.exists(directory):
            logger.info("Creating directory structure for %s", directory)
            os.makedirs(directory, exist_ok=True)

        try:
            if file_format.lower() == "csv":
                df.to_csv(full_path, index=False, **kwargs)
            elif file_format.lower() == "parquet":
                df.to_parquet(full_path, index=False, **kwargs)
            elif file_format.lower() == "json":
                df.to_json(full_path, orient="records", **kwargs)
            else:
                logger.error("Unsupported file format for writing: %s", file_format)
                raise ValueError(f"Unsupported file format: {file_format}")
            
            logger.info("Successfully wrote file to %s", full_path)
            
        except Exception as e:
            logger.error("Failed to write file %s: %s", full_path, str(e))
            raise