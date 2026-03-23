import os
import logging
from pathlib import Path
import pandas as pd

from src.db_connection.base import BaseConnector

logger = logging.getLogger(__name__)

class LocalFilesConnector(BaseConnector):
    """
    Connector for managing local file operations.
    Supports reading from and writing to local file formats (e.g., CSV, Parquet).
    """

    def __init__(self) -> None:
        """
        Initialize the LocalFilesConnector.
        """
        super().__init__()
        self.is_connected = False

    def connect(self) -> None:
        """
        Establish a 'connection' to the local file system.
        For local files, this verifies basic filesystem accessibility and updates state.
        """
        logger.info("Initializing local file system connector.")
        self.is_connected = True

    def disconnect(self) -> None:
        """
        Close the 'connection' to the local file system.
        """
        logger.info("Closing local file system connector.")
        self.is_connected = False

    def read(self, file_path: str, file_format: str = "csv") -> pd.DataFrame:
        """
        Read data from a local file into a Pandas DataFrame.
        
        Args:
            file_path (str): The path to the file to be read.
            file_format (str): The format of the file ('csv', 'json', 'parquet').
            
        Returns:
            pd.DataFrame: The loaded data.
            
        Raises:
            FileNotFoundError: If the specified file does not exist.
            ValueError: If the file format is not supported.
        """
        if not os.path.exists(file_path):
            logger.error("File not found: %s", file_path)
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info("Reading %s file from %s", file_format, file_path)
        
        file_format = file_format.lower()
        try:
            if file_format == "csv":
                return pd.read_csv(file_path)
            elif file_format == "json":
                return pd.read_json(file_path)
            elif file_format == "parquet":
                return pd.read_parquet(file_path)
            else:
                logger.error("Unsupported file format for reading: %s", file_format)
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            logger.error("Failed to read file %s: %s", file_path, str(e))
            raise

    def write(self, df: pd.DataFrame, file_path: str, file_format: str = "csv") -> None:
        """
        Write a Pandas DataFrame to a local file.
        
        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_path (str): The destination file path.
            file_format (str): The format of the file ('csv', 'json', 'parquet').
            
        Raises:
            ValueError: If the file format is not supported.
            Exception: If writing to the file fails.
        """
        path_obj = Path(file_path)
        
        # Ensure target directory exists
        try:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error("Failed to create parent directories for %s: %s", file_path, str(e))
            raise

        logger.info("Writing DataFrame to %s file at %s", file_format, file_path)
        
        file_format = file_format.lower()
        try:
            if file_format == "csv":
                df.to_csv(file_path, index=False)
            elif file_format == "json":
                df.to_json(file_path, orient="records")
            elif file_format == "parquet":
                df.to_parquet(file_path, index=False)
            else:
                logger.error("Unsupported file format for writing: %s", file_format)
                raise ValueError(f"Unsupported file format: {file_format}")
            
            logger.info("Successfully wrote data to %s", file_path)
        except Exception as e:
            logger.error("Failed to write to file %s: %s", file_path, str(e))
            raise