"""
Base connection module.
Defines the abstract base class for all data connectors in the pipeline.
"""

import logging
from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for all data source and destination connectors.
    Enforces standard read and write interfaces across the Medallion architecture.
    """

    def __init__(self) -> None:
        """
        Initializes the BaseConnector and sets up the class logger.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """
        Reads data from the source.

        Args:
            **kwargs: Implementation-specific reading parameters (e.g., file_path, query, table_name).

        Returns:
            pd.DataFrame: The ingested data as a pandas DataFrame.
            
        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Writes data to the target destination.

        Args:
            df (pd.DataFrame): The pandas DataFrame to be written.
            **kwargs: Implementation-specific writing parameters (e.g., file_path, table_name, mode).

        Returns:
            None
            
        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass