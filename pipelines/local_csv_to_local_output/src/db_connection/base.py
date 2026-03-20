# src/db_connection/base.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Any, Dict, Optional

class BaseConnector(ABC):
    """
    Abstract Base Class for all data source/sink connectors.
    Defines the interface for connecting, reading, and writing data.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initializes the connector with configuration.
        Args:
            config (Optional[Dict[str, Any]]): Configuration dictionary for the connector.
        """
        self.config = config if config is not None else {}

    @abstractmethod
    def connect(self) -> Any:
        """
        Establishes a connection to the data source/sink.
        Returns:
            Any: The connection object or client instance.
        """
        pass

    @abstractmethod
    def read(self, **kwargs: Any) -> pd.DataFrame:
        """
        Reads data from the connected source.
        Args:
            **kwargs (Any): Additional keyword arguments for the read operation
                            (e.g., query, file_path, table_name).
        Returns:
            pd.DataFrame: A pandas DataFrame containing the read data.
        """
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, **kwargs: Any) -> None:
        """
        Writes data to the connected sink.
        Args:
            df (pd.DataFrame): The DataFrame to write.
            **kwargs (Any): Additional keyword arguments for the write operation
                            (e.g., table_name, file_path, if_exists).
        """
        pass

    def close(self) -> None:
        """
        Closes the connection, if applicable.
        This method is optional to implement for connectors that don't maintain
        persistent connections (e.g., local file connectors).
        """
        pass