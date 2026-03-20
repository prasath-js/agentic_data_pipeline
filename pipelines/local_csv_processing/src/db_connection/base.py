import abc
import pandas as pd
from typing import Any

class BaseConnector(abc.ABC):
    """
    Abstract base class for all data connectors.

    This class defines the interface that all specific data connectors
    (e.g., CSV, SQL, API) must implement for reading and writing data.
    """

    @abc.abstractmethod
    def read(self, **kwargs: Any) -> pd.DataFrame:
        """
        Abstract method to read data from a source into a pandas DataFrame.

        Implementations should handle connection details, query execution,
        and data retrieval specific to their source type.

        Args:
            **kwargs: Arbitrary keyword arguments specific to the connector
                      and read operation (e.g., file_path, table_name, query).

        Returns:
            pd.DataFrame: A DataFrame containing the read data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def write(self, df: pd.DataFrame, **kwargs: Any) -> None:
        """
        Abstract method to write a pandas DataFrame to a target destination.

        Implementations should handle connection details, data serialization,
        and writing specific to their target type.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            **kwargs: Arbitrary keyword arguments specific to the connector
                      and write operation (e.g., file_path, table_name, if_exists).
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        Optional method to close any open connections or release resources.

        This method can be overridden by concrete connectors if they manage
        persistent connections that need explicit closing.
        """
        pass