from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.

    Defines the common interface for reading and writing data
    from various sources and targets.
    """

    @abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """
        Abstract method to read data from a source.

        All concrete connector implementations must provide an
        implementation for this method.

        Args:
            **kwargs: Arbitrary keyword arguments specific to the connector
                      and the read operation (e.g., file path, table name, query).

        Returns:
            pd.DataFrame: A DataFrame containing the read data.
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Abstract method to write data to a target.

        All concrete connector implementations must provide an
        implementation for this method.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            **kwargs: Arbitrary keyword arguments specific to the connector
                      and the write operation (e.g., file path, table name, mode).
        """
        raise NotImplementedError
