import abc
import logging
import pandas as pd


class BaseConnector(abc.ABC):
    """
    Abstract base class for all data connectors.
    
    Enforces a standard interface for reading and writing data across 
    the bronze, silver, and gold layers of the pipeline.
    """

    def __init__(self) -> None:
        """
        Initializes the base connector and sets up the class-specific logger.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """
        Reads data from the source and returns it as a pandas DataFrame.

        Args:
            **kwargs: Connector-specific read arguments (e.g., file_path, query, table_name).

        Returns:
            pd.DataFrame: The extracted raw data.
        """
        pass

    @abc.abstractmethod
    def write(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Writes a pandas DataFrame to the target destination.

        Args:
            df (pd.DataFrame): The data to be written.
            **kwargs: Connector-specific write arguments (e.g., file_path, table_name, mode).

        Returns:
            None
        """
        pass