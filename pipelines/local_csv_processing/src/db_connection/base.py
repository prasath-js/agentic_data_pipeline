from abc import ABC, abstractmethod
import pandas as pd
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """
    Abstract Base Class for database and data source connectors.

    This class defines the interface for data connectors used throughout
    the ETL pipeline, ensuring a consistent contract for reading and writing data.
    """

    def __init__(self) -> None:
        """
        Initializes the BaseConnector.
        Specific connection parameters should be handled by concrete implementations.
        """
        logger.debug("BaseConnector initialized.")

    @abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """
        Abstract method to read data from a source into a Pandas DataFrame.

        Concrete implementations must provide logic to connect to a specific
        data source and retrieve data.

        Args:
            **kwargs: Arbitrary keyword arguments specific to the connector's read operation.
                      Examples might include file paths, table names, queries, etc.

        Returns:
            pd.DataFrame: A DataFrame containing the read data.
        """
        raise NotImplementedError("Subclasses must implement the 'read' method.")

    @abstractmethod
    def write(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Abstract method to write a Pandas DataFrame to a destination.

        Concrete implementations must provide logic to connect to a specific
        data destination and store the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            **kwargs: Arbitrary keyword arguments specific to the connector's write operation.
                      Examples might include file paths, table names, write modes, etc.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses must implement the 'write' method.")