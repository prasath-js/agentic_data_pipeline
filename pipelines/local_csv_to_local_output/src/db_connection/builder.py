# src/db_connection/builder.py
import logging
from typing import Type, Dict, Any, Optional
from src.db_connection.base import BaseConnector

# Import specific connectors here
from src.db_connection.connectors.local_files_connector import LocalFilesConnector

logger = logging.getLogger(__name__)

class ConnectionBuilder:
    """
    A factory class for creating database and file system connectors.
    It provides a centralized way to instantiate the correct connector
    based on the specified type.
    """

    # A registry of available connectors
    _connectors: Dict[str, Type[BaseConnector]] = {
        "local_files": LocalFilesConnector,
        # Add other connectors here as they are implemented
        # "postgresql": PostgreSQLConnector,
        # "s3": S3Connector,
    }

    @classmethod
    def register_connector(cls, connector_type: str, connector_class: Type[BaseConnector]) -> None:
        """
        Registers a new connector type with the builder.

        Args:
            connector_type (str): A string identifier for the connector (e.g., "postgresql").
            connector_class (Type[BaseConnector]): The class of the connector to register.
        """
        if not issubclass(connector_class, BaseConnector):
            raise TypeError("Connector class must inherit from BaseConnector.")
        cls._connectors[connector_type] = connector_class
        logger.info(f"Connector type '{connector_type}' registered.")

    @classmethod
    def get_connector(cls, connector_type: str, config: Optional[Dict[str, Any]] = None) -> BaseConnector:
        """
        Retrieves an instance of the specified connector type.

        Args:
            connector_type (str): The type of connector to retrieve (e.g., "local_files").
            config (Optional[Dict[str, Any]]): A dictionary of configuration parameters
                                                for the connector.

        Returns:
            BaseConnector: An instance of the requested connector.

        Raises:
            ValueError: If the connector_type is not registered.
        """
        connector_class = cls._connectors.get(connector_type)
        if connector_class is None:
            raise ValueError(f"Connector type '{connector_type}' is not registered. "
                             f"Available types: {', '.join(cls._connectors.keys())}")
        logger.debug(f"Creating instance of {connector_class.__name__} for type '{connector_type}'.")
        return connector_class(config=config)