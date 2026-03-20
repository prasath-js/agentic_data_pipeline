import logging
from typing import Dict, Any

from src.db_connection.connectors.local_files_connector import LocalFilesConnector
from src.db_connection.base import BaseConnector

logger = logging.getLogger(__name__)

class ConnectionBuilder:
    """
    A builder class for creating database connectors based on configuration.
    Supports a static method to build connectors.
    """

    @staticmethod
    def build_connector(config: Dict[str, Any]) -> BaseConnector:
        """
        Builds and returns a connector instance based on the provided configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing connection configuration,
                                     including a 'type' key to specify the connector type.

        Returns:
            BaseConnector: An instance of the appropriate connector.

        Raises:
            ValueError: If an unsupported connector type is specified in the config.
        """
        connector_type = config.get("type")
        if not connector_type:
            logger.error("Connector configuration missing 'type' key.")
            raise ValueError("Connector configuration missing 'type' key.")

        if connector_type == "local_files":
            logger.info("Building LocalFilesConnector.")
            return LocalFilesConnector(config)
        else:
            logger.error(f"Unsupported connector type: {connector_type}")
            raise ValueError(f"Unsupported connector type: {connector_type}")

def build_connector(config: Dict[str, Any]) -> BaseConnector:
    """
    Standalone function to build and return a connector instance based on the provided configuration.

    Args:
        config (Dict[str, Any]): A dictionary containing connection configuration,
                                 including a 'type' key to specify the connector type.

    Returns:
        BaseConnector: An instance of the appropriate connector.

    Raises:
        ValueError: If an unsupported connector type is specified in the config.
    """
    return ConnectionBuilder.build_connector(config)