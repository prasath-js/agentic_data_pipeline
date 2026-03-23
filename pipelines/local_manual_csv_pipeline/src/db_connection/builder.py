import logging
from typing import Dict, Any

from src.db_connection.base import BaseConnector
from src.db_connection.connectors.local_files_connector import LocalFilesConnector

logger = logging.getLogger(__name__)

class ConnectionBuilder:
    """
    Factory class to build database or file system connectors based on configuration.
    """

    @staticmethod
    def build_connector(config: Dict[str, Any]) -> BaseConnector:
        """
        Build and return a specific connector instance based on the provided configuration type.

        Args:
            config (Dict[str, Any]): Configuration dictionary containing at least a 'type' key.

        Returns:
            BaseConnector: An instance of a class derived from BaseConnector.

        Raises:
            ValueError: If the connection type is not supported or missing.
        """
        conn_type = config.get("type")

        if not conn_type:
            logger.error("Connection type missing in configuration.")
            raise ValueError("Connection type must be provided in the config.")

        logger.info("Building connector for type: %s", conn_type)

        if conn_type == "local_files":
            logger.info("Instantiating LocalFilesConnector.")
            return LocalFilesConnector(config)
        else:
            logger.error("Unsupported connection type: %s", conn_type)
            raise ValueError(f"Unsupported connection type: {conn_type}")