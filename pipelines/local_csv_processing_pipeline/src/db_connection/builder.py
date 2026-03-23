import logging
from typing import Any, Dict, Optional

from src.db_connection.connectors.local_files_connector import LocalFileConnector
from src.db_connection.base import BaseConnector

logger = logging.getLogger(__name__)

class ConnectionBuilder:
    """
    Factory class for building database connectors based on configuration.
    Supports various connector types defined in the project.
    """

    @staticmethod
    def build_connector(config: Dict[str, Any]) -> BaseConnector:
        """
        Builds and returns a connector instance based on the provided configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing connection parameters,
                                     including a 'type' field specifying the connector type.

        Returns:
            BaseConnector: An initialized connector instance.

        Raises:
            ValueError: If an unsupported connector type is specified in the config.
        """
        connector_type: Optional[str] = config.get("type")

        if connector_type == "local_files":
            logger.info("Building LocalFileConnector.")
            # LocalFileConnector doesn't typically require complex connection strings
            # for "connection_string" but might use 'path' or 'file_type'
            # We pass the relevant parts of the config directly.
            return LocalFileConnector(
                file_path=config.get("path"),
                file_type=config.get("file_type", "csv")
            )
        else:
            error_message = f"Unsupported connector type: {connector_type}"
            logger.error(error_message)
            raise ValueError(error_message)
