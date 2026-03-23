import logging
from typing import Dict, Any

from .base import BaseConnector
from .connectors.local_files_connector import LocalFilesConnector

logger = logging.getLogger(__name__)

class ConnectionBuilder:
    """
    Factory class to build and return the appropriate database or file system connector
    based on the provided configuration.
    """

    @staticmethod
    def build_connector(config: Dict[str, Any]) -> BaseConnector:
        """
        Builds a connector instance depending on the 'type' specified in the config.

        Args:
            config (Dict[str, Any]): A dictionary containing connection configuration.
                                     Must include a 'type' key.

        Returns:
            BaseConnector: An instantiated connector object extending BaseConnector.

        Raises:
            ValueError: If the 'type' is not supported or is missing from the config.
        """
        connector_type = config.get("type")

        if not connector_type:
            logger.error("Connector configuration is missing the 'type' key.")
            raise ValueError("Connector configuration must include a 'type' key.")

        logger.info("Building connector for type: %s", connector_type)

        if connector_type == "local_files":
            return LocalFilesConnector(config)
        else:
            logger.error("Unsupported connector type requested: %s", connector_type)
            raise ValueError(f"Unsupported connector type: {connector_type}")