from typing import Dict, Any

from src.db_connection.base import BaseConnector
from src.db_connection.connectors.local_files_connector import LocalFileConnector


class ConnectionBuilder:
    """
    Factory class for building database connectors based on configuration.
    """

    @staticmethod
    def build_connector(config: Dict[str, Any]) -> BaseConnector:
        """
        Builds and returns a connector instance based on the provided configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing connection configuration,
                                     including a 'type' field to specify the connector type.

        Returns:
            BaseConnector: An instance of the appropriate connector.

        Raises:
            ValueError: If an unsupported connection type is provided in the configuration.
        """
        connector_type = config.get("type")

        if connector_type == "local_files":
            return LocalFileConnector(config=config)
        else:
            raise ValueError(f"Unsupported connection type: {connector_type}")