import logging
import os
from pathlib import Path

# Define the directory for logs
LOGS_DIR = Path("logs")

def configure_logging() -> None:
    """
    Configures the logging for the ETL pipeline.

    - Creates 'logs' directory if it doesn't exist.
    - Sets up a root logger with console output and file outputs for info.log and error.log.
    - Log level is determined by the 'LOG_LEVEL' environment variable, defaulting to INFO.
    """
    # Ensure the logs directory exists
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Determine the log level from environment variable, default to INFO
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers to prevent duplicate logs if called multiple times
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    # Define a common formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # File handler for all INFO level messages and above
    info_file_handler = logging.FileHandler(LOGS_DIR / "info.log")
    info_file_handler.setFormatter(formatter)
    info_file_handler.setLevel(logging.INFO)
    root_logger.addHandler(info_file_handler)

    # File handler for ERROR level messages and above
    error_file_handler = logging.FileHandler(LOGS_DIR / "error.log")
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(logging.ERROR)
    root_logger.addHandler(error_file_handler)

    # Set basic config for any other potential loggers that might be initialized
    # This is often done by libraries, so setting root_logger handlers is usually sufficient
    # but for completeness, we ensure the root logger is the primary point of control.

    # Example of how to use it:
    # logger = logging.getLogger(__name__)
    # logger.info("Logging configured successfully.")