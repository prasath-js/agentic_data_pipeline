import logging
import os
from logging.handlers import RotatingFileHandler

def configure_logging() -> None:
    """
    Configures the logging system for the ETL pipeline.

    - Creates 'logs/' directory if it doesn't exist.
    - Sets up a root logger with a configurable level from LOG_LEVEL environment variable (default: INFO).
    - Adds a console handler for stdout logging.
    - Adds file handlers for info, warning, and error logs, with rotation.
    """
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Configure the root logger
    logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Get the root logger
    root_logger = logging.getLogger()
    # Clear existing handlers to prevent duplicate logs if called multiple times
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler for INFO and higher
    info_file_handler = RotatingFileHandler(
        os.path.join(log_dir, "info.log"), maxBytes=10485760, backupCount=5
    ) # 10 MB per file, 5 backup files
    info_file_handler.setLevel(logging.INFO)
    info_file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    info_file_handler.setFormatter(info_file_formatter)
    root_logger.addHandler(info_file_handler)

    # File handler for WARNING and higher
    warning_file_handler = RotatingFileHandler(
        os.path.join(log_dir, "warning.log"), maxBytes=10485760, backupCount=5
    )
    warning_file_handler.setLevel(logging.WARNING)
    warning_file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    warning_file_handler.setFormatter(warning_file_formatter)
    root_logger.addHandler(warning_file_handler)

    # File handler for ERROR and higher
    error_file_handler = RotatingFileHandler(
        os.path.join(log_dir, "error.log"), maxBytes=10485760, backupCount=5
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    error_file_handler.setFormatter(error_file_formatter)
    root_logger.addHandler(error_file_handler)

    logging.getLogger(__name__).info(f"Logging configured with level: {log_level_str}")

if __name__ == "__main__":
    # Example usage for testing logging configuration
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.debug("This is a DEBUG message.")
    logger.info("This is an INFO message.")
    logger.warning("This is a WARNING message.")
    logger.error("This is an ERROR message.")
    logger.critical("This is a CRITICAL message.")

    # Test with a different LOG_LEVEL
    os.environ["LOG_LEVEL"] = "DEBUG"
    configure_logging()
    logger.info("Reconfigured logging to DEBUG level.")
    logger.debug("This DEBUG message should now be visible.")
    del os.environ["LOG_LEVEL"]