import logging
import os
from logging.handlers import RotatingFileHandler

def configure_logging() -> None:
    """
    Configures the logging for the local_csv_processing_pipeline.

    Sets up a main logger, console handler, and file handlers for info and error logs.
    The log level is determined by the LOG_LEVEL environment variable,
    defaulting to INFO if not set.
    """
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Determine log level from environment variable, default to INFO
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Get the root logger for the application
    logger = logging.getLogger("local_csv_processing_pipeline")
    logger.setLevel(log_level)

    # Clear existing handlers to prevent duplicate logs in case of re-configuration
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    # Define a common formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Info File Handler (all levels >= INFO)
    info_file_handler = RotatingFileHandler(
        os.path.join(log_dir, "info.log"),
        maxBytes=10485760,  # 10 MB
        backupCount=5
    )
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(formatter)
    logger.addHandler(info_file_handler)

    # Error File Handler (only ERROR and CRITICAL levels)
    error_file_handler = RotatingFileHandler(
        os.path.join(log_dir, "error.log"),
        maxBytes=10485760,  # 10 MB
        backupCount=5
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)

    logger.info("Logging configured for pipeline 'local_csv_processing_pipeline'.")
    logger.info(f"Effective log level: {logging.getLevelName(logger.level)}")