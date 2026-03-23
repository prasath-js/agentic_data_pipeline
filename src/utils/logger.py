import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(pipeline_name: str = "sales_pipeline") -> None:
    """
    Sets up logging for the pipeline, configuring both console and file handlers.

    Logs are written to a file named after the pipeline in the 'logs' directory.
    The log file will rotate when it reaches 5MB, keeping 5 backup files.
    The logging level is configurable via an environment variable.

    Args:
        pipeline_name (str): The name of the pipeline, used for log file naming.
    """
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, f"{pipeline_name}.log")

    # Get log level from environment variable, default to INFO
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level_str, None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid LOG_LEVEL: {log_level_str}")

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers to prevent duplicate logs in case of multiple calls
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File Handler with rotation
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Set pandas logger to warning to reduce verbosity
    logging.getLogger("pandas").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)

    root_logger.info(f"Logging configured for pipeline: {pipeline_name} at level {log_level_str}")
    root_logger.info(f"Log messages will be written to console and file: {log_file_path}")

if __name__ == "__main__":
    # Example usage:
    # To test, set an environment variable, e.g., export LOG_LEVEL=DEBUG
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")

    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("An exception occurred during division.")
