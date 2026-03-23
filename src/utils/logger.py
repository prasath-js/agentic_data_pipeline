import logging
import os
from datetime import datetime

def setup_logging(
    log_dir: str = "logs",
    pipeline_name: str = "sales_pipeline",
    log_level: str = "INFO"
) -> None:
    """
    Configures the logging for the pipeline, setting up both console and file handlers.

    Logs will be written to a file named 'pipeline_name_YYYYMMDD.log' in the specified log directory,
    and also output to the console.

    Args:
        log_dir (str): The directory where log files will be stored. Defaults to 'logs'.
        pipeline_name (str): The name of the pipeline, used in the log file name.
                             Defaults to 'sales_pipeline'.
        log_level (str): The minimum level of messages to log (e.g., 'DEBUG', 'INFO', 'WARNING',
                         'ERROR', 'CRITICAL'). Defaults to 'INFO'.
    """
    # Ensure the log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Define log file name with current date
    today_date = datetime.now().strftime("%Y%m%d")
    log_file_path = os.path.join(log_dir, f"{pipeline_name}_{today_date}.log")

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())

    # Clear existing handlers to prevent duplicate logs
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Formatter for log messages
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Prevent messages from propagating to the root logger if specific loggers are used
    # and configured elsewhere, to avoid duplicate console output.
    # However, for this setup, we're configuring the root logger directly.
    # logging.getLogger('some_module').propagate = False

    logging.info(f"Logging configured. Log level: {log_level.upper()}. "
                 f"Log file: {os.path.abspath(log_file_path)}")

if __name__ == "__main__":
    # Example usage when run directly
    # Configure logging using environment variables or defaults
    LOG_DIR = os.getenv("LOG_DIR", "logs")
    PIPELINE_NAME = os.getenv("PIPELINE_NAME", "sales_pipeline")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    setup_logging(log_dir=LOG_DIR, pipeline_name=PIPELINE_NAME, log_level=LOG_LEVEL)

    # Get a logger for this module for testing
    logger = logging.getLogger(__name__)

    logger.debug("This is a debug message.")
    logger.info("This is an info message from the main block.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    try:
        raise ValueError("An example error")
    except ValueError as e:
        logger.exception("An exception occurred during example execution.")
