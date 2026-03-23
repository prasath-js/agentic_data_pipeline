import logging
import os
import sys
from pathlib import Path

def configure_logging() -> None:
    """
    Configures logging for the local_csv_processing_pipeline.
    
    Sets up a root logger with:
    - A console handler for standard output.
    - A file handler for general logs (logs/info.log).
    - A file handler for error logs (logs/error.log).
    
    The log level is determined by the LOG_LEVEL environment variable,
    defaulting to INFO. Ensures the 'logs' directory exists.
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Determine log level from environment variable
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers to prevent duplicate logs if called multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Define standard formatter (plain ASCII)
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # 2. Info File Handler
    info_file_handler = logging.FileHandler(log_dir / "info.log", mode="a", encoding="utf-8")
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(formatter)

    # 3. Error File Handler
    error_file_handler = logging.FileHandler(log_dir / "error.log", mode="a", encoding="utf-8")
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)

    # Add handlers to the root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(info_file_handler)
    root_logger.addHandler(error_file_handler)