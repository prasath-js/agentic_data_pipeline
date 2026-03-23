import os
import sys
import logging

def configure_logging() -> None:
    """
    Configure logging for the pipeline.
    
    Sets up console and file handlers, uses the LOG_LEVEL environment variable,
    and creates the 'logs/' directory if it does not exist.
    """
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_path = os.path.join(log_dir, "local_manual_csv_pipeline.log")

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Prevent adding duplicate handlers if configure_logging is called multiple times
    if not root_logger.handlers:
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

    logging.info("Logging configured successfully for local_manual_csv_pipeline.")