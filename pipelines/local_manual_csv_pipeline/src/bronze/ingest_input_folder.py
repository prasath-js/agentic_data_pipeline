import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def ingest_input_folder(file_path: str) -> pd.DataFrame:
    """
    Ingests raw data from a local file source without applying any transformations.
    
    Args:
        file_path (str): The path to the local input file (CSV expected).
        
    Returns:
        pd.DataFrame: A pandas DataFrame containing the raw ingested data.
        
    Raises:
        FileNotFoundError: If the specified file_path does not exist.
        ValueError: If the file is empty or cannot be read as a CSV.
        Exception: For any other unforeseen ingestion errors.
    """
    logger.info("Starting bronze ingestion from source: %s", file_path)
    
    if not file_path:
        error_msg = "File path provided is empty or None."
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    if not os.path.exists(file_path):
        error_msg = f"Input file not found at path: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
        
    try:
        df = pd.read_csv(file_path)
        logger.info("Successfully ingested %d rows and %d columns from %s.", 
                    df.shape[0], df.shape[1], file_path)
        return df
        
    except pd.errors.EmptyDataError:
        error_msg = f"The file at {file_path} is empty."
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    except Exception as e:
        logger.error("Failed to ingest data from %s. Error: %s", file_path, str(e))
        raise