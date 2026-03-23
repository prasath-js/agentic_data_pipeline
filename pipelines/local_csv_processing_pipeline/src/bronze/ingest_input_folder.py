import os
import glob
import logging
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

def ingest_input_folder(input_path: Optional[str] = None) -> pd.DataFrame:
    """
    Ingests raw data from local CSV files in the specified input folder.

    This function represents the Bronze layer ingestion for the 'input_folder' source.
    It reads raw data without applying any transformations.

    Args:
        input_path (Optional[str]): Path to the directory containing input CSV files.
                                    If None, falls back to the 'INPUT_FOLDER_PATH'
                                    environment variable.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the raw combined data from all
                      CSV files in the target directory. Expected columns include:
                      order_id, customer_id, customer_name, email, amount, status,
                      region, order_date.

    Raises:
        ValueError: If the input path is not provided and the environment variable is missing,
                    or if the provided path does not exist.
        FileNotFoundError: If no CSV files are found in the target directory.
    """
    path = input_path or os.getenv("INPUT_FOLDER_PATH")

    if not path:
        logger.error("INPUT_FOLDER_PATH environment variable is not set.")
        raise ValueError("Input path must be provided or set via INPUT_FOLDER_PATH.")

    if not os.path.exists(path):
        logger.error(f"Configured input path does not exist: {path}")
        raise ValueError(f"Input path does not exist: {path}")

    logger.info(f"Starting batch ingestion from local folder: {path}")

    search_pattern = os.path.join(path, "*.csv")
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        logger.warning(f"No CSV files found matching pattern: {search_pattern}")
        raise FileNotFoundError(f"No CSV files found in {path}")

    dataframes = []
    for file_path in csv_files:
        logger.info(f"Reading raw data from file: {file_path}")
        try:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Failed to read file {file_path}. Error: {str(e)}")
            raise

    if not dataframes:
        logger.warning("All files were empty or could not be read.")
        return pd.DataFrame()

    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Successfully ingested {len(combined_df)} raw rows from {len(csv_files)} files.")

    return combined_df