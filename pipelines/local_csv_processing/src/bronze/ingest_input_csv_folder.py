import os
import logging
import pandas as pd
import glob
from typing import List, Optional

# Configure logger for this module
logger = logging.getLogger(__name__)

def ingest_input_csv_folder(
    folder_path: str,
    file_pattern: str = "*.csv",
    delimiter: str = ",",
    encoding: str = "utf-8"
) -> Optional[pd.DataFrame]:
    """
    Ingests raw data from multiple CSV files within a specified local folder.

    This function reads all CSV files matching the given pattern in the
    `folder_path` into a single Pandas DataFrame. It performs no
    transformations, adhering strictly to the Bronze layer's raw ingestion
    principle. The `folder_path` should be configured via environment variables
    (e.g., `os.getenv('BRONZE_CSV_FOLDER_PATH')`) in `config/settings.py` or
    `main.py` to avoid hardcoding.

    Args:
        folder_path (str): The absolute or relative path to the folder
                           containing the CSV files. This path must not be
                           hardcoded and should be sourced from environment
                           variables.
        file_pattern (str): The glob pattern to match CSV files (e.g., "*.csv").
                            Defaults to "*.csv".
        delimiter (str): The delimiter used in the CSV files. Defaults to ','.
        encoding (str): The encoding of the CSV files. Defaults to 'utf-8'.

    Returns:
        Optional[pd.DataFrame]: A concatenated DataFrame containing data from
                                all ingested CSV files, or None if no files
                                were found, the folder does not exist, or an
                                error occurred during ingestion.
    """
    all_data_frames: List[pd.DataFrame] = []
    
    # Validate that the folder_path exists
    if not os.path.isdir(folder_path):
        logger.error(f"Bronze Ingestion Error: Folder not found at '{folder_path}'. Please check the path configuration.")
        return None

    try:
        # Find all files matching the pattern within the specified folder
        file_paths = glob.glob(os.path.join(folder_path, file_pattern))

        if not file_paths:
            logger.warning(f"Bronze Ingestion Warning: No files matching '{file_pattern}' found in '{folder_path}'.")
            return None

        # Iterate through each file and ingest its content
        for file_path in file_paths:
            try:
                logger.info(f"Bronze Ingestion: Starting ingestion from file: {file_path}")
                df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
                all_data_frames.append(df)
                logger.info(f"Bronze Ingestion: Successfully ingested {len(df)} rows from {file_path}.")
            except FileNotFoundError:
                logger.error(f"Bronze Ingestion Error: File not found during ingestion: {file_path}. Skipping.")
                continue
            except pd.errors.EmptyDataError:
                logger.warning(f"Bronze Ingestion Warning: File is empty, skipping: {file_path}.")
                continue
            except Exception as e:
                logger.error(f"Bronze Ingestion Error: Failed to read CSV file {file_path}: {e}", exc_info=True)
                continue

        if not all_data_frames:
            logger.warning("Bronze Ingestion Warning: No data frames were successfully ingested from any file.")
            return None

        # Concatenate all successfully ingested data frames into a single one
        bronze_df = pd.concat(all_data_frames, ignore_index=True)
        logger.info(f"Bronze Ingestion Complete: Successfully aggregated {len(all_data_frames)} files into a single DataFrame with {len(bronze_df)} rows.")
        return bronze_df

    except Exception as e:
        logger.error(f"Bronze Ingestion Fatal Error: An unexpected error occurred during CSV folder ingestion from {folder_path}: {e}", exc_info=True)
        return None