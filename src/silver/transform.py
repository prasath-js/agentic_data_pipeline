import os
import logging
import pandas as pd
import hashlib
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def hash_value(value: str) -> str:
    """Hashes a string value using SHA-256."""
    if pd.isna(value):
        return None
    return hashlib.sha256(str(value).encode('utf-8')).hexdigest()

class SilverTransformer:
    """
    Transforms bronze layer data into silver layer data.
    This includes cleaning, type casting, PII masking, and joining data sources.
    """

    def __init__(self, bronze_dir: str, silver_dir: str):
        """
        Initializes the SilverTransformer with input and output directories.

        Args:
            bronze_dir (str): Path to the bronze layer directory.
            silver_dir (str): Path to the silver layer directory where transformed data will be stored.
        """
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        os.makedirs(self.silver_dir, exist_ok=True)
        logger.info(f"SilverTransformer initialized with bronze_dir: {self.bronze_dir}, silver_dir: {self.silver_dir}")

    def _load_bronze_data(self, source_name: str) -> pd.DataFrame:
        """
        Loads bronze layer data for a given source.

        Args:
            source_name (str): The name of the data source (e.g., 'sales').

        Returns:
            pd.DataFrame: Loaded bronze data.

        Raises:
            FileNotFoundError: If the bronze parquet file does not exist.
            Exception: For other errors during file loading.
        """
        file_path = os.path.join(self.bronze_dir, f"{source_name}_raw.parquet")
        df = pd.DataFrame()
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Bronze file not found: {file_path}")
            df = pd.read_parquet(file_path)
            logger.info(f"Successfully loaded bronze data from {file_path}. Rows: {len(df)}")
        except FileNotFoundError as e:
            logger.error(f"Error loading bronze data: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading bronze data from {file_path}: {e}")
            raise
        return df

    def _apply_type_casting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies type casting to numerical and date columns.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with applied type casting.
        """
        logger.info("Applying type casting to DataFrame.")
        # Type casting for 'sales' source columns
        numeric_cols = ['quantity', 'unit_price', 'total_amount']
        date_cols = ['order_date']

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.debug(f"Casted '{col}' to numeric.")
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.debug(f"Casted '{col}' to datetime.")
        return df

    def _mask_pii(self, df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
        """
        Masks PII columns in the DataFrame using SHA-256 hashing.

        Args:
            df (pd.DataFrame): The input DataFrame.
            pii_columns (List[str]): List of column names identified as PII.

        Returns:
            pd.DataFrame: DataFrame with PII columns masked.
        """
        logger.info(f"Masking PII columns: {pii_columns}")
        for col in pii_columns:
            if col in df.columns:
                df[f'{col}_masked'] = df[col].astype(str).apply(hash_value)
                df = df.drop(columns=[col]) # Drop original PII column after masking
                logger.debug(f"Masked and dropped '{col}'. New column: '{col}_masked'")
            else:
                logger.warning(f"PII column '{col}' not found in DataFrame. Skipping masking.")
        return df

    def transform_sales_data(self) -> None:
        """
        Performs the full silver layer transformation for the 'sales' data.
        This includes loading, cleaning, PII masking, and writing to silver layer.
        """
        logger.info("Starting silver layer transformation for 'sales' data.")
        try:
            sales_df = self._load_bronze_data('sales')

            # Data Cleaning & Type Casting
            sales_df = self._apply_type_casting(sales_df)
            sales_df = sales_df.dropna(subset=['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_date'])
            logger.info(f"After cleaning and type casting, 'sales' data has {len(sales_df)} rows.")

            # PII Masking
            pii_columns = ['customer_name', 'customer_email', 'product_name']
            sales_df = self._mask_pii(sales_df, pii_columns)

            # No joins specified for this pipeline, so proceeding to save.

            # Write to Silver Layer
            output_path = os.path.join(self.silver_dir, 'sales_pipeline_sales_silver.parquet')
            sales_df.to_parquet(output_path, index=False)
            logger.info(f"Successfully transformed and saved 'sales' silver data to {output_path}. Rows: {len(sales_df)}")

        except FileNotFoundError:
            logger.error("Required bronze file not found. Silver transformation aborted.")
        except Exception as e:
            logger.error(f"An error occurred during 'sales' silver transformation: {e}", exc_info=True)

def main() -> None:
    """
    Main function to run the silver layer transformation.
    Reads configuration from environment variables.
    """
    logger.info("Starting sales_pipeline silver layer processing.")

    bronze_dir = os.getenv('BRONZE_LAYER_DIR', './data/bronze')
    silver_dir = os.getenv('SILVER_LAYER_DIR', './data/silver')

    # Ensure directories exist for output
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)

    transformer = SilverTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    transformer.transform_sales_data()

    logger.info("sales_pipeline silver layer processing finished.")

if __name__ == "__main__":
    main()
