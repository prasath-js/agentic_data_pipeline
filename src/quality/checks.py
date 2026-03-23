import logging
import os
import pandas as pd
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Performs data quality checks across different layers of the sales pipeline.

    This class provides methods to validate row counts, null rates, and schema consistency
    between Bronze, Silver, and Gold layers of a data pipeline.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataQualityChecker with configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing configuration parameters
                                     like staging directories.
        """
        self.config = config
        self.bronze_staging_dir = config.get('bronze_staging_dir', './data/bronze/sales_pipeline')
        self.silver_staging_dir = config.get('silver_staging_dir', './data/silver/sales_pipeline')
        self.gold_output_dir = config.get('gold_output_dir', './data/gold/sales_pipeline')

    def _load_data(self, file_path: str) -> pd.DataFrame:
        """
        Loads data from a Parquet file.

        Args:
            file_path (str): The path to the Parquet file.

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            FileNotFoundError: If the file does not exist.
            Exception: For other errors during file loading.
        """
        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Successfully loaded data from {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            raise

    def check_row_counts(self, bronze_file: str, silver_file: str, gold_file: str) -> bool:
        """
        Validates row counts between Bronze, Silver, and Gold layers.

        Compares row counts to ensure data integrity during transformations.
        Expects Bronze and Silver to have similar counts, and Gold to be
        potentially aggregated (lower count).

        Args:
            bronze_file (str): Filename for the bronze layer data.
            silver_file (str): Filename for the silver layer data.
            gold_file (str): Filename for the gold layer data.

        Returns:
            bool: True if row count checks pass, False otherwise.
        """
        logger.info("Starting row count checks...")
        bronze_df = None
        silver_df = None
        gold_df = None

        try:
            bronze_df = self._load_data(os.path.join(self.bronze_staging_dir, bronze_file))
            silver_df = self._load_data(os.path.join(self.silver_staging_dir, silver_file))
            gold_df = self._load_data(os.path.join(self.gold_output_dir, gold_file))

            bronze_count = len(bronze_df)
            silver_count = len(silver_df)
            gold_count = len(gold_df)

            logger.info(f"Bronze layer row count: {bronze_count}")
            logger.info(f"Silver layer row count: {silver_count}")
            logger.info(f"Gold layer row count: {gold_count}")

            # Bronze to Silver check: Expect similar counts, Silver might have fewer due to filtering/deduplication
            # For 'sales_pipeline' with no explicit filtering, expect similar counts
            if abs(bronze_count - silver_count) > bronze_count * 0.05: # Allow up to 5% difference
                logger.warning(f"Row count mismatch between Bronze ({bronze_count}) and Silver ({silver_count}). Difference is significant.")
                # Depending on pipeline, this might be a failure. For now, just warn.
            else:
                logger.info("Bronze to Silver row count check passed (within 5% tolerance).")

            # Silver to Gold check: Expect Gold count to be less or equal due to aggregation
            if gold_count > silver_count:
                logger.error(f"Gold layer row count ({gold_count}) is greater than Silver layer row count ({silver_count}). This indicates a potential issue with aggregation.")
                return False
            else:
                logger.info("Silver to Gold row count check passed (Gold count is less than or equal to Silver).")

            logger.info("Row count checks completed.")
            return True
        except FileNotFoundError as e:
            logger.error(f"One or more files not found for row count check: {e}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during row count checks: {e}")
            return False

    def check_null_rates(self, file_path: str, critical_columns: List[str], max_null_rate: float = 0.01) -> bool:
        """
        Checks for excessive null values in critical columns of a given dataset.

        Args:
            file_path (str): The full path to the Parquet file to check.
            critical_columns (List[str]): A list of column names considered critical.
            max_null_rate (float): The maximum allowed null rate (e.g., 0.01 for 1%).

        Returns:
            bool: True if null rates are within limits for critical columns, False otherwise.
        """
        logger.info(f"Starting null rate checks for {file_path} on columns: {critical_columns}")
        df = None
        try:
            df = self._load_data(file_path)

            passed = True
            for col in critical_columns:
                if col not in df.columns:
                    logger.warning(f"Critical column '{col}' not found in {file_path}. Skipping null check for this column.")
                    continue

                null_count = df[col].isnull().sum()
                row_count = len(df)
                if row_count == 0:
                    logger.warning(f"File {file_path} is empty. Cannot perform null rate check for column '{col}'.")
                    continue

                null_rate = null_count / row_count
                logger.info(f"Column '{col}' in {os.path.basename(file_path)}: Null count={null_count}, Null rate={null_rate:.4f}")

                if null_rate > max_null_rate:
                    logger.error(f"Null rate for column '{col}' ({null_rate:.4f}) exceeds maximum allowed ({max_null_rate:.4f}) in {os.path.basename(file_path)}.")
                    passed = False
            
            if passed:
                logger.info(f"Null rate checks passed for critical columns in {os.path.basename(file_path)}.")
            else:
                logger.warning(f"Null rate checks failed for one or more critical columns in {os.path.basename(file_path)}.")
            return passed
        except FileNotFoundError as e:
            logger.error(f"File not found for null rate check: {e}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during null rate checks for {file_path}: {e}")
            return False

    def check_schema_consistency(self, expected_schema: Dict[str, str], file_path: str, layer_name: str) -> bool:
        """
        Checks if the DataFrame's schema matches an expected schema.

        Args:
            expected_schema (Dict[str, str]): A dictionary mapping column names to expected data types (e.g., 'object', 'int64', 'datetime64[ns]').
            file_path (str): The full path to the Parquet file to check.
            layer_name (str): The name of the data layer (e.g., "Silver", "Gold").

        Returns:
            bool: True if the schema is consistent, False otherwise.
        """
        logger.info(f"Starting schema consistency check for {layer_name} layer at {file_path}...")
        df = None
        try:
            df = self._load_data(file_path)

            current_schema = df.dtypes.apply(lambda x: str(x)).to_dict()
            passed = True

            # Check for missing columns in current schema
            for col, expected_dtype in expected_schema.items():
                if col not in current_schema:
                    logger.error(f"Column '{col}' is missing in the {layer_name} layer data at {file_path}.")
                    passed = False
                elif current_schema[col] != expected_dtype:
                    # Allow some flexibility for int/float, but warn
                    if ('int' in expected_dtype and 'float' in current_schema[col]) or \
                       ('float' in expected_dtype and 'int' in current_schema[col]):
                        logger.warning(f"Column '{col}' in {layer_name} layer has dtype '{current_schema[col]}' but expected '{expected_dtype}'. (Coercion may occur)")
                    elif expected_dtype == 'datetime64[ns]' and not current_schema[col].startswith('datetime'):
                        logger.error(f"Column '{col}' in {layer_name} layer has dtype '{current_schema[col]}' but expected '{expected_dtype}'.")
                        passed = False
                    elif current_schema[col] != expected_dtype:
                        logger.error(f"Column '{col}' in {layer_name} layer has dtype '{current_schema[col]}' but expected '{expected_dtype}'.")
                        passed = False

            # Check for unexpected columns in current schema
            for col in current_schema.keys():
                if col not in expected_schema:
                    logger.warning(f"Column '{col}' found in {layer_name} layer data at {file_path} but is not in the expected schema.")

            if passed:
                logger.info(f"Schema consistency check passed for {layer_name} layer.")
            else:
                logger.warning(f"Schema consistency check failed for {layer_name} layer.")
            return passed
        except FileNotFoundError as e:
            logger.error(f"File not found for schema consistency check: {e}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during schema consistency check for {layer_name} layer at {file_path}: {e}")
            return False

    def run_all_checks(self,
                       bronze_file_name: str,
                       silver_file_name: str,
                       gold_file_name: str,
                       silver_expected_schema: Dict[str, str],
                       gold_expected_schema: Dict[str, str],
                       silver_critical_cols: List[str],
                       gold_critical_cols: List[str]) -> bool:
        """
        Runs all defined data quality checks.

        Args:
            bronze_file_name (str): The filename of the bronze layer data.
            silver_file_name (str): The filename of the silver layer data.
            gold_file_name (str): The filename of the gold layer data.
            silver_expected_schema (Dict[str, str]): Expected schema for the silver layer.
            gold_expected_schema (Dict[str, str]): Expected schema for the gold layer.
            silver_critical_cols (List[str]): Critical columns for null checks in the silver layer.
            gold_critical_cols (List[str]): Critical columns for null checks in the gold layer.

        Returns:
            bool: True if all checks pass, False otherwise.
        """
        logger.info("Starting all data quality checks for sales_pipeline...")
        overall_status = True

        bronze_full_path = os.path.join(self.bronze_staging_dir, bronze_file_name)
        silver_full_path = os.path.join(self.silver_staging_dir, silver_file_name)
        gold_full_path = os.path.join(self.gold_output_dir, gold_file_name)

        # Ensure files exist before starting detailed checks
        for path in [bronze_full_path, silver_full_path, gold_full_path]:
            if not os.path.exists(path):
                logger.error(f"Required data file for quality checks not found: {path}")
                return False

        # Row Count Checks
        if not self.check_row_counts(bronze_file_name, silver_file_name, gold_file_name):
            overall_status = False

        # Null Rate Checks
        if not self.check_null_rates(silver_full_path, silver_critical_cols):
            overall_status = False
        if not self.check_null_rates(gold_full_path, gold_critical_cols):
            overall_status = False

        # Schema Consistency Checks
        if not self.check_schema_consistency(silver_expected_schema, silver_full_path, "Silver"):
            overall_status = False
        if not self.check_schema_consistency(gold_expected_schema, gold_full_path, "Gold"):
            overall_status = False

        if overall_status:
            logger.info("All data quality checks passed for sales_pipeline.")
        else:
            logger.error("One or more data quality checks failed for sales_pipeline.")
        return overall_status

def main() -> None:
    """
    Main function to run data quality checks for the sales pipeline.
    """
    logger.info("Starting sales_pipeline data quality checks.")

    # Configuration for staging and output directories
    # These should ideally come from a config file or environment variables in a real setup
    # For demonstration, using hardcoded paths relative to where this script might run
    # Assume script runs from project root or similar. Adjust paths as necessary.
    config = {
        'bronze_staging_dir': os.getenv('BRONZE_STAGING_DIR', './data/bronze/sales_pipeline'),
        'silver_staging_dir': os.getenv('SILVER_STAGING_DIR', './data/silver/sales_pipeline'),
        'gold_output_dir': os.getenv('GOLD_OUTPUT_DIR', './data/gold/sales_pipeline')
    }

    # Ensure directories exist for testing/running purposes
    for path in [config['bronze_staging_dir'], config['silver_staging_dir'], config['gold_output_dir']]:
        os.makedirs(path, exist_ok=True)

    # Define file names
    bronze_file = "sales_raw.parquet"
    silver_file = "sales_cleaned.parquet"
    gold_file = "sales_aggregated.parquet"

    # Define expected schemas for Silver and Gold layers
    # Based on the pipeline description:
    # Bronze: ['order_id', 'customer_id', 'customer_name', 'customer_email', 'product_id', 'product_name', 'quantity', 'unit_price', 'total_amount', 'order_date', 'region', 'status']
    # Silver: Type casting for numerical and date columns, PII masking.
    # Gold: Aggregation of total sales and quantity by order_date and region.
    
    # Expected Silver Schema after type casting and PII masking
    silver_expected_schema = {
        'order_id': 'object',           # Typically string/object
        'customer_id': 'object',        # Typically string/object
        'customer_name': 'object',      # Masked, so still object (string)
        'customer_email': 'object',     # Masked, so still object (string)
        'product_id': 'object',         # Typically string/object
        'product_name': 'object',       # Masked, so still object (string)
        'quantity': 'int64',            # Type cast to int
        'unit_price': 'float64',        # Type cast to float
        'total_amount': 'float64',      # Type cast to float
        'order_date': 'datetime64[ns]', # Type cast to datetime
        'region': 'object',
        'status': 'object'
    }

    # Expected Gold Schema after aggregation by order_date and region
    gold_expected_schema = {
        'order_date': 'datetime64[ns]', # Group by key
        'region': 'object',             # Group by key
        'total_sales_amount': 'float64',# Aggregated column
        'total_quantity_sold': 'int64'  # Aggregated column
    }

    # Define critical columns for null checks
    # These are columns that absolutely should not have significant nulls
    silver_critical_columns = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_date', 'region']
    gold_critical_columns = ['order_date', 'region', 'total_sales_amount', 'total_quantity_sold']

    checker = DataQualityChecker(config)
    
    # Run all checks
    checks_passed = checker.run_all_checks(
        bronze_file_name=bronze_file,
        silver_file_name=silver_file,
        gold_file_name=gold_file,
        silver_expected_schema=silver_expected_schema,
        gold_expected_schema=gold_expected_schema,
        silver_critical_cols=silver_critical_columns,
        gold_critical_cols=gold_critical_columns
    )

    if checks_passed:
        logger.info("Sales pipeline data quality checks completed successfully.")
        # Exit with a success code
        exit(0)
    else:
        logger.error("Sales pipeline data quality checks failed.")
        # Exit with a failure code
        exit(1)

if __name__ == "__main__":
    main()
