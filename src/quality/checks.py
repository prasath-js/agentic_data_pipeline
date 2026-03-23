import logging
import os
import pandas as pd
from typing import Dict, Any, List, Optional
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    """
    Performs data quality checks across different layers of the sales_pipeline.
    Validates row counts, null rates, and schema consistency.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataQualityChecker with pipeline configuration.

        Args:
            config (Dict[str, Any]): A dictionary containing configuration parameters
                                     such as data paths and expected schemas.
        """
        self.config = config
        self.report_path = config.get("report_path", "data_quality_report.json")
        logger.info("DataQualityChecker initialized with configuration.")

    def _load_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Loads a Parquet file into a pandas DataFrame.

        Args:
            file_path (str): The path to the Parquet file.

        Returns:
            Optional[pd.DataFrame]: The loaded DataFrame, or None if an error occurs.
        """
        try:
            logger.info(f"Attempting to load data from: {file_path}")
            df = pd.read_parquet(file_path)
            logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None
        except (pd.errors.ParserError, OSError, ValueError) as e: # Specific exceptions for file loading
            logger.error(f"Error loading data from {file_path}: {e}")
            return None

    def check_row_counts(
        self,
        bronze_df: Optional[pd.DataFrame],
        silver_df: Optional[pd.DataFrame],
        gold_df: Optional[pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Compares row counts between Bronze, Silver, and Gold layers.

        Args:
            bronze_df (Optional[pd.DataFrame]): DataFrame from the Bronze layer.
            silver_df (Optional[pd.DataFrame]): DataFrame from the Silver layer.
            gold_df (Optional[pd.DataFrame]): DataFrame from the Gold layer.

        Returns:
            Dict[str, Any]: A dictionary containing the results of row count checks.
        """
        results = {}
        bronze_rows = len(bronze_df) if bronze_df is not None else 0
        silver_rows = len(silver_df) if silver_df is not None else 0
        gold_rows = len(gold_df) if gold_df is not None else 0

        results["bronze_row_count"] = bronze_rows
        results["silver_row_count"] = silver_rows
        results["gold_row_count"] = gold_rows

        # Bronze to Silver check (deduplication)
        check_bs_pass = True
        if bronze_df is not None and silver_df is not None:
            if silver_rows <= bronze_rows:
                logger.info(f"Bronze-Silver row count check passed: {silver_rows} <= {bronze_rows}")
            else:
                logger.warning(f"Bronze-Silver row count check failed: Silver rows ({silver_rows}) > Bronze rows ({bronze_rows}). Expected deduplication.")
                check_bs_pass = False
        else:
            logger.warning("Skipping Bronze-Silver row count check due to missing DataFrame(s).")
            check_bs_pass = False # Consider a check failed if data is missing

        results["bronze_to_silver_check"] = {
            "passed": check_bs_pass,
            "message": "Silver rows are less than or equal to Bronze rows (due to deduplication)." if check_bs_pass else "Silver rows are greater than Bronze rows or data is missing."
        }

        # Silver to Gold check (aggregation)
        check_sg_pass = True
        if silver_df is not None and gold_df is not None:
            if gold_rows <= silver_rows:
                logger.info(f"Silver-Gold row count check passed: {gold_rows} <= {silver_rows}")
            else:
                logger.warning(f"Silver-Gold row count check failed: Gold rows ({gold_rows}) > Silver rows ({silver_rows}). Expected aggregation.")
                check_sg_pass = False
        else:
            logger.warning("Skipping Silver-Gold row count check due to missing DataFrame(s).")
            check_sg_pass = False # Consider a check failed if data is missing

        results["silver_to_gold_check"] = {
            "passed": check_sg_pass,
            "message": "Gold rows are less than or equal to Silver rows (due to aggregation)." if check_sg_pass else "Gold rows are greater than Silver rows or data is missing."
        }

        logger.info("Row count checks completed.")
        return results

    def check_null_rates(self, df: pd.DataFrame, columns: List[str], max_null_rate: float, layer_name: str) -> Dict[str, Any]:
        """
        Checks null rates for specified columns in a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            columns (List[str]): A list of column names to check for nulls.
            max_null_rate (float): The maximum allowed null rate (e.g., 0.0 for 0%).
            layer_name (str): The name of the data layer (e.g., "Silver").

        Returns:
            Dict[str, Any]: A dictionary containing the results of null rate checks.
        """
        results = {"layer": layer_name, "column_checks": []}
        overall_pass = True
        if df is None or df.empty:
            logger.warning(f"DataFrame for {layer_name} is empty or None. Skipping null rate check.")
            results["overall_passed"] = False
            results["message"] = f"DataFrame for {layer_name} is empty or None."
            return results

        total_rows = len(df)
        if total_rows == 0:
            logger.warning(f"DataFrame for {layer_name} has 0 rows. Skipping null rate check.")
            results["overall_passed"] = False
            results["message"] = f"DataFrame for {layer_name} has 0 rows."
            return results

        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                null_rate = null_count / total_rows
                check_pass = null_rate <= max_null_rate
                if not check_pass:
                    overall_pass = False
                    logger.warning(f"Null rate check failed for {layer_name}.{col}: {null_rate:.2%} (expected <= {max_null_rate:.2%})")
                else:
                    logger.info(f"Null rate check passed for {layer_name}.{col}: {null_rate:.2%}")

                results["column_checks"].append({
                    "column": col,
                    "null_count": null_count,
                    "null_rate": round(null_rate, 4),
                    "max_allowed_null_rate": max_null_rate,
                    "passed": check_pass
                })
            else:
                overall_pass = False
                logger.warning(f"Column '{col}' not found in {layer_name} DataFrame. Skipping null rate check for this column.")
                results["column_checks"].append({
                    "column": col,
                    "message": f"Column not found in {layer_name} DataFrame.",
                    "passed": False
                })

        results["overall_passed"] = overall_pass
        logger.info(f"Null rate checks for {layer_name} completed. Overall passed: {overall_pass}")
        return results

    def check_schema_consistency(
        self,
        expected_schema: Dict[str, str],
        actual_df: Optional[pd.DataFrame],
        layer_name: str
    ) -> Dict[str, Any]:
        """
        Validates column names and data types against an expected schema.

        Args:
            expected_schema (Dict[str, str]): A dictionary where keys are column names
                                               and values are expected pandas dtypes (e.g., 'int64', 'object').
            actual_df (Optional[pd.DataFrame]): The DataFrame to check.
            layer_name (str): The name of the data layer (e.g., "Silver").

        Returns:
            Dict[str, Any]: A dictionary containing the results of schema consistency checks.
        """
        results = {"layer": layer_name, "column_checks": []}
        overall_pass = True

        if actual_df is None:
            logger.warning(f"DataFrame for {layer_name} is None. Skipping schema consistency check.")
            results["overall_passed"] = False
            results["message"] = f"DataFrame for {layer_name} is None."
            return results

        actual_columns = set(actual_df.columns)
        expected_columns = set(expected_schema.keys())

        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            overall_pass = False
            logger.warning(f"Schema check failed for {layer_name}: Missing expected columns: {missing_columns}")
            for col in missing_columns:
                results["column_checks"].append({
                    "column": col,
                    "check": "column_presence",
                    "expected_dtype": expected_schema.get(col, "N/A"),
                    "actual_dtype": "N/A",
                    "passed": False,
                    "message": "Column is missing"
                })

        # Check for unexpected columns
        unexpected_columns = actual_columns - expected_columns
        if unexpected_columns:
            overall_pass = False # Mark as failed due to unexpected columns
            logger.warning(f"Schema check for {layer_name}: Found unexpected columns: {unexpected_columns}")
            for col in unexpected_columns:
                 results["column_checks"].append({
                    "column": col,
                    "check": "column_presence",
                    "expected_dtype": "N/A",
                    "actual_dtype": str(actual_df[col].dtype),
                    "passed": False, # Changed to False because unexpected columns indicate a deviation
                    "message": "Column is unexpected but present"
                })


        # Check existing columns for data types
        for col, expected_dtype in expected_schema.items():
            if col in actual_df.columns:
                actual_dtype = str(actual_df[col].dtype)
                # Handle pandas numeric types more broadly (e.g., int64 vs int32)
                # and datetime types (datetime64[ns] vs datetime64)
                dtype_match = False
                if 'int' in expected_dtype and 'int' in actual_dtype:
                    dtype_match = True
                elif 'float' in expected_dtype and 'float' in actual_dtype:
                    dtype_match = True
                elif 'object' in expected_dtype and 'object' in actual_dtype:
                    dtype_match = True
                elif 'datetime' in expected_dtype and 'datetime' in actual_dtype:
                    dtype_match = True
                elif expected_dtype == actual_dtype:
                    dtype_match = True

                if not dtype_match:
                    overall_pass = False
                    logger.warning(f"Schema check failed for {layer_name}.{col}: Expected dtype '{expected_dtype}', got '{actual_dtype}'")
                else:
                    logger.info(f"Schema check passed for {layer_name}.{col}: Dtype '{actual_dtype}' matches expected '{expected_dtype}'")

                results["column_checks"].append({
                    "column": col,
                    "check": "dtype_consistency",
                    "expected_dtype": expected_dtype,
                    "actual_dtype": actual_dtype,
                    "passed": dtype_match,
                    "message": "Data type matches expected" if dtype_match else "Data type mismatch"
                })

        results["overall_passed"] = overall_pass
        logger.info(f"Schema consistency checks for {layer_name} completed. Overall passed: {overall_pass}")
        return results

    def run_all_checks(
        self,
        bronze_path: str,
        silver_path: str,
        gold_path: str
    ) -> Dict[str, Any]:
        """
        Orchestrates all data quality checks across Bronze, Silver, and Gold layers.

        Args:
            bronze_path (str): Path to the Bronze layer Parquet file.
            silver_path (str): Path to the Silver layer Parquet file.
            gold_path (str): Path to the Gold layer Parquet file.

        Returns:
            Dict[str, Any]: A comprehensive report of all data quality checks.
        """
        logger.info("Starting all data quality checks for sales_pipeline.")
        full_report: Dict[str, Any] = {"pipeline": "sales_pipeline", "timestamp": pd.Timestamp.now().isoformat()}

        bronze_df = self._load_data(bronze_path)
        silver_df = self._load_data(silver_path)
        gold_df = self._load_data(gold_path)

        # 1. Row Counts Check
        logger.info("Running row count checks...")
        full_report["row_count_checks"] = self.check_row_counts(bronze_df, silver_df, gold_df)

        # 2. Null Rate Checks
        logger.info("Running null rate checks...")
        critical_cols_silver = self.config.get("critical_columns_silver", [])
        critical_cols_gold = self.config.get("critical_columns_gold", [])
        max_null_rate = self.config.get("max_null_rate", 0.0)

        full_report["null_rate_checks_silver"] = self.check_null_rates(silver_df, critical_cols_silver, max_null_rate, "Silver")
        full_report["null_rate_checks_gold"] = self.check_null_rates(gold_df, critical_cols_gold, max_null_rate, "Gold")

        # 3. Schema Consistency Checks
        logger.info("Running schema consistency checks...")
        expected_schema_bronze = self.config.get("expected_schema_bronze", {})
        expected_schema_silver = self.config.get("expected_schema_silver", {})
        expected_schema_gold = self.config.get("expected_schema_gold", {})

        full_report["schema_checks_bronze"] = self.check_schema_consistency(expected_schema_bronze, bronze_df, "Bronze")
        full_report["schema_checks_silver"] = self.check_schema_consistency(expected_schema_silver, silver_df, "Silver")
        full_report["schema_checks_gold"] = self.check_schema_consistency(expected_schema_gold, gold_df, "Gold")

        # Determine overall status
        overall_dq_pass = True
        for check_type in ["row_count_checks", "null_rate_checks_silver", "null_rate_checks_gold",
                           "schema_checks_bronze", "schema_checks_silver", "schema_checks_gold"]:
            if check_type == "row_count_checks":
                if not full_report[check_type]["bronze_to_silver_check"]["passed"]:
                    overall_dq_pass = False
                if not full_report[check_type]["silver_to_gold_check"]["passed"]:
                    overall_dq_pass = False
            else:
                if not full_report[check_type].get("overall_passed", True): # Default to True if key is missing, to not fail if a check itself had issues
                    overall_dq_pass = False

        full_report["overall_data_quality_passed"] = overall_dq_pass

        # Save report
        try:
            os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
            with open(self.report_path, 'w') as f:
                json.dump(full_report, f, indent=4)
            logger.info(f"Data quality report saved to: {self.report_path}")
        except Exception as e:
            logger.error(f"Failed to save data quality report to {self.report_path}: {e}")

        logger.info(f"All data quality checks completed. Overall status: {'PASSED' if overall_dq_pass else 'FAILED'}")
        return full_report

def main() -> None:
    """
    Main function to run the data quality checks for the sales_pipeline.
    Configuration is loaded from environment variables or hardcoded defaults for demonstration.
    """
    logger.info("Starting data quality pipeline execution.")

    # Define paths from environment variables or provide defaults
    BRONZE_PATH = os.getenv("SALES_PIPELINE_BRONZE_PATH", "data/sales_pipeline/bronze/sales_data.parquet")
    SILVER_PATH = os.getenv("SALES_PIPELINE_SILVER_PATH", "data/sales_pipeline/silver/sales_data_cleaned.parquet")
    GOLD_PATH = os.getenv("SALES_PIPELINE_GOLD_PATH", "data/sales_pipeline/gold/sales_summary.parquet")
    REPORT_PATH = os.getenv("SALES_PIPELINE_DQ_REPORT_PATH", "reports/sales_pipeline_dq_report.json")

    # Expected schemas for each layer based on pipeline context
    # Note: Using pandas-like dtype strings. 'object' for strings, 'int64'/'float64' for numbers, 'datetime64[ns]' for dates.
    expected_schema_bronze: Dict[str, str] = {
        'order_id': 'object',
        'customer_id': 'object',
        'customer_name': 'object',
        'customer_email': 'object',
        'product_id': 'object',
        'product_name': 'object',
        'quantity': 'int64',
        'unit_price': 'float64',
        'total_amount': 'float64',
        'order_date': 'datetime64[ns]',
        'region': 'object',
        'status': 'object'
    }

    expected_schema_silver: Dict[str, str] = {
        'order_id': 'object',
        'customer_id': 'object',
        'customer_name_masked': 'object',
        'customer_email_masked': 'object',
        'product_id': 'object',
        'product_name_masked': 'object',
        'quantity': 'int64',
        'unit_price': 'float64',
        'total_amount': 'float64',
        'order_date': 'datetime64[ns]',
        'region': 'object',
        'status': 'object'
    }

    expected_schema_gold: Dict[str, str] = {
        'order_date': 'datetime64[ns]',
        'region': 'object',
        'total_sales': 'float64',
        'total_quantity': 'int64'
    }

    # Critical columns for null rate checks in Silver and Gold
    critical_columns_silver: List[str] = [
        'order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_date', 'region'
    ]
    critical_columns_gold: List[str] = [
        'order_date', 'region', 'total_sales', 'total_quantity'
    ]

    dq_config = {
        "report_path": REPORT_PATH,
        "expected_schema_bronze": expected_schema_bronze,
        "expected_schema_silver": expected_schema_silver,
        "expected_schema_gold": expected_schema_gold,
        "critical_columns_silver": critical_columns_silver,
        "critical_columns_gold": critical_columns_gold,
        "max_null_rate": 0.0 # Expect 0 nulls for critical columns
    }

    checker = DataQualityChecker(dq_config)
    report = checker.run_all_checks(BRONZE_PATH, SILVER_PATH, GOLD_PATH)

    if report.get("overall_data_quality_passed"):
        logger.info("Data quality checks passed successfully!")
    else:
        logger.error("Data quality checks failed. Review the report for details.")

if __name__ == "__main__":
    main()
