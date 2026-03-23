import unittest
import os
import pandas as pd
import tempfile
import shutil
import logging
from unittest.mock import patch, MagicMock
from datetime import datetime

# Configure logging for the test module
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import the module under test
# Assuming the bronze layer code is in src/sales_pipeline/bronze.py
try:
    from src.sales_pipeline import bronze
except ImportError:
    logger.error("Could not import src.sales_pipeline.bronze. Make sure the package structure is correct and __init__.py files exist.")
    raise

class TestBronzeLayer(unittest.TestCase):
    """
    Unit tests for the sales_pipeline bronze layer ingestion process.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Set up common resources for all tests in this class."""
        logger.info("Setting up TestBronzeLayer class.")
        cls.temp_dir = tempfile.mkdtemp()
        logger.info(f"Created temporary directory: {cls.temp_dir}")

        cls.sales_source_path = os.path.join(cls.temp_dir, "sales.csv")
        cls.bronze_staging_path = os.path.join(cls.temp_dir, "bronze")

        # Sample data for sales.csv
        cls.sales_data = {
            'order_id': [1, 2, 3, 4],
            'customer_id': [101, 102, 103, 104],
            'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Diana Miller'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com'],
            'product_id': [1001, 1002, 1001, 1003],
            'product_name': ['Laptop', 'Mouse', 'Laptop', 'Keyboard'],
            'quantity': [1, 2, 1, 3],
            'unit_price': [1200.00, 25.50, 1200.00, 75.00],
            'total_amount': [1200.00, 51.00, 1200.00, 225.00],
            'order_date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02'],
            'region': ['North', 'South', 'North', 'West'],
            'status': ['Completed', 'Pending', 'Completed', 'Completed']
        }
        cls.sales_df = pd.DataFrame(cls.sales_data)

        # Write the dummy CSV file
        cls.sales_df.to_csv(cls.sales_source_path, index=False)
        logger.info(f"Created dummy sales.csv at {cls.sales_source_path}")

    @classmethod
    def tearDownClass(cls) -> None:
        """Clean up common resources after all tests in this class."""
        logger.info(f"Tearing down TestBronzeLayer class. Removing temporary directory: {cls.temp_dir}")
        if os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    @patch.dict(os.environ, {
        "BRONZE_STAGING_PATH": "", # Will be set dynamically by test methods
        "SALES_SOURCE_PATH": ""    # Will be set dynamically by test methods
    })
    def test_ingest_sales_data_source_success(self) -> None:
        """
        Test successful ingestion of sales data to the bronze layer.
        """
        current_date_str = datetime.now().strftime("%Y%m%d")
        expected_output_filename = f"sales_{current_date_str}.parquet"
        expected_output_path = os.path.join(self.bronze_staging_path, expected_output_filename)

        logger.info(f"Running test_ingest_sales_data_source_success. Expected output: {expected_output_path}")

        # Set environment variables for this test
        os.environ["BRONZE_STAGING_PATH"] = self.bronze_staging_path
        os.environ["SALES_SOURCE_PATH"] = self.sales_source_path

        # Ensure bronze staging directory is clean before test
        if os.path.exists(self.bronze_staging_path):
            shutil.rmtree(self.bronze_staging_path)
        os.makedirs(self.bronze_staging_path)

        try:
            # Call the main function of the bronze layer, which should orchestrate ingestion
            bronze.main()

            # Assertions
            self.assertTrue(os.path.exists(expected_output_path),
                            f"Expected parquet file not found at {expected_output_path}")

            ingested_df = pd.read_parquet(expected_output_path)

            self.assertEqual(len(ingested_df), len(self.sales_df),
                             "Ingested DataFrame row count mismatch.")
            pd.testing.assert_frame_equal(ingested_df, self.sales_df, check_dtype=True)

            logger.info("Test test_ingest_sales_data_source_success passed successfully.")
        except Exception as e:
            self.fail(f"test_ingest_sales_data_source_success failed with an unexpected error: {e}")
        finally:
            # Clean up generated files/dirs for this specific test, though tearDownClass will handle the main temp_dir
            if os.path.exists(self.bronze_staging_path):
                shutil.rmtree(self.bronze_staging_path)


    @patch.dict(os.environ, {
        "BRONZE_STAGING_PATH": "",
        "SALES_SOURCE_PATH": ""
    })
    def test_ingest_sales_data_source_missing_source_path(self) -> None:
        """
        Test ingestion when SALES_SOURCE_PATH environment variable is missing.
        """
        logger.info("Running test_ingest_sales_data_source_missing_source_path.")
        os.environ["BRONZE_STAGING_PATH"] = self.bronze_staging_path
        # SALES_SOURCE_PATH is intentionally not set

        with self.assertRaisesRegex(ValueError, "SALES_SOURCE_PATH not set."):
            bronze.main()
        logger.info("Test test_ingest_sales_data_source_missing_source_path passed successfully.")

    @patch.dict(os.environ, {
        "BRONZE_STAGING_PATH": "",
        "SALES_SOURCE_PATH": ""
    })
    def test_ingest_sales_data_source_missing_staging_path(self) -> None:
        """
        Test ingestion when BRONZE_STAGING_PATH environment variable is missing.
        """
        logger.info("Running test_ingest_sales_data_source_missing_staging_path.")
        # BRONZE_STAGING_PATH is intentionally not set
        os.environ["SALES_SOURCE_PATH"] = self.sales_source_path

        with self.assertRaisesRegex(ValueError, "BRONZE_STAGING_PATH not set."):
            bronze.main()
        logger.info("Test test_ingest_sales_data_source_missing_staging_path passed successfully.")

    @patch('pandas.read_csv', side_effect=pd.errors.EmptyDataError("No columns to parse from file"))
    @patch.dict(os.environ, {
        "BRONZE_STAGING_PATH": "",
        "SALES_SOURCE_PATH": ""
    })
    def test_ingest_sales_data_source_empty_csv(self, mock_read_csv: MagicMock) -> None:
        """
        Test ingestion with an empty CSV file.
        """
        logger.info("Running test_ingest_sales_data_source_empty_csv.")
        os.environ["BRONZE_STAGING_PATH"] = self.bronze_staging_path
        os.environ["SALES_SOURCE_PATH"] = self.sales_source_path # Path still points to mock source

        # Ensure bronze staging directory is clean before test
        if os.path.exists(self.bronze_staging_path):
            shutil.rmtree(self.bronze_staging_path)
        os.makedirs(self.bronze_staging_path)

        with self.assertRaises(pd.errors.EmptyDataError):
            bronze.main()

        # No parquet file should be created for an empty CSV if read_csv fails early
        current_date_str = datetime.now().strftime("%Y%m%d")
        expected_output_filename = f"sales_{current_date_str}.parquet"
        expected_output_path = os.path.join(self.bronze_staging_path, expected_output_filename)
        self.assertFalse(os.path.exists(expected_output_path),
                         f"Parquet file was created unexpectedly at {expected_output_path}")

        logger.info("Test test_ingest_sales_data_source_empty_csv passed successfully.")
        if os.path.exists(self.bronze_staging_path):
            shutil.rmtree(self.bronze_staging_path)

    @patch('pandas.read_csv', side_effect=IOError("File not found"))
    @patch.dict(os.environ, {
        "BRONZE_STAGING_PATH": "",
        "SALES_SOURCE_PATH": ""
    })
    def test_ingest_sales_data_source_file_not_found(self, mock_read_csv: MagicMock) -> None:
        """
        Test ingestion when the source CSV file does not exist.
        """
        logger.info("Running test_ingest_sales_data_source_file_not_found.")
        os.environ["BRONZE_STAGING_PATH"] = self.bronze_staging_path
        os.environ["SALES_SOURCE_PATH"] = "/nonexistent/path/sales.csv" # Mock a non-existent path

        # Ensure bronze staging directory is clean before test
        if os.path.exists(self.bronze_staging_path):
            shutil.rmtree(self.bronze_staging_path)
        os.makedirs(self.bronze_staging_path)

        with self.assertRaises(IOError):
            bronze.main()

        # No parquet file should be created
        current_date_str = datetime.now().strftime("%Y%m%d")
        expected_output_filename = f"sales_{current_date_str}.parquet"
        expected_output_path = os.path.join(self.bronze_staging_path, expected_output_filename)
        self.assertFalse(os.path.exists(expected_output_path),
                         f"Parquet file was created unexpectedly at {expected_output_path}")

        logger.info("Test test_ingest_sales_data_source_file_not_found passed successfully.")
        if os.path.exists(self.bronze_staging_path):
            shutil.rmtree(self.bronze_staging_path)


if __name__ == '__main__':
    unittest.main()
