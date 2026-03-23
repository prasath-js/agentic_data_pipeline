import unittest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock
import hashlib
import logging

# Configure logging for the test module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the src directory to the system path to allow imports from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Import the silver layer module assuming the structure src/sales_pipeline/silver.py
try:
    from sales_pipeline import silver
except ImportError as e:
    logger.error(f"Failed to import silver module: {e}")
    logger.error("Please ensure src/sales_pipeline/silver.py exists and is discoverable by the test runner.")
    sys.exit(1)


class TestSilverLayer(unittest.TestCase):
    """
    Unit tests for the sales pipeline silver layer transformations.

    This class tests the core logic of the silver layer, including
    type casting, PII masking, and deduplication, by mocking
    file I/O operations to isolate the transformation logic.
    """

    def setUp(self) -> None:
        """
        Set up common test data and configurations before each test.

        This includes sample bronze sales data containing raw values,
        duplicates, and PII, as well as pre-calculated hashed PII values
        for verification. Environment variables for staging paths are
        also mocked as the silver layer might depend on them.
        """
        logger.info("Setting up test data for TestSilverLayer.")
        # Sample bronze sales data including duplicates and PII
        self.bronze_sales_data = pd.DataFrame({
            'order_id': ['1', '2', '3', '1', '4'],
            'customer_id': ['C1', 'C2', 'C3', 'C1', 'C5'],
            'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Alice Smith', 'David Green'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'alice@example.com', 'david@example.com'],
            'product_id': ['P1', 'P2', 'P3', 'P1', 'P4'],
            'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Laptop', 'Monitor'],
            'quantity': ['1', '2', '1', '1', '3'],  # String to test type casting
            'unit_price': ['1200.50', '25.00', '75.99', '1200.50', '300.00'],  # String to test type casting
            'total_amount': ['1200.50', '50.00', '75.99', '1200.50', '900.00'],  # String to test type casting
            'order_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-01', '2023-01-04'],  # String to test type casting
            'region': ['East', 'West', 'North', 'East', 'South'],
            'status': ['Completed', 'Pending', 'Completed', 'Completed', 'Completed']
        })

        # Expected masked PII values for common test data
        self.hashed_alice = hashlib.sha256('Alice Smith'.encode()).hexdigest()
        self.hashed_laptop = hashlib.sha256('Laptop'.encode()).hexdigest()
        self.hashed_alice_email = hashlib.sha256('alice@example.com'.encode()).hexdigest()

        # Mock environment variables for paths, as the silver layer module might use them
        os.environ['BRONZE_STAGING_PATH'] = '/tmp/bronze_sales_pipeline_test'
        os.environ['SILVER_STAGING_PATH'] = '/tmp/silver_sales_pipeline_test'
        logger.info(f"Environment variables set: BRONZE_STAGING_PATH={os.environ['BRONZE_STAGING_PATH']}, SILVER_STAGING_PATH={os.environ['SILVER_STAGING_PATH']}")


    def _check_sha256_hash(self, value: str) -> bool:
        """
        Helper method to check if a string looks like a valid SHA-256 hash.

        A SHA-256 hash is a 64-character hexadecimal string.
        """
        return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)

    @patch('sales_pipeline.silver.read_bronze_sales')
    @patch('sales_pipeline.silver.write_silver_sales')
    def test_process_silver_layer_transformations(self, mock_write_silver_sales: MagicMock, mock_read_bronze_sales: MagicMock) -> None:
        """
        Test the end-to-end silver layer processing function (`process_silver_layer`).

        This test verifies that:
        1. Bronze data is read by calling `read_bronze_sales`.
        2. Transformations (deduplication, type casting, PII masking) are applied correctly.
        3. Transformed silver data is passed to `write_silver_sales`.
        """
        logger.info("Running test_process_silver_layer_transformations.")
        # Configure the mock to return our sample bronze data when read_bronze_sales is called
        mock_read_bronze_sales.return_value = self.bronze_sales_data

        # Call the main silver layer processing function
        # This function should internally call read_bronze_sales and write_silver_sales
        silver.process_silver_layer()

        # 1. Verify that read_bronze_sales was called
        mock_read_bronze_sales.assert_called_once()
        logger.info(f"read_bronze_sales called with: {mock_read_bronze_sales.call_args}")
        # Assuming the argument passed to read_bronze_sales is a path containing 'sales_bronze.parquet'
        self.assertIn('sales_bronze.parquet', mock_read_bronze_sales.call_args[0][0],
                      "read_bronze_sales should be called with a path to sales_bronze.parquet.")

        # 2. Verify that write_silver_sales was called and capture the DataFrame it was given
        self.assertTrue(mock_write_silver_sales.called, "write_silver_sales was not called.")
        logger.info(f"write_silver_sales called with: {mock_write_silver_sales.call_args}")
        written_df = mock_write_silver_sales.call_args[0][0]  # The first argument is the DataFrame

        # Basic check for DataFrame type and non-empty
        self.assertIsInstance(written_df, pd.DataFrame, "The result should be a pandas DataFrame.")
        self.assertFalse(written_df.empty, "Written DataFrame should not be empty after transformation.")

        # 3. Test Deduplication:
        # Expected unique order_ids: '1', '2', '3', '4' (original data has '1' twice)
        # Deduplication is usually based on order_id, keeping the first occurrence.
        self.assertEqual(len(written_df), 4, "DataFrame should have 4 unique rows after deduplication.")
        # Ensure 'order_id' '1' is present only once
        self.assertEqual(written_df[written_df['order_id'] == '1'].shape[0], 1,
                         "Order ID '1' should appear exactly once after deduplication.")
        # Check that the set of order_ids matches expected unique ones
        self.assertSetEqual(set(written_df['order_id'].tolist()), {'1', '2', '3', '4'},
                            "Deduplication failed or returned incorrect unique order_ids.")
        logger.info("Deduplication test passed.")

        # 4. Test Type Casting:
        self.assertEqual(str(written_df['quantity'].dtype), 'int64',
                         f"quantity should be int64, but is {written_df['quantity'].dtype}.")
        self.assertEqual(str(written_df['unit_price'].dtype), 'float64',
                         f"unit_price should be float64, but is {written_df['unit_price'].dtype}.")
        self.assertEqual(str(written_df['total_amount'].dtype), 'float64',
                         f"total_amount should be float64, but is {written_df['total_amount'].dtype}.")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(written_df['order_date']),
                        f"order_date should be datetime type, but is {written_df['order_date'].dtype}.")
        logger.info("Type casting test passed.")

        # Check a specific row's converted values for correctness (after deduplication, '1' should be the first instance)
        row_id_1 = written_df[written_df['order_id'] == '1'].iloc[0]
        self.assertEqual(row_id_1['quantity'], 1, "Quantity for order '1' should be 1 (int).")
        self.assertEqual(row_id_1['unit_price'], 1200.50, "Unit price for order '1' should be 1200.50 (float).")
        self.assertEqual(row_id_1['total_amount'], 1200.50, "Total amount for order '1' should be 1200.50 (float).")
        self.assertEqual(row_id_1['order_date'], pd.Timestamp('2023-01-01'),
                         "Order date for order '1' should be 2023-01-01 (datetime).")

        # 5. Test PII Masking:
        # Check if customer_name, customer_email, product_name are hashed for the first deduplicated row ('order_id' == '1')
        self.assertTrue(self._check_sha256_hash(row_id_1['customer_name']),
                        "customer_name should be SHA-256 hashed.")
        self.assertTrue(self._check_sha256_hash(row_id_1['customer_email']),
                        "customer_email should be SHA-256 hashed.")
        self.assertTrue(self._check_sha256_hash(row_id_1['product_name']),
                        "product_name should be SHA-256 hashed.")

        # Verify specific hash values against pre-calculated hashes
        self.assertEqual(row_id_1['customer_name'], self.hashed_alice, "customer_name hash mismatch.")
        self.assertEqual(row_id_1['customer_email'], self.hashed_alice_email, "customer_email hash mismatch.")
        self.assertEqual(row_id_1['product_name'], self.hashed_laptop, "product_name hash mismatch.")

        # Ensure other non-PII columns are untouched (e.g., customer_id, region, status)
        self.assertEqual(row_id_1['customer_id'], 'C1', "customer_id should remain unchanged.")
        self.assertEqual(row_id_1['region'], 'East', "region should remain unchanged.")
        self.assertEqual(row_id_1['status'], 'Completed', "status should remain unchanged.")
        logger.info("PII masking test passed.")
        logger.info("Test test_process_silver_layer_transformations completed successfully.")


if __name__ == '__main__':
    unittest.main()
