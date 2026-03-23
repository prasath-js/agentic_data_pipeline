import unittest
import pandas as pd
import hashlib
import os
import logging
from unittest.mock import patch, MagicMock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mock the os.getenv to provide test values
@patch('os.getenv')
class TestSilverLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up class-level resources."""
        cls.test_data_path = 'test_bronze_sales.parquet'
        cls.output_silver_path = 'test_silver_sales.parquet'

    def setUp(self):
        """Set up test-specific resources."""
        # Configure the class-level patched os.getenv directly
        os.getenv.side_effect = lambda x, default=None: {
            'SALES_PIPELINE_SILVER_STAGE_DIR': 'tmp_silver',
            'SALES_PIPELINE_BRONZE_STAGE_DIR': 'tmp_bronze',
        }.get(x, default)
        
        # Ensure temporary directories exist for the test
        os.makedirs(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR'), exist_ok=True)
        os.makedirs(os.getenv('SALES_PIPELINE_SILVER_STAGE_DIR'), exist_ok=True)

        # Create a dummy bronze layer parquet file for testing
        bronze_df = pd.DataFrame({
            'order_id': [1, 2, 3, 4],
            'customer_id': [101, 102, 101, 103],
            'customer_name': ['Alice Smith', 'Bob Johnson', 'Alice Smith', 'Charlie Brown'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'alice@example.com', 'charlie@example.com'],
            'product_id': [1001, 1002, 1001, 1003],
            'product_name': ['Laptop', 'Mouse', 'Laptop', 'Keyboard'],
            'quantity': ['1', '2', '1', '3'],  # Test string to int conversion
            'unit_price': ['1200.50', '25.00', '1200.50', '75.25'], # Test string to float conversion
            'total_amount': ['1200.50', '50.00', '1200.50', '225.75'], # Test string to float conversion
            'order_date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03'],
            'region': ['East', 'West', 'East', 'North'],
            'status': ['Completed', 'Pending', 'Completed', 'Completed']
        })
        bronze_file_path = os.path.join(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR'), self.test_data_path)
        bronze_df.to_parquet(bronze_file_path, index=False)
        logger.info(f"Created dummy bronze data at {bronze_file_path}")

    def tearDown(self):
        """Clean up after each test."""
        bronze_file_path = os.path.join(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR'), self.test_data_path)
        silver_file_path = os.path.join(os.getenv('SALES_PIPELINE_SILVER_STAGE_DIR'), self.output_silver_path)

        if os.path.exists(bronze_file_path):
            os.remove(bronze_file_path)
        if os.path.exists(silver_file_path):
            os.remove(silver_file_path)

        # Remove temporary directories if empty
        if os.path.exists(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR')) and not os.listdir(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR')):
            os.rmdir(os.getenv('SALES_PIPELINE_BRONZE_STAGE_DIR'))
        if os.path.exists(os.getenv('SALES_PIPELINE_SILVER_STAGE_DIR')) and not os.listdir(os.getenv('SALES_PIPELINE_SILVER_STAGE_DIR')):
            os.rmdir(os.getenv('SALES_PIPELINE_SILVER_STAGE_DIR'))
        logger.info("Cleaned up temporary files and directories.")

    def test_silver_layer_transformation(self, mock_getenv):
        """
        Test the complete silver layer transformation process, including
        type casting, PII masking, and output to parquet.
        """
        # The 'mock_getenv' argument is the same mock as 'os.getenv'
        # which was configured in setUp. No need to re-assign side_effect.

        # --- Simulate the silver layer script execution ---
        bronze_stage_dir = mock_getenv('SALES_PIPELINE_BRONZE_STAGE_DIR')
        silver_stage_dir = mock_getenv('SALES_PIPELINE_SILVER_STAGE_DIR')
        bronze_file_name = self.test_data_path
        silver_file_name = self.output_silver_path

        input_path = os.path.join(bronze_stage_dir, bronze_file_name)
        output_path = os.path.join(silver_stage_dir, silver_file_name)

        # Read bronze data
        try:
            df = pd.read_parquet(input_path)
            logger.info(f"Successfully read bronze data from {input_path}. Rows: {len(df)}")
        except Exception as e:
            self.fail(f"Failed to read bronze data: {e}")

        # Apply type casting
        df['quantity'] = pd.to_numeric(df['quantity'])
        df['unit_price'] = pd.to_numeric(df['unit_price'])
        df['total_amount'] = pd.to_numeric(df['total_amount'])
        df['order_date'] = pd.to_datetime(df['order_date'])
        logger.info("Applied type casting.")

        # Apply PII masking
        pii_columns = ['customer_name', 'customer_email', 'product_name']
        for col in pii_columns:
            df[col] = df[col].astype(str).apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
        logger.info("Applied PII masking.")

        # Write silver data
        try:
            df.to_parquet(output_path, index=False)
            logger.info(f"Successfully wrote silver data to {output_path}. Rows: {len(df)}")
        except Exception as e:
            self.fail(f"Failed to write silver data: {e}")

        # --- Assertions ---

        # 1. Check if the silver file was created
        self.assertTrue(os.path.exists(output_path))
        logger.info(f"Verified silver output file exists at {output_path}")

        # 2. Read the silver data and check its contents
        silver_df = pd.read_parquet(output_path)
        logger.info(f"Read silver data for assertions. Rows: {len(silver_df)}")

        # Check row count
        self.assertEqual(len(silver_df), 4)

        # Check column names (should be the same as bronze after transformations)
        expected_columns = [
            'order_id', 'customer_id', 'customer_name', 'customer_email',
            'product_id', 'product_name', 'quantity', 'unit_price',
            'total_amount', 'order_date', 'region', 'status'
        ]
        self.assertListEqual(list(silver_df.columns), expected_columns)

        # Check data types after casting
        self.assertEqual(silver_df['quantity'].dtype, 'int64')
        self.assertEqual(silver_df['unit_price'].dtype, 'float64')
        self.assertEqual(silver_df['total_amount'].dtype, 'float64')
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(silver_df['order_date']))
        logger.info("Verified data types after casting.")

        # Check PII masking
        original_customer_name = 'Alice Smith'
        hashed_customer_name = hashlib.sha256(original_customer_name.encode()).hexdigest()
        self.assertEqual(silver_df.loc[0, 'customer_name'], hashed_customer_name)
        self.assertNotEqual(silver_df.loc[0, 'customer_name'], original_customer_name)

        original_customer_email = 'bob@example.com'
        hashed_customer_email = hashlib.sha256(original_customer_email.encode()).hexdigest()
        self.assertEqual(silver_df.loc[1, 'customer_email'], hashed_customer_email)
        self.assertNotEqual(silver_df.loc[1, 'customer_email'], original_customer_email)

        original_product_name = 'Keyboard'
        hashed_product_name = hashlib.sha256(original_product_name.encode()).hexdigest()
        self.assertEqual(silver_df.loc[3, 'product_name'], hashed_product_name)
        self.assertNotEqual(silver_df.loc[3, 'product_name'], original_product_name)
        logger.info("Verified PII masking.")

        # Check numerical values after conversion
        self.assertEqual(silver_df.loc[0, 'quantity'], 1)
        self.assertEqual(silver_df.loc[1, 'unit_price'], 25.00)
        self.assertEqual(silver_df.loc[2, 'total_amount'], 1200.50)
        logger.info("Verified numerical values.")

        # Check date values after conversion
        self.assertEqual(str(silver_df.loc[0, 'order_date']), '2023-01-01 00:00:00')
        logger.info("Verified date values.")

if __name__ == '__main__':
    unittest.main()
