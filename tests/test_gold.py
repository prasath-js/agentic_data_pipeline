import unittest
import pandas as pd
from datetime import datetime
import sys
import os

# Add the project root to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.sales_pipeline.gold import transform_gold_data, GOLD_OUTPUT_PATH, GOLD_FILE_NAME

class TestGoldLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test data for all tests."""
        # Create a dummy silver DataFrame
        cls.silver_df = pd.DataFrame({
            'order_id': [1, 2, 3, 4, 5, 6],
            'customer_id': ['C1', 'C2', 'C1', 'C3', 'C2', 'C1'],
            'customer_name_hashed': ['hash1', 'hash2', 'hash1', 'hash3', 'hash2', 'hash1'],
            'customer_email_hashed': ['emailhash1', 'emailhash2', 'emailhash1', 'emailhash3', 'emailhash2', 'emailhash1'],
            'product_id': ['P1', 'P2', 'P1', 'P3', 'P2', 'P1'],
            'product_name_hashed': ['prodhash1', 'prodhash2', 'prodhash1', 'prodhash3', 'prodhash2', 'prodhash1'],
            'quantity': [10, 5, 20, 15, 8, 12],
            'unit_price': [10.0, 20.0, 10.0, 5.0, 20.0, 10.0],
            'total_amount': [100.0, 100.0, 200.0, 75.0, 160.0, 120.0],
            'order_date': [datetime(2023, 1, 1), datetime(2023, 1, 1), datetime(2023, 1, 2),
                           datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 3)],
            'region': ['East', 'West', 'East', 'Central', 'West', 'East'],
            'status': ['completed', 'completed', 'pending', 'completed', 'completed', 'completed']
        })

        # Ensure the output directory exists for writing files
        os.makedirs(GOLD_OUTPUT_PATH, exist_ok=True)

    def test_aggregation_logic(self):
        """Test if the aggregation logic for total sales and quantity is correct."""
        gold_df = transform_gold_data(self.silver_df)

        # Expected data after aggregation
        expected_data = {
            'order_date': [datetime(2023, 1, 1), datetime(2023, 1, 1),
                           datetime(2023, 1, 2), datetime(2023, 1, 2),
                           datetime(2023, 1, 3), datetime(2023, 1, 3)],
            'region': ['East', 'West', 'Central', 'East', 'East', 'West'],
            'total_sales': [100.0, 100.0, 75.0, 200.0, 120.0, 160.0],
            'total_quantity_sold': [10, 5, 15, 20, 12, 8]
        }
        expected_df = pd.DataFrame(expected_data)
        # Sort both dataframes for consistent comparison
        gold_df_sorted = gold_df.sort_values(by=['order_date', 'region']).reset_index(drop=True)
        expected_df_sorted = expected_df.sort_values(by=['order_date', 'region']).reset_index(drop=True)

        pd.testing.assert_frame_equal(gold_df_sorted, expected_df_sorted, check_dtype=True)

    def test_output_columns(self):
        """Test if the output DataFrame contains the correct columns."""
        gold_df = transform_gold_data(self.silver_df)
        expected_columns = ['order_date', 'region', 'total_sales', 'total_quantity_sold']
        self.assertListEqual(list(gold_df.columns), expected_columns)

    def test_empty_silver_dataframe(self):
        """Test behavior with an empty silver DataFrame."""
        empty_silver_df = pd.DataFrame(columns=[
            'order_id', 'customer_id', 'customer_name_hashed', 'customer_email_hashed',
            'product_id', 'product_name_hashed', 'quantity', 'unit_price', 'total_amount',
            'order_date', 'region', 'status'
        ])
        gold_df = transform_gold_data(empty_silver_df)
        self.assertTrue(gold_df.empty)
        self.assertListEqual(list(gold_df.columns), ['order_date', 'region', 'total_sales', 'total_quantity_sold'])

    def test_data_types(self):
        """Test if the data types of the output columns are correct."""
        gold_df = transform_gold_data(self.silver_df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(gold_df['order_date']))
        self.assertTrue(pd.api.types.is_string_dtype(gold_df['region']))
        self.assertTrue(pd.api.types.is_float_dtype(gold_df['total_sales']))
        self.assertTrue(pd.api.types.is_integer_dtype(gold_df['total_quantity_sold']))

    def test_single_row_data(self):
        """Test with a single row in the silver DataFrame."""
        single_row_df = pd.DataFrame({
            'order_id': [1],
            'customer_id': ['C1'],
            'customer_name_hashed': ['hash1'],
            'customer_email_hashed': ['emailhash1'],
            'product_id': ['P1'],
            'product_name_hashed': ['prodhash1'],
            'quantity': [10],
            'unit_price': [10.0],
            'total_amount': [100.0],
            'order_date': [datetime(2023, 1, 1)],
            'region': ['East'],
            'status': ['completed']
        })
        gold_df = transform_gold_data(single_row_df)
        expected_df = pd.DataFrame({
            'order_date': [datetime(2023, 1, 1)],
            'region': ['East'],
            'total_sales': [100.0],
            'total_quantity_sold': [10]
        })
        pd.testing.assert_frame_equal(gold_df.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=True)

if __name__ == '__main__':
    unittest.main()
