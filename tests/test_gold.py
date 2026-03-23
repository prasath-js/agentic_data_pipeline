import unittest
import pandas as pd
from datetime import datetime
from src.gold.sales_pipeline_gold import aggregate_sales_data

class TestGoldLayer(unittest.TestCase):
    """
    Unit tests for the Gold layer aggregations of the sales pipeline.
    """

    def setUp(self) -> None:
        """
        Set up common test data for the Gold layer tests.
        """
        # Sample silver data reflecting the output of the silver layer
        self.silver_data = pd.DataFrame({
            'order_id': ['1', '2', '3', '4', '5'],
            'customer_id': ['C1', 'C2', 'C1', 'C3', 'C2'],
            'customer_name_masked': ['hash1', 'hash2', 'hash1', 'hash3', 'hash2'],
            'customer_email_masked': ['hash_e1', 'hash_e2', 'hash_e1', 'hash_e3', 'hash_e2'],
            'product_id': ['P1', 'P2', 'P1', 'P3', 'P2'],
            'product_name_masked': ['hash_p1', 'hash_p2', 'hash_p1', 'hash_p3', 'hash_p2'],
            'quantity': [2, 1, 3, 1, 2],
            'unit_price': [10.0, 25.0, 10.0, 50.0, 25.0],
            'total_amount': [20.0, 25.0, 30.0, 50.0, 50.0],
            'order_date': [
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 2).date(),
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 2).date()
            ],
            'region': ['East', 'West', 'East', 'South', 'West'],
            'status': ['completed', 'pending', 'completed', 'completed', 'pending']
        })

    def test_aggregate_sales_data_basic(self) -> None:
        """
        Test basic aggregation of sales data.
        Verifies correct sum of total_sales and total_quantity by order_date and region.
        """
        aggregated_df = aggregate_sales_data(self.silver_data.copy())

        # Expected data for 2023-01-01, East: total_amount=20.0, quantity=2
        # Expected data for 2023-01-01, West: total_amount=25.0, quantity=1
        # Expected data for 2023-01-01, South: total_amount=50.0, quantity=1
        # Expected data for 2023-01-02, East: total_amount=30.0, quantity=3
        # Expected data for 2023-01-02, West: total_amount=50.0, quantity=2

        expected_data = pd.DataFrame({
            'order_date': [
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 1).date(),
                datetime(2023, 1, 2).date(),
                datetime(2023, 1, 2).date()
            ],
            'region': ['East', 'South', 'West', 'East', 'West'],
            'total_sales': [20.0, 50.0, 25.0, 30.0, 50.0],
            'total_quantity': [2, 1, 1, 3, 2]
        })

        # Sort both dataframes to ensure consistent order for comparison
        aggregated_df = aggregated_df.sort_values(by=['order_date', 'region']).reset_index(drop=True)
        expected_data = expected_data.sort_values(by=['order_date', 'region']).reset_index(drop=True)

        pd.testing.assert_frame_equal(aggregated_df, expected_data, check_dtype=True)

    def test_aggregate_sales_data_empty_input(self) -> None:
        """
        Test aggregation with an empty input DataFrame.
        Should return an empty DataFrame with the correct columns.
        """
        empty_df = pd.DataFrame(columns=[
            'order_id', 'customer_id', 'customer_name_masked', 'customer_email_masked',
            'product_id', 'product_name_masked', 'quantity', 'unit_price',
            'total_amount', 'order_date', 'region', 'status'
        ])
        aggregated_df = aggregate_sales_data(empty_df)

        expected_columns = ['order_date', 'region', 'total_sales', 'total_quantity']
        self.assertTrue(aggregated_df.empty)
        self.assertListEqual(list(aggregated_df.columns), expected_columns)

    def test_aggregate_sales_data_single_entry(self) -> None:
        """
        Test aggregation with a single entry in the input DataFrame.
        """
        single_entry_df = pd.DataFrame({
            'order_id': ['1'],
            'customer_id': ['C1'],
            'customer_name_masked': ['hash1'],
            'customer_email_masked': ['hash_e1'],
            'product_id': ['P1'],
            'product_name_masked': ['hash_p1'],
            'quantity': [5],
            'unit_price': [10.0],
            'total_amount': [50.0],
            'order_date': [datetime(2023, 1, 1).date()],
            'region': ['North'],
            'status': ['completed']
        })
        aggregated_df = aggregate_sales_data(single_entry_df)

        expected_data = pd.DataFrame({
            'order_date': [datetime(2023, 1, 1).date()],
            'region': ['North'],
            'total_sales': [50.0],
            'total_quantity': [5]
        })
        pd.testing.assert_frame_equal(aggregated_df, expected_data, check_dtype=True)

    def test_aggregate_sales_data_multiple_regions_same_date(self) -> None:
        """
        Test aggregation with multiple regions for the same order date.
        """
        data_multiple_regions = pd.DataFrame({
            'order_id': ['1', '2', '3'],
            'customer_id': ['C1', 'C2', 'C3'],
            'customer_name_masked': ['hash1', 'hash2', 'hash3'],
            'customer_email_masked': ['hash_e1', 'hash_e2', 'hash_e3'],
            'product_id': ['P1', 'P2', 'P3'],
            'product_name_masked': ['hash_p1', 'hash_p2', 'hash_p3'],
            'quantity': [10, 5, 2],
            'unit_price': [10.0, 20.0, 30.0],
            'total_amount': [100.0, 100.0, 60.0],
            'order_date': [
                datetime(2023, 2, 1).date(),
                datetime(2023, 2, 1).date(),
                datetime(2023, 2, 1).date()
            ],
            'region': ['North', 'South', 'North'],
            'status': ['completed', 'completed', 'pending']
        })
        aggregated_df = aggregate_sales_data(data_multiple_regions)

        expected_data = pd.DataFrame({
            'order_date': [
                datetime(2023, 2, 1).date(),
                datetime(2023, 2, 1).date()
            ],
            'region': ['North', 'South'],
            'total_sales': [160.0, 100.0],
            'total_quantity': [12, 5]
        })

        aggregated_df = aggregated_df.sort_values(by=['order_date', 'region']).reset_index(drop=True)
        expected_data = expected_data.sort_values(by=['order_date', 'region']).reset_index(drop=True)

        pd.testing.assert_frame_equal(aggregated_df, expected_data, check_dtype=True)

if __name__ == '__main__':
    unittest.main()
