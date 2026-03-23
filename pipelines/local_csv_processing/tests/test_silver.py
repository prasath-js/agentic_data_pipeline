import pandas as pd
import pytest
from datetime import datetime
import numpy as np

# Adjust import path based on the project structure
from src.silver.transform_silver import transform_silver
from src.utils.pii_masker import mask_dataframe_columns # Used for setup/validation


@pytest.fixture
def sample_bronze_data() -> pd.DataFrame:
    """
    Fixture to provide a sample bronze DataFrame for testing.
    Includes data for PII masking, date conversion, nulls, and invalid rows.
    """
    data = {
        "order_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "customer_id": [101, 102, 103, 104, 105, 106, 107, None],  # Null customer_id for testing
        "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown", "David Green", "Eve White", "Frank Black", "Grace Hall", "Hannah Grey"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "david@example.com", "eve@example.com", "frank@example.com", "grace@example.com", "hannah@example.com"],
        "amount": [100.50, 200.75, -50.00, 300.20, 150.00, 0.00, 250.00, 400.00], # Negative/zero amount for filtering
        "status": ["completed", "pending", "returned", "shipped", "invalid", "completed", "pending", "completed"], # Invalid status for filtering
        "region": ["North", "South", "East", "West", "North", "South", "East", "West"],
        "order_date": ["01/01/2023", "15/02/2023", "10/03/2023", "20/04/2023", "05/05/2023", "12/06/2023", "25/07/2023", "30/08/2023"] # DD/MM/YYYY format
    }
    df = pd.DataFrame(data)
    # Introduce a null order_id to test null removal
    df.loc[2, 'order_id'] = None
    return df


def test_pii_masking(sample_bronze_data: pd.DataFrame) -> None:
    """
    Tests that PII columns 'customer_name' and 'email' are masked.
    """
    bronze_dataframes = {"input_csv_folder": sample_bronze_data}
    silver_df = transform_silver(bronze_dataframes)

    expected_mask_value = "***MASKED***"
    assert (silver_df["customer_name"] == expected_mask_value).all()
    assert (silver_df["email"] == expected_mask_value).all()


def test_date_conversion(sample_bronze_data: pd.DataFrame) -> None:
    """
    Tests that 'order_date' column is converted from DD/MM/YYYY to YYYY-MM-DD.
    The transformation should convert it to datetime objects, which can then be formatted.
    """
    bronze_dataframes = {"input_csv_folder": sample_bronze_data}
    silver_df = transform_silver(bronze_dataframes)

    # Filter out rows that might have been dropped due to other transformations
    # and ensure 'order_date' is still present.
    # The original row 2 was dropped due to null order_id.
    # Rows with amount <=0 (3, 5) or invalid status (4) will also be dropped.
    # The original row 7 (index 7) with None customer_id will also be dropped.
    # So, we expect rows 0, 1, 6
    expected_dates = ["2023-01-01", "2023-02-15", "2023-07-25"]
    
    # After transformation, order_date should be datetime objects
    assert pd.api.types.is_datetime64_any_dtype(silver_df['order_date'])

    # Convert to string for direct comparison with expected YYYY-MM-DD format
    actual_dates_str = silver_df['order_date'].dt.strftime('%Y-%m-%d').tolist()

    assert actual_dates_str == expected_dates


def test_null_removal(sample_bronze_data: pd.DataFrame) -> None:
    """
    Tests that rows with null values in critical columns ('order_id', 'customer_id') are removed.
    """
    bronze_dataframes = {"input_csv_folder": sample_bronze_data}
    silver_df = transform_silver(bronze_dataframes)

    # Original sample_bronze_data:
    # Row 2 has order_id = None
    # Row 7 has customer_id = None
    # These two rows should be removed due to nulls in critical columns.
    # Also, rows 3, 5 (amount <= 0) and 4 (invalid status) will be removed later.
    # So, the final DataFrame should not contain any of these original rows.

    # Check that 'order_id' and 'customer_id' columns have no nulls
    assert silver_df["order_id"].isnull().sum() == 0
    assert silver_df["customer_id"].isnull().sum() == 0

    # Ensure the specific rows that *had* nulls are not in the final DataFrame
    # Original order_id for row 2 was 3, for row 7 was 8. These should not be present.
    assert 3 not in silver_df["order_id"].values
    assert 8 not in silver_df["order_id"].values # Row 7's order_id was 8


def test_invalid_row_filtering(sample_bronze_data: pd.DataFrame) -> None:
    """
    Tests that invalid rows (amount <= 0 or invalid status) are filtered out.
    """
    bronze_dataframes = {"input_csv_folder": sample_bronze_data}
    silver_df = transform_silver(bronze_dataframes)

    # Original sample_bronze_data (after null removal):
    # Rows with amount <= 0:
    #   - order_id 3 (index 2, but already removed by nulls)
    #   - order_id 5 (index 4) - amount is 150.00 - NOT THIS ONE
    #   - order_id 6 (index 5) - amount is 0.00
    #
    # Rows with invalid status:
    #   - order_id 3 (index 2, but already removed by nulls) - status 'returned' (valid for example logic)
    #   - order_id 5 (index 4) - status 'invalid'
    #   - order_id 6 (index 5) - status 'completed' (valid)

    # Based on the sample data and expected transformations:
    # Original IDs: [1, 2, 3, 4, 5, 6, 7, 8]
    # Removed due to nulls: 3 (order_id=None), 8 (customer_id=None)
    # Remaining IDs: [1, 2, 4, 5, 6, 7]

    # From remaining:
    # ID 1: amount=100.50, status=completed -> KEEP
    # ID 2: amount=200.75, status=pending -> KEEP
    # ID 4: amount=300.20, status=shipped -> KEEP
    # ID 5: amount=150.00, status=invalid -> REMOVE (invalid status)
    # ID 6: amount=0.00, status=completed -> REMOVE (amount <= 0)
    # ID 7: amount=250.00, status=pending -> KEEP

    # Expected order_ids in the final silver_df: [1, 2, 4, 7]
    expected_order_ids = {1, 2, 4, 7}
    actual_order_ids = set(silver_df["order_id"].values)

    assert actual_order_ids == expected_order_ids
    assert len(silver_df) == 4 # Total expected rows


def test_output_dataframe_structure(sample_bronze_data: pd.DataFrame) -> None:
    """
    Tests that the output DataFrame contains expected columns and appropriate data types.
    """
    bronze_dataframes = {"input_csv_folder": sample_bronze_data}
    silver_df = transform_silver(bronze_dataframes)

    expected_columns = [
        "order_id", "customer_id", "customer_name", "email",
        "amount", "status", "region", "order_date"
    ]
    assert list(silver_df.columns) == expected_columns

    # Check data types for key columns
    assert pd.api.types.is_integer_dtype(silver_df['order_id']) or pd.api.types.is_int64_dtype(silver_df['order_id'])
    assert pd.api.types.is_integer_dtype(silver_df['customer_id']) or pd.api.types.is_int64_dtype(silver_df['customer_id'])
    assert pd.api.types.is_string_dtype(silver_df['customer_name'])
    assert pd.api.types.is_string_dtype(silver_df['email'])
    assert pd.api.types.is_float_dtype(silver_df['amount'])
    assert pd.api.types.is_string_dtype(silver_df['status'])
    assert pd.api.types.is_string_dtype(silver_df['region'])
    assert pd.api.types.is_datetime64_any_dtype(silver_df['order_date'])