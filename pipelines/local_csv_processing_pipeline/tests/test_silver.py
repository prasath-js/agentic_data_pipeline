import pytest
import pandas as pd
from datetime import datetime

# Assuming transform_silver is defined in src/silver/transform_silver.py
# and the function name is transform_silver
from src.silver.transform_silver import transform_silver

@pytest.fixture
def sample_bronze_data() -> dict[str, pd.DataFrame]:
    """
    Provides sample Bronze layer data for testing the Silver transformation.
    Includes data for PII masking, date conversion, null removal, and invalid row filtering.
    """
    data = {
        "local_csv_data": pd.DataFrame({
            "order_id": [101, 102, 103, 104, 105, 106, 107, 108],
            "customer_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown", "David Lee", "Eve Davis", "Frank White", "Grace Green", "Hannah Black"],
            "email": ["alice@example.com", "bob@test.com", "charlie@mail.com", "david@domain.com", "eve@web.com", "frank@corp.com", "grace@email.com", "hannah@provider.com"],
            "amount": [100.50, 200.00, 50.25, -10.00, 150.75, None, 25.00, 300.00],  # -10 for invalid amount, None for null amount
            "status": ["completed", "pending", "cancelled", "completed", "invalid_status", "completed", "completed", "pending"],  # invalid_status for filtering
            "region": ["East", "West", "North", "South", "East", "West", "North", "South"],
            "order_date": ["01/01/2023", "15/02/2023", "10/03/2023", "20/04/2023", "05/05/2023", "25/06/2023", "30/07/2023", "12/08/2023"]  # DD/MM/YYYY format
        })
    }
    # Introduce nulls in critical columns (order_id, customer_id) for testing null removal
    data["local_csv_data"].loc[6, 'order_id'] = None  # Row 107, this row should be removed due to null order_id
    data["local_csv_data"].loc[7, 'customer_id'] = None  # Row 108, this row should be removed due to null customer_id
    return data

def test_silver_pii_masking(sample_bronze_data: dict[str, pd.DataFrame]) -> None:
    """
    Tests that PII columns 'customer_name' and 'email' are masked in the Silver layer.
    """
    silver_df = transform_silver(sample_bronze_data)

    assert "customer_name" in silver_df.columns
    assert "email" in silver_df.columns

    # Check that all values in PII columns are masked
    assert (silver_df["customer_name"] == "***MASKED***").all()
    assert (silver_df["email"] == "***MASKED***").all()

def test_silver_date_conversion(sample_bronze_data: dict[str, pd.DataFrame]) -> None:
    """
    Tests that 'order_date' is correctly converted to YYYY-MM-DD format and is of datetime type.
    """
    silver_df = transform_silver(sample_bronze_data)

    assert "order_date" in silver_df.columns
    
    # Check if the column is datetime type
    assert pd.api.types.is_datetime64_any_dtype(silver_df["order_date"])

    # Check specific converted dates for rows that are expected to remain (101, 102, 103)
    if 101 in silver_df["order_id"].values:
        converted_date = silver_df.loc[silver_df["order_id"] == 101, "order_date"].iloc[0]
        assert converted_date == datetime(2023, 1, 1)

    if 102 in silver_df["order_id"].values:
        converted_date = silver_df.loc[silver_df["order_id"] == 102, "order_date"].iloc[0]
        assert converted_date == datetime(2023, 2, 15)
        
    if 103 in silver_df["order_id"].values:
        converted_date = silver_df.loc[silver_df["order_id"] == 103, "order_date"].iloc[0]
        assert converted_date == datetime(2023, 3, 10)

def test_silver_null_removal(sample_bronze_data: dict[str, pd.DataFrame]) -> None:
    """
    Tests that rows with null values in critical columns ('order_id', 'customer_id', 'amount') are removed.
    """
    silver_df = transform_silver(sample_bronze_data)

    # Original rows that should be removed due to critical nulls:
    # order_id 107 (original index 6) had order_id = None
    # order_id 108 (original index 7) had customer_id = None
    # order_id 106 (original index 5) had amount = None (assuming amount is also critical for null removal)
    
    # Check that order_id 106, 107, 108 are NOT in the silver DataFrame
    assert 106 not in silver_df["order_id"].values
    assert 107 not in silver_df["order_id"].values
    assert 108 not in silver_df["order_id"].values

    # Additionally, ensure no nulls remain in specified critical columns in the output
    assert silver_df["order_id"].isnull().sum() == 0
    assert silver_df["customer_id"].isnull().sum() == 0
    assert silver_df["amount"].isnull().sum() == 0


def test_silver_invalid_row_filtering(sample_bronze_data: dict[str, pd.DataFrame]) -> None:
    """
    Tests that invalid rows (e.g., negative amount, invalid status) are filtered out.
    """
    silver_df = transform_silver(sample_bronze_data)

    # Original rows that should be removed:
    # order_id 104: amount = -10.00 (negative amount)
    # order_id 105: status = "invalid_status"

    # Check that order_id 104 and 105 are NOT in the silver DataFrame
    assert 104 not in silver_df["order_id"].values
    assert 105 not in silver_df["order_id"].values

    # Check that all remaining amounts are non-negative
    assert (silver_df["amount"] >= 0).all()

    # Check that all remaining statuses are valid
    valid_statuses = ["completed", "pending", "cancelled"]
    assert silver_df["status"].isin(valid_statuses).all()

def test_silver_output_dataframe_structure_and_types(sample_bronze_data: dict[str, pd.DataFrame]) -> None:
    """
    Tests the final structure, column names, and data types of the Silver DataFrame.
    """
    silver_df = transform_silver(sample_bronze_data)

    expected_columns = ["order_id", "customer_id", "customer_name", "email", "amount", "status", "region", "order_date"]
    assert sorted(silver_df.columns.tolist()) == sorted(expected_columns) # Ensure all expected columns are present and no extra ones

    # Check data types for key columns
    assert pd.api.types.is_integer_dtype(silver_df["order_id"])
    assert pd.api.types.is_integer_dtype(silver_df["customer_id"])
    assert pd.api.types.is_string_dtype(silver_df["customer_name"])
    assert pd.api.types.is_string_dtype(silver_df["email"])
    assert pd.api.types.is_float_dtype(silver_df["amount"])
    assert pd.api.types.is_string_dtype(silver_df["status"])
    assert pd.api.types.is_string_dtype(silver_df["region"])
    assert pd.api.types.is_datetime64_any_dtype(silver_df["order_date"])

    # Check final row count after all transformations
    # Original rows: 8
    # Removed due to:
    # - Negative amount (order_id 104)
    # - Invalid status (order_id 105)
    # - Null amount (order_id 106)
    # - Null order_id (order_id 107)
    # - Null customer_id (order_id 108)
    # Total removed: 5 rows
    # Expected remaining: 8 - 5 = 3 rows (order_id 101, 102, 103)
    assert len(silver_df) == 3
