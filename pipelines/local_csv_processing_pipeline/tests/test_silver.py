import pytest
import pandas as pd
from typing import Dict

from src.silver.transform_silver import process_silver


@pytest.fixture
def sample_bronze_data() -> Dict[str, pd.DataFrame]:
    """
    Provides a sample dictionary of DataFrames simulating Bronze layer output.
    Contains PII, non-ISO dates (DD/MM/YYYY), nulls, and invalid amounts.
    """
    return {
        "input_folder": pd.DataFrame({
            "order_id": [1001, 1002, None, 1004],  # Row 3 has null order_id
            "customer_id": [501, 502, 503, 504],
            "customer_name": ["Alice Smith", "Bob Jones", "Charlie Brown", "Diana Prince"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com"],
            "amount": [250.00, 15.50, 99.99, -50.00],  # Row 4 has invalid negative amount
            "status": ["COMPLETED", "PENDING", "COMPLETED", "FAILED"],
            "region": ["NA", "EU", "NA", "APAC"],
            "order_date": ["15/01/2023", "20/02/2023", "05/03/2023", "10/04/2023"] # DD/MM/YYYY format
        })
    }


def test_pii_masking(sample_bronze_data: Dict[str, pd.DataFrame]) -> None:
    """
    Test that specified PII columns are masked with '***MASKED***'.
    """
    silver_df = process_silver(sample_bronze_data)
    
    assert (silver_df["customer_name"] == "***MASKED***").all(), "customer_name column was not masked"
    assert (silver_df["email"] == "***MASKED***").all(), "email column was not masked"


def test_date_conversion(sample_bronze_data: Dict[str, pd.DataFrame]) -> None:
    """
    Test that non-ISO dates (DD/MM/YYYY) are correctly converted to standard ISO YYYY-MM-DD.
    """
    silver_df = process_silver(sample_bronze_data)
    
    # After dropping nulls and invalids, order_id 1001 and 1002 should remain
    date_values = silver_df["order_date"].astype(str).tolist()
    
    # Check if '15/01/2023' got converted properly to '2023-01-15'
    assert "2023-01-15" in date_values, "Date format was not correctly resolved to YYYY-MM-DD"


def test_null_removal(sample_bronze_data: Dict[str, pd.DataFrame]) -> None:
    """
    Test that rows with nulls in critical columns (e.g., order_id) are removed.
    """
    silver_df = process_silver(sample_bronze_data)
    
    # Original data had 4 rows, 1 missing order_id
    assert silver_df["order_id"].isnull().sum() == 0, "Null values in order_id were not removed"


def test_invalid_row_filtering(sample_bronze_data: Dict[str, pd.DataFrame]) -> None:
    """
    Test that invalid rows (e.g., negative amounts) are filtered out.
    """
    silver_df = process_silver(sample_bronze_data)
    
    # The negative amount (-50.00) row should be removed
    assert (silver_df["amount"] > 0).all(), "Rows with negative amounts were not filtered out"


def test_silver_row_count(sample_bronze_data: Dict[str, pd.DataFrame]) -> None:
    """
    Test the overall record count after applying all silver transformations.
    Expected: 2 records remaining (dropped 1 for null order_id, 1 for negative amount).
    """
    silver_df = process_silver(sample_bronze_data)
    
    assert len(silver_df) == 2, f"Expected 2 rows after filtering, but got {len(silver_df)}"