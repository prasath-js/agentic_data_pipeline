import os
import pytest
import pandas as pd
from pathlib import Path
from src.gold.gold_local_files import process_gold_data

@pytest.fixture
def sample_silver_data() -> pd.DataFrame:
    """
    Provides a sample Silver-level DataFrame with cleaned data and masked PII.
    """
    return pd.DataFrame({
        "order_id": ["O001", "O002", "O003", "O004"],
        "customer_id": ["C001", "C002", "C001", "C003"],
        "customer_name": ["***MASKED***", "***MASKED***", "***MASKED***", "***MASKED***"],
        "email": ["***MASKED***", "***MASKED***", "***MASKED***", "***MASKED***"],
        "amount": [100.0, 200.0, 150.0, 300.0],
        "status": ["COMPLETED", "PENDING", "COMPLETED", "COMPLETED"],
        "region": ["NORTH", "SOUTH", "NORTH", "SOUTH"],
        "order_date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]
    })

def test_gold_aggregation_and_output(sample_silver_data: pd.DataFrame, tmp_path: Path) -> None:
    """
    Tests the gold layer processing logic, verifying data aggregation
    and physical output file creation.
    """
    output_dir = str(tmp_path)
    
    # Execute the Gold layer logic
    result_df = process_gold_data(sample_silver_data, output_dir)
    
    # Verify output file creation
    expected_file = tmp_path / "gold_aggregated_orders.csv"
    assert expected_file.exists(), f"Gold layer failed to create expected output file at {expected_file}"
    
    # Read back the saved file to ensure validity
    saved_df = pd.read_csv(expected_file)
    assert not saved_df.empty, "Saved Gold file is empty."
    
    # Verify aggregation logic structure in the returned DataFrame
    expected_columns = {"region", "status", "total_amount", "order_count"}
    assert expected_columns.issubset(set(result_df.columns)), "Missing expected aggregated columns."
    
    # Verify specific aggregated business metrics
    # NORTH + COMPLETED should have 2 orders and 250.0 total amount
    north_completed = result_df[
        (result_df["region"] == "NORTH") & 
        (result_df["status"] == "COMPLETED")
    ]
    
    assert not north_completed.empty, "Aggregation missing 'NORTH' and 'COMPLETED' group."
    assert north_completed.iloc[0]["total_amount"] == 250.0, "Incorrect sum aggregation for total_amount."
    assert north_completed.iloc[0]["order_count"] == 2, "Incorrect count aggregation for order_count."

    # SOUTH + COMPLETED should have 1 order and 300.0 total amount
    south_completed = result_df[
        (result_df["region"] == "SOUTH") & 
        (result_df["status"] == "COMPLETED")
    ]
    assert south_completed.iloc[0]["total_amount"] == 300.0, "Incorrect total_amount for SOUTH region."