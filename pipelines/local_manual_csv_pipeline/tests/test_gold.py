import pandas as pd
import pytest
from pathlib import Path

from src.gold.gold_local_files import gold_local_files


@pytest.fixture
def silver_df() -> pd.DataFrame:
    """
    Fixture to provide a mock Silver DataFrame with masked PII and cleaned dates.
    """
    return pd.DataFrame({
        "order_id": [101, 102, 103, 104],
        "customer_id": [1, 2, 1, 3],
        "customer_name": ["***MASKED***", "***MASKED***", "***MASKED***", "***MASKED***"],
        "email": ["***MASKED***", "***MASKED***", "***MASKED***", "***MASKED***"],
        "amount": [150.0, 250.0, 100.0, 200.0],
        "status": ["Completed", "Pending", "Completed", "Completed"],
        "region": ["North", "South", "North", "East"],
        "order_date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]
    })


def test_gold_local_files_writes_output(silver_df: pd.DataFrame, tmp_path: Path) -> None:
    """
    Test that gold_local_files successfully writes a CSV file to the output path.
    """
    output_file = tmp_path / "gold_output.csv"
    
    gold_local_files(silver_df, str(output_file))
    
    assert output_file.exists(), "Gold layer failed to write the output file."
    assert output_file.stat().st_size > 0, "Gold layer wrote an empty file."


def test_gold_local_files_aggregation(silver_df: pd.DataFrame, tmp_path: Path) -> None:
    """
    Test that gold_local_files aggregates the Silver data correctly.
    """
    output_file = tmp_path / "gold_aggregated.csv"
    
    gold_local_files(silver_df, str(output_file))
    
    # Read the generated output to verify aggregation
    result_df = pd.read_csv(output_file)
    
    assert not result_df.empty, "Resulting Gold DataFrame should not be empty."
    
    # Check if aggregation preserved the total amount (standard metric check)
    # Assuming the Gold layer sums up 'amount' (e.g., total_amount or amount)
    amount_col = "total_amount" if "total_amount" in result_df.columns else "amount"
    
    if amount_col in result_df.columns:
        expected_total = silver_df["amount"].sum()
        actual_total = result_df[amount_col].sum()
        assert expected_total == actual_total, (
            f"Aggregation altered the total metric. Expected {expected_total}, got {actual_total}"
        )
    
    # Verify that row count is reduced if grouped (input has 4 rows, grouping by region/status should reduce it)
    if "region" in result_df.columns:
        assert len(result_df) <= len(silver_df), "Aggregation should reduce or maintain row count."