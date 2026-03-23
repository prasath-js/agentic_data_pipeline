import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from pathlib import Path

# Assuming the gold layer module for local_files output is named gold_local_files.py
# and contains a function `process_gold_data` that takes a DataFrame, performs aggregation,
# and writes to a path retrieved from an environment variable (e.g., 'GOLD_OUTPUT_PATH').
from src.gold.gold_local_files import process_gold_data

@pytest.fixture
def sample_silver_df() -> pd.DataFrame:
    """
    Provides a sample DataFrame representing the Silver layer output,
    ready for Gold layer aggregation.
    """
    data = {
        'order_id': ['O1', 'O2', 'O3', 'O4', 'O5', 'O6'],
        'customer_id': ['C1', 'C2', 'C1', 'C3', 'C2', 'C1'],
        'customer_name': ['***MASKED***'] * 6, # PII masked in Silver
        'email': ['***MASKED***'] * 6,         # PII masked in Silver
        'amount': [100.50, 200.00, 150.75, 50.00, 300.25, 120.00],
        'status': ['completed', 'pending', 'completed', 'cancelled', 'completed', 'pending'],
        'region': ['North', 'South', 'North', 'East', 'South', 'North'],
        'order_date': ['2023-01-01', '2023-01-02', '2023-01-01', '2023-01-03', '2023-01-02', '2023-01-04']
    }
    df = pd.DataFrame(data)
    # Ensure 'order_date' is datetime for consistency, though not used in this specific aggregation test
    df['order_date'] = pd.to_datetime(df['order_date'])
    return df

@pytest.fixture
def expected_gold_df() -> pd.DataFrame:
    """
    Provides the expected aggregated DataFrame based on the sample_silver_df.
    Aggregation logic: sum of 'amount' and count of 'order_id' grouped by 'region' and 'status'.
    """
    data = {
        'region': ['East', 'North', 'North', 'South', 'South'],
        'status': ['cancelled', 'completed', 'pending', 'completed', 'pending'],
        'total_amount': [50.00, 251.25, 120.00, 300.25, 200.00],
        'order_count': [1, 2, 1, 1, 1]
    }
    df = pd.DataFrame(data)
    # Sort for consistent comparison, as groupby output order can vary
    df = df.sort_values(by=['region', 'status']).reset_index(drop=True)
    # Ensure dtypes match what pandas.read_csv would infer
    df['total_amount'] = df['total_amount'].astype(float)
    df['order_count'] = df['order_count'].astype(int)
    return df

@patch('os.getenv')
def test_gold_output_file_creation_and_aggregation(
    mock_getenv: MagicMock,
    sample_silver_df: pd.DataFrame,
    expected_gold_df: pd.DataFrame,
    tmp_path: Path,
) -> None:
    """
    Test that the gold layer correctly performs aggregation and writes the
    aggregated data to an output file.

    It mocks `os.getenv` to provide a temporary file path for the gold output,
    then calls the gold processing function, and finally verifies the existence
    and content of the generated output file.
    """
    # Define a temporary output path for the CSV file within the pytest tmp_path fixture
    output_file_path = tmp_path / "gold_aggregated_data.csv"

    # Configure mock_getenv to return this temporary path when 'GOLD_OUTPUT_PATH' is requested
    mock_getenv.return_value = str(output_file_path)

    # Call the gold layer processing function
    # This function is expected to aggregate `sample_silver_df` and write to `output_file_path`
    process_gold_data(sample_silver_df)

    # Assert that the output file was created at the specified path
    assert output_file_path.exists(), f"Gold output file not found at {output_file_path}"

    # Read the content of the output file into a DataFrame
    actual_gold_df = pd.read_csv(output_file_path)

    # Ensure consistent column order and sorting for comparison
    actual_gold_df = actual_gold_df.sort_values(by=['region', 'status']).reset_index(drop=True)
    expected_gold_df = expected_gold_df.sort_values(by=['region', 'status']).reset_index(drop=True)

    # Compare the actual DataFrame from the output file with the expected DataFrame
    pd.testing.assert_frame_equal(
        actual_gold_df,
        expected_gold_df,
        check_dtype=True,   # Check if data types are consistent
        check_exact=False,  # Allow for floating point comparison with tolerance
        atol=1e-2           # Absolute tolerance for float comparisons
    )