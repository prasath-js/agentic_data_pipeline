import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
import logging

# Import the gold layer function
from src.gold.gold_local_files import gold_local_files

# Configure basic logging for tests to suppress actual output during tests
# and allow mocking of logging calls.
logging.basicConfig(level=logging.CRITICAL)

@pytest.fixture
def sample_silver_df() -> pd.DataFrame:
    """
    Fixture to provide a sample Silver layer DataFrame for testing.
    Assumes PII is masked and dates are in ISO format (YYYY-MM-DD).
    """
    data = {
        'order_id': [1, 2, 3, 4, 5],
        'customer_id': [101, 102, 101, 103, 102],
        'customer_name': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'email': ['***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***', '***MASKED***'],
        'amount': [100.50, 200.00, 150.75, 50.25, 300.00],
        'status': ['Completed', 'Pending', 'Completed', 'Cancelled', 'Completed'],
        'region': ['North', 'South', 'North', 'West', 'South'],
        'order_date': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19'])
    }
    df = pd.DataFrame(data)
    return df

@patch('src.gold.gold_local_files.logging') # Mock logging calls
def test_gold_aggregation_logic(mock_logging: MagicMock, sample_silver_df: pd.DataFrame) -> None:
    """
    Test the aggregation logic within the gold layer function.
    Mocks the file writing operation to intercept the aggregated DataFrame.
    The aggregation performed is sum of 'amount' and count of 'order_id' grouped by 'region'.
    """
    # Expected aggregation result
    expected_aggregated_data = {
        'region': ['North', 'South', 'West'],
        'total_amount': [251.25, 500.00, 50.25], # North: 100.50 + 150.75; South: 200.00 + 300.00
        'order_count': [2, 2, 1] # North: 2; South: 2; West: 1
    }
    expected_df = pd.DataFrame(expected_aggregated_data)
    # Sort for reliable comparison, as order might not be guaranteed without it
    expected_df = expected_df.sort_values(by='region').reset_index(drop=True)

    # Mock os.getenv to provide a dummy output path, as the gold layer will try to retrieve it
    with patch('os.getenv', return_value='/tmp/dummy_output.csv'):
        # Mock pandas.DataFrame.to_csv to prevent actual file writing
        # and to intercept the DataFrame that would be written.
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            gold_local_files(sample_silver_df)

            # Assert that to_csv was called exactly once
            mock_to_csv.assert_called_once()

            # Get the DataFrame that was passed as the first argument to to_csv
            actual_aggregated_df = mock_to_csv.call_args[0][0]

            # Sort the actual DataFrame for reliable comparison
            actual_aggregated_df = actual_aggregated_df.sort_values(by='region').reset_index(drop=True)

            # Assert that the aggregated DataFrame matches the expected DataFrame
            pd.testing.assert_frame_equal(actual_aggregated_df, expected_df, check_dtype=True)

    # Verify logging calls
    mock_logging.info.assert_any_call("Gold layer processing started.")
    mock_logging.info.assert_any_call("Aggregating data by region...")
    mock_logging.info.assert_any_call(f"Writing aggregated data to /tmp/dummy_output.csv...")
    mock_logging.info.assert_any_call("Gold layer processing completed.")

@patch('src.gold.gold_local_files.logging') # Mock logging calls
def test_gold_output_file_creation(mock_logging: MagicMock, sample_silver_df: pd.DataFrame, tmp_path: Path) -> None:
    """
    Test that the gold layer correctly writes the aggregated data to a file.
    Uses pytest's tmp_path fixture for creating a temporary directory and file.
    """
    # Define a temporary output file path within the tmp_path fixture
    output_file_path = tmp_path / "aggregated_orders.csv"

    # Mock os.getenv to return our temporary output path for the test
    with patch('os.getenv', return_value=str(output_file_path)):
        # Call the gold layer function
        gold_local_files(sample_silver_df)

        # Assert that the output file exists
        assert output_file_path.exists()
        assert output_file_path.is_file()

        # Read the content of the created file
        actual_output_df = pd.read_csv(output_file_path)

        # Expected aggregated data to compare against the file content
        expected_aggregated_data = {
            'region': ['North', 'South', 'West'],
            'total_amount': [251.25, 500.00, 50.25],
            'order_count': [2, 2, 1]
        }
        expected_df = pd.DataFrame(expected_aggregated_data)
        # Sort both for reliable comparison
        expected_df = expected_df.sort_values(by='region').reset_index(drop=True)
        actual_output_df = actual_output_df.sort_values(by='region').reset_index(drop=True)

        # Assert that the content of the file matches the expected DataFrame
        pd.testing.assert_frame_equal(actual_output_df, expected_df, check_dtype=True)

    # Verify logging calls
    mock_logging.info.assert_any_call("Gold layer processing started.")
    mock_logging.info.assert_any_call("Aggregating data by region...")
    mock_logging.info.assert_any_call(f"Writing aggregated data to {output_file_path}...")
    mock_logging.info.assert_any_call("Gold layer processing completed.")

@patch('src.gold.gold_local_files.logging')
def test_gold_error_handling_no_output_path(mock_logging: MagicMock, sample_silver_df: pd.DataFrame) -> None:
    """
    Test that the gold layer handles cases where the output file path is not provided.
    """
    # Mock os.getenv to return None for the GOLD_OUTPUT_FILE_PATH
    with patch('os.getenv', side_effect=lambda key: None if key == "GOLD_OUTPUT_FILE_PATH" else os.environ.get(key)):
        # Ensure that no file operations are attempted
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            gold_local_files(sample_silver_df)
            mock_to_csv.assert_not_called() # to_csv should not be called

    # Verify that an error message is logged
    mock_logging.error.assert_any_call("GOLD_OUTPUT_FILE_PATH environment variable not set. Cannot write gold data.")
    mock_logging.info.assert_any_call("Gold layer processing completed with errors.")