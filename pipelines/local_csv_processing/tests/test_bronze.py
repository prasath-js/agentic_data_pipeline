import pytest
import pandas as pd
import os
import logging
from src.bronze.ingest_local_csv_input import ingest

# Fixture to capture log messages during tests
@pytest.fixture(autouse=True)
def cap_base_logging(caplog):
    """Fixture to capture log messages during tests."""
    with caplog.at_level(logging.INFO):
        yield

def test_ingest_local_csv_input_success(tmp_path, monkeypatch):
    """
    Test successful ingestion of a local CSV file.
    """
    # Define mock CSV content with all expected columns
    csv_content = """opportunity_id,account_id,value,close_date,stage,transaction_id,customer_id,quantity,amount,transaction_date
1,101,1000.50,2023-01-15,Prospecting,T1,C1,10,100.00,2023-01-01
2,102,2000.75,2023-02-20,Closed Won,T2,C2,20,200.00,2023-01-02
3,103,500.00,2023-03-25,Negotiation,T3,C3,5,50.00,2023-01-03
"""
    mock_csv_path = tmp_path / "mock_local_csv_input.csv"
    mock_csv_path.write_text(csv_content)

    # Mock the environment variable that points to the CSV file
    monkeypatch.setenv("LOCAL_CSV_INPUT_PATH", str(mock_csv_path))

    # Define expected columns
    expected_columns = [
        "opportunity_id", "account_id", "value", "close_date", "stage",
        "transaction_id", "customer_id", "quantity", "amount", "transaction_date"
    ]

    # Call the ingest function
    df = ingest()

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, "DataFrame should not be empty for successful ingestion."
    assert len(df) == 3, "DataFrame should have 3 rows."
    assert all(col in df.columns for col in expected_columns), "All expected columns should be present."
    assert df["opportunity_id"].iloc[0] == 1
    assert df["value"].iloc[1] == 2000.75
    assert df["stage"].iloc[2] == "Negotiation"

def test_ingest_local_csv_input_file_not_found(monkeypatch, caplog):
    """
    Test handling of FileNotFoundError during CSV ingestion when the file does not exist.
    """
    non_existent_path = "/path/to/non_existent_local_csv_input.csv"
    monkeypatch.setenv("LOCAL_CSV_INPUT_PATH", non_existent_path)

    # Call the ingest function
    df = ingest()

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert df.empty, "DataFrame should be empty when file is not found."
    assert f"File not found at: {non_existent_path}" in caplog.text
    assert "Returning an empty DataFrame." in caplog.text

def test_ingest_local_csv_input_empty_file(tmp_path, monkeypatch):
    """
    Test ingestion of an empty CSV file (only headers).
    """
    # Create an empty mock CSV file with just headers
    csv_content = """opportunity_id,account_id,value,close_date,stage,transaction_id,customer_id,quantity,amount,transaction_date
"""
    mock_csv_path = tmp_path / "empty_local_csv_input.csv"
    mock_csv_path.write_text(csv_content)

    monkeypatch.setenv("LOCAL_CSV_INPUT_PATH", str(mock_csv_path))

    expected_columns = [
        "opportunity_id", "account_id", "value", "close_date", "stage",
        "transaction_id", "customer_id", "quantity", "amount", "transaction_date"
    ]

    # Call the ingest function
    df = ingest()

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert df.empty, "DataFrame should be empty if the CSV contains only headers."
    assert all(col in df.columns for col in expected_columns), "Headers should still be parsed correctly."
    assert "successfully ingested, 0 rows found." in caplog.text

def test_ingest_local_csv_input_missing_env_var(monkeypatch, caplog):
    """
    Test handling when the environment variable for the file path is not set.
    """
    # Ensure the environment variable is not set
    monkeypatch.delenv("LOCAL_CSV_INPUT_PATH", raising=False)

    # Call the ingest function
    df = ingest()

    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert df.empty, "DataFrame should be empty when environment variable is missing."
    assert "Environment variable 'LOCAL_CSV_INPUT_PATH' not set." in caplog.text
    assert "Returning an empty DataFrame." in caplog.text

def test_ingest_local_csv_input_malformed_csv(tmp_path, monkeypatch, caplog):
    """
    Test handling of a malformed CSV file.
    """
    malformed_csv_content = """opportunity_id,account_id,value
1,101
2,102,2000.75,extra_value
""" # Row 2 has too many columns
    mock_csv_path = tmp_path / "malformed_local_csv_input.csv"
    mock_csv_path.write_text(malformed_csv_content)

    monkeypatch.setenv("LOCAL_CSV_INPUT_PATH", str(mock_csv_path))

    # Call the ingest function
    df = ingest()

    # Assertions
    assert isinstance(df, pd.DataFrame)
    # Pandas read_csv is robust, it might parse it with NaNs or raise a parser error.
    # We expect it to log an error and potentially return a partial or empty DF depending on error handling.
    # For this test, we expect it to try to read and log an issue.
    assert not df.empty # Pandas might still parse the first valid row
    assert len(df) == 2 # Pandas might parse the first two rows, with issues in the second
    assert "Error reading CSV file" in caplog.text # Check for general error logging