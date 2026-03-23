import pytest
import pandas as pd
import os
from pathlib import Path
from src.bronze.ingest_local_csv_data import ingest_local_csv_data

@pytest.fixture
def mock_local_csv_data(tmp_path: Path, mocker) -> str:
    """
    Creates a temporary CSV file with sample data and mocks the environment variable
    that points to this file.
    """
    data = {
        "order_id": [1, 2, 3, 4],
        "customer_id": ["C001", "C002", "C003", "C004"],
        "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown", "Diana Prince"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com"],
        "amount": [100.50, 200.75, 50.25, 150.00],
        "status": ["Completed", "Pending", "Completed", "Cancelled"],
        "region": ["North", "South", "East", "West"],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023", "04/01/2023"], # DD/MM/YYYY format
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "sample_local_csv_data.csv"
    df.to_csv(file_path, index=False)

    # Mock the environment variable expected by the bronze ingest function
    mocker.patch.dict(os.environ, {"LOCAL_CSV_DATA_PATH": str(file_path)})
    return str(file_path)

@pytest.fixture
def mock_empty_local_csv_data(tmp_path: Path, mocker) -> str:
    """
    Creates an empty temporary CSV file with only headers and mocks the environment variable.
    """
    file_path = tmp_path / "empty_local_csv_data.csv"
    headers = "order_id,customer_id,customer_name,email,amount,status,region,order_date\n"
    file_path.write_text(headers)

    mocker.patch.dict(os.environ, {"LOCAL_CSV_DATA_PATH": str(file_path)})
    return str(file_path)

def test_ingest_local_csv_data_success(mock_local_csv_data: str) -> None:
    """
    Tests successful ingestion of local CSV data into a DataFrame.
    Verifies DataFrame type, non-emptiness, row count, and column names.
    """
    df = ingest_local_csv_data()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 4

    expected_columns = [
        "order_id", "customer_id", "customer_name", "email",
        "amount", "status", "region", "order_date"
    ]
    assert list(df.columns) == expected_columns

    # Verify a few data points to ensure correct ingestion
    assert df["order_id"].iloc[0] == 1
    assert df["customer_name"].iloc[1] == "Bob Johnson"
    assert df["order_date"].iloc[3] == "04/01/2023" # Bronze layer should not transform date formats

def test_ingest_local_csv_data_missing_file(mocker) -> None:
    """
    Tests error handling when the specified local CSV file does not exist.
    """
    # Mock the environment variable to point to a non-existent file path
    mocker.patch.dict(os.environ, {"LOCAL_CSV_DATA_PATH": "/path/to/nonexistent/file_for_test.csv"})

    with pytest.raises(FileNotFoundError, match="Local CSV file not found"):
        ingest_local_csv_data()

def test_ingest_local_csv_data_empty_file(mock_empty_local_csv_data: str) -> None:
    """
    Tests ingestion of an empty CSV file (only headers).
    Ensures an empty DataFrame with correct columns is returned.
    """
    df = ingest_local_csv_data()

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert len(df) == 0

    expected_columns = [
        "order_id", "customer_id", "customer_name", "email",
        "amount", "status", "region", "order_date"
    ]
    assert list(df.columns) == expected_columns

def test_ingest_local_csv_data_env_var_not_set(mocker) -> None:
    """
    Tests handling when the required environment variable is not set.
    """
    # Ensure the environment variable is not present
    if "LOCAL_CSV_DATA_PATH" in os.environ:
        del os.environ["LOCAL_CSV_DATA_PATH"]
    
    # Use mocker to temporary remove it for the test scope if it was there
    mocker.patch.dict(os.environ, {}, clear=True)

    with pytest.raises(ValueError, match="Environment variable LOCAL_CSV_DATA_PATH not set."):
        ingest_local_csv_data()
