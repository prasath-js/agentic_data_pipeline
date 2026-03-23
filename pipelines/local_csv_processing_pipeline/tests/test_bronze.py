import os
import pytest
import pandas as pd
from unittest.mock import patch

# Assuming the ingestion function is named ingest_input_folder and resides in src.bronze.ingest_input_folder
from src.bronze.ingest_input_folder import ingest_input_folder

@pytest.fixture
def mock_csv_data(tmp_path: pytest.TempPathFactory) -> str:
    """
    Creates a temporary CSV file with mock data mimicking the input_folder source.
    """
    data = {
        "order_id": [1, 2],
        "customer_id": [101, 102],
        "customer_name": ["Alice Smith", "Bob Jones"],
        "email": ["alice@example.com", "bob@example.com"],
        "amount": [250.00, 150.50],
        "status": ["completed", "pending"],
        "region": ["North", "South"],
        "order_date": ["15/01/2023", "20/02/2023"]
    }
    df = pd.DataFrame(data)
    
    file_path = tmp_path / "mock_input_folder.csv"
    df.to_csv(file_path, index=False)
    
    return str(file_path)

def test_ingest_input_folder_success(mock_csv_data: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests successful ingestion of local files from the input_folder.
    Verifies that a Pandas DataFrame is returned and contains the expected columns.
    """
    # Mock the environment variable expected by the bronze layer
    monkeypatch.setenv("INPUT_FOLDER_PATH", mock_csv_data)
    
    df = ingest_input_folder()
    
    # Assertions
    assert isinstance(df, pd.DataFrame), "Output must be a Pandas DataFrame"
    assert not df.empty, "DataFrame should not be empty"
    assert len(df) == 2, "DataFrame should contain exactly 2 rows"
    
    expected_columns = [
        "order_id", "customer_id", "customer_name", "email", 
        "amount", "status", "region", "order_date"
    ]
    for col in expected_columns:
        assert col in df.columns, f"Expected column '{col}' missing from ingested data"

def test_ingest_input_folder_missing_file(tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Tests the error handling of the ingestion function when the local file is missing.
    """
    missing_file_path = str(tmp_path / "non_existent_file.csv")
    monkeypatch.setenv("INPUT_FOLDER_PATH", missing_file_path)
    
    # Depending on the implementation, it might raise FileNotFoundError or a custom exception.
    # We will test for a standard FileNotFoundError or a general Exception raised by pandas/bronze logic.
    with pytest.raises((FileNotFoundError, Exception)) as exc_info:
        ingest_input_folder()
    
    assert exc_info is not None, "Expected an exception to be raised for missing file"