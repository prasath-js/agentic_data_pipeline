import os
import tempfile
import pytest
import pandas as pd
from src.bronze.ingest_input_folder import ingest_input_folder

@pytest.fixture
def mock_csv_file() -> str:
    """
    Fixture to create a temporary CSV file mimicking the 'input_folder' source data.
    """
    data = (
        "order_id,customer_id,customer_name,email,amount,status,region,order_date\n"
        "1,101,John Doe,john@example.com,150.00,completed,North,25/12/2023\n"
        "2,102,Jane Smith,jane@example.com,200.50,pending,South,26/12/2023\n"
    )
    
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, 'w') as f:
        f.write(data)
        
    yield path
    
    if os.path.exists(path):
        os.remove(path)

def test_ingest_input_folder_success(mock_csv_file: str) -> None:
    """
    Test successful ingestion of the input_folder CSV file.
    """
    df = ingest_input_folder(mock_csv_file)
    
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 2
    
    expected_columns = [
        "order_id", "customer_id", "customer_name", "email", 
        "amount", "status", "region", "order_date"
    ]
    assert list(df.columns) == expected_columns
    assert df.iloc[0]["customer_name"] == "John Doe"

def test_ingest_input_folder_file_not_found() -> None:
    """
    Test that a FileNotFoundError is raised when an invalid file path is provided.
    """
    invalid_path = "/path/to/nonexistent/file_12345.csv"
    
    with pytest.raises(FileNotFoundError):
        ingest_input_folder(invalid_path)