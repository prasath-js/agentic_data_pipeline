import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import os

# Assuming the ingest function for 'input_csv_folder' is named 'ingest_data'
# and is located in src/bronze/ingest_input_csv_folder.py
from src.bronze.ingest_input_csv_folder import ingest_data

# Define mock data for successful ingestion
@pytest.fixture
def sample_csv_data() -> pd.DataFrame:
    """
    Returns sample data for a CSV file, matching the specified columns and a non-ISO date format.
    """
    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": ["C101", "C102", "C103"],
        "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown"],
        "email": ["alice.s@example.com", "bob.j@example.com", "charlie.b@example.com"],
        "amount": [100.50, 200.00, 75.25],
        "status": ["Completed", "Pending", "Cancelled"],
        "region": ["East", "West", "North"],
        "order_date": ["01/01/2023", "15/02/2023", "30/03/2023"] # Non-ISO DD/MM/YYYY format
    })

def test_ingest_input_csv_folder_success(sample_csv_data: pd.DataFrame) -> None:
    """
    Tests successful ingestion of data from local CSV files.
    Mocks the LocalFilesConnector to return predefined sample data.
    """
    mock_connector = MagicMock()
    mock_connector.read_data.return_value = sample_csv_data

    mock_builder = MagicMock()
    mock_builder.get_connector.return_value = mock_connector

    # Patch ConnectionBuilder to return our mock builder instance
    with patch('src.db_connection.builder.ConnectionBuilder', return_value=mock_builder):
        df_ingested = ingest_data()

        # Assert that get_connector was called with expected arguments
        mock_builder.get_connector.assert_called_once_with("local_files", "input_csv_folder")
        # Assert that read_data was called on the connector
        mock_connector.read_data.assert_called_once()

        # Assert the ingested DataFrame matches the sample data
        pd.testing.assert_frame_equal(df_ingested, sample_csv_data)
        assert not df_ingested.empty
        assert len(df_ingested) == 3
        expected_columns = [
            "order_id", "customer_id", "customer_name", "email",
            "amount", "status", "region", "order_date"
        ]
        assert list(df_ingested.columns) == expected_columns

def test_ingest_input_csv_folder_file_not_found() -> None:
    """
    Tests error handling when the source CSV file(s) are not found.
    Mocks the LocalFilesConnector to raise FileNotFoundError.
    """
    mock_connector = MagicMock()
    mock_connector.read_data.side_effect = FileNotFoundError("Mock: CSV file(s) not found.")

    mock_builder = MagicMock()
    mock_builder.get_connector.return_value = mock_connector

    with patch('src.db_connection.builder.ConnectionBuilder', return_value=mock_builder):
        with pytest.raises(FileNotFoundError, match="CSV file\\(s\\) not found"):
            ingest_data()

        # Assert that get_connector was called
        mock_builder.get_connector.assert_called_once_with("local_files", "input_csv_folder")
        # Assert that read_data was called
        mock_connector.read_data.assert_called_once()

def test_ingest_input_csv_folder_empty_data() -> None:
    """
    Tests ingestion when the source contains no data (e.g., empty CSV or no files matched).
    Mocks the LocalFilesConnector to return an empty DataFrame with expected columns.
    """
    # Define expected columns for an empty DataFrame
    expected_columns = [
        "order_id", "customer_id", "customer_name", "email",
        "amount", "status", "region", "order_date"
    ]
    mock_connector = MagicMock()
    mock_connector.read_data.return_value = pd.DataFrame(columns=expected_columns)

    mock_builder = MagicMock()
    mock_builder.get_connector.return_value = mock_connector

    with patch('src.db_connection.builder.ConnectionBuilder', return_value=mock_builder):
        df_ingested = ingest_data()

        mock_builder.get_connector.assert_called_once_with("local_files", "input_csv_folder")
        mock_connector.read_data.assert_called_once()

        assert df_ingested.empty
        assert len(df_ingested) == 0
        # Check if columns are preserved even if DataFrame is empty
        assert list(df_ingested.columns) == expected_columns

def test_ingest_input_csv_folder_general_exception() -> None:
    """
    Tests error handling for a general exception during ingestion (e.g., parsing error, permissions).
    Mocks the LocalFilesConnector to raise a generic Exception.
    """
    mock_connector = MagicMock()
    mock_connector.read_data.side_effect = Exception("Mock: Generic ingestion error occurred.")

    mock_builder = MagicMock()
    mock_builder.get_connector.return_value = mock_connector

    with patch('src.db_connection.builder.ConnectionBuilder', return_value=mock_builder):
        with pytest.raises(Exception, match="Generic ingestion error occurred"):
            ingest_data()

        mock_builder.get_connector.assert_called_once_with("local_files", "input_csv_folder")
        mock_connector.read_data.assert_called_once()