import pandas as pd
import pytest
from src.silver.transform_silver import transform_silver

@pytest.fixture
def sample_bronze_data() -> pd.DataFrame:
    """Provides sample bronze data mimicking the input_folder schema."""
    data = {
        "order_id": [1, 2, 3, None, 5],
        "customer_id": [101, 102, 103, 104, 105],
        "customer_name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "david@example.com", "eve@example.com"],
        "amount": [100.50, 200.00, None, 400.00, 500.25],
        "status": ["Completed", "Pending", "Completed", "Failed", "Completed"],
        "region": ["North", "South", "East", "West", "North"],
        "order_date": ["15/01/2023", "20/02/2023", "05/03/2023", "10/04/2023", "25/05/2023"]
    }
    return pd.DataFrame(data)

def test_transform_silver_pii_masking(sample_bronze_data: pd.DataFrame) -> None:
    """Test that specified PII columns are masked."""
    pii_columns = ["customer_name", "email"]
    
    df_silver = transform_silver(sample_bronze_data, pii_columns)
    
    assert "customer_name" in df_silver.columns
    assert "email" in df_silver.columns
    assert all(df_silver["customer_name"] == "***MASKED***"), "Customer name column was not properly masked."
    assert all(df_silver["email"] == "***MASKED***"), "Email column was not properly masked."

def test_transform_silver_date_fixing(sample_bronze_data: pd.DataFrame) -> None:
    """Test that 'DD/MM/YYYY' dates are converted to valid ISO date formats."""
    pii_columns = ["customer_name", "email"]
    
    df_silver = transform_silver(sample_bronze_data, pii_columns)
    
    # Assert date column is now a datetime object
    assert pd.api.types.is_datetime64_any_dtype(df_silver["order_date"]), "order_date was not converted to datetime."
    
    # Verify exact parsing of DD/MM/YYYY
    # The first row in the valid set should be 15/01/2023 -> 2023-01-15
    first_valid_date = df_silver.iloc[0]["order_date"]
    assert first_valid_date.year == 2023
    assert first_valid_date.month == 1
    assert first_valid_date.day == 15

def test_transform_silver_null_removal(sample_bronze_data: pd.DataFrame) -> None:
    """Test that rows with nulls in critical columns are removed."""
    pii_columns = ["customer_name", "email"]
    
    initial_row_count = len(sample_bronze_data)
    df_silver = transform_silver(sample_bronze_data, pii_columns)
    
    # Rows with null order_id (index 3) or null amount (index 2) should be dropped
    assert len(df_silver) < initial_row_count
    assert df_silver["order_id"].isnull().sum() == 0, "Nulls remain in order_id column."
    assert df_silver["amount"].isnull().sum() == 0, "Nulls remain in amount column."

def test_transform_silver_empty_dataframe() -> None:
    """Test silver transformation with an empty dataframe."""
    empty_df = pd.DataFrame(columns=[
        "order_id", "customer_id", "customer_name", "email", 
        "amount", "status", "region", "order_date"
    ])
    pii_columns = ["customer_name", "email"]
    
    df_silver = transform_silver(empty_df, pii_columns)
    
    assert df_silver.empty
    assert list(df_silver.columns) == list(empty_df.columns)