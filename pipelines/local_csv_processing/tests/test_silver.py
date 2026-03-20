import pytest
import pandas as pd
from src.silver.transform_silver import transform_silver
from unittest.mock import patch, MagicMock


@pytest.fixture
def sample_bronze_data_local_csv():
    """
    Fixture to provide sample bronze data for local_csv_input.
    Includes cases for:
    - Valid data
    - Invalid date format
    - Null values in critical columns
    - Invalid amount (<= 0)
    """
    data = {
        "opportunity_id": ["OPP001", "OPP002", "OPP003", "OPP004", "OPP005", "OPP006"],
        "account_id": ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005", "ACC006"],
        "value": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0],
        "close_date": ["2023-01-15", "1/2/2023", "2023-03-01", "2023-04-01", "2023-05-01", "2023-06-01"],
        "stage": ["Closed Won", "Open", "Closed Lost", "Open", "Closed Won", "Open"],
        "transaction_id": ["TXN001", "TXN002", "TXN003", "TXN004", "TXN005", "TXN006"],
        "customer_id": ["CUST001", "CUST002", "CUST003", "CUST004", "CUST005", "CUST006"],
        "quantity": [10, 20, 30, 40, 50, 60],
        "amount": [100.0, 200.0, 0.0, 400.0, -50.0, None],  # 0.0, -50.0 are invalid, None will be dropped
        "transaction_date": ["2022-12-01", "12/15/2022", "2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"],
    }
    df = pd.DataFrame(data)
    # Introduce a row with null critical ID for testing null removal
    df.loc[6] = ["OPP007", None, 7000.0, "2023-07-01", "Open", "TXN007", "CUST007", 70, 700.0, "2023-05-01"]
    df.loc[7] = [None, "ACC008", 8000.0, "2023-08-01", "Open", "TXN008", "CUST008", 80, 800.0, "2023-06-01"]
    return {"local_csv_input": df}


@patch("src.silver.transform_silver.logger", new_callable=MagicMock)
def test_date_conversion(mock_logger, sample_bronze_data_local_csv):
    """
    Test that date columns are correctly converted to ISO (YYYY-MM-DD) format.
    """
    bronze_data = sample_bronze_data_local_csv
    silver_df = transform_silver(bronze_data)["local_csv_input"]

    # Check specific converted dates
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "close_date"].iloc[0] == "2023-01-15"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP002", "close_date"].iloc[0] == "2023-01-02"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "transaction_date"].iloc[0] == "2022-12-01"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP002", "transaction_date"].iloc[0] == "2022-12-15"

    # Ensure all date columns are strings and in YYYY-MM-DD format
    for col in ["close_date", "transaction_date"]:
        assert silver_df[col].dtype == "object"
        assert all(silver_df[col].str.match(r"^\d{4}-\d{2}-\d{2}$").dropna())


@patch("src.silver.transform_silver.logger", new_callable=MagicMock)
def test_null_removal(mock_logger, sample_bronze_data_local_csv):
    """
    Test that rows with nulls in critical columns (opportunity_id, account_id) are removed.
    """
    bronze_data = sample_bronze_data_local_csv
    silver_df = transform_silver(bronze_data)["local_csv_input"]

    # Original data had 8 rows + 2 null ID rows = 10 rows.
    # OPP003 (amount=0), OPP005 (amount=-50), OPP006 (amount=None) should be removed.
    # OPP007 (account_id=None), OPP008 (opportunity_id=None) should be removed.
    # Expected rows: OPP001, OPP002, OPP004 = 3 rows.
    assert len(silver_df) == 3
    assert "OPP007" not in silver_df["opportunity_id"].values
    assert "OPP008" not in silver_df["opportunity_id"].values
    assert not silver_df["opportunity_id"].isnull().any()
    assert not silver_df["account_id"].isnull().any()


@patch("src.silver.transform_silver.logger", new_callable=MagicMock)
def test_invalid_row_filtering(mock_logger, sample_bronze_data_local_csv):
    """
    Test that rows with amount <= 0 or amount is null are removed.
    """
    bronze_data = sample_bronze_data_local_csv
    silver_df = transform_silver(bronze_data)["local_csv_input"]

    # Rows with amount 0.0, -50.0, None should be filtered out.
    # These correspond to OPP003, OPP005, OPP006.
    assert "OPP003" not in silver_df["opportunity_id"].values
    assert "OPP005" not in silver_df["opportunity_id"].values
    assert "OPP006" not in silver_df["opportunity_id"].values
    assert all(silver_df["amount"] > 0)
    assert not silver_df["amount"].isnull().any()


@patch("src.silver.transform_silver.logger", new_callable=MagicMock)
def test_pii_masking_no_op(mock_logger, sample_bronze_data_local_csv):
    """
    Test that no PII masking occurs since PII_COLUMNS_TO_MASK is empty.
    """
    bronze_data = sample_bronze_data_local_csv
    silver_df = transform_silver(bronze_data)["local_csv_input"]

    # Since PII_COLUMNS_TO_MASK is empty, no column should be masked.
    # We can check a column like 'customer_id' or 'transaction_id' to ensure it's unchanged
    # for a row that passed all other filters.
    original_customer_id_opp001 = sample_bronze_data_local_csv["local_csv_input"].loc[0, "customer_id"]
    original_transaction_id_opp001 = sample_bronze_data_local_csv["local_csv_input"].loc[0, "transaction_id"]

    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "customer_id"].iloc[0] == original_customer_id_opp001
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "transaction_id"].iloc[0] == original_transaction_id_opp001


@patch("src.silver.transform_silver.logger", new_callable=MagicMock)
def test_overall_transformation(mock_logger, sample_bronze_data_local_csv):
    """
    Test the combined effect of all silver layer transformations.
    Expected output should only contain valid, transformed rows.
    """
    bronze_data = sample_bronze_data_local_csv
    silver_result = transform_silver(bronze_data)
    silver_df = silver_result["local_csv_input"]

    # Expected rows after all filters:
    # OPP001: Valid, amount > 0, date ISO
    # OPP002: Valid, amount > 0, date non-ISO -> ISO
    # OPP003: Invalid amount (0) -> removed
    # OPP004: Valid, amount > 0, date ISO
    # OPP005: Invalid amount (<0) -> removed
    # OPP006: Invalid amount (None) -> removed
    # OPP007: Null account_id -> removed
    # OPP008: Null opportunity_id -> removed

    # So, only OPP001, OPP002, OPP004 should remain.
    expected_opportunity_ids = {"OPP001", "OPP002", "OPP004"}
    assert set(silver_df["opportunity_id"].values) == expected_opportunity_ids
    assert len(silver_df) == 3

    # Check date formats for remaining rows
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "close_date"].iloc[0] == "2023-01-15"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP002", "close_date"].iloc[0] == "2023-01-02"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP004", "close_date"].iloc[0] == "2023-04-01"

    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "transaction_date"].iloc[0] == "2022-12-01"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP002", "transaction_date"].iloc[0] == "2022-12-15"
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP004", "transaction_date"].iloc[0] == "2023-02-01"

    # Check amounts for remaining rows
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "amount"].iloc[0] == 100.0
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP002", "amount"].iloc[0] == 200.0
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP004", "amount"].iloc[0] == 400.0

    # Ensure no PII masking occurred
    assert silver_df.loc[silver_df["opportunity_id"] == "OPP001", "customer_id"].iloc[0] == "CUST001"