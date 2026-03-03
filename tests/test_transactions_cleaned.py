import pytest
import duckdb
import pandas as pd
import os

# Define the DB_PATH for testing
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture to establish and close DuckDB connection for tests."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

@pytest.fixture(scope="module", autouse=True)
def setup_and_run_transformation(duckdb_connection):
    """Fixture to set up bronze data and run the main transformation once for all tests."""
    con = duckdb_connection
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Clear tables from previous runs if they exist
    con.execute("DROP TABLE IF EXISTS bronze.transactions_raw;")
    con.execute("DROP TABLE IF EXISTS silver.transactions_cleaned;")
    con.execute("DROP TABLE IF EXISTS rejected.rejected_rows;")

    # Create a dummy bronze.transactions_raw table with various cases
    transactions_data = {
        'transaction_id': [101.0, 102.0, None, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0],
        'customer_id': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        'quantity': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'amount': ['$100.00 ', '$200.50', '  $0.00', '  -$50.00', None, 'abc', '$300.00', '$150.00', ' $25.00', ' $10.00'],
        'transaction_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10'],
        '_source_file': ['file1', 'file1', 'file1', 'file1', 'file1', 'file1', 'file1', 'file1', 'file1', 'file1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1', 'ts1']
    }
    transactions_df = pd.DataFrame(transactions_data)
    con.execute("CREATE OR REPLACE TABLE bronze.transactions_raw AS SELECT * FROM transactions_df")

    # Run the main transformation function
    main()

def test_silver_transactions_cleaned_exists_and_has_rows(duckdb_connection):
    """Test that silver.transactions_cleaned table exists and contains data."""
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM silver.transactions_cleaned").fetchone()[0]
    assert result > 0, "silver.transactions_cleaned should exist and have rows."
    assert result == 6, "Expected 6 valid rows in silver.transactions_cleaned." # 10 rows total - 4 rejected = 6 valid

def test_no_nulls_in_transaction_id_silver(duckdb_connection):
    """Test that transaction_id in silver.transactions_cleaned has no nulls."""
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE transaction_id IS NULL").fetchone()[0]
    assert result == 0, "transaction_id in silver.transactions_cleaned should not have nulls."

def test_amount_is_numeric_and_positive_silver(duckdb_connection):
    """Test that amount in silver.transactions_cleaned is numeric and strictly positive."""
    con = duckdb_connection
    df = con.execute("SELECT amount FROM silver.transactions_cleaned").df()
    assert pd.api.types.is_numeric_dtype(df['amount']), "Amount column should be numeric."
    assert (df['amount'] > 0).all(), "All amounts in silver.transactions_cleaned should be strictly positive."

def test_string_columns_are_stripped_silver(duckdb_connection):
    """Test that string columns in silver.transactions_cleaned are stripped of whitespace."""
    con = duckdb_connection
    # Check a specific row where original amount had leading/trailing spaces
    df = con.execute("SELECT amount FROM silver.transactions_cleaned WHERE transaction_id = 101.0").df()
    # The original amount was '$100.00 '. After stripping and conversion, it should be 100.00
    assert df['amount'].iloc[0] == 100.00, "Amount column should be stripped of whitespace before conversion."

def test_rejected_rows_table_exists(duckdb_connection):
    """Test that rejected.rejected_rows table exists."""
    con = duckdb_connection
    result = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
    ).fetchone()[0]
    assert result == 1, "rejected.rejected_rows table should exist."

def test_rejected_rows_count(duckdb_connection):
    """Test that the correct number of rows were rejected."""
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]
    # Expected rejections:
    # 1. transaction_id = None (amount was '$0.00') -> Rejected for Null ID AND Invalid Amount
    # 2. transaction_id = 104.0 (amount was '-$50.00') -> Rejected for Invalid Amount
    # 3. transaction_id = 105.0 (amount was None) -> Rejected for Invalid Amount
    # 4. transaction_id = 106.0 (amount was 'abc') -> Rejected for Invalid Amount
    assert result == 4, "Expected 4 rejected rows."

def test_rejected_rows_reasons_null_id(duckdb_connection):
    """Test rejection reason for null transaction_id."""
    con = duckdb_connection
    df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id IS NULL").df()
    assert not df.empty, "Should have rejected rows with null transaction_id."
    assert "Null transaction_id" in df['rejection_reason'].iloc[0], "Rejection reason for null ID is incorrect."
    assert "Invalid amount" in df['rejection_reason'].iloc[0], "Rejection reason for null ID should also include invalid amount."


def test_rejected_rows_reasons_invalid_amount(duckdb_connection):
    """Test rejection reason for invalid amount (zero, negative, non-numeric, null)."""
    con = duckdb_connection
    # These are the transaction_ids for rows rejected ONLY due to invalid amount (not null ID)
    df_invalid_amount = con.execute(
        "SELECT transaction_id, rejection_reason FROM rejected.rejected_rows WHERE transaction_id IN (104.0, 105.0, 106.0)"
    ).df()

    assert not df_invalid_amount.empty, "Should have rejected rows with invalid amounts."
    assert len(df_invalid_amount) == 3, "Expected 3 rows rejected solely for invalid amount."

    # Check for negative amount (transaction_id 104.0)
    reason_neg = df_invalid_amount[df_invalid_amount['transaction_id'] == 104.0]['rejection_reason'].iloc[0]
    assert "Invalid amount" in reason_neg, "Rejection reason for negative amount is incorrect."
    assert "Null transaction_id" not in reason_neg, "Negative amount row should not have null ID reason."

    # Check for null amount (transaction_id 105.0)
    reason_null = df_invalid_amount[df_invalid_amount['transaction_id'] == 105.0]['rejection_reason'].iloc[0]
    assert "Invalid amount" in reason_null, "Rejection reason for null amount is incorrect."
    assert "Null transaction_id" not in reason_null, "Null amount row should not have null ID reason."

    # Check for non-numeric amount (transaction_id 106.0)
    reason_abc = df_invalid_amount[df_invalid_amount['transaction_id'] == 106.0]['rejection_reason'].iloc[0]
    assert "Invalid amount" in reason_abc, "Rejection reason for non-numeric amount is incorrect."
    assert "Null transaction_id" not in reason_abc, "Non-numeric amount row should not have null ID reason."


def test_valid_rows_are_not_in_rejected(duckdb_connection):
    """Ensure no valid rows are present in the rejected table."""
    con = duckdb_connection
    valid_ids_df = con.execute("SELECT transaction_id FROM silver.transactions_cleaned").df()
    rejected_ids_df = con.execute("SELECT transaction_id FROM rejected.rejected_rows").df()

    if not valid_ids_df.empty and not rejected_ids_df.empty:
        # Convert to sets for efficient comparison, handling potential NaN in transaction_id
        valid_ids = set(valid_ids_df['transaction_id'].dropna())
        rejected_ids = set(rejected_ids_df['transaction_id'].dropna())
        
        common_ids = valid_ids.intersection(rejected_ids)
        assert not common_ids, f"Common transaction_ids found in both valid and rejected tables: {common_ids}"
    elif not valid_ids_df.empty and rejected_ids_df.empty:
        pass # All rows were valid, no rejections, so no overlap possible.
    elif valid_ids_df.empty and not rejected_ids_df.empty:
        pass # All rows were rejected, no valid rows, so no overlap possible.
    else: # Both are empty
        pass # No data, no overlap.