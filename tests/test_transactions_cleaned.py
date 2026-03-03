import pytest
import duckdb
import pandas as pd
import os

# Use the DB_PATH constant from the environment
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_database():
    # Ensure the database file is clean for testing
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Create dummy bronze.transactions_raw table
    transactions_data = [
        # Valid rows
        (101.0, 1.0, 10.0, "$100.50 ", "2023-01-01", "file1.csv", "2023-01-01T00:00:00Z"),
        (102.0, 2.0, 5.0, " 200.00", "2023-01-02", "file2.csv", "2023-01-01T00:00:00Z"),
        (103.0, 3.0, 1.0, "30.00", "2023-01-03", "file3.csv", "2023-01-01T00:00:00Z"),
        # Another valid row with whitespace in string columns
        (104.0, 12.0, 2.0, " 45.75 ", " 2023-01-04 ", " file4.csv ", " 2023-01-01T00:00:00Z "),
        # Rejected: Null transaction_id
        (None, 4.0, 2.0, "40.00", "2023-01-04", "file4.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Null amount
        (105.0, 5.0, 3.0, None, "2023-01-05", "file5.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Zero amount
        (106.0, 6.0, 4.0, "0.00", "2023-01-06", "file6.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Negative amount
        (107.0, 7.0, 5.0, "-50.00", "2023-01-07", "file7.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Amount not coercible to numeric (will become NaN, then rejected by <=0)
        (108.0, 8.0, 6.0, "abc", "2023-01-08", "file8.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Null transaction_id AND Null amount
        (None, 9.0, 7.0, None, "2023-01-09", "file9.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Null transaction_id AND Zero amount
        (None, 10.0, 8.0, "0.00", "2023-01-10", "file10.csv", "2023-01-01T00:00:00Z"),
        # Rejected: Null transaction_id AND Negative amount
        (None, 11.0, 9.0, "-10.00", "2023-01-11", "file11.csv", "2023-01-01T00:00:00Z"),
    ]
    transactions_df = pd.DataFrame(transactions_data, columns=[
        "transaction_id", "customer_id", "quantity", "amount", 
        "transaction_date", "_source_file", "_ingest_ts"
    ])
    con.execute("CREATE TABLE bronze.transactions_raw AS SELECT * FROM transactions_df")

    con.close()
    yield
    # Teardown: Clean up database file after tests
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

@pytest.fixture(scope="function")
def duckdb_connection():
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

def test_silver_table_exists_and_not_empty(setup_database, duckdb_connection):
    main() # Run the transformation
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM silver.transactions_cleaned").fetchone()[0]
    assert result > 0, "silver.transactions_cleaned should exist and have rows"

def test_rejected_table_exists_and_not_empty(setup_database, duckdb_connection):
    main() # Run the transformation
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]
    assert result > 0, "rejected.rejected_rows should exist and have rows"

def test_no_null_transaction_id_in_silver(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    null_ids = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE transaction_id IS NULL").fetchone()[0]
    assert null_ids == 0, "transaction_id in silver.transactions_cleaned should not be null"

def test_amount_is_numeric_and_positive_in_silver(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check data type (DuckDB's type system might infer DOUBLE for float64)
    amount_type = con.execute("PRAGMA table_info('silver.transactions_cleaned')").df()
    amount_type = amount_type[amount_type['name'] == 'amount']['type'].iloc[0]
    assert 'DOUBLE' in amount_type.upper() or 'FLOAT' in amount_type.upper(), f"Amount column should be numeric, got {amount_type}"

    # Check for non-positive amounts
    non_positive_amounts = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE amount <= 0 OR amount IS NULL").fetchone()[0]
    assert non_positive_amounts == 0, "Amount in silver.transactions_cleaned should be strictly positive and not null"

def test_string_columns_stripped_in_silver(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check a specific row that had whitespace in raw data
    # Row (104.0, 12.0, 2.0, " 45.75 ", " 2023-01-04 ", " file4.csv ", " 2023-01-01T00:00:00Z ")
    df_silver = con.execute("SELECT transaction_date, _source_file, _ingest_ts FROM silver.transactions_cleaned WHERE transaction_id = 104.0").df()
    assert not df_silver.empty, "Test row 104.0 not found in silver.transactions_cleaned"
    
    row = df_silver.iloc[0]
    assert row['transaction_date'] == "2023-01-04", f"transaction_date not stripped: '{row['transaction_date']}'"
    assert row['_source_file'] == "file4.csv", f"_source_file not stripped: '{row['_source_file']}'"
    assert row['_ingest_ts'] == "2023-01-01T00:00:00Z", f"_ingest_ts not stripped: '{row['_ingest_ts']}'"

def test_rejected_rows_contain_null_id_reason(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected only due to null transaction_id
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 4.0").df()
    assert not rejected_df.empty, "Row with customer_id 4.0 (null transaction_id) not found in rejected"
    assert "Null transaction_id" in rejected_df['rejection_reason'].iloc[0], "Incorrect rejection reason for null transaction_id"
    assert "Invalid amount" not in rejected_df['rejection_reason'].iloc[0], "Should not have invalid amount reason"

def test_rejected_rows_contain_invalid_amount_reason(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected only due to null amount
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 5.0").df()
    assert not rejected_df.empty, "Row with customer_id 5.0 (null amount) not found in rejected"
    assert "Invalid amount" in rejected_df['rejection_reason'].iloc[0], "Incorrect rejection reason for null amount"
    assert "Null transaction_id" not in rejected_df['rejection_reason'].iloc[0], "Should not have null transaction_id reason"

def test_rejected_rows_contain_zero_amount_reason(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected due to zero amount
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 6.0").df()
    assert not rejected_df.empty, "Row with customer_id 6.0 (zero amount) not found in rejected"
    assert "Invalid amount" in rejected_df['rejection_reason'].iloc[0], "Incorrect rejection reason for zero amount"

def test_rejected_rows_contain_negative_amount_reason(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected due to negative amount
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 7.0").df()
    assert not rejected_df.empty, "Row with customer_id 7.0 (negative amount) not found in rejected"
    assert "Invalid amount" in rejected_df['rejection_reason'].iloc[0], "Incorrect rejection reason for negative amount"

def test_rejected_rows_contain_uncoercible_amount_reason(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected due to uncoercible amount ('abc')
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 8.0").df()
    assert not rejected_df.empty, "Row with customer_id 8.0 (uncoercible amount) not found in rejected"
    assert "Invalid amount" in rejected_df['rejection_reason'].iloc[0], "Incorrect rejection reason for uncoercible amount"

def test_rejected_rows_contain_combined_reasons(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    # Check for a row rejected due to null transaction_id AND null amount
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 9.0").df()
    assert not rejected_df.empty, "Row with customer_id 9.0 (null id and null amount) not found in rejected"
    reason = rejected_df['rejection_reason'].iloc[0]
    assert "Null transaction_id" in reason, "Missing 'Null transaction_id' reason"
    assert "Invalid amount" in reason, "Missing 'Invalid amount' reason"
    # Check for a row rejected due to null transaction_id AND zero amount
    rejected_df_zero = con.execute("SELECT * FROM rejected.rejected_rows WHERE customer_id = 10.0").df()
    assert not rejected_df_zero.empty, "Row with customer_id 10.0 (null id and zero amount) not found in rejected"
    reason_zero = rejected_df_zero['rejection_reason'].iloc[0]
    assert "Null transaction_id" in reason_zero, "Missing 'Null transaction_id' reason for zero amount"
    assert "Invalid amount" in reason_zero, "Missing 'Invalid amount' reason for zero amount"

def test_silver_and_rejected_are_mutually_exclusive(setup_database, duckdb_connection):
    main()
    con = duckdb_connection
    
    original_count = con.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0]
    silver_count = con.execute("SELECT count(*) FROM silver.transactions_cleaned").fetchone()[0]
    rejected_count = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]

    assert (silver_count + rejected_count) == original_count, "Sum of silver and rejected rows should equal original raw rows"