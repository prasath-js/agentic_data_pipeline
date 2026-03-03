import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for tests. This will be overridden by the actual DB_PATH
# in the execution environment, but provides a default for local testing.
DB_PATH = os.environ.get("DB_PATH", "test_pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_database():
    """
    Fixture to set up the DuckDB database with bronze data, run the transformation,
    and clean up afterwards.
    """
    # Ensure a clean slate for the database file
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    # Create bronze.transactions_raw with diverse test data
    transactions_data = [
        # Valid rows
        (101.0, 1.0, 10.0, "$100.00", " 2023-01-01 ", "file1.csv", "ts1"), # Valid, with whitespace
        (102.0, 2.0, 20.0, "250.50", "2023-01-02", "file1.csv", "ts1"),   # Valid
        (103.0, 3.0, 30.0, "1.00", "2023-01-03", "file1.csv", "ts1"),     # Valid, min positive amount

        # Rejected: Null transaction_id
        (None, 4.0, 40.0, "$50.00", "2023-01-04", "file1.csv", "ts1"),

        # Rejected: Null amount (from None)
        (104.0, 5.0, 50.0, None, "2023-01-05", "file1.csv", "ts1"),
        # Rejected: Null amount (from string 'NULL' which becomes NaN)
        (105.0, 6.0, 60.0, "NULL", "2023-01-06", "file1.csv", "ts1"),

        # Rejected: Zero amount
        (106.0, 7.0, 70.0, "$0.00", "2023-01-07", "file1.csv", "ts1"),

        # Rejected: Negative amount
        (107.0, 8.0, 80.0, "-$10.00", "2023-01-08", "file1.csv", "ts1"),

        # Rejected: Invalid amount string (will become NaN)
        (108.0, 9.0, 90.0, "abc", "2023-01-09", "file1.csv", "ts1"),

        # Rejected: Both null id and invalid amount (NaN from 'invalid')
        (None, 10.0, 100.0, "invalid", "2023-01-10", "file1.csv", "ts1"),

        # Rejected: Both null id and zero amount
        (None, 11.0, 110.0, "$0.00", "2023-01-11", "file1.csv", "ts1"),
    ]
    transactions_df = pd.DataFrame(transactions_data, columns=[
        'transaction_id', 'customer_id', 'quantity', 'amount', 'transaction_date', '_source_file', '_ingest_ts'
    ])
    con.execute("CREATE TABLE bronze.transactions_raw AS SELECT * FROM transactions_df")
    con.close()

    # Run the main transformation function
    main()

    yield # This allows tests to run

    # Teardown: Clean up the database file after all tests are done
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

@pytest.fixture
def duckdb_connection():
    """
    Fixture to provide a DuckDB connection for each test.
    """
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

def test_silver_table_exists_and_has_rows(setup_database, duckdb_connection):
    """
    Test that silver.transactions_cleaned table exists and contains valid rows.
    """
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM silver.transactions_cleaned").fetchone()[0]
    assert result > 0, "silver.transactions_cleaned table should exist and have rows"

def test_rejected_table_exists_and_has_rows(setup_database, duckdb_connection):
    """
    Test that rejected.rejected_rows table exists and contains rejected rows.
    """
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]
    assert result > 0, "rejected.rejected_rows table should exist and have rows"

def test_no_null_transaction_id_in_silver(setup_database, duckdb_connection):
    """
    Test that silver.transactions_cleaned does not contain rows with null transaction_id.
    """
    con = duckdb_connection
    null_ids_count = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE transaction_id IS NULL").fetchone()[0]
    assert null_ids_count == 0, "silver.transactions_cleaned should not have null transaction_id"

def test_amount_is_numeric_and_positive_in_silver(setup_database, duckdb_connection):
    """
    Test that the 'amount' column in silver.transactions_cleaned is numeric and strictly positive.
    """
    con = duckdb_connection
    # Check data type (DuckDB's describe will show the type)
    schema_info = con.execute("DESCRIBE silver.transactions_cleaned").df()
    amount_type = schema_info[schema_info['column_name'] == 'amount']['column_type'].iloc[0]
    assert 'DOUBLE' in amount_type or 'FLOAT' in amount_type, f"Amount column should be numeric, got {amount_type}"

    # Check for non-positive amounts
    non_positive_amount_count = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE amount <= 0").fetchone()[0]
    assert non_positive_amount_count == 0, "silver.transactions_cleaned should not have non-positive amounts"

def test_string_columns_stripped_in_silver(setup_database, duckdb_connection):
    """
    Test that string columns in silver.transactions_cleaned have been stripped of whitespace.
    """
    con = duckdb_connection
    # The test data includes ' 2023-01-01 ' for transaction_date.
    # After stripping, it should be '2023-01-01'.
    # Check if any leading/trailing spaces exist in string columns.
    whitespace_count = con.execute("""
        SELECT count(*) FROM silver.transactions_cleaned
        WHERE transaction_date LIKE ' %' OR transaction_date LIKE '% '
    """).fetchone()[0]
    assert whitespace_count == 0, "String columns in silver.transactions_cleaned should be stripped of whitespace"

def test_rejected_rows_reasons(setup_database, duckdb_connection):
    """
    Test that rejected rows have the correct rejection_reason assigned.
    """
    con = duckdb_connection
    # Test specific rejection reasons based on the test data
    # Null transaction_id
    reason_null_id = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id IS NULL AND amount = 50.0").fetchone()[0]
    assert "Null transaction_id" in reason_null_id

    # Null amount (from None or 'NULL' string)
    reason_null_amount_1 = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 104.0").fetchone()[0]
    assert "Invalid amount (null or non-positive)" in reason_null_amount_1
    reason_null_amount_2 = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 105.0").fetchone()[0]
    assert "Invalid amount (null or non-positive)" in reason_null_amount_2

    # Zero amount
    reason_zero_amount = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 106.0").fetchone()[0]
    assert "Invalid amount (null or non-positive)" in reason_zero_amount

    # Negative amount
    reason_negative_amount = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 107.0").fetchone()[0]
    assert "Invalid amount (null or non-positive)" in reason_negative_amount

    # Invalid amount string (becomes NaN)
    reason_invalid_string_amount = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 108.0").fetchone()[0]
    assert "Invalid amount (null or non-positive)" in reason_invalid_string_amount

    # Both null id and invalid amount (NaN from 'invalid')
    reason_both_invalid_1 = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE customer_id = 10.0").fetchone()[0]
    assert "Null transaction_id" in reason_both_invalid_1 and "Invalid amount (null or non-positive)" in reason_both_invalid_1

    # Both null id and zero amount
    reason_both_invalid_2 = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE customer_id = 11.0").fetchone()[0]
    assert "Null transaction_id" in reason_both_invalid_2 and "Invalid amount (null or non-positive)" in reason_both_invalid_2

def test_total_rows_conservation(setup_database, duckdb_connection):
    """
    Test that the total number of rows is conserved (bronze = silver + rejected).
    """
    con = duckdb_connection
    bronze_count = con.execute("SELECT count(*) FROM bronze.transactions_raw").fetchone()[0]
    silver_count = con.execute("SELECT count(*) FROM silver.transactions_cleaned").fetchone()[0]
    rejected_count = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]
    assert bronze_count == (silver_count + rejected_count), "Total rows in bronze should equal sum of silver and rejected"

def test_rejected_rows_schema(setup_database, duckdb_connection):
    """
    Test that rejected.rejected_rows table has the 'rejection_reason' column with a string type.
    """
    con = duckdb_connection
    schema_info = con.execute("DESCRIBE rejected.rejected_rows").df()
    assert 'rejection_reason' in schema_info['column_name'].values, "rejected.rejected_rows should have 'rejection_reason' column"
    reason_type = schema_info[schema_info['column_name'] == 'rejection_reason']['column_type'].iloc[0]
    assert 'VARCHAR' in reason_type or 'STRING' in reason_type, f"rejection_reason should be string type, got {reason_type}"