import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for tests
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture for a DuckDB connection, ensuring cleanup."""
    # Ensure the directory for the DB_PATH exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Attempt to remove the DB file from a previous run, handling potential locks
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError as e:
            # If removal fails, it means the DB file is locked by another process.
            # This is a critical setup failure for isolated tests.
            pytest.fail(f"CRITICAL ERROR: Could not remove existing DB file '{DB_PATH}' during setup: {e}. "
                        "Ensure no other process is using the database file and try again.")
    
    con = duckdb.connect(DB_PATH)
    
    # Setup: Create bronze schema and customers_raw table with test data
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")
    
    # Create a dummy customers_raw table for testing
    # Include cases for null customer_id, whitespace, invalid date, duplicates, email validity
    con.execute("""
        CREATE OR REPLACE TABLE bronze.customers_raw (
            customer_id DOUBLE,
            name VARCHAR,
            email VARCHAR,
            address VARCHAR,
            join_date VARCHAR,
            _source_file VARCHAR,
            _ingest_ts VARCHAR
        );
    """)
    con.execute("""
        INSERT INTO bronze.customers_raw VALUES
        (1.0, ' John Doe ', 'john.doe@example.com', '123 Main St ', '2023-01-01', 'file1.csv', '2023-01-01T00:00:00'),
        (2.0, ' Jane Smith ', 'jane.smith@test.com ', '456 Oak Ave', '2023-02-01', 'file1.csv', '2023-01-01T00:00:00'),
        (1.0, ' John Doe ', 'john.doe.old@example.com', '122 Main St', '2022-12-31', 'file1.csv', '2023-01-01T00:00:00'), -- Duplicate, older date
        (3.0, ' Peter Jones ', 'peter.jones@invalid', '789 Pine Rd', '2023-03-01', 'file2.csv', '2023-01-01T00:00:00'),
        (4.0, ' Alice Brown ', NULL, '101 Elm St', '2023-04-01', 'file2.csv', '2023-01-01T00:00:00'),
        (NULL, ' Rejected Customer ', 'rejected@example.com', '999 Null St', '2023-05-01', 'file3.csv', '2023-01-01T00:00:00'), -- Null customer_id
        (5.0, ' Bob White ', 'bob.white@domain.com', '222 River Ln', 'INVALID_DATE', 'file3.csv', '2023-01-01T00:00:00'), -- Invalid date
        (6.0, ' Charlie Green ', 'no-at-sign.com', '333 Hill Rd', '2023-06-01', 'file4.csv', '2023-01-01T00:00:00'),
        (7.0, ' David Black ', 'david.black@example.com', '444 Lake Dr', '2023-07-01', 'file4.csv', '2023-01-01T00:00:00')
    """)
    
    # Run the transformation, passing the fixture's connection
    from __main__ import main
    main(con)
    
    yield con
    
    # Teardown: Close connection and clean up tables/schemas if necessary
    con.close()
    # Remove the DB file to ensure a clean state for subsequent test runs
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError as e:
            print(f"WARNING: Could not remove DB file '{DB_PATH}' during teardown: {e}")

def test_customers_cleaned_exists_and_has_rows(duckdb_connection):
    """Test that silver.customers_cleaned table exists and contains data."""
    con = duckdb_connection
    df = con.execute("SELECT COUNT(*) FROM silver.customers_cleaned;").df()
    assert df.iloc[0, 0] > 0, "silver.customers_cleaned should exist and have rows."

def test_no_null_customer_id_in_cleaned(duckdb_connection):
    """Test that there are no null customer_ids in silver.customers_cleaned."""
    con = duckdb_connection
    df = con.execute("SELECT COUNT(*) FROM silver.customers_cleaned WHERE customer_id IS NULL;").df()
    assert df.iloc[0, 0] == 0, "silver.customers_cleaned should not have null customer_id."

def test_join_date_datatype(duckdb_connection):
    """Test that join_date in silver.customers_cleaned is of datetime type."""
    con = duckdb_connection
    # DuckDB's describe function returns 'TIMESTAMP' for datetime
    df_info = con.execute("DESCRIBE silver.customers_cleaned;").df()
    join_date_type = df_info[df_info['column_name'] == 'join_date']['column_type'].iloc[0]
    assert 'TIMESTAMP' in join_date_type, f"join_date should be TIMESTAMP, but is {join_date_type}"

def test_email_is_valid_datatype(duckdb_connection):
    """Test that email_is_valid in silver.customers_cleaned is of boolean type."""
    con = duckdb_connection
    df_info = con.execute("DESCRIBE silver.customers_cleaned;").df()
    email_valid_type = df_info[df_info['column_name'] == 'email_is_valid']['column_type'].iloc[0]
    assert 'BOOLEAN' in email_valid_type, f"email_is_valid should be BOOLEAN, but is {email_valid_type}"

def test_rejected_rows_table_exists(duckdb_connection):
    """Test that rejected.rejected_rows table exists."""
    con = duckdb_connection
    # Check if the table exists and can be queried
    try:
        con.execute("SELECT COUNT(*) FROM rejected.rejected_rows;").df()
        table_exists = True
    except duckdb.CatalogException:
        table_exists = False
    assert table_exists, "rejected.rejected_rows table should exist."

def test_rejected_rows_contain_null_customer_id_and_reason(duckdb_connection):
    """Test that rejected rows have null customer_id and a rejection_reason."""
    con = duckdb_connection
    rejected_df = con.execute("SELECT customer_id, rejection_reason FROM rejected.rejected_rows;").df()
    assert not rejected_df.empty, "rejected.rejected_rows should not be empty."
    assert rejected_df['customer_id'].isnull().all(), "All rejected rows should have null customer_id."
    assert (rejected_df['rejection_reason'] == 'customer_id is null').all(), "Rejection reason should be 'customer_id is null'."

def test_string_columns_stripped(duckdb_connection):
    """Test that string columns like 'name' and 'address' have leading/trailing whitespace stripped."""
    con = duckdb_connection
    df = con.execute("SELECT name, address FROM silver.customers_cleaned WHERE customer_id = 1.0;").df()
    assert df['name'].iloc[0] == 'John Doe', f"Name not stripped: '{df['name'].iloc[0]}'"
    assert df['address'].iloc[0] == '123 Main St', f"Address not stripped: '{df['address'].iloc[0]}'"

def test_email_is_valid_logic(duckdb_connection):
    """Test the logic for email_is_valid column."""
    con = duckdb_connection
    df = con.execute("SELECT email, email_is_valid FROM silver.customers_cleaned ORDER BY customer_id;").df()
    
    # Valid email
    assert df[df['email'] == 'john.doe@example.com']['email_is_valid'].iloc[0] == True
    # Email with whitespace, should still be valid after strip (strip happens before email_is_valid check)
    # The test data has 'jane.smith@test.com ' which should be stripped to 'jane.smith@test.com'
    assert df[df['email'] == 'jane.smith@test.com']['email_is_valid'].iloc[0] == True
    # Invalid email (no @)
    assert df[df['email'] == 'peter.jones@invalid']['email_is_valid'].iloc[0] == False
    # Null email (from test data: Alice Brown has NULL email)
    assert df[df['email'].isnull()]['email_is_valid'].iloc[0] == False
    # Email without @ sign
    assert df[df['email'] == 'no-at-sign.com']['email_is_valid'].iloc[0] == False

def test_no_duplicate_customer_id(duckdb_connection):
    """Test that there are no duplicate customer_ids in silver.customers_cleaned."""
    con = duckdb_connection
    df = con.execute("SELECT customer_id FROM silver.customers_cleaned;").df()
    assert df['customer_id'].is_unique, "customer_id column should be unique."

def test_duplicate_customer_id_keeps_latest_join_date(duckdb_connection):
    """Test that for duplicate customer_ids, the row with the latest join_date is kept."""
    con = duckdb_connection
    df = con.execute("SELECT customer_id, join_date, email FROM silver.customers_cleaned WHERE customer_id = 1.0;").df()
    assert len(df) == 1, "Only one row for customer_id 1.0 should remain."
    assert df['join_date'].iloc[0].strftime('%Y-%m-%d') == '2023-01-01', "Latest join_date (2023-01-01) should be kept for customer_id 1.0."
    assert df['email'].iloc[0] == 'john.doe@example.com', "Email corresponding to the latest join_date should be kept."

def test_row_counts_match_after_rejection(duckdb_connection):
    """Verify total rows in raw = cleaned + rejected."""
    con = duckdb_connection
    raw_count = con.execute("SELECT COUNT(*) FROM bronze.customers_raw;").df().iloc[0, 0]
    cleaned_count = con.execute("SELECT COUNT(*) FROM silver.customers_cleaned;").df().iloc[0, 0]
    rejected_count = con.execute("SELECT COUNT(*) FROM rejected.rejected_rows;").df().iloc[0, 0]
    
    assert raw_count == (cleaned_count + rejected_count), "Total raw rows should equal cleaned + rejected rows."