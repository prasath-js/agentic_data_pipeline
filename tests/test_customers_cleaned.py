import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for tests
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_bronze_data():
    """Fixture to set up test data in bronze.customers_raw."""
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    test_data = pd.DataFrame({
        'customer_id': [1.0, 2.0, 1.0, None, 3.0, 4.0, 5.0, 6.0],
        'name': [' John Doe ', 'Jane Smith', 'John Doe', 'Null ID', 'Alice', 'Bob', 'Charlie', 'David'],
        'email': ['john.doe@example.com', 'jane.smith@test.com ', 'john.doe@example.com', 'invalid-email', 'alice@no-domain', 'bob@example.com', 'charlie', 'david@example.com'],
        'address': ['123 Main St', '456 Oak Ave', '123 Main St', '789 Pine Ln', '101 Elm St', '202 Maple Dr', '303 Birch Rd', '404 Cedar Ct'],
        'join_date': ['2023-01-01', '2023-02-01', '2023-01-05', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01'],
        '_source_file': ['file1.csv']*8,
        '_ingest_ts': ['2023-01-01T00:00:00Z']*8
    })
    con.execute("CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM test_data")
    con.close() # IMPORTANT: Close this connection immediately after use.
    yield # This fixture yields, then the next one runs.

@pytest.fixture(scope="module")
def run_transformation(setup_bronze_data): # Depends on setup_bronze_data
    """Fixture to run the transformation once before tests."""
    # Clear previous rejected rows for a clean test run (using a fresh connection)
    con_cleanup = duckdb.connect(DB_PATH)
    con_cleanup.execute("DROP TABLE IF EXISTS rejected.rejected_rows")
    con_cleanup.close()

    # Run the main transformation function
    main() # main() opens its own connection, does work, closes it.
    yield

@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture to provide a DuckDB connection for tests."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

# Mark all tests in this module to use the run_transformation fixture
pytestmark = pytest.mark.usefixtures("run_transformation")

def test_customers_cleaned_exists_and_has_rows(duckdb_connection):
    """Verify that silver.customers_cleaned table exists and contains data."""
    con = duckdb_connection
    df = con.execute("SELECT * FROM silver.customers_cleaned").df()
    assert not df.empty, "silver.customers_cleaned should not be empty"
    assert len(df) > 0, "silver.customers_cleaned should have more than 0 rows"

def test_no_null_customer_id_in_cleaned(duckdb_connection):
    """Verify that there are no null customer_ids in silver.customers_cleaned."""
    con = duckdb_connection
    null_ids_count = con.execute("SELECT COUNT(*) FROM silver.customers_cleaned WHERE customer_id IS NULL").fetchone()[0]
    assert null_ids_count == 0, "customer_id column in silver.customers_cleaned should not contain nulls"

def test_join_date_datatype(duckdb_connection):
    """Verify that join_date column in silver.customers_cleaned is of TIMESTAMP type."""
    con = duckdb_connection
    schema_info = con.execute("DESCRIBE silver.customers_cleaned").df()
    join_date_type = schema_info[schema_info['column_name'] == 'join_date']['column_type'].iloc[0]
    assert 'TIMESTAMP' in join_date_type, f"join_date should be TIMESTAMP, but is {join_date_type}"

def test_email_is_valid_datatype(duckdb_connection):
    """Verify that email_is_valid column in silver.customers_cleaned is of BOOLEAN type."""
    con = duckdb_connection
    schema_info = con.execute("DESCRIBE silver.customers_cleaned").df()
    email_is_valid_type = schema_info[schema_info['column_name'] == 'email_is_valid']['column_type'].iloc[0]
    assert 'BOOLEAN' in email_is_valid_type, f"email_is_valid should be BOOLEAN, but is {email_is_valid_type}"

def test_no_duplicate_customer_ids(duckdb_connection):
    """Verify that there are no duplicate customer_ids in silver.customers_cleaned."""
    con = duckdb_connection
    duplicate_ids_count = con.execute(
        "SELECT COUNT(customer_id) FROM (SELECT customer_id FROM silver.customers_cleaned GROUP BY customer_id HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert duplicate_ids_count == 0, "There should be no duplicate customer_ids in silver.customers_cleaned"

def test_email_is_valid_logic(duckdb_connection):
    """Verify the logic for email_is_valid column."""
    con = duckdb_connection
    df = con.execute("SELECT email, email_is_valid FROM silver.customers_cleaned").df()

    # Test cases for valid emails
    valid_emails = df[df['email_is_valid'] == True]
    assert all(valid_emails['email'].astype(str).str.contains('@')), "Emails marked valid should contain '@'"

    # Test cases for invalid emails
    invalid_emails = df[df['email_is_valid'] == False]
    assert all(~invalid_emails['email'].astype(str).str.contains('@')), "Emails marked invalid should not contain '@'"

def test_rejected_rows_exist(duckdb_connection):
    """Verify that rejected.rejected_rows table exists and contains rejected data."""
    con = duckdb_connection
    df = con.execute("SELECT * FROM rejected.rejected_rows").df()
    assert not df.empty, "rejected.rejected_rows should not be empty"
    assert len(df) > 0, "rejected.rejected_rows should have more than 0 rows"

def test_rejected_rows_reason_for_null_customer_id(duckdb_connection):
    """Verify that rejected rows have the correct rejection_reason for null customer_id."""
    con = duckdb_connection
    rejected_df = con.execute("SELECT customer_id, rejection_reason FROM rejected.rejected_rows").df()
    assert all(rejected_df['customer_id'].isnull()), "All rejected rows should have null customer_id"
    assert all(rejected_df['rejection_reason'] == 'customer_id is null'), "Rejection reason should be 'customer_id is null'"

def test_whitespace_stripped(duckdb_connection):
    """Verify that string columns have leading/trailing whitespace stripped."""
    con = duckdb_connection
    # Test data for customer_id 1.0 has ' John Doe ' and for 2.0 has 'jane.smith@test.com '
    df_name = con.execute("SELECT name FROM silver.customers_cleaned WHERE customer_id = 1.0").df()
    assert df_name['name'].iloc[0] == 'John Doe', f"Name not stripped: '{df_name['name'].iloc[0]}'"

    df_email = con.execute("SELECT email FROM silver.customers_cleaned WHERE customer_id = 2.0").df()
    assert df_email['email'].iloc[0] == 'jane.smith@test.com', f"Email not stripped: '{df_email['email'].iloc[0]}'"

def test_duplicate_customer_id_resolution(duckdb_connection):
    """Verify that duplicate customer_ids are resolved by keeping the latest join_date."""
    con = duckdb_connection
    # Test data has two entries for customer_id 1.0: '2023-01-01' and '2023-01-05'.
    # The one with '2023-01-05' should be kept.
    df = con.execute("SELECT customer_id, join_date FROM silver.customers_cleaned WHERE customer_id = 1.0").df()
    assert len(df) == 1, "There should be only one entry for customer_id 1.0 after deduplication"
    assert df['join_date'].iloc[0].strftime('%Y-%m-%d') == '2023-01-05', \
        f"Incorrect join_date kept for customer_id 1.0: {df['join_date'].iloc[0].strftime('%Y-%m-%d')}"

def test_total_row_counts(duckdb_connection):
    """Verify that total rows from bronze equals cleaned rows plus rejected rows."""
    con = duckdb_connection
    bronze_count = con.execute("SELECT COUNT(*) FROM bronze.customers_raw").fetchone()[0]
    silver_count = con.execute("SELECT COUNT(*) FROM silver.customers_cleaned").fetchone()[0]
    rejected_count = con.execute("SELECT COUNT(*) FROM rejected.rejected_rows").fetchone()[0]

    assert bronze_count == (silver_count + rejected_count), \
        f"Row count mismatch: Bronze ({bronze_count}) != Silver ({silver_count}) + Rejected ({rejected_count})"