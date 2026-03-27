import pytest
import duckdb
import pandas as pd
import os
import time
import gc # Import gc module

# Define DB_PATH for tests
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_database():
    """
    Fixture to set up a clean database, populate bronze.accounts_raw,
    run the transformation, and then clean up.
    """
    # Ensure a clean database for testing
    # Attempt to remove the database file, with retries for Windows PermissionError
    for _ in range(20): # Increased retries
        try:
            if os.path.exists(DB_PATH):
                gc.collect() # Force garbage collection before attempting to remove
                os.remove(DB_PATH)
            break # If successful, break the loop
        except PermissionError:
            time.sleep(1.0) # Increased sleep time
    else:
        # If loop completes without breaking, it means os.remove failed repeatedly
        raise PermissionError(f"Could not remove database file {DB_PATH} after multiple retries during setup.")

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA silver")
    con.execute("CREATE SCHEMA rejected")

    # Create dummy data for bronze.accounts_raw
    # Includes:
    # - Null account_id (for rejection)
    # - All-whitespace account_id (for rejection after stripping)
    # - Duplicate account_id (for deduplication)
    # - Leading/trailing spaces (for stripping)
    # - Mixed case industry (for uppercasing)
    # - Valid rows
    data = {
        'account_id': ['ACC001 ', 'ACC002', 'ACC001', None, '   ', 'ACC003', 'ACC004 ', 'ACC005'],
        'account_name': ['  Account One  ', 'Account Two', 'Account One Duplicate', 'Null ID Account', 'Whitespace ID Account', 'Account Three', 'Account Four', 'Account Five'],
        'industry': ['tech ', 'Finance', 'TECH', 'Retail', 'Manufacturing', 'Healthcare', None, 'IT'],
        'region': ['USA', 'Europe', 'USA', 'Asia', 'Europe', 'USA', 'Africa', 'Asia'],
        '_source_file': ['file1.csv'] * 8,
        '_ingest_ts': ['2023-01-01'] * 8
    }
    bronze_accounts_raw_df = pd.DataFrame(data)
    con.execute("CREATE TABLE bronze.accounts_raw AS SELECT * FROM bronze_accounts_raw_df")

    con.close() # Close the connection used for setup before main() runs

    # Run the transformation
    main() 

    yield # This is where the tests run

    # Teardown: Clean up after tests
    con = duckdb.connect(DB_PATH) # Re-open connection for teardown
    con.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
    con.execute("DROP SCHEMA IF EXISTS silver CASCADE")
    con.execute("DROP SCHEMA IF EXISTS rejected CASCADE")
    con.close() # Close connection after teardown operations

    # Attempt to remove the database file again, with retries
    for _ in range(20):
        try:
            if os.path.exists(DB_PATH):
                gc.collect() # Force garbage collection before attempting to remove
                os.remove(DB_PATH)
            break
        except PermissionError:
            time.sleep(1.0)
    else:
        print(f"Warning: Could not remove database file {DB_PATH} during teardown.")

@pytest.fixture(scope="function")
def db_connection(setup_database):
    """Fixture to provide a fresh connection for each test function."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close() # Ensure connection is closed after each test

# --- Test Cases ---

def test_accounts_cleaned_exists_and_has_rows(db_connection):
    """Verify that silver.accounts_cleaned table exists and contains data."""
    df = db_connection.execute("SELECT * FROM silver.accounts_cleaned").df()
    assert not df.empty, "silver.accounts_cleaned should not be empty"
    assert len(df) > 0, "silver.accounts_cleaned should have more than 0 rows"

def test_no_null_account_id_in_cleaned(db_connection):
    """Verify that there are no null account_id values in the cleaned table."""
    df = db_connection.execute("SELECT account_id FROM silver.accounts_cleaned").df()
    assert df['account_id'].isnull().sum() == 0, "account_id in silver.accounts_cleaned should not have nulls"

def test_no_duplicate_account_id_in_cleaned(db_connection):
    """Verify that there are no duplicate account_id values in the cleaned table."""
    df = db_connection.execute("SELECT account_id FROM silver.accounts_cleaned").df()
    assert df['account_id'].duplicated().sum() == 0, "account_id in silver.accounts_cleaned should not have duplicates"

def test_string_columns_stripped(db_connection):
    """Verify that string columns have leading/trailing whitespace stripped."""
    df = db_connection.execute("SELECT account_name FROM silver.accounts_cleaned WHERE account_id = 'ACC001'").df()
    # Original: '  Account One  ', expected: 'Account One'
    assert df['account_name'].iloc[0] == 'Account One', "account_name should be stripped of whitespace"

def test_industry_uppercased(db_connection):
    """Verify that the 'industry' column values are all uppercase."""
    df = db_connection.execute("SELECT industry FROM silver.accounts_cleaned WHERE account_id IN ('ACC001', 'ACC002')").df()
    # Original: 'tech ', 'Finance', 'TECH' -> Expected: 'TECH', 'FINANCE'
    # Dropna to handle potential None values which become 'NAN' after transformation
    assert all(val == str(val).upper() for val in df['industry'].dropna()), "industry column should be entirely uppercase"
    assert 'TECH' in df['industry'].values
    assert 'FINANCE' in df['industry'].values

def test_rejected_rows_table_exists(db_connection):
    """Verify that the rejected.rejected_rows table exists."""
    table_exists = db_connection.execute("""
        SELECT count(*)
        FROM duckdb_tables()
        WHERE schema_name = 'rejected' AND table_name = 'rejected_rows'
    """).fetchone()[0] > 0
    assert table_exists, "rejected.rejected_rows table should exist"

def test_rejected_rows_content(db_connection):
    """Verify the content of the rejected.rejected_rows table."""
    rejected_df = db_connection.execute("SELECT * FROM rejected.rejected_rows").df()
    assert not rejected_df.empty, "rejected.rejected_rows should not be empty"
    assert len(rejected_df) == 2, "rejected.rejected_rows should contain exactly 2 rows from this transformation"
    assert rejected_df['account_id'].isnull().all(), "All rejected rows should have null account_id"
    assert (rejected_df['rejection_reason'] == 'account_id is null or empty after stripping').all(), "Rejection reason should be 'account_id is null or empty after stripping'"
    assert 'Null ID Account' in rejected_df['account_name'].values, "Rejected row content mismatch for Null ID Account"
    assert 'Whitespace ID Account' in rejected_df['account_name'].values, "Rejected row content mismatch for Whitespace ID Account"


def test_row_counts_match(db_connection):
    """Verify that row counts across raw, cleaned, and rejected tables are consistent."""
    raw_count = db_connection.execute("SELECT count(*) FROM bronze.accounts_raw").fetchone()[0]
    cleaned_count = db_connection.execute("SELECT count(*) FROM silver.accounts_cleaned").fetchone()[0]
    rejected_count = db_connection.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]

    # Based on the fixture data:
    # Total raw rows: 8
    # Rows with null/empty account_id after stripping: 2 (None, '   ') -> rejected
    # Non-null/empty account_ids in raw (after stripping): 6 (['ACC001', 'ACC002', 'ACC001', 'ACC003', 'ACC004', 'ACC005'])
    # Unique non-null/empty account_ids after stripping: 5 (['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'])
    
    expected_cleaned_count = 5
    expected_rejected_count = 2

    assert cleaned_count == expected_cleaned_count, \
        f"Cleaned row count mismatch. Expected {expected_cleaned_count}, got {cleaned_count}."
    assert rejected_count == expected_rejected_count, \
        f"Rejected row count mismatch. Expected {expected_rejected_count}, got {rejected_count}."
    
    # The sum of cleaned and rejected should equal the raw count minus the duplicates that were dropped
    # (duplicates are only dropped from the non-rejected set)
    total_processed_rows = cleaned_count + rejected_count
    
    # Calculate duplicates dropped from the non-rejected set:
    # Raw rows: 8
    # Rejected rows: 2 (None, '   ')
    # Remaining for cleaning: 6 (['ACC001 ', 'ACC002', 'ACC001', 'ACC003', 'ACC004 ', 'ACC005'])
    # After stripping and taking unique: 5 (['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'])
    # Duplicates dropped from this set: 6 - 5 = 1
    duplicates_dropped_from_non_rejected = 1

    assert total_processed_rows == (raw_count - duplicates_dropped_from_non_rejected), \
        f"Sum of cleaned ({cleaned_count}) and rejected ({rejected_count}) rows " \
        f"should equal raw ({raw_count}) minus duplicates dropped ({duplicates_dropped_from_non_rejected})."


def test_data_types_in_cleaned(db_connection):
    """Verify the data types of important columns in the cleaned table."""
    df = db_connection.execute("SELECT account_id, account_name, industry, region FROM silver.accounts_cleaned LIMIT 1").df()
    
    assert pd.api.types.is_string_dtype(df['account_id']), "account_id should be string type"
    assert pd.api.types.is_string_dtype(df['account_name']), "account_name should be string type"
    # Industry can be None/NaN, which pandas might infer as object if all values are None/NaN.
    # But if there are strings, it should be string.
    # The transformation converts None/NaN to 'NAN' then back to pd.NA, so it should be object or string.
    assert pd.api.types.is_string_dtype(df['industry']) or pd.api.types.is_object_dtype(df['industry']), "industry should be string or object type"
    assert pd.api.types.is_string_dtype(df['region']), "region should be string type"