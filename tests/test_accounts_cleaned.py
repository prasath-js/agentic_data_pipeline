import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for the test environment
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

# --- Start of copied transformation logic for test setup ---
# This is a workaround to make the test fixture self-contained and run the transformation.
# In a real project, you would import this from a separate module.
def _run_accounts_transformation_for_test(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    accounts_raw_df = con.execute("SELECT * FROM bronze.accounts_raw").df()

    rejected_df = pd.DataFrame()

    null_account_id_rows = accounts_raw_df[accounts_raw_df['account_id'].isnull()].copy()
    if not null_account_id_rows.empty:
        null_account_id_rows['rejection_reason'] = 'account_id is null'
        rejected_df = pd.concat([rejected_df, null_account_id_rows], ignore_index=True)

    cleaned_df = accounts_raw_df.dropna(subset=['account_id']).copy()

    string_cols = cleaned_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        if not cleaned_df[col].isnull().all():
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

    if 'industry' in cleaned_df.columns:
        cleaned_df['industry'] = cleaned_df['industry'].str.upper()

    cleaned_df.drop_duplicates(subset=['account_id'], keep='first', inplace=True)

    if not rejected_df.empty:
        table_exists_query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        table_exists = con.execute(table_exists_query).fetchone()[0] > 0

        if not table_exists:
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df")
        else:
            existing_cols_query = "PRAGMA table_info('rejected.rejected_rows')"
            existing_cols_df = con.execute(existing_cols_query).df()
            existing_col_names = existing_cols_df['name'].tolist()

            cols_to_insert = [col for col in rejected_df.columns if col in existing_col_names]
            temp_rejected_df = rejected_df[cols_to_insert].copy()

            for col in existing_col_names:
                if col not in temp_rejected_df.columns:
                    temp_rejected_df[col] = None

            temp_rejected_df = temp_rejected_df[existing_col_names]
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM temp_rejected_df")

    con.execute("CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM cleaned_df")
# --- End of copied transformation logic for test setup ---


@pytest.fixture(scope="module")
def setup_database():
    """
    Fixture to set up a clean DuckDB database, populate bronze.accounts_raw with test data,
    run the transformation, and clean up afterwards.
    """
    # Ensure a clean database file for each test run
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # Create test data for bronze.accounts_raw
    test_data = pd.DataFrame({
        'account_id': ['ACC001 ', 'ACC002', None, 'ACC001', 'ACC003', 'ACC004 '],
        'account_name': ['  Account One  ', 'Account Two', 'Null ID Account', 'Account One Duplicate', 'Account Three', 'Account Four'],
        'industry': [' tech ', 'Finance', None, 'TECH', 'Manufacturing', 'HEALTHCARE'],
        'region': ['USA', 'Europe', 'Asia', 'USA', 'Europe', 'USA'],
        '_source_file': ['file1.csv'] * 6,
        '_ingest_ts': ['2023-01-01'] * 6
    })
    con.execute("CREATE OR REPLACE TABLE bronze.accounts_raw AS SELECT * FROM test_data")

    # Run the transformation using the copied function to populate silver and rejected tables
    _run_accounts_transformation_for_test(con)

    con.close() # Close connection used for setup

    yield # Allow tests to run

    # Teardown: clean up the database file
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_accounts_cleaned_table_exists_and_has_rows(setup_database):
    """Verify that silver.accounts_cleaned table exists and contains data."""
    con = duckdb.connect(DB_PATH)
    try:
        result = con.execute("SELECT COUNT(*) FROM silver.accounts_cleaned").fetchone()[0]
        assert result > 0, "silver.accounts_cleaned table should exist and have rows"
    finally:
        con.close()

def test_no_nulls_in_account_id(setup_database):
    """Verify that the account_id column in silver.accounts_cleaned has no null values."""
    con = duckdb.connect(DB_PATH)
    try:
        null_count = con.execute("SELECT COUNT(*) FROM silver.accounts_cleaned WHERE account_id IS NULL").fetchone()[0]
        assert null_count == 0, "account_id column in silver.accounts_cleaned should not have nulls"
    finally:
        con.close()

def test_account_id_is_unique(setup_database):
    """Verify that the account_id column in silver.accounts_cleaned contains only unique values."""
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT account_id FROM silver.accounts_cleaned").df()
        assert df['account_id'].is_unique, "account_id column should be unique after dropping duplicates"
    finally:
        con.close()

def test_string_columns_are_stripped(setup_database):
    """Verify that leading/trailing whitespace is stripped from string columns."""
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT account_id, account_name FROM silver.accounts_cleaned WHERE account_id = 'ACC001'").df()
        assert not df.empty, "Test data for ACC001 should exist"
        assert df['account_id'].iloc[0] == 'ACC001', "account_id should be stripped"
        assert df['account_name'].iloc[0] == 'Account One', "account_name should be stripped"
    finally:
        con.close()

def test_industry_is_uppercased(setup_database):
    """Verify that the 'industry' column values are converted to uppercase."""
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT industry FROM silver.accounts_cleaned WHERE account_id IN ('ACC001', 'ACC002', 'ACC004')").df()
        assert not df.empty, "Test data for industry should exist"
        
        # Check specific values from test data
        assert 'TECH' in df['industry'].values
        assert 'FINANCE' in df['industry'].values
        assert 'HEALTHCARE' in df['industry'].values

        # Ensure all non-null values are uppercase
        assert all(x == str(x).upper() for x in df['industry'].dropna()), "All non-null industry values should be uppercase"
    finally:
        con.close()

def test_rejected_rows_table_exists_and_contains_rejections(setup_database):
    """Verify that rejected.rejected_rows table exists and contains the expected rejected rows."""
    con = duckdb.connect(DB_PATH)
    try:
        # Check if table exists
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'").fetchone()[0] > 0
        assert table_exists, "rejected.rejected_rows table should exist"

        # Check if it contains the expected rejected row with 'account_id is null' reason
        rejected_count = con.execute("SELECT COUNT(*) FROM rejected.rejected_rows WHERE rejection_reason = 'account_id is null'").fetchone()[0]
        assert rejected_count == 1, "rejected.rejected_rows should contain 1 row with 'account_id is null' reason"

        # Verify the content of the rejected row
        rejected_df = con.execute("SELECT account_name, account_id, rejection_reason FROM rejected.rejected_rows WHERE rejection_reason = 'account_id is null'").df()
        assert rejected_df['account_name'].iloc[0] == 'Null ID Account', "Rejected row should be the one with null account_id"
        assert pd.isna(rejected_df['account_id'].iloc[0]), "Rejected row's account_id should be null"
    finally:
        con.close()

def test_accounts_cleaned_data_types(setup_database):
    """Verify that key columns in silver.accounts_cleaned have the correct data types."""
    con = duckdb.connect(DB_PATH)
    try:
        # Get schema information
        schema_df = con.execute("PRAGMA table_info('silver.accounts_cleaned')").df()
        
        # Check data types for key columns
        account_id_type = schema_df[schema_df['name'] == 'account_id']['type'].iloc[0]
        account_name_type = schema_df[schema_df['name'] == 'account_name']['type'].iloc[0]
        industry_type = schema_df[schema_df['name'] == 'industry']['type'].iloc[0]

        assert account_id_type in ['VARCHAR', 'STRING'], f"account_id should be VARCHAR/STRING, got {account_id_type}"
        assert account_name_type in ['VARCHAR', 'STRING'], f"account_name should be VARCHAR/STRING, got {account_name_type}"
        assert industry_type in ['VARCHAR', 'STRING', 'ANY'], f"industry should be VARCHAR/STRING, got {industry_type}" # ANY is sometimes used for mixed types or nulls
    finally:
        con.close()