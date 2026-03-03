import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for tests
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture to provide a DuckDB connection for tests."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

@pytest.fixture(scope="module")
def setup_bronze_data(duckdb_connection):
    """
    Sets up dummy bronze data for testing.
    This ensures tests are self-contained and don't rely on external data.
    The transformation's main() function is assumed to be executed externally
    before the pytest suite runs.
    """
    con = duckdb_connection
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Create dummy customers_raw data
    customers_raw_data = [
        {'customer_id': 1.0, 'name': '  Alice   ', 'email': 'alice@example.com ', 'address': '123 Main St', 'join_date': '2023-01-01', '_source_file': 'a.csv', '_ingest_ts': 'ts1'},
        {'customer_id': 2.0, 'name': 'Bob', 'email': 'bob@example.com', 'address': '456 Oak Ave', 'join_date': '2023-01-02', '_source_file': 'a.csv', '_ingest_ts': 'ts1'},
        {'customer_id': 1.0, 'name': 'Alice B', 'email': 'alice.b@example.com', 'address': '789 Pine Ln', 'join_date': '2023-01-05', '_source_file': 'b.csv', '_ingest_ts': 'ts2'}, # Duplicate, later join_date
        {'customer_id': None, 'name': 'Charlie', 'email': 'charlie@example.com', 'address': '101 Maple Dr', 'join_date': '2023-01-03', '_source_file': 'a.csv', '_ingest_ts': 'ts1'}, # Null customer_id
        {'customer_id': 3.0, 'name': 'David', 'email': 'invalid-email', 'address': '202 Birch Rd', 'join_date': '2023-01-04', '_source_file': 'a.csv', '_ingest_ts': 'ts1'},
        {'customer_id': 4.0, 'name': 'Eve', 'email': None, 'address': '303 Cedar Ct', 'join_date': '2023-01-06', '_source_file': 'a.csv', '_ingest_ts': 'ts1'},
        {'customer_id': 5.0, 'name': 'Frank', 'email': 'frank@test.com', 'address': '404 Elm St', 'join_date': '2023-01-07', '_source_file': 'a.csv', '_ingest_ts': 'ts1'},
        {'customer_id': 5.0, 'name': 'Frank', 'email': 'frank@test.com', 'address': '404 Elm St', 'join_date': '2023-01-06', '_source_file': 'a.csv', '_ingest_ts': 'ts1'}, # Duplicate, earlier join_date
    ]
    customers_raw_df = pd.DataFrame(customers_raw_data)
    con.execute("CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM customers_raw_df;")

    yield # Bronze data is set up, transformation is assumed to have run.

def test_customers_cleaned_table_exists(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    tables = con.execute("SHOW TABLES IN silver;").df()
    assert 'customers_cleaned' in tables['name'].tolist()

def test_customers_cleaned_has_rows(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT * FROM silver.customers_cleaned;").df()
    assert not df.empty
    assert len(df) > 0

def test_customer_id_no_nulls_in_silver(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT customer_id FROM silver.customers_cleaned;").df()
    assert df['customer_id'].isnull().sum() == 0

def test_join_date_is_datetime(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    # DuckDB's Python client returns datetime objects for DATE/TIMESTAMP columns
    df = con.execute("SELECT join_date FROM silver.customers_cleaned LIMIT 1;").df()
    if 'join_date' in df.columns:
        assert pd.api.types.is_datetime64_any_dtype(df['join_date'])
    else:
        pytest.fail("join_date column not found in silver.customers_cleaned")

def test_email_is_valid_is_boolean(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT email_is_valid FROM silver.customers_cleaned LIMIT 1;").df()
    assert pd.api.types.is_bool_dtype(df['email_is_valid'])

def test_rejected_rows_table_exists(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    tables = con.execute("SHOW TABLES IN rejected;").df()
    assert 'rejected_rows' in tables['name'].tolist()

def test_rejected_rows_contains_null_customer_id(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows;").df()
    assert not rejected_df.empty
    assert 'rejection_reason' in rejected_df.columns
    assert (rejected_df['rejection_reason'] == 'null customer_id').any()
    assert rejected_df['customer_id'].isnull().any()

def test_email_is_valid_logic(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT customer_id, email, email_is_valid FROM silver.customers_cleaned;").df()

    # Check specific customer_ids and their email_is_valid status based on the transformation logic
    assert df[df['customer_id'] == 1]['email_is_valid'].iloc[0] == True
    assert df[df['customer_id'] == 2]['email_is_valid'].iloc[0] == True
    assert df[df['customer_id'] == 3]['email_is_valid'].iloc[0] == False
    assert df[df['customer_id'] == 4]['email_is_valid'].iloc[0] == False
    assert df[df['customer_id'] == 5]['email_is_valid'].iloc[0] == True

    # Also check the actual email values to ensure they are as expected after deduplication and stripping
    assert df[df['customer_id'] == 1]['email'].iloc[0] == 'alice.b@example.com'
    assert df[df['customer_id'] == 3]['email'].iloc[0] == 'invalid-email'
    assert pd.isna(df[df['customer_id'] == 4]['email'].iloc[0])

def test_no_duplicate_customer_ids(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT customer_id FROM silver.customers_cleaned;").df()
    assert df['customer_id'].duplicated().sum() == 0

def test_whitespace_stripped(duckdb_connection, setup_bronze_data):
    con = duckdb_connection
    df = con.execute("SELECT name, email FROM silver.customers_cleaned WHERE customer_id = 1;").df()
    # The kept row for customer_id 1.0 is 'Alice B' and 'alice.b@example.com'
    assert df['name'].iloc[0] == 'Alice B'
    assert df['email'].iloc[0] == 'alice.b@example.com'