import pytest
import duckdb
import pandas as pd
import os

# Fixture for DuckDB connection
@pytest.fixture(scope="module")
def duckdb_conn():
    DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

def test_customers_cleaned_table_exists_and_has_data(duckdb_conn):
    """Verify that the silver.customers_cleaned table exists and contains data."""
    result = duckdb_conn.execute("SELECT COUNT(*) FROM silver.customers_cleaned").fetchone()[0]
    assert result > 0, "silver.customers_cleaned table should exist and have data"

def test_no_null_customer_id_in_cleaned_table(duckdb_conn):
    """Ensure no customer_id is null in the cleaned customers table."""
    result = duckdb_conn.execute("SELECT COUNT(*) FROM silver.customers_cleaned WHERE customer_id IS NULL").fetchone()[0]
    assert result == 0, "customer_id should not be null in silver.customers_cleaned"

def test_join_date_datatype(duckdb_conn):
    """Verify that the join_date column (if present) has a TIMESTAMP data type."""
    columns = duckdb_conn.execute("PRAGMA table_info('silver.customers_cleaned')").df()
    if 'join_date' in columns['name'].values:
        join_date_type = columns[columns['name'] == 'join_date']['type'].iloc[0]
        assert 'TIMESTAMP' in join_date_type, f"join_date column should be TIMESTAMP, but is {join_date_type}"
    else:
        # If join_date doesn't exist, the test should still pass as it's optional
        pass

def test_email_is_valid_datatype_and_logic(duckdb_conn):
    """Verify email_is_valid column is boolean and its logic is correct."""
    columns = duckdb_conn.execute("PRAGMA table_info('silver.customers_cleaned')").df()
    email_is_valid_type = columns[columns['name'] == 'email_is_valid']['type'].iloc[0]
    assert 'BOOLEAN' in email_is_valid_type, f"email_is_valid column should be BOOLEAN, but is {email_is_valid_type}"

    # Test logic: emails with '@' should be True, without should be False
    df = duckdb_conn.execute("SELECT email, email_is_valid FROM silver.customers_cleaned WHERE email IS NOT NULL LIMIT 100").df()
    if not df.empty:
        for _, row in df.iterrows():
            expected_valid = '@' in str(row['email'])
            assert row['email_is_valid'] == expected_valid, f"Email '{row['email']}' validation failed. Expected {expected_valid}, got {row['email_is_valid']}"

def test_no_duplicate_customer_ids(duckdb_conn):
    """Ensure there are no duplicate customer_ids in the cleaned table."""
    result = duckdb_conn.execute("SELECT COUNT(customer_id) FROM silver.customers_cleaned GROUP BY customer_id HAVING COUNT(customer_id) > 1").fetchone()
    assert result is None, "There should be no duplicate customer_ids in silver.customers_cleaned"

def test_rejected_rows_table_exists(duckdb_conn):
    """Verify that the rejected.rejected_rows table exists."""
    table_exists = duckdb_conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'").fetchone()[0]
    assert table_exists == 1, "rejected.rejected_rows table should exist"

def test_rejected_rows_contain_null_customer_id_reasons(duckdb_conn):
    """Verify that rejected.rejected_rows contains entries for null customer_ids with the correct reason."""
    # Check if there are any rejected rows with the specific reason
    result = duckdb_conn.execute("SELECT COUNT(*) FROM rejected.rejected_rows WHERE rejection_reason = 'null customer_id'").fetchone()[0]
    assert result > 0, "rejected.rejected_rows should contain rows rejected due to 'null customer_id'"

    # Also check that customer_id is indeed null for these rows
    null_id_in_rejected = duckdb_conn.execute("SELECT COUNT(*) FROM rejected.rejected_rows WHERE rejection_reason = 'null customer_id' AND customer_id IS NULL").fetchone()[0]
    assert null_id_in_rejected == result, "All rows rejected for 'null customer_id' should actually have a NULL customer_id"

def test_string_columns_stripped(duckdb_conn):
    """Verify that string columns in silver.customers_cleaned have no leading/trailing whitespace."""
    # Sample a few string columns and check for leading/trailing whitespace
    df = duckdb_conn.execute("SELECT name, email, address FROM silver.customers_cleaned LIMIT 100").df()
    if not df.empty:
        for col in ['name', 'email', 'address']:
            if col in df.columns:
                # Convert to string first to handle potential NaNs, then check for unstripped whitespace
                has_whitespace = df[col].astype(str).apply(lambda x: x != x.strip()).any()
                assert not has_whitespace, f"Column '{col}' in silver.customers_cleaned contains unstripped whitespace"