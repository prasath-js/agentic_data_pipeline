import pytest
import duckdb
import pandas as pd
import os

@pytest.fixture(scope="module")
def db_path_for_tests(tmp_path_factory):
    """Fixture to provide a unique temporary DuckDB path for the module."""
    temp_db_path = str(tmp_path_factory.mktemp("duckdb_test_dir") / "pipeline.duckdb")
    return temp_db_path

@pytest.fixture(scope="function")
def duckdb_connection(db_path_for_tests):
    """Fixture to establish and close a DuckDB connection for each test."""
    con = duckdb.connect(db_path_for_tests)
    yield con
    con.close()

@pytest.fixture(scope="module")
def setup_bronze_data(db_path_for_tests):
    """Fixture to set up dummy bronze data for testing."""
    con_setup = duckdb.connect(db_path_for_tests)
    con_setup.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    opportunities_data = [
        ('OPP001', 'ACC001 ', 100.50, '2024-01-01', 'Prospecting ', 'file1.csv', '2023-01-01'),
        ('OPP002', 'ACC002', 200.75, '2024-01-05', 'Negotiation', 'file1.csv', '2023-01-01'),
        ('OPP003', None, 300.00, '2024-01-10', 'Closed Won', 'file2.csv', '2023-01-02'), # Rejected: account_id is null
        ('OPP004', 'ACC004', 400.25, '2024-01-15', None, 'file2.csv', '2023-01-02'), # Rejected: stage is null
        ('OPP005', 'ACC005', 'invalid_value', '2024-01-20', 'Closed Lost', 'file3.csv', '2023-01-03'), # Valid, value coerced to NaN
        ('OPP006', None, 600.00, '2024-01-25', None, 'file3.csv', '2023-01-03'), # Rejected: both null
        ('OPP007', 'ACC007', 700.00, '2024-01-26', 'Closed Won', 'file3.csv', '2023-01-03'),
        ('OPP008', 'ACC008', None, '2024-01-27', 'Closed Lost', 'file3.csv', '2023-01-03'), # Valid, value is null
    ]
    opportunities_raw_df = pd.DataFrame(opportunities_data, columns=[
        'opportunity_id', 'account_id', 'value', 'close_date', 'stage', '_source_file', '_ingest_ts'
    ])
    con_setup.execute("CREATE OR REPLACE TABLE bronze.opportunities_raw AS SELECT * FROM opportunities_raw_df")
    con_setup.close()

    yield

@pytest.fixture(scope="module", autouse=True)
def run_transformation(setup_bronze_data, db_path_for_tests):
    """Fixture to run the main transformation function once for all tests."""
    # Import main from the transformation code
    from __main__ import main
    # Call main, passing the temporary database path
    main(db_path_for_tests)

def test_opportunities_cleaned_exists_and_has_rows(duckdb_connection):
    """Test that silver.opportunities_cleaned table exists and contains data."""
    con = duckdb_connection
    result = con.execute("SELECT COUNT(*) FROM silver.opportunities_cleaned").fetchone()[0]
    assert result > 0, "silver.opportunities_cleaned should exist and have rows."

def test_rejected_rows_exists_and_has_rows(duckdb_connection):
    """Test that rejected.rejected_rows table exists and contains data."""
    con = duckdb_connection
    result = con.execute("SELECT COUNT(*) FROM rejected.rejected_rows").fetchone()[0]
    assert result > 0, "rejected.rejected_rows should exist and have rows."

def test_no_nulls_in_key_columns_cleaned(duckdb_connection):
    """Test that account_id and stage columns in silver.opportunities_cleaned have no nulls."""
    con = duckdb_connection
    cleaned_df = con.execute("SELECT account_id, stage FROM silver.opportunities_cleaned").df()
    assert cleaned_df['account_id'].isnull().sum() == 0, "account_id in silver.opportunities_cleaned should not have nulls."
    assert cleaned_df['stage'].isnull().sum() == 0, "stage in silver.opportunities_cleaned should not have nulls."

def test_opportunity_value_is_numeric(duckdb_connection):
    """Test that the 'value' column in silver.opportunities_cleaned is numeric."""
    con = duckdb_connection
    cleaned_df = con.execute("SELECT value FROM silver.opportunities_cleaned").df()
    assert pd.api.types.is_numeric_dtype(cleaned_df['value']), "value column should be numeric."

def test_string_columns_are_stripped(duckdb_connection):
    """Test that string columns in silver.opportunities_cleaned have no leading/trailing whitespace."""
    con = duckdb_connection
    cleaned_df = con.execute("SELECT account_id, stage FROM silver.opportunities_cleaned").df()
    # Filter out None/NaN values before checking strip, as .str.strip() on None/NaN returns None/NaN
    assert all(cleaned_df['account_id'].dropna() == cleaned_df['account_id'].dropna().str.strip()), "account_id should be stripped."
    assert all(cleaned_df['stage'].dropna() == cleaned_df['stage'].dropna().str.strip()), "stage should be stripped."

def test_rejected_rows_criteria(duckdb_connection):
    """Test that rows in rejected.rejected_rows meet the rejection criteria."""
    con = duckdb_connection
    rejected_df = con.execute("SELECT opportunity_id, account_id, stage, rejection_reason FROM rejected.rejected_rows").df()

    # All rejected rows must have either account_id IS NULL or stage IS NULL
    assert all(rejected_df['account_id'].isnull() | rejected_df['stage'].isnull()), \
        "All rejected rows must have null account_id or null stage."

    # Check specific rejection reasons
    assert rejected_df.loc[rejected_df['opportunity_id'] == 'OPP003', 'rejection_reason'].iloc[0] == 'account_id is null'
    assert rejected_df.loc[rejected_df['opportunity_id'] == 'OPP004', 'rejection_reason'].iloc[0] == 'stage is null'
    assert rejected_df.loc[rejected_df['opportunity_id'] == 'OPP006', 'rejection_reason'].iloc[0] == 'account_id is null; stage is null'


def test_total_row_counts(duckdb_connection):
    """Test that the sum of cleaned and rejected rows equals the raw rows count."""
    con = duckdb_connection
    raw_count = con.execute("SELECT COUNT(*) FROM bronze.opportunities_raw").fetchone()[0]
    cleaned_count = con.execute("SELECT COUNT(*) FROM silver.opportunities_cleaned").fetchone()[0]
    rejected_count = con.execute("SELECT COUNT(*) FROM rejected.rejected_rows").fetchone()[0]
    assert (cleaned_count + rejected_count) == raw_count, \
        "Sum of cleaned and rejected rows should equal raw rows count."

def test_rejection_reason_populated(duckdb_connection):
    """Test that the 'rejection_reason' column is populated for rejected rows."""
    con = duckdb_connection
    rejected_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows").df()
    assert not rejected_df['rejection_reason'].isnull().any(), "rejection_reason should not have nulls."
    assert not (rejected_df['rejection_reason'] == '').any(), "rejection_reason should not be empty strings."

def test_value_coerced_to_nan(duckdb_connection):
    """Test that 'invalid_value' in 'value' column is coerced to NaN."""
    con = duckdb_connection
    cleaned_df = con.execute("SELECT opportunity_id, value FROM silver.opportunities_cleaned WHERE opportunity_id = 'OPP005'").df()
    assert pd.isna(cleaned_df['value'].iloc[0]), \
        "Value 'invalid_value' should be coerced to NaN/None."
    
    # Also check a row where value was originally None
    cleaned_df_none_value = con.execute("SELECT opportunity_id, value FROM silver.opportunities_cleaned WHERE opportunity_id = 'OPP008'").df()
    assert pd.isna(cleaned_df_none_value['value'].iloc[0]), \
        "Original None value should remain NaN/None."