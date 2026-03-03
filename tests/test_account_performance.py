import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for tests
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def duckdb_connection():
    """Provides a DuckDB connection for the test module."""
    # Ensure a clean slate for the module
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()
    # Clean up the database file after all tests in the module are done
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

@pytest.fixture(autouse=True)
def setup_silver_tables(duckdb_connection):
    """
    Sets up dummy silver tables and cleans up gold/rejected schemas before each test.
    """
    con = duckdb_connection
    
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Clean up previous test runs for tables created by the transformation
    con.execute("DROP TABLE IF EXISTS gold.account_performance;")
    con.execute("DROP TABLE IF EXISTS rejected.rejected_rows;")
    # Clean up silver tables for fresh data in each test
    con.execute("DROP TABLE IF EXISTS silver.accounts_cleaned;")
    con.execute("DROP TABLE IF EXISTS silver.opportunities_cleaned;")

    # Create dummy data for silver.accounts_cleaned
    accounts_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006', 'ACC007', None, 'ACC008'],
        'account_name': ['Account A', 'Account B', 'Account C', 'Account D', 'Account E', 'Account F', 'Account G', 'Null Account', 'Account H'],
        'industry': ['Tech', 'Finance', 'Tech', 'Finance', 'Tech', 'Tech', 'Retail', 'Other', 'Finance'],
        'region': ['USA', 'USA', 'Europe', 'Europe', 'USA', 'USA', 'USA', 'USA', 'Asia'],
        '_source_file': ['acc.csv']*9,
        '_ingest_ts': ['2023-01-01T00:00:00Z']*9
    }
    accounts_df = pd.DataFrame(accounts_data)
    con.execute("CREATE TABLE silver.accounts_cleaned AS SELECT * FROM accounts_df;")

    # Create dummy data for silver.opportunities_cleaned
    opportunities_data = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003', 'OPP004', 'OPP005', 'OPP006', 'OPP007', 'OPP008', 'OPP009', 'OPP010', 'OPP011', 'OPP012'],
        'account_id': ['ACC001', 'ACC001', 'ACC002', 'ACC002', 'ACC003', 'ACC003', 'ACC005', 'ACC006', 'ACC001', None, 'ACC008', 'ACC008'],
        'value': [10000.0, 5000.0, 20000.0, 15000.0, 30000.0, 10000.0, 25000.0, 35000.0, 2000.0, 5000.0, 10000.0, 5000.0],
        'close_date': ['2024-01-01']*12,
        'stage': ['Closed Won', 'Closed Lost', 'Closed Won', 'Negotiation', 'Closed Won', 'Closed Won', 'Closed Won', 'Closed Won', 'Closed Won', 'Closed Lost', 'Closed Won', 'Closed Lost'],
        '_source_file': ['opp.csv']*12,
        '_ingest_ts': ['2023-01-01T00:00:00Z']*12
    }
    opportunities_df = pd.DataFrame(opportunities_data)
    con.execute("CREATE TABLE silver.opportunities_cleaned AS SELECT * FROM opportunities_df;")

    # Run the main transformation function
    main()

def test_gold_table_exists_and_has_rows(duckdb_connection):
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM gold.account_performance;").fetchone()[0]
    assert result > 0, "gold.account_performance table should exist and have rows."

def test_no_nulls_in_key_columns(duckdb_connection):
    con = duckdb_connection
    df = con.execute("SELECT account_id FROM gold.account_performance;").df()
    assert df['account_id'].notnull().all(), "account_id column in gold.account_performance should not have nulls."

def test_correct_data_types(duckdb_connection):
    con = duckdb_connection
    df = con.execute("SELECT * FROM gold.account_performance LIMIT 1;").df()
    assert pd.api.types.is_string_dtype(df['account_id']), "account_id should be string"
    assert pd.api.types.is_string_dtype(df['account_name']), "account_name should be string"
    assert pd.api.types.is_string_dtype(df['industry']), "industry should be string"
    assert pd.api.types.is_string_dtype(df['region']), "region should be string"
    assert pd.api.types.is_float_dtype(df['pipeline_value']), "pipeline_value should be float"
    assert pd.api.types.is_integer_dtype(df['opportunity_count']), "opportunity_count should be integer"
    assert pd.api.types.is_float_dtype(df['win_rate']), "win_rate should be float"
    assert pd.api.types.is_integer_dtype(df['rank_within_industry']), "rank_within_industry should be integer"

def test_rejected_rows_table_exists(duckdb_connection):
    con = duckdb_connection
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows';").fetchone()[0]
    assert table_exists == 1, "rejected.rejected_rows table should exist."

def test_rejected_rows_content(duckdb_connection):
    con = duckdb_connection
    rejected_df = con.execute("SELECT * FROM rejected.rejected_rows;").df()
    assert not rejected_df.empty, "rejected.rejected_rows should not be empty."
    assert (rejected_df['account_id'].isnull()).all(), "All rejected rows should have null account_id."
    assert (rejected_df['rejection_reason'] == 'account_id is null').all(), "Rejection reason should be 'account_id is null'."
    assert 'Null Account' in rejected_df['account_name'].tolist(), "Rejected account name 'Null Account' should be present."
    # Ensure the rejected account is not in the gold table
    gold_accounts = con.execute("SELECT account_id FROM gold.account_performance;").df()['account_id'].tolist()
    assert 'Null Account' not in gold_accounts, "Rejected account should not be in gold.account_performance."

def test_pipeline_value_calculation(duckdb_connection):
    con = duckdb_connection
    # Account A: OPP001 (10000), OPP002 (5000), OPP009 (2000) -> Total 17000
    df = con.execute("SELECT pipeline_value FROM gold.account_performance WHERE account_id = 'ACC001';").df()
    assert not df.empty, "Account ACC001 not found in gold.account_performance."
    assert df['pipeline_value'].iloc[0] == 17000.0, "Pipeline value for ACC001 is incorrect."

def test_opportunity_count_calculation(duckdb_connection):
    con = duckdb_connection
    # Account A: 3 opportunities
    # Account G: 0 opportunities
    df_acc001 = con.execute("SELECT opportunity_count FROM gold.account_performance WHERE account_id = 'ACC001';").df()
    df_acc007 = con.execute("SELECT opportunity_count FROM gold.account_performance WHERE account_id = 'ACC007';").df()
    assert df_acc001['opportunity_count'].iloc[0] == 3, "Opportunity count for ACC001 is incorrect."
    assert df_acc007['opportunity_count'].iloc[0] == 0, "Opportunity count for ACC007 (no opportunities) is incorrect."

def test_win_rate_calculation(duckdb_connection):
    con = duckdb_connection
    # Account A: OPP001 (Won), OPP002 (Lost), OPP009 (Won) -> 2 Won / 3 Total = 0.666...
    # Account B: OPP003 (Won), OPP004 (Negotiation) -> 1 Won / 2 Total = 0.5
    # Account G: 0 opportunities -> win_rate should be 0.0
    df_acc001 = con.execute("SELECT win_rate FROM gold.account_performance WHERE account_id = 'ACC001';").df()
    df_acc002 = con.execute("SELECT win_rate FROM gold.account_performance WHERE account_id = 'ACC002';").df()
    df_acc007 = con.execute("SELECT win_rate FROM gold.account_performance WHERE account_id = 'ACC007';").df()

    assert pytest.approx(df_acc001['win_rate'].iloc[0]) == (2/3), "Win rate for ACC001 is incorrect."
    assert pytest.approx(df_acc002['win_rate'].iloc[0]) == 0.5, "Win rate for ACC002 is incorrect."
    assert pytest.approx(df_acc007['win_rate'].iloc[0]) == 0.0, "Win rate for ACC007 (no opportunities) should be 0.0."

def test_ranking_within_industry(duckdb_connection):
    con = duckdb_connection
    # Industry 'Tech':
    # ACC003: 40000 (OPP005, OPP006)
    # ACC006: 35000 (OPP008)
    # ACC005: 25000 (OPP007)
    # ACC001: 17000 (OPP001, OPP002, OPP009)
    # Expected ranks for Tech (ordered by pipeline_value DESC):
    # ACC003 (40000) -> Rank 1
    # ACC006 (35000) -> Rank 2
    # ACC005 (25000) -> Rank 3
    # ACC001 (17000) -> Rank 4

    tech_accounts_df = con.execute("SELECT account_id, pipeline_value, rank_within_industry FROM gold.account_performance WHERE industry = 'Tech' ORDER BY pipeline_value DESC;").df()
    
    expected_ranks = {
        'ACC003': 1, # 40000
        'ACC006': 2, # 35000
        'ACC005': 3, # 25000
        'ACC001': 4  # 17000
    }

    for _, row in tech_accounts_df.iterrows():
        assert row['rank_within_industry'] == expected_ranks[row['account_id']], \
            f"Rank for {row['account_id']} in Tech industry is incorrect. Expected {expected_ranks[row['account_id']]}, got {row['rank_within_industry']}."

    # Industry 'Finance':
    # ACC002: 35000 (OPP003, OPP004)
    # ACC008: 15000 (OPP011, OPP012)
    # ACC004: 0 (no opportunities)
    # Expected ranks for Finance (ordered by pipeline_value DESC):
    # ACC002 (35000) -> Rank 1
    # ACC008 (15000) -> Rank 2
    # ACC004 (0) -> Rank 3

    finance_accounts_df = con.execute("SELECT account_id, pipeline_value, rank_within_industry FROM gold.account_performance WHERE industry = 'Finance' ORDER BY pipeline_value DESC;").df()

    expected_ranks_finance = {
        'ACC002': 1, # 35000
        'ACC008': 2, # 15000
        'ACC004': 3  # 0
    }

    for _, row in finance_accounts_df.iterrows():
        assert row['rank_within_industry'] == expected_ranks_finance[row['account_id']], \
            f"Rank for {row['account_id']} in Finance industry is incorrect. Expected {expected_ranks_finance[row['account_id']]}, got {row['rank_within_industry']}."
```