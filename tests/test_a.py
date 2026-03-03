import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for testing
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_database():
    """
    Sets up a clean DuckDB database with sample data for testing.
    Runs the transformation and yields the connection.
    """
    # Ensure the directory for the DB exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Sample data for silver.transactions_cleaned
    # Includes valid, invalid, and null transaction_dates
    transactions_data = {
        'transaction_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        'customer_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        'amount': [100.0, 200.0, 150.0, 250.0, 300.0, 100.0, 50.0, 75.0, 120.0, 180.0, 200.0],
        'transaction_date': [
            '2023-01-01', '2023-01-15',  # Jan: 300
            '2023-02-01', '2023-02-10',  # Feb: 400 (33.33% growth from Jan)
            '2023-03-05', '2023-03-20',  # Mar: 400 (0% growth from Feb)
            'invalid-date',              # Rejected
            None,                        # Rejected
            '2023-04-01', '2023-04-10',  # Apr: 300 (-25% growth from Mar)
            '2023-05-01'                 # May: 200 (-33.33% growth from Apr)
        ]
    }
    transactions_df = pd.DataFrame(transactions_data)
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM transactions_df")

    # Run the transformation using the fixture's connection
    from __main__ import run_transformation
    run_transformation(con)

    yield con
    
    # Clean up the database file after tests
    con.close()
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_gold_table_exists_and_has_data(setup_database):
    con = setup_database
    result = con.execute("SELECT count(*) FROM gold.monthly_sales_summary").fetchone()[0]
    assert result > 0, "gold.monthly_sales_summary should exist and have data"

def test_no_nulls_in_year_month(setup_database):
    con = setup_database
    result = con.execute("SELECT count(*) FROM gold.monthly_sales_summary WHERE year_month IS NULL").fetchone()[0]
    assert result == 0, "There should be no nulls in the year_month column"

def test_correct_data_types(setup_database):
    con = setup_database
    df = con.execute("SELECT * FROM gold.monthly_sales_summary LIMIT 1").df()
    assert pd.api.types.is_string_dtype(df['year_month']), "year_month should be string"
    assert pd.api.types.is_float_dtype(df['monthly_revenue']), "monthly_revenue should be float"
    assert pd.api.types.is_integer_dtype(df['transaction_count']), "transaction_count should be integer"
    assert pd.api.types.is_float_dtype(df['mom_growth']), "mom_growth should be float"

def test_rejected_rows_table_exists(setup_database):
    con = setup_database
    result = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
    ).fetchone()[0]
    assert result == 1, "rejected.rejected_rows table should exist"

def test_rejected_rows_content(setup_database):
    con = setup_database
    rejected_df = con.execute("SELECT transaction_date, rejection_reason FROM rejected.rejected_rows ORDER BY transaction_date NULLS FIRST").df()
    assert len(rejected_df) == 2, "There should be 2 rejected rows"
    # Pandas reads SQL NULL as None
    assert rejected_df.iloc[0]['transaction_date'] is None, "First rejected row should have None transaction_date"
    assert rejected_df.iloc[1]['transaction_date'] == 'invalid-date', "Second rejected row should have 'invalid-date' transaction_date"
    assert all(rejected_df['rejection_reason'].str.contains('transaction_date could not be parsed')), "Rejection reason is incorrect"

def test_aggregation_and_mom_growth(setup_database):
    con = setup_database
    df = con.execute("SELECT year_month, monthly_revenue, transaction_count, mom_growth FROM gold.monthly_sales_summary ORDER BY year_month").df()

    # Test January 2023
    jan_data = df[df['year_month'] == '2023-01'].iloc[0]
    assert jan_data['monthly_revenue'] == 300.0, "January monthly_revenue incorrect"
    assert jan_data['transaction_count'] == 2, "January transaction_count incorrect"
    assert pd.isna(jan_data['mom_growth']), "January mom_growth should be NaN"

    # Test February 2023
    feb_data = df[df['year_month'] == '2023-02'].iloc[0]
    assert feb_data['monthly_revenue'] == 400.0, "February monthly_revenue incorrect"
    assert feb_data['transaction_count'] == 2, "February transaction_count incorrect"
    assert abs(feb_data['mom_growth'] - 33.3333) < 0.01, "February mom_growth incorrect" # (400-300)/300 * 100 = 33.33%

    # Test March 2023
    mar_data = df[df['year_month'] == '2023-03'].iloc[0]
    assert mar_data['monthly_revenue'] == 400.0, "March monthly_revenue incorrect"
    assert mar_data['transaction_count'] == 2, "March transaction_count incorrect"
    assert abs(mar_data['mom_growth'] - 0.0) < 0.01, "March mom_growth incorrect" # (400-400)/400 * 100 = 0%

    # Test April 2023
    apr_data = df[df['year_month'] == '2023-04'].iloc[0]
    assert apr_data['monthly_revenue'] == 300.0, "April monthly_revenue incorrect"
    assert apr_data['transaction_count'] == 2, "April transaction_count incorrect"
    assert abs(apr_data['mom_growth'] - -25.0) < 0.01, "April mom_growth incorrect" # (300-400)/400 * 100 = -25%

    # Test May 2023
    may_data = df[df['year_month'] == '2023-05'].iloc[0]
    assert may_data['monthly_revenue'] == 200.0, "May monthly_revenue incorrect"
    assert may_data['transaction_count'] == 1, "May transaction_count incorrect"
    assert abs(may_data['mom_growth'] - -33.3333) < 0.01, "May mom_growth incorrect" # (200-300)/300 * 100 = -33.33%

def test_sort_order(setup_database):
    con = setup_database
    df = con.execute("SELECT year_month FROM gold.monthly_sales_summary").df()
    expected_order = sorted(df['year_month'].tolist())
    assert df['year_month'].tolist() == expected_order, "year_month should be sorted ascending"