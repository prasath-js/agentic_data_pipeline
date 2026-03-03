import pytest
import duckdb
import pandas as pd
import os

# Define DB_PATH for testing environment
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def db_setup_and_teardown():
    """
    Fixture to set up the DuckDB database, populate silver tables, run the main transformation,
    and clean up the database file after all tests in the module.
    """
    # Ensure the directory exists for the DB file
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Clean up any previous DB file to ensure a fresh start
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except OSError as e:
            print(f"Warning: Could not remove old DB file {DB_PATH} before setup: {e}")

    con_setup = None
    try:
        con_setup = duckdb.connect(DB_PATH)
        
        # Create schemas
        con_setup.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        con_setup.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con_setup.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        con_setup.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

        # Create dummy silver tables for the main transformation to use
        customers_data = {
            'customer_id': ['C1', 'C2', 'C3', 'C4', 'C5'],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
            'join_date': ['2023-01-01', '2023-01-05', '2023-01-10', '2023-01-15', '2023-01-20']
        }
        customers_df = pd.DataFrame(customers_data)
        con_setup.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")

        transactions_data = {
            'transaction_id': ['T101', 'T102', 'T103', 'T104', 'T105', 'T106', 'T107', 'T108', 'T109', 'T110', 'T111', 'T112', 'T113'],
            'customer_id': ['C1', 'C1', 'C2', 'C2', 'C3', 'C3', 'C3', 'C4', 'C4', 'C5', 'C_NONEXISTENT', None, 'C1'], # C_NONEXISTENT and None for rejection
            'amount': [100.00, 200.00, 500.00, 600.00, 1000.00, 5000.00, 5000.00, 10000.00, 1000.00, 100.00, 75.00, 120.00, 15000.00], # C1 will have 15300 total
            'transaction_date': ['2023-02-01', '2023-02-02', '2023-02-03', '2023-02-04', '2023-02-05', '2023-02-06', '2023-02-07', '2023-02-08', '2023-02-09', '2023-02-10', '2023-02-11', '2023-02-12', '2023-02-13']
        }
        transactions_df = pd.DataFrame(transactions_data)
        con_setup.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM transactions_df;")

        con_setup.close() # Close the setup connection before main() runs

        # Run the main transformation
        os.environ["DB_PATH"] = DB_PATH
        from __main__ import main 
        main() # This main() will open its own connection and close it.
        del os.environ["DB_PATH"]

        yield # Yield control to tests

    finally:
        # Teardown: Remove the database file after all tests are done
        if os.path.exists(DB_PATH):
            try:
                os.remove(DB_PATH)
            except OSError as e:
                print(f"Warning: Could not remove DB file {DB_PATH} during teardown: {e}")

@pytest.fixture(scope="function")
def duckdb_connection(db_setup_and_teardown):
    """Fixture to provide a fresh DuckDB connection for each test."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close() # Ensure connection is closed after each test

def test_gold_table_exists_and_has_rows(duckdb_connection):
    """Test that gold.customer_spending table exists and contains data."""
    con = duckdb_connection
    result = con.execute("SELECT COUNT(*) FROM gold.customer_spending;").fetchone()
    assert result is not None, "gold.customer_spending table does not exist."
    assert result[0] > 0, "gold.customer_spending table is empty."

def test_no_nulls_in_key_columns(duckdb_connection):
    """Test that customer_id in gold.customer_spending has no nulls."""
    con = duckdb_connection
    null_customer_ids = con.execute("SELECT COUNT(*) FROM gold.customer_spending WHERE customer_id IS NULL;").fetchone()[0]
    assert null_customer_ids == 0, "customer_id column in gold.customer_spending contains NULL values."

def test_correct_data_types(duckdb_connection):
    """Test that important columns have the correct data types."""
    con = duckdb_connection
    schema_info = con.execute("DESCRIBE gold.customer_spending;").fetchdf()

    customer_id_dtype = schema_info[schema_info['column_name'] == 'customer_id']['column_type'].iloc[0]
    total_spent_dtype = schema_info[schema_info['column_name'] == 'total_spent']['column_type'].iloc[0]
    transaction_count_dtype = schema_info[schema_info['column_name'] == 'transaction_count']['column_type'].iloc[0]
    average_transaction_value_dtype = schema_info[schema_info['column_name'] == 'average_transaction_value']['column_type'].iloc[0]
    spending_tier_dtype = schema_info[schema_info['column_name'] == 'spending_tier']['column_type'].iloc[0]

    assert 'VARCHAR' in customer_id_dtype, f"customer_id has incorrect type: {customer_id_dtype}"
    assert 'DOUBLE' in total_spent_dtype or 'DECIMAL' in total_spent_dtype, f"total_spent has incorrect type: {total_spent_dtype}"
    assert 'BIGINT' in transaction_count_dtype or 'INTEGER' in transaction_count_dtype, f"transaction_count has incorrect type: {transaction_count_dtype}"
    assert 'DOUBLE' in average_transaction_value_dtype or 'DECIMAL' in average_transaction_value_dtype, f"average_transaction_value has incorrect type: {average_transaction_value_dtype}"
    assert 'VARCHAR' in spending_tier_dtype, f"spending_tier has incorrect type: {spending_tier_dtype}"

def test_rejected_rows_table_exists(duckdb_connection):
    """Test that the rejected.rejected_rows table exists."""
    con = duckdb_connection
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows';").fetchone()[0] > 0
    assert table_exists, "rejected.rejected_rows table does not exist."

def test_rejected_rows_content(duckdb_connection):
    """Test that rejected.rejected_rows contains the expected rejected transactions."""
    con = duckdb_connection
    rejected_df = con.execute("SELECT original_transaction_customer_id, transaction_id FROM rejected.rejected_rows ORDER BY transaction_id;").fetchdf()
    
    # Expected rejected transactions: C_NONEXISTENT and None
    expected_rejected = pd.DataFrame({
        'original_transaction_customer_id': ['C_NONEXISTENT', None],
        'transaction_id': ['T111', 'T112']
    }).sort_values(by='transaction_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(rejected_df, expected_rejected, check_dtype=False)

def test_spending_tier_logic(duckdb_connection):
    """Test that spending tiers are assigned correctly based on total_spent."""
    con = duckdb_connection
    
    # C1: 100 + 200 + 15000 = 15300 -> High
    # C2: 500 + 600 = 1100 -> Medium
    # C3: 1000 + 5000 + 5000 = 11000 -> High
    # C4: 10000 + 1000 = 11000 -> High
    # C5: 100 -> Low

    spending_tiers = con.execute("SELECT customer_id, total_spent, spending_tier FROM gold.customer_spending ORDER BY customer_id;").fetchdf()
    
    c1_tier = spending_tiers[spending_tiers['customer_id'] == 'C1']['spending_tier'].iloc[0]
    c2_tier = spending_tiers[spending_tiers['customer_id'] == 'C2']['spending_tier'].iloc[0]
    c3_tier = spending_tiers[spending_tiers['customer_id'] == 'C3']['spending_tier'].iloc[0]
    c4_tier = spending_tiers[spending_tiers['customer_id'] == 'C4']['spending_tier'].iloc[0]
    c5_tier = spending_tiers[spending_tiers['customer_id'] == 'C5']['spending_tier'].iloc[0]

    assert c1_tier == 'High', f"C1 tier incorrect: {c1_tier}"
    assert c2_tier == 'Medium', f"C2 tier incorrect: {c2_tier}"
    assert c3_tier == 'High', f"C3 tier incorrect: {c3_tier}"
    assert c4_tier == 'High', f"C4 tier incorrect: {c4_tier}"
    assert c5_tier == 'Low', f"C5 tier incorrect: {c5_tier}"

def test_calculation_accuracy(duckdb_connection):
    """Test the accuracy of total_spent, transaction_count, and average_transaction_value."""
    con = duckdb_connection
    
    # Test for C1:
    # Transactions: T101 (100), T102 (200), T113 (15000)
    # Expected: total_spent = 15300.00, transaction_count = 3, average_transaction_value = 15300 / 3 = 5100.00
    
    c1_data = con.execute("SELECT total_spent, transaction_count, average_transaction_value FROM gold.customer_spending WHERE customer_id = 'C1';").fetchdf()
    
    assert not c1_data.empty, "No data found for customer C1."
    assert c1_data['total_spent'].iloc[0] == 15300.00, f"C1 total_spent incorrect: {c1_data['total_spent'].iloc[0]}"
    assert c1_data['transaction_count'].iloc[0] == 3, f"C1 transaction_count incorrect: {c1_data['transaction_count'].iloc[0]}"
    assert c1_data['average_transaction_value'].iloc[0] == 5100.00, f"C1 average_transaction_value incorrect: {c1_data['average_transaction_value'].iloc[0]}"

    # Test for C5:
    # Transactions: T110 (100)
    # Expected: total_spent = 100.00, transaction_count = 1, average_transaction_value = 100 / 1 = 100.00
    c5_data = con.execute("SELECT total_spent, transaction_count, average_transaction_value FROM gold.customer_spending WHERE customer_id = 'C5';").fetchdf()
    
    assert not c5_data.empty, "No data found for customer C5."
    assert c5_data['total_spent'].iloc[0] == 100.00, f"C5 total_spent incorrect: {c5_data['total_spent'].iloc[0]}"
    assert c5_data['transaction_count'].iloc[0] == 1, f"C5 transaction_count incorrect: {c5_data['transaction_count'].iloc[0]}"
    assert c5_data['average_transaction_value'].iloc[0] == 100.00, f"C5 average_transaction_value incorrect: {c5_data['average_transaction_value'].iloc[0]}"