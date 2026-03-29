import pytest
import duckdb
import pandas as pd
import os

DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def setup_database():
    # Ensure the directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Attempt to remove the database file, handling potential PermissionError
    if os.path.exists(DB_PATH):
        try:
            # Try to connect and immediately close to release any lingering locks
            # This is a common workaround for PermissionError on Windows with DuckDB
            temp_con = duckdb.connect(DB_PATH)
            temp_con.close()
            os.remove(DB_PATH)
        except PermissionError as e:
            # If we still get a PermissionError, it means another process has a lock.
            # This is a critical failure for test setup, so re-raise.
            pytest.fail(f"Failed to remove existing DB file {DB_PATH} due to PermissionError: {e}. "
                        "Ensure no other process is using the database file.")
        except Exception as e:
            # Catch other potential errors during removal but allow tests to proceed if possible
            print(f"Warning: Could not remove existing DB file {DB_PATH} during setup: {e}")

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Create bronze.transactions_raw with test data
    test_data = [
        # Valid rows
        (101.0, 1.0, 10.0, "$100.50 ", "2023-01-01", "file1.csv ", "ts1"), # _source_file has trailing space
        (102.0, 2.0, 5.0, "200.00", "2023-01-02", "file2.csv", "ts2"),
        (103.0, 3.0, 1.0, " 50.25", "2023-01-03", "file3.csv", "ts3"),
        (109.0, 10.0, 8.0, "150.75", "2023-01-10", " file10.csv", "ts10"), # _source_file has leading space
        # Rejected: Null transaction_id
        (None, 4.0, 2.0, "$75.00", "2023-01-04", "file4.csv", "ts4"),
        # Rejected: Null amount
        (105.0, 5.0, 3.0, None, "2023-01-05", "file5.csv", "ts5"),
        # Rejected: Zero amount
        (106.0, 6.0, 4.0, "$0.00", "2023-01-06", "file6.csv", "ts6"),
        # Rejected: Negative amount
        (107.0, 7.0, 5.0, "-10.00", "2023-01-07", "file7.csv", "ts7"),
        # Rejected: Non-numeric amount (will become NaN after coerce)
        (108.0, 8.0, 6.0, "abc", "2023-01-08", "file8.csv", "ts8"),
        # Rejected: Null transaction_id AND invalid amount (zero)
        (None, 9.0, 7.0, "$0.00", "2023-01-09", "file9.csv", "ts9"),
    ]
    transactions_raw_df = pd.DataFrame(test_data, columns=[
        "transaction_id", "customer_id", "quantity", "amount", "transaction_date", "_source_file", "_ingest_ts"
    ])
    con.execute("CREATE OR REPLACE TABLE bronze.transactions_raw AS SELECT * FROM transactions_raw_df;")

    con.close()
    yield DB_PATH # Provide the DB_PATH to tests
    # Teardown: Clean up the database file
    if os.path.exists(DB_PATH):
        try:
            # Try to connect and immediately close to release any lingering locks before removal
            temp_con = duckdb.connect(DB_PATH)
            temp_con.close()
            os.remove(DB_PATH)
        except Exception as e:
            print(f"Warning: Could not remove DB file {DB_PATH} during teardown: {e}")
            # Don't re-raise here, as teardown should ideally complete gracefully

@pytest.fixture(autouse=True)
def run_main_transformation(setup_database):
    # Call the main transformation function directly
    main()

class TestTransactionsCleaning:
    def test_silver_table_exists_and_not_empty(self, setup_database):
        con = duckdb.connect(setup_database)
        result = con.execute("SELECT count(*) FROM silver.transactions_cleaned;").fetchone()[0]
        con.close()
        assert result > 0, "silver.transactions_cleaned table should exist and not be empty"

    def test_rejected_table_exists_and_not_empty(self, setup_database):
        con = duckdb.connect(setup_database)
        result = con.execute("SELECT count(*) FROM rejected.rejected_rows;").fetchone()[0]
        con.close()
        assert result > 0, "rejected.rejected_rows table should exist and not be empty"

    def test_silver_no_null_transaction_id(self, setup_database):
        con = duckdb.connect(setup_database)
        null_ids = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE transaction_id IS NULL;").fetchone()[0]
        con.close()
        assert null_ids == 0, "silver.transactions_cleaned should not have null transaction_id"

    def test_silver_amount_is_numeric_and_positive(self, setup_database):
        con = duckdb.connect(setup_database)
        # Check data type
        schema_info = con.execute("PRAGMA table_info('silver.transactions_cleaned');").df()
        amount_type = schema_info[schema_info['name'] == 'amount']['type'].iloc[0]
        assert amount_type == 'DOUBLE', f"Amount column in silver should be DOUBLE, but is {amount_type}"

        # Check for non-positive amounts
        non_positive_amounts = con.execute("SELECT count(*) FROM silver.transactions_cleaned WHERE amount <= 0;").fetchone()[0]
        con.close()
        assert non_positive_amounts == 0, "silver.transactions_cleaned should only have positive amounts"

    def test_silver_string_columns_stripped(self, setup_database):
        con = duckdb.connect(setup_database)
        # Check _source_file for leading/trailing spaces for the rows that had them in test data
        df = con.execute("SELECT _source_file FROM silver.transactions_cleaned WHERE transaction_id IN (101.0, 109.0);").df()
        con.close()
        assert not df['_source_file'].str.contains(r'^\s|\s$', regex=True).any(), "String columns in silver should have leading/trailing whitespace stripped"

    def test_rejected_rows_contain_null_id_rejection(self, setup_database):
        con = duckdb.connect(setup_database)
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id IS NULL AND amount = '$75.00';").df()
        con.close()
        assert not reasons_df.empty, "Should have a rejected row for null transaction_id"
        assert "Null transaction_id" in reasons_df['rejection_reason'].iloc[0], "Rejection reason should include 'Null transaction_id'"

    def test_rejected_rows_contain_invalid_amount_rejection(self, setup_database):
        con = duckdb.connect(setup_database)
        # Test for null amount
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 105.0;").df()
        assert not reasons_df.empty, "Should have a rejected row for null amount"
        assert "Invalid amount" in reasons_df['rejection_reason'].iloc[0], "Rejection reason should include 'Invalid amount' for null amount"

        # Test for zero amount
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 106.0;").df()
        assert not reasons_df.empty, "Should have a rejected row for zero amount"
        assert "Invalid amount" in reasons_df['rejection_reason'].iloc[0], "Rejection reason should include 'Invalid amount' for zero amount"

        # Test for negative amount
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 107.0;").df()
        assert not reasons_df.empty, "Should have a rejected row for negative amount"
        assert "Invalid amount" in reasons_df['rejection_reason'].iloc[0], "Rejection reason should include 'Invalid amount' for negative amount"

        # Test for non-numeric amount
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id = 108.0;").df()
        con.close()
        assert not reasons_df.empty, "Should have a rejected row for non-numeric amount"
        assert "Invalid amount" in reasons_df['rejection_reason'].iloc[0], "Rejection reason should include 'Invalid amount' for non-numeric amount"


    def test_rejected_rows_contain_both_reasons(self, setup_database):
        con = duckdb.connect(setup_database)
        reasons_df = con.execute("SELECT rejection_reason FROM rejected.rejected_rows WHERE transaction_id IS NULL AND amount = '$0.00';").df()
        con.close()
        assert not reasons_df.empty, "Should have a rejected row for both null transaction_id and invalid amount"
        assert "Null transaction_id" in reasons_df['rejection_reason'].iloc[0] and \
               "Invalid amount" in reasons_df['rejection_reason'].iloc[0], \
               "Rejection reason should include both 'Null transaction_id' and 'Invalid amount'"

    def test_rejected_amount_column_type(self, setup_database):
        con = duckdb.connect(setup_database)
        schema_info = con.execute("PRAGMA table_info('rejected.rejected_rows');").df()
        amount_type = schema_info[schema_info['name'] == 'amount']['type'].iloc[0]
        con.close()
        assert amount_type == 'VARCHAR', f"Amount column in rejected should be VARCHAR (original type), but is {amount_type}"

    def test_no_rejected_in_silver(self, setup_database):
        con = duckdb.connect(setup_database)
        rejected_ids = con.execute("SELECT transaction_id FROM rejected.rejected_rows WHERE transaction_id IS NOT NULL;").df()['transaction_id'].tolist()
        silver_ids = con.execute("SELECT transaction_id FROM silver.transactions_cleaned;").df()['transaction_id'].tolist()
        con.close()
        # Convert to sets for efficient comparison
        rejected_ids_set = set(rejected_ids)
        silver_ids_set = set(silver_ids)
        assert rejected_ids_set.isdisjoint(silver_ids_set), "No rejected transaction_id should be present in silver.transactions_cleaned"

    def test_no_valid_in_rejected(self, setup_database):
        con = duckdb.connect(setup_database)
        valid_ids = con.execute("SELECT transaction_id FROM silver.transactions_cleaned;").df()['transaction_id'].tolist()
        rejected_ids = con.execute("SELECT transaction_id FROM rejected.rejected_rows WHERE transaction_id IS NOT NULL;").df()['transaction_id'].tolist()
        con.close()
        valid_ids_set = set(valid_ids)
        rejected_ids_set = set(rejected_ids)
        assert valid_ids_set.isdisjoint(rejected_ids_set), "No valid transaction_id should be present in rejected.rejected_rows"