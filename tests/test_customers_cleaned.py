import pytest
import duckdb
import pandas as pd
import os

# Define the DB_PATH for testing
DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture to establish a DuckDB connection for tests."""
    con = duckdb.connect(DB_PATH)
    yield con
    con.close()

@pytest.fixture(scope="module")
def setup_bronze_data(duckdb_connection):
    """Fixture to set up bronze data before tests."""
    con = duckdb_connection
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Create some dummy data for testing
    customers_data = [
        (1.0, "  John Doe  ", "john.doe@example.com ", "123 Main St", "2023-01-01", "file1.csv", "2023-01-01T00:00:00Z"),
        (2.0, " Jane Smith ", "jane.smith@example.com", "456 Oak Ave", "2023-01-02", "file1.csv", "2023-01-01T00:00:00Z"),
        (1.0, "John Doe Alt", "john.alt@example.com", "222 Pine Rd", "2023-01-03", "file2.csv", "2023-01-02T00:00:00Z"), # Duplicate customer_id, later join_date
        (3.0, "Bob Johnson", "invalid-email", "101 Elm St", "2023-01-04", "file2.csv", "2023-01-02T00:00:00Z"),
        (None, "Null ID User", "null@example.com", "202 Cedar Ln", "2023-01-05", "file3.csv", "2023-01-03T00:00:00Z"), # Null customer_id
        (4.0, "Alice Brown", "alice.brown@example.com", "303 Birch St", "2023-01-06", "file3.csv", "2023-01-03T00:00:00Z"),
        (5.0, "Charlie Green", None, "404 Willow Dr", "2023-01-07", "file4.csv", "2023-01-04T00:00:00Z"), # Null email
        (6.0, "David White", "david@noat.com", "505 Maple Rd", "2023-01-08", "file4.csv", "2023-01-04T00:00:00Z"), # Contains '@'
        (7.0, "Eve Black", "eve.black@example.com", "606 Spruce St", "2023-01-09", "file5.csv", "2023-01-05T00:00:00Z"),
        (7.0, "Eve Black Old", "eve.old@example.com", "606 Spruce St", "2023-01-08", "file5.csv", "2023-01-05T00:00:00Z"), # Duplicate customer_id, earlier join_date
    ]
    customers_raw_df = pd.DataFrame(customers_data, columns=[
        "customer_id", "name", "email", "address", "join_date", "_source_file", "_ingest_ts"
    ])
    con.execute("CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM customers_raw_df;")

    # Run the main transformation function
    # This assumes the transformation code is in the same file or accessible
    from __main__ import main
    main()

    yield # Allow tests to run

    # Clean up after tests (optional, but good practice for isolated tests)
    con.execute("DROP SCHEMA IF EXISTS bronze CASCADE;")
    con.execute("DROP SCHEMA IF EXISTS silver CASCADE;")
    con.execute("DROP SCHEMA IF EXISTS rejected CASCADE;")


def test_customers_cleaned_exists_and_has_rows(duckdb_connection, setup_bronze_data):
    """Test that silver.customers_cleaned table exists and contains data."""
    con = duckdb_connection
    result = con.execute("SELECT count(*) FROM silver.customers_cleaned;").fetchone()[0]
    assert result > 0, "silver.customers_cleaned should exist and have more than 0 rows."

def test_no_null_customer_id_in_cleaned(duckdb_connection, setup_bronze_data):
    """Test that customer_id column in silver.customers_cleaned has no nulls."""
    con = duckdb_connection
    null_customer_ids = con.execute("SELECT count(*) FROM silver.customers_cleaned WHERE customer_id IS NULL;").fetchone()[0]
    assert null_customer_ids == 0, "customer_id in silver.customers_cleaned should not contain null values."

def test_join_date_datatype(duckdb_connection, setup_bronze_data):
    """Test that join_date column in silver.customers_cleaned has a datetime type."""
    con = duckdb_connection
    # DuckDB stores dates as DATE or TIMESTAMP, check for either
    schema_info = con.execute("DESCRIBE silver.customers_cleaned;").df()
    join_date_type = schema_info[schema_info['column_name'] == 'join_date']['column_type'].iloc[0]
    assert 'DATE' in join_date_type or 'TIMESTAMP' in join_date_type, f"join_date should be a DATE or TIMESTAMP type, but is {join_date_type}"

def test_email_is_valid_datatype(duckdb_connection, setup_bronze_data):
    """Test that email_is_valid column in silver.customers_cleaned has a boolean type."""
    con = duckdb_connection
    schema_info = con.execute("DESCRIBE silver.customers_cleaned;").df()
    email_is_valid_type = schema_info[schema_info['column_name'] == 'email_is_valid']['column_type'].iloc[0]
    assert 'BOOLEAN' in email_is_valid_type, f"email_is_valid should be BOOLEAN type, but is {email_is_valid_type}"

def test_no_duplicate_customer_id(duckdb_connection, setup_bronze_data):
    """Test that there are no duplicate customer_id values in silver.customers_cleaned."""
    con = duckdb_connection
    duplicates = con.execute("SELECT customer_id FROM silver.customers_cleaned GROUP BY customer_id HAVING count(*) > 1;").df()
    assert duplicates.empty, f"Found duplicate customer_ids in silver.customers_cleaned: {duplicates.to_dict('records')}"

def test_whitespace_stripped(duckdb_connection, setup_bronze_data):
    """Test that string columns have leading/trailing whitespace stripped."""
    con = duckdb_connection
    # Check 'name' column for example
    name_with_whitespace = con.execute("SELECT count(*) FROM silver.customers_cleaned WHERE name LIKE ' %' OR name LIKE '% ';").fetchone()[0]
    assert name_with_whitespace == 0, "Found names with leading/trailing whitespace."
    # Check 'email' column for example
    email_with_whitespace = con.execute("SELECT count(*) FROM silver.customers_cleaned WHERE email LIKE ' %' OR email LIKE '% ';").fetchone()[0]
    assert email_with_whitespace == 0, "Found emails with leading/trailing whitespace."


def test_email_is_valid_logic(duckdb_connection, setup_bronze_data):
    """Test the logic of the email_is_valid column."""
    con = duckdb_connection
    # Check a valid email (customer_id 1)
    valid_email_check = con.execute("SELECT email_is_valid FROM silver.customers_cleaned WHERE customer_id = 1;").fetchone()[0]
    assert valid_email_check is True, "Email 'john.alt@example.com' should be valid."

    # Check an invalid email (no '@', customer_id 3)
    invalid_email_check = con.execute("SELECT email_is_valid FROM silver.customers_cleaned WHERE customer_id = 3;").fetchone()[0]
    assert invalid_email_check is False, "Email 'invalid-email' should be invalid."

    # Check an email with '@' but not a typical email structure (customer_id 6)
    # Rule: "True when email contains '@'"
    contains_at_email_check = con.execute("SELECT email_is_valid FROM silver.customers_cleaned WHERE customer_id = 6;").fetchone()[0]
    assert contains_at_email_check is True, "Email 'david@noat.com' contains '@' and should be valid by the rule."

    # Check a null email (customer_id 5)
    null_email_check = con.execute("SELECT email_is_valid FROM silver.customers_cleaned WHERE customer_id = 5;").fetchone()[0]
    assert null_email_check is False, "Null email should be invalid (na=False)."


def test_rejected_rows_exists_and_content(duckdb_connection, setup_bronze_data):
    """Test that rejected.rejected_rows table exists and contains the expected rejected row."""
    con = duckdb_connection
    rejected_count = con.execute("SELECT count(*) FROM rejected.rejected_rows;").fetchone()[0]
    assert rejected_count > 0, "rejected.rejected_rows should exist and have more than 0 rows."

    rejected_data = con.execute("SELECT customer_id, rejection_reason FROM rejected.rejected_rows;").df()
    assert len(rejected_data) == 1, "Expected exactly one rejected row for null customer_id."
    assert rejected_data['customer_id'].isnull().all(), "Rejected row should have null customer_id."
    assert rejected_data['rejection_reason'].iloc[0] == 'null customer_id', "Rejection reason should be 'null customer_id'."

def test_duplicate_customer_id_resolution(duckdb_connection, setup_bronze_data):
    """Test that duplicate customer_id rows are resolved by keeping the latest join_date."""
    con = duckdb_connection
    # Customer ID 1 had two entries: 2023-01-01 and 2023-01-03. The one with 2023-01-03 should be kept.
    customer_1_data = con.execute("SELECT name, join_date FROM silver.customers_cleaned WHERE customer_id = 1;").df()
    assert len(customer_1_data) == 1, "Only one entry for customer_id 1 should remain."
    assert customer_1_data['name'].iloc[0] == "John Doe Alt", "The entry with the latest join_date for customer_id 1 was not kept."
    assert customer_1_data['join_date'].iloc[0] == pd.Timestamp("2023-01-03"), "The entry with the latest join_date for customer_id 1 was not kept."

    # Customer ID 7 had two entries: 2023-01-09 and 2023-01-08. The one with 2023-01-09 should be kept.
    customer_7_data = con.execute("SELECT name, join_date FROM silver.customers_cleaned WHERE customer_id = 7;").df()
    assert len(customer_7_data) == 1, "Only one entry for customer_id 7 should remain."
    assert customer_7_data['name'].iloc[0] == "Eve Black", "The entry with the latest join_date for customer_id 7 was not kept."
    assert customer_7_data['join_date'].iloc[0] == pd.Timestamp("2023-01-09"), "The entry with the latest join_date for customer_id 7 was not kept."

def test_total_row_counts(duckdb_connection, setup_bronze_data):
    """Verify BRONZE_total_rows = SILVER_cleaned_rows + REJECTED_rows + Deduplicated_rows."""
    con = duckdb_connection
    bronze_total_rows = con.execute("SELECT count(*) FROM bronze.customers_raw;").fetchone()[0]
    silver_cleaned_rows = con.execute("SELECT count(*) FROM silver.customers_cleaned;").fetchone()[0]
    rejected_rows = con.execute("SELECT count(*) FROM rejected.rejected_rows;").fetchone()[0]

    # In our test data:
    # bronze_total_rows = 10
    # rejected_rows = 1 (for the row with customer_id = None)
    # Deduplicated rows = 2 (one for customer_id 1, one for customer_id 7)
    # Expected silver_cleaned_rows = bronze_total_rows - rejected_rows - deduplicated_rows = 10 - 1 - 2 = 7
    assert bronze_total_rows == (rejected_rows + silver_cleaned_rows + 2), \
        f"Row count mismatch: {bronze_total_rows} (bronze) != {rejected_rows} (rejected) + {silver_cleaned_rows} (silver) + 2 (deduplicated)"

    # Also check that the number of cleaned rows matches the number of unique non-null customer_ids in bronze
    unique_non_null_bronze = con.execute("SELECT count(DISTINCT customer_id) FROM bronze.customers_raw WHERE customer_id IS NOT NULL;").fetchone()[0]
    assert silver_cleaned_rows == unique_non_null_bronze, \
        f"Number of rows in silver.customers_cleaned ({silver_cleaned_rows}) should match unique non-null customer_ids from bronze ({unique_non_null_bronze})."