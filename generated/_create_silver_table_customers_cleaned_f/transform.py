import duckdb
import pandas as pd
import os

# Pre-injected constants (for local testing, define them if not injected)
# DB_PATH = "data/warehouse.duckdb"
# DASHBOARD = "data/dashboard"

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the raw customer data.

    - Strips whitespace from all string columns.
    - Parses 'join_date' to datetime if it exists.
    - Drops duplicate rows by 'customer_id', keeping the one with the latest 'join_date'.
    - Creates a boolean column 'email_is_valid' (True when email contains '@').
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id (ascending) and join_date (descending)
    # This ensures that when duplicates are dropped, the one with the latest join_date is kept.
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        df = df.drop_duplicates(subset=['customer_id'], keep='first')
    elif 'customer_id' in df.columns: # If join_date is missing, just drop duplicates on customer_id
        df = df.drop_duplicates(subset=['customer_id'], keep='first')


    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].astype(str).str.contains('@', na=False)
    else:
        df['email_is_valid'] = False # Default to False if email column is missing

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # Load data from bronze.customers_raw
    print("Loading data from bronze.customers_raw...")
    customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw;").fetch_df()
    print(f"Loaded {len(customers_raw_df)} rows from bronze.customers_raw.")

    # Transform data
    print("Transforming customer data...")
    customers_cleaned_df = transform_customers(customers_raw_df.copy())
    print(f"Transformed to {len(customers_cleaned_df)} rows.")

    # Write to silver.customers_cleaned
    print("Writing transformed data to silver.customers_cleaned...")
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_cleaned_df;")
    print("Successfully wrote to silver.customers_cleaned.")

    con.close()
    print("DuckDB connection closed.")

if __name__ == "__main__":
    # For local testing, define DB_PATH and DASHBOARD if not pre-injected
    if 'DB_PATH' not in locals():
        DB_PATH = "data/warehouse.duckdb"
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        # Create a dummy bronze.customers_raw table for local testing
        con_test = duckdb.connect(DB_PATH)
        con_test.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        dummy_data = pd.DataFrame({
            'customer_id': [1.0, 2.0, 1.0, 3.0, 4.0, 2.0],
            'name': [' John Doe ', 'Jane Smith', 'John Doe', 'Alice', 'Bob', 'Jane Smith '],
            'email': ['john.doe@example.com', 'jane.smith@example.com', 'john.doe@example.com', 'alice@test.com', 'invalid-email', 'jane.smith@example.com'],
            'address': ['123 Main St', '456 Oak Ave', '123 Main St', '789 Pine Ln', '101 Elm St', '456 Oak Ave'],
            'join_date': ['2023-01-01', '2023-01-05', '2023-01-02', '2023-01-10', '2023-01-15', '2023-01-04'],
            '_source_file': ['file1.csv']*6,
            '_ingest_ts': ['2023-01-01T00:00:00Z']*6
        })
        con_test.execute("CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM dummy_data;")
        con_test.close()
        print(f"Created dummy bronze.customers_raw at {DB_PATH} for local testing.")

    if 'DASHBOARD' not in locals():
        DASHBOARD = "data/dashboard"
        os.makedirs(DASHBOARD, exist_ok=True)

    main()