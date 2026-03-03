import duckdb
import pandas as pd
import os

# Pre-injected constants (for main function)
# DB_PATH = "my_duckdb.db"
# DASHBOARD = "./dashboard_output"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the raw customer data.

    Args:
        df (pd.DataFrame): The input DataFrame from bronze.customers_raw.

    Returns:
        pd.DataFrame: The transformed DataFrame for silver.customers_cleaned.
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        # Sort by customer_id (ascending) and join_date (descending)
        # This ensures that for each customer_id, the row with the latest join_date comes first
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        # Drop duplicates based on customer_id, keeping the first (which is the latest join_date)
        df = df.drop_duplicates(subset='customer_id', keep='first')
    elif 'customer_id' in df.columns: # If join_date is missing, just drop duplicates by customer_id
        df = df.drop_duplicates(subset='customer_id', keep='first')


    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        # If email column doesn't exist, create email_is_valid as False
        df['email_is_valid'] = False

    return df

def main():
    # Ensure the dashboard directory exists
    os.makedirs(DASHBOARD, exist_ok=True)

    con = duckdb.connect(database=DB_PATH, read_only=False)

    # Create schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # Example: Create a dummy bronze.customers_raw table for demonstration if it doesn't exist
    # In a real scenario, this table would already be populated by an upstream process.
    try:
        con.execute("SELECT * FROM bronze.customers_raw LIMIT 1")
    except duckdb.CatalogException:
        print("bronze.customers_raw not found, creating a dummy table for demonstration.")
        dummy_data = {
            'customer_id': [1.0, 2.0, 1.0, 3.0, 4.0, 2.0, 5.0],
            'name': [' John Doe ', 'Jane Smith', 'John Doe', 'Alice', 'Bob', 'Jane Smith', 'Charlie'],
            'email': ['john.doe@example.com ', 'jane@example.com', 'john.doe.old@example.com', 'alice@test.com', 'bob.com', 'jane@example.com', None],
            'address': ['123 Main St', '456 Oak Ave', '123 Main St', '789 Pine Ln', '101 Elm St', '456 Oak Ave', '202 Maple Dr'],
            'join_date': ['2023-01-15', '2023-02-20', '2023-01-01', '2023-03-10', '2023-04-05', '2023-02-20', '2023-05-01'],
            '_source_file': ['f1', 'f1', 'f1', 'f2', 'f2', 'f1', 'f3'],
            '_ingest_ts': ['ts1', 'ts1', 'ts1', 'ts2', 'ts2', 'ts1', 'ts3']
        }
        dummy_df = pd.DataFrame(dummy_data)
        con.execute("CREATE TABLE bronze.customers_raw AS SELECT * FROM dummy_df;")
        print("Dummy bronze.customers_raw created.")


    # Read data from bronze layer
    df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Apply transformations
    result_df = transform(df)

    # Write result to silver layer
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result_df;")

    con.close()

if __name__ == "__main__":
    # These would be pre-injected in the actual execution environment
    # For local testing, define them here
    if 'DB_PATH' not in locals():
        DB_PATH = "data/pipeline.duckdb"
    if 'DASHBOARD' not in locals():
        DASHBOARD = "dashboard_output"

    # Ensure the directory for DB_PATH exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    main()