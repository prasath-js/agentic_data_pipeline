import duckdb
import pandas as pd
import os

# Constants (DB_PATH is injected into the execution environment)
# DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")
# DASHBOARD = os.environ.get("DASHBOARD", "dashboard")

def main():
    con = duckdb.connect(DB_PATH)
    # Ensure DuckDB correctly infers string types from pandas object columns
    con.execute("SET GLOBAL pandas_analyze_string_object_columns=true;")

    # 1. Load customers_raw from BRONZE
    customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Initialize DataFrame for rejected rows
    rejected_rows_df = pd.DataFrame()

    # 2. Identify rows with null customer_id and reject them
    null_customer_id_mask = customers_raw_df['customer_id'].isnull()
    if null_customer_id_mask.any():
        rejected_null_id = customers_raw_df[null_customer_id_mask].copy()
        rejected_null_id['rejection_reason'] = 'customer_id is null'
        # Ensure all columns from original df are present in rejected_null_id before concat
        # This is important if rejected_rows_df was initialized empty and needs schema definition
        # For this specific case, rejected_null_id already has the original schema + rejection_reason
        rejected_rows_df = pd.concat([rejected_rows_df, rejected_null_id], ignore_index=True)

    # Filter out rejected rows from the main DataFrame
    customers_df = customers_raw_df[~null_customer_id_mask].copy()

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        customers_df[col] = customers_df[col].astype(str).str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id (ascending) and join_date (descending)
    customers_df = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
    # Drop duplicates on customer_id, keeping the first (which will be the latest join_date due to sorting)
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    # Convert email to string to handle potential NaN values gracefully before .str.contains
    customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)

    # 6. Write cleaned rows to SILVER as customers_cleaned
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df")

    # Write rejected rows to rejected.rejected_rows
    if not rejected_rows_df.empty:
        con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

        # Check if rejected.rejected_rows table exists
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0

        if table_exists:
            # Append to existing rejected.rejected_rows table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_rows_df")
        else:
            # Create rejected.rejected_rows table if it doesn't exist
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_rows_df")

    con.close()

if __name__ == "__main__":
    main()