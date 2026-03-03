import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # 1. Load customers_raw from BRONZE
    customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Initialize an empty DataFrame for rejected rows to accumulate
    all_rejected_rows = pd.DataFrame()

    # 2. Identify rows with null customer_id and reject them
    null_customer_id_rejected = customers_raw_df[customers_raw_df['customer_id'].isnull()].copy()
    if not null_customer_id_rejected.empty:
        null_customer_id_rejected['rejection_reason'] = 'null customer_id'
        all_rejected_rows = pd.concat([all_rejected_rows, null_customer_id_rejected], ignore_index=True)

    # Filter out rejected rows from the main DataFrame
    customers_df = customers_raw_df.dropna(subset=['customer_id']).copy()

    # Convert customer_id to nullable integer after dropping nulls
    # This is important as customer_id is float64 in bronze, but should be an ID.
    customers_df['customer_id'] = customers_df['customer_id'].astype('Int64')

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        customers_df[col] = customers_df[col].str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id and join_date (descending) to keep the latest
    if 'join_date' in customers_df.columns:
        customers_df = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
    else: # If join_date doesn't exist, just drop duplicates on customer_id
        customers_df = customers_df.sort_values(by=['customer_id'], ascending=[True]) # Consistent sort for deterministic keep
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)

    # 6. Write cleaned rows to SILVER as customers_cleaned
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df")

    # Write rejected rows to rejected.rejected_rows
    if not all_rejected_rows.empty:
        con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")
        # Check if table exists to decide between CREATE OR REPLACE and INSERT INTO
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'").fetchone()[0] > 0
        if table_exists:
            # Append to existing rejected_rows table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM all_rejected_rows")
        else:
            # Create new rejected_rows table
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM all_rejected_rows")

    con.close()

if __name__ == "__main__":
    main()