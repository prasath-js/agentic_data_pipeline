import duckdb
import pandas as pd
import os

def main():
    # Establish connection to DuckDB
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # 1. Load & Initial Transform
    # Read bronze.transactions_raw into a pandas DataFrame
    transactions_raw_df = con.execute("SELECT * FROM bronze.transactions_raw").df()

    # Create a copy to work with
    df = transactions_raw_df.copy()

    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].str.strip()

    # Coerce amount column to numeric (errors='coerce')
    # First, remove '$' sign if present, then convert to numeric
    if 'amount' in df.columns and df['amount'].dtype == 'object':
        df['amount'] = df['amount'].astype(str).str.replace('$', '', regex=False)
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # 2. Identify Rejections
    # Rows with null transaction_id are REJECTED
    is_null_id = df['transaction_id'].isnull()

    # Rows with null or zero or negative amount are REJECTED
    is_invalid_amount = df['amount'].isnull() | (df['amount'] <= 0)

    # Combine all rejection conditions
    rejected_mask = is_null_id | is_invalid_amount

    # 3. Separate & Assign Reasons
    valid_df = df[~rejected_mask].copy()
    rejected_df = df[rejected_mask].copy()

    # Add rejection_reason column to rejected_df
    if not rejected_df.empty:
        rejected_df['rejection_reason'] = ''
        rejected_df.loc[is_null_id[rejected_mask], 'rejection_reason'] += 'Null transaction_id; '
        rejected_df.loc[is_invalid_amount[rejected_mask], 'rejection_reason'] += 'Invalid amount; '
        rejected_df['rejection_reason'] = rejected_df['rejection_reason'].str.strip('; ')
    else:
        # If no rejections, ensure the column exists for schema consistency if it were to be appended later
        rejected_df['rejection_reason'] = pd.Series(dtype='str')

    # 4. Write SILVER table: transactions_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM valid_df")

    # 5. Write Rejected rows to rejected.rejected_rows
    # Check if rejected.rejected_rows table exists
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
    ).fetchone()[0]

    if table_exists > 0:
        # If table exists, append
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df")
    else:
        # If table does not exist, create it
        con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df")

    # Close connection
    con.close()

if __name__ == "__main__":
    main()