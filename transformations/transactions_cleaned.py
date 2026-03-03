import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # Load data from bronze.transactions_raw
    df = con.execute("SELECT * FROM bronze.transactions_raw").df()

    # Strip whitespace from all string columns
    # Use .copy() to avoid SettingWithCopyWarning if df is a slice
    df_processed = df.copy()
    string_cols = df_processed.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Ensure column is treated as string before stripping, handling potential non-string types
        df_processed[col] = df_processed[col].astype(str).str.strip()

    # Coerce 'amount' column to numeric, errors='coerce' will turn unparseable values into NaN
    df_processed['amount'] = pd.to_numeric(df_processed['amount'], errors='coerce')

    # Identify rejected rows based on conditions
    is_null_id = df_processed['transaction_id'].isnull()
    is_invalid_amount = df_processed['amount'].isnull() | (df_processed['amount'] <= 0)

    rejected_mask = is_null_id | is_invalid_amount

    # Prepare rejection reasons for all rows
    df_processed['rejection_reason'] = ''
    df_processed.loc[is_null_id, 'rejection_reason'] += 'Null transaction_id; '
    df_processed.loc[is_invalid_amount, 'rejection_reason'] += 'Invalid amount (null or non-positive); '
    df_processed['rejection_reason'] = df_processed['rejection_reason'].str.strip('; ')

    # Separate valid and rejected rows
    valid_df = df_processed[~rejected_mask].drop(columns=['rejection_reason']).copy()
    rejected_df = df_processed[rejected_mask].copy()

    # Write valid rows to silver.transactions_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM valid_df")

    # Handle rejected rows: Create or replace rejected.rejected_rows
    if not rejected_df.empty:
        con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df")
    else:
        # If no rejected rows, create an empty table with the expected schema
        # This ensures the table always exists with the correct schema for downstream processes
        # We need to ensure 'rejection_reason' column is present even if empty
        dummy_rejected_schema = df_processed.head(0).copy()
        dummy_rejected_schema['rejection_reason'] = pd.Series(dtype='string')
        con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM dummy_rejected_schema")

    con.close()

if __name__ == "__main__":
    main()