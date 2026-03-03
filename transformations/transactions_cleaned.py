import duckdb
import pandas as pd
import os

# DB_PATH and DASHBOARD are injected into the environment
# DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")
# DASHBOARD = os.environ.get("DASHBOARD", "data/dashboard")

def main():
    con = duckdb.connect(DB_PATH)
    
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # Load data from bronze.transactions_raw
    df = con.execute("SELECT * FROM bronze.transactions_raw").df()

    # Create a copy to avoid SettingWithCopyWarning
    df_processed = df.copy()

    # Strip whitespace from all string columns
    # Use include=['object', 'string'] for compatibility with newer pandas versions
    string_cols = df_processed.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Ensure column is treated as string before stripping, handling potential non-string types
        df_processed[col] = df_processed[col].astype(str).str.strip()

    # Coerce 'amount' column to numeric, errors='coerce' will turn unparseable values into NaN
    # First, remove currency symbols if present, then convert
    if 'amount' in df_processed.columns:
        # Convert to string first to handle potential non-string types before regex
        df_processed['amount'] = df_processed['amount'].astype(str).str.replace(r'[$,]', '', regex=True)
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

    # Handle rejected rows: Append if table exists, otherwise create
    if not rejected_df.empty:
        # Register the rejected_df for DuckDB to use
        con.register('rejected_df_temp_view', rejected_df)

        # Check if rejected.rejected_rows table already exists
        table_exists = con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0

        if table_exists:
            # Append to the existing table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_temp_view")
        else:
            # Create the table if it does not exist
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df_temp_view")
        
        con.unregister('rejected_df_temp_view') # Clean up temp view

    con.close()

if __name__ == "__main__":
    main()