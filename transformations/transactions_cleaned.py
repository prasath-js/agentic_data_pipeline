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
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Ensure column is treated as string before stripping
        df[col] = df[col].astype(str).str.strip()

    # Coerce 'amount' column to numeric, errors='coerce' will turn unparseable values into NaN
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Identify rejected rows based on conditions
    is_null_id = df['transaction_id'].isnull()
    is_invalid_amount = df['amount'].isnull() | (df['amount'] <= 0)

    rejected_mask = is_null_id | is_invalid_amount

    # Prepare rejection reasons for all rows
    df['rejection_reason'] = ''
    df.loc[is_null_id, 'rejection_reason'] += 'Null transaction_id; '
    df.loc[is_invalid_amount, 'rejection_reason'] += 'Invalid amount (null or non-positive); '
    df['rejection_reason'] = df['rejection_reason'].str.strip('; ')

    # Separate valid and rejected rows
    valid_df = df[~rejected_mask].drop(columns=['rejection_reason']).copy()
    rejected_df = df[rejected_mask].copy()

    # Write valid rows to silver.transactions_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM valid_df")

    # Handle rejected rows: append to rejected.rejected_rows if it exists, otherwise create
    table_exists_query = """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'
    """
    table_exists = con.execute(table_exists_query).fetchone()[0] > 0

    if not rejected_df.empty:
        # Register rejected_df as a temporary table for easier SQL operations
        con.create_table("temp_rejected_df", rejected_df)

        if table_exists:
            # Append to existing table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM temp_rejected_df")
        else:
            # Create new table
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM temp_rejected_df")

        # Clean up temporary table
        con.execute("DROP TABLE temp_rejected_df")
    elif not table_exists:
        # If rejected_df is empty and the table doesn't exist, create an empty table
        # with the expected schema (original df columns + rejection_reason)
        dummy_rejected_schema = df.head(0).copy()
        dummy_rejected_schema['rejection_reason'] = pd.Series(dtype='string')
        con.create_table("temp_empty_rejected_df", dummy_rejected_schema)
        con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM temp_empty_rejected_df")
        con.execute("DROP TABLE temp_empty_rejected_df")

    con.close()

if __name__ == "__main__":
    main()