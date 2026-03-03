import duckdb
import pandas as pd
import os
import time
import gc # Import gc module

# DB_PATH is available as a global constant in the execution environment

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # 1. Load Data
    try:
        accounts_raw_df = con.execute("SELECT * FROM bronze.accounts_raw").df()
    except duckdb.CatalogException:
        print("Error: bronze.accounts_raw table not found. Skipping transformation.")
        con.close()
        return

    # Define the full set of columns for the rejected table, including 'rejection_reason'
    rejected_table_cols = accounts_raw_df.columns.tolist() + ['rejection_reason']
    rejected_df_current_batch = pd.DataFrame(columns=rejected_table_cols)

    # Normalize account_id for validation: strip whitespace and treat empty strings as NA
    # Create a temporary column for this validation
    accounts_raw_df['_normalized_account_id'] = accounts_raw_df['account_id'].astype(str).str.strip().replace('', pd.NA)

    # 2. Handle Null `account_id` (including those that become null after stripping)
    null_account_id_rows_mask = accounts_raw_df['_normalized_account_id'].isnull()
    
    # Separate rejected rows
    if null_account_id_rows_mask.any():
        rejected_null_id_df = accounts_raw_df[null_account_id_rows_mask].copy()
        rejected_null_id_df['rejection_reason'] = 'account_id is null or empty after stripping'
        
        # Drop the temporary column before adding to rejected_df_current_batch
        rejected_null_id_df.drop(columns=['_normalized_account_id'], inplace=True, errors='ignore')

        # Ensure rejected_null_id_df has all columns defined for the rejected table
        for col in rejected_table_cols:
            if col not in rejected_null_id_df.columns:
                rejected_null_id_df[col] = pd.NA
        rejected_null_id_df = rejected_null_id_df[rejected_table_cols]

        rejected_df_current_batch = pd.concat([rejected_df_current_batch, rejected_null_id_df], ignore_index=True)
    
    # Continue with cleaned rows
    cleaned_df = accounts_raw_df[~null_account_id_rows_mask].copy()

    # Replace the original account_id with the normalized one in cleaned_df
    if 'account_id' in cleaned_df.columns:
        cleaned_df['account_id'] = cleaned_df['_normalized_account_id']
    
    # Drop the temporary column
    cleaned_df.drop(columns=['_normalized_account_id'], inplace=True, errors='ignore')

    # 3. Strip Whitespace from all other string columns
    # account_id is already stripped and normalized
    string_cols = cleaned_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        if col != 'account_id': # account_id already handled
            # Ensure column is string type before applying .str accessor and handle potential NaN
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip().replace('', pd.NA)

    # 4. Uppercase Industry if column exists
    if 'industry' in cleaned_df.columns:
        # Ensure column is string type before applying .str accessor and handle potential NaN
        cleaned_df['industry'] = cleaned_df['industry'].astype(str).str.upper().replace('NAN', pd.NA)

    # 5. Drop Duplicate rows by account_id
    if 'account_id' in cleaned_df.columns:
        # account_id should already be non-null and stripped at this point
        cleaned_df.drop_duplicates(subset=['account_id'], keep='first', inplace=True)

    # 6. Write Outputs
    # Write cleaned rows to SILVER
    con.execute("CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM cleaned_df")

    # Write rejected rows to rejected.rejected_rows
    if not rejected_df_current_batch.empty:
        # Check if rejected.rejected_rows table exists
        table_exists_query = """
            SELECT count(*)
            FROM duckdb_tables()
            WHERE schema_name = 'rejected' AND table_name = 'rejected_rows'
        """
        table_exists = con.execute(table_exists_query).fetchone()[0] > 0

        if table_exists:
            # Append to existing table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_current_batch")
        else:
            # Create new table
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df_current_batch")
    # If rejected_df_current_batch is empty, no rejections from this run, so nothing to write.

    con.close()

if __name__ == "__main__":
    main()