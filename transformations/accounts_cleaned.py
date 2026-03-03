import duckdb
import pandas as pd
import os

def run_accounts_transformation(con):
    """
    Transforms the bronze.accounts_raw table into silver.accounts_cleaned.
    Handles null account_ids as rejections, strips whitespace, uppercases industry,
    and drops duplicate account_ids.
    """
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # 1. Load accounts_raw from BRONZE into a pandas DataFrame.
    accounts_raw_df = con.execute("SELECT * FROM bronze.accounts_raw").df()

    # Initialize DataFrame for rejected rows
    rejected_df = pd.DataFrame()

    # 2. Identify rows with null account_id. Capture these as rejections.
    # Use .copy() to avoid SettingWithCopyWarning
    null_account_id_rows = accounts_raw_df[accounts_raw_df['account_id'].isnull()].copy()
    if not null_account_id_rows.empty:
        null_account_id_rows['rejection_reason'] = 'account_id is null'
        # Ensure all columns from accounts_raw_df are present in rejected_df, plus rejection_reason
        # This handles cases where accounts_raw_df might have fewer columns than a previously created rejected_rows table
        rejected_df = pd.concat([rejected_df, null_account_id_rows], ignore_index=True)

    # Filter out rejected rows from the main DataFrame for further cleaning
    cleaned_df = accounts_raw_df.dropna(subset=['account_id']).copy()

    # 3. For remaining rows: Iterate through columns, if a column is of string type, apply str.strip().
    string_cols = cleaned_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Apply strip only if the column is not entirely NaN
        if not cleaned_df[col].isnull().all():
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
            # Convert back to original type if it was not object/string and became object due to NaN
            # This is a more robust way to handle mixed types or NaNs in string columns
            # For this specific case, we assume they are strings or can be treated as such.
            # If original type was not string, it might be converted to object by .astype(str)
            # and then remain object. This is generally acceptable for cleaned string columns.

    # 4. If the 'industry' column exists, convert its values to uppercase.
    if 'industry' in cleaned_df.columns:
        # .str.upper() handles NaN values by returning NaN, which is desired.
        cleaned_df['industry'] = cleaned_df['industry'].str.upper()

    # 5. Drop duplicate rows based on the account_id column, keeping the first occurrence.
    cleaned_df.drop_duplicates(subset=['account_id'], keep='first', inplace=True)

    # Write rejected rows to rejected.rejected_rows
    if not rejected_df.empty:
        # Check if rejected.rejected_rows table exists
        table_exists_query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        table_exists = con.execute(table_exists_query).fetchone()[0] > 0

        if not table_exists:
            # If table doesn't exist, create it with the schema of the current rejected_df
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df")
        else:
            # If table exists, append to it.
            # To ensure compatibility, we select columns from rejected_df that match the existing table.
            existing_cols_query = "PRAGMA table_info('rejected.rejected_rows')"
            existing_cols_df = con.execute(existing_cols_query).df()
            existing_col_names = existing_cols_df['name'].tolist()

            # Filter rejected_df to only include columns that exist in the target table
            cols_to_insert = [col for col in rejected_df.columns if col in existing_col_names]
            
            # Create a temporary DataFrame with aligned columns for insertion
            temp_rejected_df = rejected_df[cols_to_insert].copy() # Use .copy() to avoid SettingWithCopyWarning

            # If there are columns in the existing table not in temp_rejected_df, add them as NULL
            for col in existing_col_names:
                if col not in temp_rejected_df.columns:
                    temp_rejected_df[col] = None # Add missing columns as NULL

            # Ensure column order matches the target table
            temp_rejected_df = temp_rejected_df[existing_col_names]

            # Insert into the existing table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM temp_rejected_df")

    # 6. Write the final cleaned DataFrame to SILVER as accounts_cleaned.
    con.execute("CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM cleaned_df")

def main():
    """
    Main function to connect to DuckDB and run the transformation.
    """
    con = duckdb.connect(DB_PATH)
    try:
        run_accounts_transformation(con)
    finally:
        con.close()

if __name__ == "__main__":
    main()