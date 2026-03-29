import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    try:
        # 1. Load opportunities_raw from DuckDB into a pandas DataFrame.
        opportunities_raw_df = con.execute("SELECT * FROM bronze.opportunities_raw").df()

        df = opportunities_raw_df.copy()

        # 2. Apply transformations:
        #    - Iterate string columns, apply df[col] = df[col].str.strip().
        string_cols = df.select_dtypes(include=['object', 'string']).columns
        for col in string_cols:
            # Ensure column is treated as string before stripping
            df[col] = df[col].astype(str).str.strip()

        #    - Coerce df['value'] to pd.to_numeric(errors='coerce').
        #      (Using 'value' as per sample, not 'opportunity_value')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # 3. Identify rejected rows: Create a boolean mask for df['account_id'].isnull() | df['stage'].isnull().
        rejected_mask = df['account_id'].isnull() | df['stage'].isnull()

        # 4. Separate DataFrame: Split into valid_df and rejected_df based on the mask.
        rejected_df = df[rejected_mask].copy()
        valid_df = df[~rejected_mask].copy()

        # Add rejection_reason column to rejected_df
        if not rejected_df.empty:
            rejected_df['rejection_reason'] = ''
            rejected_df.loc[rejected_df['account_id'].isnull(), 'rejection_reason'] += 'account_id is null; '
            rejected_df.loc[rejected_df['stage'].isnull(), 'rejection_reason'] += 'stage is null; '
            rejected_df['rejection_reason'] = rejected_df['rejection_reason'].str.strip('; ')
        else:
            # If no rejections, ensure the column exists for schema consistency if it were to be created
            # This ensures that if rejected_df is empty, the schema for rejected.rejected_rows would still include 'rejection_reason'
            # if it were the first time creating the table.
            rejected_df['rejection_reason'] = pd.Series(dtype='str')


        # 5. Write to DuckDB:
        #    - Overwrite/replace silver.opportunities_cleaned with valid_df.
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM valid_df")

        #    - Append rejected_df to rejected.rejected_rows.
        con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

        # Check if rejected.rejected_rows table exists
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0

        if not rejected_df.empty:
            if table_exists:
                # Append to existing table
                con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df")
            else:
                # Create new table if it doesn't exist
                con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df")
        # If rejected_df is empty and table doesn't exist, we don't create an empty rejected_rows table.
        # If rejected_df is empty and table exists, we don't insert anything. This is the desired behavior.

    finally:
        con.close()

if __name__ == "__main__":
    main()