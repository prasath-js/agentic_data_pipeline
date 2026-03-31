import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

    # Load raw opportunities data
    opportunities_raw_df = con.execute("SELECT * FROM bronze.opportunities_raw").df()

    # --- Transformations ---

    # 1. Strip whitespace from all string columns
    string_cols = opportunities_raw_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        opportunities_raw_df[col] = opportunities_raw_df[col].str.strip()

    # 2. Coerce 'value' column to numeric (errors='coerce')
    # The sample shows 'value' column, not 'opportunity_value'
    opportunities_raw_df['value'] = pd.to_numeric(opportunities_raw_df['value'], errors='coerce')

    # --- Rejection Handling ---

    # Identify rejected rows based on null account_id or stage
    rejected_mask = opportunities_raw_df['account_id'].isnull() | opportunities_raw_df['stage'].isnull()

    rejected_df = opportunities_raw_df[rejected_mask].copy()
    valid_df = opportunities_raw_df[~rejected_mask].copy()

    # Add rejection_reason column to rejected_df
    rejected_df['rejection_reason'] = ''
    rejected_df.loc[rejected_df['account_id'].isnull(), 'rejection_reason'] += 'account_id is null; '
    rejected_df.loc[rejected_df['stage'].isnull(), 'rejection_reason'] += 'stage is null; '
    rejected_df['rejection_reason'] = rejected_df['rejection_reason'].str.strip('; ')

    # Ensure the rejected_rows table exists with the correct schema
    # Create an empty table with the combined schema of opportunities_raw_df and rejection_reason
    if not rejected_df.empty:
        # Get schema of rejected_df including the new 'rejection_reason' column
        rejected_table_schema = ", ".join([f"{col} {dtype}" for col, dtype in rejected_df.dtypes.items()])
        con.execute(f"CREATE TABLE IF NOT EXISTS rejected.rejected_rows ({rejected_table_schema})")

        # Append rejected rows to the rejected.rejected_rows table
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df")
    else:
        # If no rejections, ensure the table still exists but is empty
        # Use the schema of the original df + rejection_reason column
        temp_df_for_schema = opportunities_raw_df.copy()
        temp_df_for_schema['rejection_reason'] = pd.Series(dtype='str')
        rejected_table_schema = ", ".join([f"{col} {dtype}" for col, dtype in temp_df_for_schema.dtypes.items()])
        con.execute(f"CREATE TABLE IF NOT EXISTS rejected.rejected_rows ({rejected_table_schema})")


    # Write valid rows to silver.opportunities_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM valid_df")

    con.close()

if __name__ == "__main__":
    main()