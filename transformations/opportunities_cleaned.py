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

    # Prepare a dummy DataFrame to define the full schema for rejected.rejected_rows
    # This ensures the 'rejection_reason' column is always part of the schema,
    # even if no rows are rejected in a particular run.
    full_rejected_schema_df = opportunities_raw_df.copy()
    full_rejected_schema_df['rejection_reason'] = pd.Series(dtype='str') # Add the column with a string type

    # Create the rejected.rejected_rows table with the full schema, but no data initially.
    # DuckDB will infer types from full_rejected_schema_df.
    con.execute("CREATE TABLE IF NOT EXISTS rejected.rejected_rows AS SELECT * FROM full_rejected_schema_df WHERE 1=0")

    # If there are rejected rows, insert them into the table.
    if not rejected_df.empty:
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df")

    # Write valid rows to silver.opportunities_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM valid_df")

    con.close()

if __name__ == "__main__":
    main()