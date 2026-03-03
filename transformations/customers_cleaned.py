import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # 1. Load customers_raw from BRONZE
    try:
        customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw").df()
    except duckdb.CatalogException:
        print("Error: bronze.customers_raw table not found. Exiting.")
        con.close()
        return

    # Create a template for rejected_df to ensure consistent schema
    # This template should include all original columns + 'rejection_reason'
    rejected_template_df = customers_raw_df.head(0).copy()
    rejected_template_df['rejection_reason'] = pd.Series(dtype='string')
    rejected_df = pd.DataFrame(columns=rejected_template_df.columns) # Initialize empty rejected_df

    # 2. Identify rows with null customer_id for rejection
    null_customer_id_mask = customers_raw_df['customer_id'].isnull()
    null_customer_id_rows = customers_raw_df[null_customer_id_mask].copy()

    if not null_customer_id_rows.empty:
        null_customer_id_rows['rejection_reason'] = 'null customer_id'
        # Ensure columns match the rejected_df template before concatenation
        rejected_df = pd.concat([rejected_df, null_customer_id_rows[rejected_template_df.columns]], ignore_index=True)
        customers_df = customers_raw_df[~null_customer_id_mask].copy()
    else:
        customers_df = customers_raw_df.copy()

    # --- Transformation steps on customers_df ---

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        customers_df[col] = customers_df[col].astype(str).str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'join_date' in customers_df.columns:
        # Sort by customer_id ascending, then join_date descending to keep the latest
        customers_df_sorted = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False], na_position='last')
    else:
        # If join_date is missing, just sort by customer_id for consistent deduplication
        customers_df_sorted = customers_df.sort_values(by=['customer_id'])
    
    customers_df = customers_df_sorted.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in customers_df.columns:
        # Convert email to string to handle potential non-string types (like None/NaN) before .str.contains
        customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)
    else:
        # If email column is missing, add email_is_valid as False for all rows
        customers_df['email_is_valid'] = False 
    
    # Ensure customer_id is float type as per sample, if it's not already
    if 'customer_id' in customers_df.columns:
        customers_df['customer_id'] = customers_df['customer_id'].astype(float)

    # 6. Write cleaned rows to SILVER as customers_cleaned
    # If customers_df is empty at this point, ensure it has the correct schema for the silver table
    if customers_df.empty:
        # Create an empty DataFrame with the *final expected schema*
        # This is derived from the original raw columns + 'email_is_valid'
        final_silver_columns = list(customers_raw_df.columns)
        if 'email_is_valid' not in final_silver_columns:
            final_silver_columns.append('email_is_valid')
        
        # Create an empty DataFrame with the correct column names and dtypes
        empty_final_silver_df = pd.DataFrame(columns=final_silver_columns)
        # Explicitly set dtypes for key columns to ensure DuckDB infers correctly
        if 'customer_id' in empty_final_silver_df.columns:
            empty_final_silver_df['customer_id'] = empty_final_silver_df['customer_id'].astype(float)
        if 'join_date' in empty_final_silver_df.columns:
            empty_final_silver_df['join_date'] = empty_final_silver_df['join_date'].astype('datetime64[ns]')
        if 'email_is_valid' in empty_final_silver_df.columns:
            empty_final_silver_df['email_is_valid'] = empty_final_silver_df['email_is_valid'].astype(bool)
        
        con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM empty_final_silver_df;")
    else:
        con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")

    # Write rejected rows to rejected.rejected_rows
    if not rejected_df.empty:
        try:
            # Try to read from the table to check its existence and schema
            # This will raise CatalogException if the table does not exist
            con.execute("SELECT * FROM rejected.rejected_rows LIMIT 0;").df()
            # If it exists, append. Ensure column order and types match.
            # DuckDB's INSERT INTO will match columns by name if not specified, but explicit is safer.
            # However, the rejected_df is built to match the original schema + rejection_reason,
            # so direct insert should work.
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df;")
        except duckdb.CatalogException:
            # If it doesn't exist, create it
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;")
    elif con.execute("SHOW TABLES IN rejected;").df()['name'].isin(['rejected_rows']).any():
        # If rejected_df is empty but the table exists from a previous run, ensure it's empty or replaced
        # For this pipeline, we want to ensure it's always fresh if no rejections occur in this run.
        # However, the requirement is to APPEND, so if no rejections, we do nothing.
        # If the table exists and is empty, that's fine.
        pass # No rejections, so nothing to write or append.

    con.close()

if __name__ == "__main__":
    main()