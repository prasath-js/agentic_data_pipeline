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

    # Initialize rejected_df with the full schema of customers_raw_df plus 'rejection_reason'
    # This ensures schema consistency when creating/inserting into rejected.rejected_rows
    rejected_df_columns = list(customers_raw_df.columns) + ['rejection_reason']
    rejected_df = pd.DataFrame(columns=rejected_df_columns)

    # 2. Identify rows with null customer_id for rejection
    null_customer_id_mask = customers_raw_df['customer_id'].isnull()
    null_customer_id_rows = customers_raw_df[null_customer_id_mask].copy()

    if not null_customer_id_rows.empty:
        null_customer_id_rows['rejection_reason'] = 'null customer_id'
        # Ensure only the expected columns are concatenated to maintain schema
        rejected_df = pd.concat([rejected_df, null_customer_id_rows[rejected_df_columns]], ignore_index=True)
        customers_df = customers_raw_df[~null_customer_id_mask].copy()
    else:
        customers_df = customers_raw_df.copy()

    # If customers_df is empty after rejection, create an empty silver table and handle rejections, then exit.
    if customers_df.empty:
        con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")
        if not rejected_df.empty:
            try:
                con.execute("SELECT * FROM rejected.rejected_rows LIMIT 0;").df() # Check if table exists
                con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df;")
            except duckdb.CatalogException:
                con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;")
        con.close()
        return

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        customers_df[col] = customers_df[col].str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id and join_date (descending)
    if 'join_date' in customers_df.columns:
        # NaT values in join_date will be sorted to the end by na_position='last'
        customers_df_sorted = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False], na_position='last')
    else:
        # If join_date doesn't exist, just drop duplicates on customer_id
        customers_df_sorted = customers_df.sort_values(by=['customer_id']) # Ensure consistent order for dropping
    
    customers_df = customers_df_sorted.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in customers_df.columns:
        customers_df['email_is_valid'] = customers_df['email'].str.contains('@', na=False)
    else:
        customers_df['email_is_valid'] = False # Default to False if email column is missing

    # 6. Write cleaned rows to SILVER as customers_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")

    # Write rejected rows to rejected.rejected_rows
    if not rejected_df.empty:
        try:
            # Try to read from the table to check its existence and schema
            con.execute("SELECT * FROM rejected.rejected_rows LIMIT 0;").df()
            # If it exists, append. Ensure column order and types match.
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df;")
        except duckdb.CatalogException:
            # If it doesn't exist, create it
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;")

    con.close()

if __name__ == "__main__":
    main()