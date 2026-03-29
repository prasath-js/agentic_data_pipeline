import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Load raw customers data
    customers_df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # --- REJECTION HANDLING: Rows with null customer_id ---
    rejected_df = customers_df[customers_df['customer_id'].isnull()].copy()
    if not rejected_df.empty:
        rejected_df['rejection_reason'] = 'customer_id is null'
        
        # Define the schema for the rejected table based on customers_raw + rejection_reason
        # This ensures consistency if the table is created or appended to.
        rejected_table_schema = """
            customer_id DOUBLE,
            name VARCHAR,
            email VARCHAR,
            address VARCHAR,
            join_date VARCHAR,
            _source_file VARCHAR,
            _ingest_ts VARCHAR,
            rejection_reason VARCHAR
        """
        
        # Create rejected.rejected_rows table if it doesn't exist
        con.execute(f"CREATE TABLE IF NOT EXISTS rejected.rejected_rows ({rejected_table_schema});")
        
        # Append rejected rows
        # Ensure column order matches the target table
        rejected_df_to_insert = rejected_df[[
            'customer_id', 'name', 'email', 'address', 'join_date', 
            '_source_file', '_ingest_ts', 'rejection_reason'
        ]]
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_to_insert;")
        
    # Filter out rejected rows from the main DataFrame
    customers_df = customers_df[customers_df['customer_id'].notnull()].copy()

    # --- DATA CLEANING AND TRANSFORMATION ---

    # 1. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        customers_df[col] = customers_df[col].astype(str).str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 3. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in customers_df.columns:
        customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)
    else:
        customers_df['email_is_valid'] = False # Default if email column is missing

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'customer_id' in customers_df.columns and 'join_date' in customers_df.columns:
        # Sort by customer_id and join_date (descending) to keep the latest
        customers_df = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        customers_df = customers_df.drop_duplicates(subset='customer_id', keep='first')
    elif 'customer_id' in customers_df.columns:
        # If join_date is missing, just drop duplicates by customer_id
        customers_df = customers_df.drop_duplicates(subset='customer_id', keep='first')

    # Write cleaned data to silver.customers_cleaned
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")

    con.close()

if __name__ == "__main__":
    main()