import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # 1. Load customers_raw from BRONZE
    customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Initialize rejected_df with the same columns as customers_raw_df plus 'rejection_reason'
    # Create an empty DataFrame with the target schema for rejected rows
    rejected_df_template = customers_raw_df.copy()
    rejected_df_template['rejection_reason'] = pd.NA # Add the new column with a nullable type
    rejected_df = rejected_df_template[0:0] # Create an empty DataFrame with the desired schema

    # 2. Identify and capture rows with null customer_id
    null_customer_id_rows = customers_raw_df[customers_raw_df['customer_id'].isnull()].copy()
    if not null_customer_id_rows.empty:
        null_customer_id_rows['rejection_reason'] = 'null customer_id'
        # Ensure columns match the target rejected_df schema before concatenation
        # This is crucial for consistent schema in the rejected table
        rejected_df = pd.concat([rejected_df, null_customer_id_rows[rejected_df.columns]], ignore_index=True)

    # Filter out rejected rows from the main DataFrame
    customers_df = customers_raw_df.dropna(subset=['customer_id']).copy()

    # Convert customer_id to nullable integer type (Int64)
    # This handles potential float representation and ensures it's an integer ID
    if 'customer_id' in customers_df.columns:
        customers_df['customer_id'] = customers_df['customer_id'].astype('Int64')

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Ensure column is treated as string before stripping, handling potential non-string types
        customers_df[col] = customers_df[col].astype(str).str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'join_date' in customers_df.columns:
        # Sort by customer_id and join_date (descending) to keep the latest
        customers_df = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
    else:
        # If join_date doesn't exist, just drop duplicates on customer_id, keeping first encountered
        customers_df = customers_df.sort_values(by=['customer_id'], ascending=[True])

    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in customers_df.columns:
        # Convert to string first to handle potential NaN/None values gracefully
        customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)
    else:
        # Default to False if email column is missing
        customers_df['email_is_valid'] = False

    # Write cleaned rows to SILVER as customers_cleaned
    # DuckDB will infer types from the DataFrame
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df;")

    # Write rejected rows to rejected.rejected_rows
    if not rejected_df.empty:
        # Check if rejected.rejected_rows table already exists
        table_exists_query = f"""
            SELECT count(*)
            FROM duckdb_tables()
            WHERE schema_name = 'rejected' AND table_name = 'rejected_rows';
        """
        table_exists = con.execute(table_exists_query).fetchone()[0] > 0

        if table_exists:
            # Append to existing table
            # Ensure column order matches existing table if appending
            existing_table_cols = con.execute("PRAGMA table_info('rejected.rejected_rows');").df()['name'].tolist()
            # Reorder rejected_df columns to match existing table
            rejected_df_reordered = rejected_df[existing_table_cols]
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_reordered;")
        else:
            # Create new table
            con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;")

    con.close()

if __name__ == "__main__":
    main()