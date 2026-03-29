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
    # Ensure all columns from customers_raw_df are present in rejected_df for consistent schema
    rejected_cols = customers_raw_df.columns.tolist() + ['rejection_reason']
    rejected_df = pd.DataFrame(columns=rejected_cols)

    # 2. Identify and capture rows with null customer_id
    null_customer_id_rows = customers_raw_df[customers_raw_df['customer_id'].isnull()]
    if not null_customer_id_rows.empty:
        null_customer_id_rows_copy = null_customer_id_rows.copy()
        null_customer_id_rows_copy['rejection_reason'] = 'null customer_id'
        # Align columns before concatenation to avoid dtype issues if rejected_df is empty
        rejected_df = pd.concat([rejected_df, null_customer_id_rows_copy[rejected_cols]], ignore_index=True)

    # Filter out rejected rows from the main DataFrame
    customers_df = customers_raw_df.dropna(subset=['customer_id']).copy()

    # Convert customer_id to integer type if it's float and has no decimals, for consistency
    if 'customer_id' in customers_df.columns and pd.api.types.is_float_dtype(customers_df['customer_id']):
        # Only convert if all values can be represented as integers without loss
        # And handle potential NaN values which would prevent direct conversion
        if customers_df['customer_id'].notna().all() and (customers_df['customer_id'] == customers_df['customer_id'].astype(int)).all():
            customers_df['customer_id'] = customers_df['customer_id'].astype(int)

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
        customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)
    else:
        customers_df['email_is_valid'] = False # Default to False if email column is missing

    # Write cleaned rows to SILVER as customers_cleaned
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
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df;")
        else:
            # Create new table
            con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;")

    con.close()

if __name__ == "__main__":
    main()