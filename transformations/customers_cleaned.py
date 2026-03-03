import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    # Ensure DuckDB correctly infers string types from pandas object columns
    con.execute("SET GLOBAL pandas_analyze_string_object_columns=true;")

    # 1. Load customers_raw from BRONZE
    customers_raw_df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Initialize a list to collect rejected dataframes
    rejected_parts = []

    # Get the original columns for the rejected table schema
    # Define a default set of columns if customers_raw_df is empty,
    # based on the expected schema from the problem description.
    if not customers_raw_df.empty:
        original_cols = customers_raw_df.columns.tolist()
    else:
        original_cols = ['customer_id', 'name', 'email', 'address', 'join_date', '_source_file', '_ingest_ts']
    
    rejected_table_cols = original_cols + ['rejection_reason']

    # 2. Identify rows with null customer_id and reject them
    null_customer_id_mask = customers_raw_df['customer_id'].isnull()
    if null_customer_id_mask.any():
        rejected_null_id_df = customers_raw_df[null_customer_id_mask].copy()
        rejected_null_id_df['rejection_reason'] = 'customer_id is null'
        
        # Create a template DataFrame for rejected rows with all expected columns
        template_rejected_df = pd.DataFrame(columns=rejected_table_cols)
        
        # Concatenate to ensure all columns are present, filling missing with NaN/NaT
        rejected_null_id_df = pd.concat([template_rejected_df, rejected_null_id_df], ignore_index=True)
        
        rejected_parts.append(rejected_null_id_df[rejected_table_cols])

    # Filter out rejected rows from the main DataFrame
    customers_df = customers_raw_df[~null_customer_id_mask].copy()

    # 3. Strip whitespace from all string columns
    string_cols = customers_df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        # Apply strip only if the value is a string, otherwise keep it as is (e.g., NaN)
        customers_df[col] = customers_df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # Parse join_date to datetime if the column exists
    if 'join_date' in customers_df.columns:
        customers_df['join_date'] = pd.to_datetime(customers_df['join_date'], errors='coerce')

    # 4. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id (ascending) and join_date (descending)
    # This ensures that for duplicates, the row with the latest join_date comes first
    # Handle potential NaT in join_date by sorting them last
    if 'join_date' in customers_df.columns:
        customers_df = customers_df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False], na_position='last')
    else:
        customers_df = customers_df.sort_values(by=['customer_id'], ascending=[True])
    
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')

    # 5. Create a boolean column email_is_valid (True when email contains '@')
    # Convert email to string to handle potential NaN values gracefully before .str.contains
    if 'email' in customers_df.columns:
        customers_df['email_is_valid'] = customers_df['email'].astype(str).str.contains('@', na=False)
    else:
        customers_df['email_is_valid'] = False # Default to False if email column is missing

    # 6. Write cleaned rows to SILVER as customers_cleaned
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM customers_df")

    # Consolidate and write rejected rows
    if rejected_parts:
        rejected_rows_df = pd.concat(rejected_parts, ignore_index=True)
        con.execute("CREATE SCHEMA IF NOT EXISTS rejected")

        # Check if rejected.rejected_rows table exists
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0

        if table_exists:
            # Append to existing rejected.rejected_rows table
            # Ensure column order matches the existing table to prevent errors
            existing_table_cols = con.execute("PRAGMA table_info('rejected.rejected_rows')").df()['name'].tolist()
            
            # Filter and reorder rejected_rows_df to match existing_table_cols
            # Add missing columns as NULL, drop extra columns
            for col in existing_table_cols:
                if col not in rejected_rows_df.columns:
                    rejected_rows_df[col] = pd.NA
            rejected_rows_df = rejected_rows_df[existing_table_cols]

            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_rows_df")
        else:
            # Create rejected.rejected_rows table if it doesn't exist
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_rows_df")

    con.close()

if __name__ == "__main__":
    main()