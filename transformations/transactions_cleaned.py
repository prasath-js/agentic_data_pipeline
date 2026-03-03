import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # 1. Load & Initial Transform
    try:
        transactions_raw_df = con.execute("SELECT * FROM bronze.transactions_raw").df()
    except duckdb.CatalogException:
        print("Error: bronze.transactions_raw table not found. Please ensure it exists.")
        con.close()
        return

    # Make a copy to avoid SettingWithCopyWarning
    df = transactions_raw_df.copy()

    # Strip whitespace from all string columns
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].str.strip()

    # Coerce amount column to numeric (errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # 2. Identify Rejections
    is_null_id = df['transaction_id'].isnull()
    is_invalid_amount = df['amount'].isnull() | (df['amount'] <= 0)
    
    rejected_mask = is_null_id | is_invalid_amount

    # 3. Separate & Assign Reasons
    # Initialize rejection_reason column for all rows to simplify logic
    df['rejection_reason'] = ''

    # Populate rejection_reason for rejected rows
    df.loc[is_null_id, 'rejection_reason'] += 'Null transaction_id; '
    df.loc[is_invalid_amount, 'rejection_reason'] += 'Invalid amount (null, zero, or negative); '

    # Clean up trailing '; ' from rejection_reason
    df['rejection_reason'] = df['rejection_reason'].str.strip('; ')

    # Separate valid and rejected DataFrames
    # Valid rows do not need the 'rejection_reason' column
    valid_df = df[~rejected_mask].drop(columns=['rejection_reason'])
    # Rejected rows include the 'rejection_reason' column
    rejected_df = df[rejected_mask]

    # 4. Write SILVER table transactions_cleaned
    if not valid_df.empty:
        con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM valid_df")
        print(f"Wrote {len(valid_df)} valid rows to silver.transactions_cleaned")
    else:
        print("No valid rows to write to silver.transactions_cleaned.")
        # Ensure table is created with correct schema even if empty
        # Infer schema from the original raw data, after initial type coercion for 'amount'
        temp_df_schema_for_silver = transactions_raw_df.copy()
        temp_df_schema_for_silver['amount'] = pd.to_numeric(temp_df_schema_for_silver['amount'], errors='coerce')
        # Ensure no 'rejection_reason' column in the silver table schema
        if 'rejection_reason' in temp_df_schema_for_silver.columns:
            temp_df_schema_for_silver = temp_df_schema_for_silver.drop(columns=['rejection_reason'])
        con.execute(f"CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM temp_df_schema_for_silver WHERE 1=0")

    # 5. Write REJECTED table rejected.rejected_rows
    # This table should accumulate rejections across pipeline runs/steps.
    # If it exists, append; otherwise, create.
    
    # First, define the expected schema for rejected.rejected_rows.
    # This ensures the table is created with consistent types, even if transactions_raw_df is empty.
    if transactions_raw_df.empty:
        # Define a default schema for rejected_df if source was empty
        rejected_df_schema_template = pd.DataFrame(columns=[
            'transaction_id', 'customer_id', 'quantity', 'amount', 
            'transaction_date', '_source_file', '_ingest_ts', 'rejection_reason'
        ]).astype({
            'transaction_id': 'float64', 'customer_id': 'float64', 'quantity': 'float64', 
            'amount': 'float64', 'transaction_date': 'string', '_source_file': 'string', 
            '_ingest_ts': 'string', 'rejection_reason': 'string'
        })
    else:
        # Use the schema from the processed df (which includes rejection_reason and coerced amount)
        rejected_df_schema_template = df.copy()
        # Ensure amount is float and rejection_reason is string for schema inference
        rejected_df_schema_template['amount'] = pd.to_numeric(rejected_df_schema_template['amount'], errors='coerce')
        rejected_df_schema_template['rejection_reason'] = rejected_df_schema_template['rejection_reason'].astype('string')

    # Create an empty table with the expected schema if it doesn't exist
    con.execute(f"CREATE TABLE IF NOT EXISTS rejected.rejected_rows AS SELECT * FROM rejected_df_schema_template WHERE 1=0")

    if not rejected_df.empty:
        # Append to the existing or newly created rejected.rejected_rows table
        # Ensure column order and types match for insertion
        # Get column names from the target table
        target_cols_info = con.execute("PRAGMA table_info('rejected.rejected_rows')").df()
        target_col_names = target_cols_info['name'].tolist()
        
        # Select and reorder columns in rejected_df to match target table
        # Any columns in rejected_df not in target_col_names will be dropped
        # Any columns in target_col_names not in rejected_df will be filled with NULL (if nullable)
        rejected_df_for_insert = rejected_df[target_col_names]
        
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_for_insert")
        print(f"Appended {len(rejected_df)} rejected rows to rejected.rejected_rows")
    else:
        print("No rejected rows to append in this run.")

    con.close()

if __name__ == "__main__":
    main()