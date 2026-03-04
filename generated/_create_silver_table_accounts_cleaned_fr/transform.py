import pandas as pd
import duckdb
import os

# Define DB_PATH as a constant
DB_PATH = 'my_pipeline.duckdb' # Placeholder for the actual database path

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df_cleaned = df.copy()

    # Strip whitespace from all string columns
    for col in df_cleaned.select_dtypes(include='object').columns:
        df_cleaned[col] = df_cleaned[col].str.strip()

    # Ensure account_id column exists and drop rows where it is null
    if 'account_id' not in df_cleaned.columns:
        raise ValueError("The 'account_id' column is missing from the input DataFrame.")
    
    df_cleaned = df_cleaned.dropna(subset=['account_id'])

    # Uppercase the industry column if it exists
    if 'industry' in df_cleaned.columns:
        df_cleaned['industry'] = df_cleaned['industry'].str.upper()

    # Drop duplicate rows by account_id, keeping the first occurrence
    df_cleaned = df_cleaned.drop_duplicates(subset=['account_id'], keep='first')

    return df_cleaned

def main():
    # Ensure the database file is clean for a fresh run
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)

    # Create schema and dummy table for demonstration
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("""
        CREATE OR REPLACE TABLE bronze.accounts_raw (
            account_id VARCHAR,
            account_name VARCHAR,
            industry VARCHAR,
            region VARCHAR
        );
    """)
    # Insert some dummy data for a complete example
    con.execute("""
        INSERT INTO bronze.accounts_raw VALUES
        ('ACC001 ', '  Company A  ', ' tech ', 'USA'),
        (' ACC002', 'Company B', '  finance  ', 'UK'),
        ('ACC003', 'Company C ', 'retail', 'Germany'),
        ('ACC001', 'Company A Old', 'tech', 'USA'), -- Duplicate
        (NULL, 'Company D', 'services', 'Canada'); -- Null account_id
    """)


    # Load data from bronze.accounts_raw
    df_raw = con.execute('SELECT * FROM bronze.accounts_raw').df()

    # Apply transformations
    df_transformed = transform(df_raw)

    # Persist the result to silver.accounts_cleaned
    con.execute('CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM df_transformed')

    con.close()

if __name__ == '__main__':
    main()