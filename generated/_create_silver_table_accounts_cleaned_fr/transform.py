import pandas as pd
import duckdb

# Define DB_PATH at the module level for main()
DB_PATH = "my_database.duckdb"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].str.strip()

    # ensure account_id column exists and drop rows where it is null
    if 'account_id' not in df.columns:
        raise ValueError("Column 'account_id' is missing from the DataFrame.")
    df = df.dropna(subset=['account_id'])

    # uppercase the industry column if it exists
    if 'industry' in df.columns:
        df['industry'] = df['industry'].str.upper()

    # drop duplicate rows by account_id
    df = df.drop_duplicates(subset=['account_id'], keep='first')

    return df

def main():
    con = duckdb.connect(DB_PATH)
    
    # Load Data
    # Ensure the bronze.accounts_raw table exists for this to run
    # Example: con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    # con.execute("CREATE TABLE IF NOT EXISTS bronze.accounts_raw AS SELECT * FROM (VALUES (' ACC001 ', '  Company A  ', ' tech ', 'USA'), ('ACC002', 'Company B', '  finance  ', 'UK')) AS t(account_id, account_name, industry, region);")
    df = con.execute('SELECT * FROM bronze.accounts_raw').df()
    
    # Apply transformations
    result = transform(df)
    
    # Persist Data
    # Ensure the silver schema exists
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute('CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM result')
    
    con.close()

if __name__ == '__main__':
    main()