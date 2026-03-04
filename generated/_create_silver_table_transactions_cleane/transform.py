import pandas as pd
import duckdb

DB_PATH = 'my_database.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip()

    # Ensure transaction_id column exists and drop rows where it is null
    if 'transaction_id' not in df.columns:
        # If transaction_id doesn't exist, create it as NaN and then drop all rows
        # This ensures the subsequent dropna works as intended for the requirement
        df['transaction_id'] = pd.NA
    df = df.dropna(subset=['transaction_id'])

    # Coerce amount column to numeric (errors='coerce')
    # First, remove any non-numeric characters like '$'
    if 'amount' in df.columns:
        df['amount'] = df['amount'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        # Drop rows where amount is null or amount <= 0
        df = df.dropna(subset=['amount'])
        df = df[df['amount'] > 0]

    # Parse transaction_date to datetime if the column exists
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Load transactions_raw from BRONZE into a pandas DataFrame
    df = con.execute('SELECT * FROM bronze.transactions_raw').df()

    # Apply transformations
    result = transform(df)

    # Write the resulting pandas DataFrame to SILVER as transactions_cleaned
    con.execute('CREATE SCHEMA IF NOT EXISTS silver')
    con.execute('CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()