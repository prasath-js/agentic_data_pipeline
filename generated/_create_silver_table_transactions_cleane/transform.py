import pandas as pd
import duckdb

# Define DB_PATH at the module level for main()
DB_PATH = 'data/pipeline.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()

    # Ensure transaction_id column exists and drop rows where it is null
    if 'transaction_id' not in df.columns:
        # If transaction_id doesn't exist, we can't proceed with the requirement
        # For this exercise, we'll assume it exists based on the sample data.
        # In a real scenario, you might raise an error or create a dummy column.
        pass 
    else:
        df = df.dropna(subset=['transaction_id'])

    # Coerce amount column to numeric (errors='coerce')
    # Drop rows where amount is null or amount <= 0
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'].astype(str).str.replace('$', '', regex=False), errors='coerce')
        df = df.dropna(subset=['amount'])
        df = df[df['amount'] > 0]

    # Parse transaction_date to datetime if the column exists
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Load transactions_raw from BRONZE
    df = con.execute('SELECT * FROM bronze.transactions_raw').df()

    # Apply transformations
    result = transform(df)

    # Write the resulting pandas DataFrame to SILVER as transactions_cleaned
    con.execute('CREATE SCHEMA IF NOT EXISTS silver')
    con.execute('CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()