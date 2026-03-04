import pandas as pd
import duckdb

DB_PATH = 'data/pipeline.duckdb' # Define DB_PATH at the top

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'customer_id' in df.columns:
        if 'join_date' in df.columns:
            # Sort by customer_id (ascending) and join_date (descending)
            # This ensures that for each customer_id, the row with the latest join_date comes first
            df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        # Drop duplicates on customer_id, keeping the first (which is the latest join_date if sorted)
        df = df.drop_duplicates(subset=['customer_id'], keep='first')

    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        # If 'email' column is missing, all emails are considered invalid or unknown
        df['email_is_valid'] = False

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Load data from BRONZE customers_raw
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Apply transformations
    result = transform(df)

    # Write result to SILVER as customers_cleaned
    con.execute('CREATE SCHEMA IF NOT EXISTS silver;')
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()