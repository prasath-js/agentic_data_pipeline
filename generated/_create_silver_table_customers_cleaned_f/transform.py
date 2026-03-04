import pandas as pd
import duckdb

# Define DB_PATH for the main function
DB_PATH = 'data/pipeline.duckdb' # Or any other suitable path for your DuckDB database

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Ensure customer_id is not null before dropping duplicates
    df = df.dropna(subset=['customer_id'])

    if 'join_date' in df.columns:
        # Sort by customer_id (ascending) and join_date (descending)
        # This ensures that when duplicates are dropped, the one with the latest join_date is kept
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
    else:
        # If join_date doesn't exist, just sort by customer_id
        df = df.sort_values(by=['customer_id'], ascending=[True])

    df = df.drop_duplicates(subset='customer_id', keep='first')

    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        df['email_is_valid'] = False # Default to False if email column is missing

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure the 'bronze' schema exists and create a dummy table for demonstration
    con.execute('CREATE SCHEMA IF NOT EXISTS bronze')
    con.execute('''
        CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM (VALUES
            (1.0, ' John Doe ', 'john@example.com ', '123 Main St', '2023-01-01'),
            (2.0, 'Jane Smith', 'invalid-email', '456 Oak Ave ', '2023-01-02'),
            (1.0, '  John Doe Old ', 'john_old@example.com ', '123 Main St Old', '2022-12-31'),
            (3.0, '  Bob  ', 'bob@test.com', '789 Pine Ln', '2023-01-03')
        ) AS t(customer_id, name, email, address, join_date);
    ''')

    # Load data from BRONZE
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Apply transformations
    result = transform(df)

    # Write result to SILVER
    con.execute('CREATE SCHEMA IF NOT EXISTS silver')
    # DuckDB can directly ingest pandas DataFrame
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()