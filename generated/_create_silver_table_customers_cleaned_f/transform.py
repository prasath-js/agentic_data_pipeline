import pandas as pd
import duckdb

# Define DB_PATH at the module level for the main function
DB_PATH = 'data.duckdb' # Placeholder: Adjust as needed for your environment

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Clean Strings: Identify all string columns and apply .str.strip()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip() # Ensure column is string type before stripping

    # Parse Date: If join_date column exists, convert it to datetime
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # Handle Duplicates: Sort by customer_id (ascending) and join_date (descending),
    # then drop duplicates on customer_id keeping the first occurrence (latest join_date)
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        df = df.drop_duplicates(subset='customer_id', keep='first')

    # Validate Email: Create a new boolean column email_is_valid
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].astype(str).str.contains('@', na=False)
    else:
        df['email_is_valid'] = False # Default to False if email column is missing

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure the bronze and silver schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # Create a dummy bronze.customers_raw table for demonstration if it doesn't exist
    # In a real pipeline, this would be populated by an upstream process
    con.execute("""
        CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM (VALUES
            (101, '  Alice  ', 'alice@example.com', '2023-03-10'),
            (102, '  Bob  ', 'bob@example.com', '2023-03-05'),
            (101, ' Alice ', 'invalid-alice', '2023-03-15'), -- Duplicate for 101, later date
            (103, ' Charlie ', 'charlie@test.com', '2023-03-01'),
            (102, ' Bob ', 'bob.dup@example.com', '2023-03-07') -- Duplicate for 102, later date
        ) AS t(customer_id, name, email, join_date);
    """)

    # Load Data
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Apply transformations
    result = transform(df)

    # Write to SILVER
    # DuckDB can directly create a table from a pandas DataFrame
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()