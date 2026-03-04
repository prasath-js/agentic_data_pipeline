import pandas as pd
import duckdb

DB_PATH = 'my_pipeline.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw customer data by cleaning strings, parsing dates,
    handling duplicates, and validating emails.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw customer data.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include='object').columns:
        # Apply strip directly; .str.strip() handles NaN values by returning NaN
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'customer_id' in df.columns:
        # If join_date exists, sort by it to keep the latest. Otherwise, just drop duplicates.
        if 'join_date' in df.columns:
            # Sort by customer_id (ascending) and join_date (descending)
            # NaNs in customer_id are treated as distinct by drop_duplicates.
            # NaNs/NaT in join_date will be sorted to the end by default (ascending=False, na_position='last').
            # This ensures that if multiple rows for a customer_id exist, the one with the latest
            # (non-NaT) join_date is kept. If all are NaT, the first encountered after sorting is kept.
            df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False], na_position='last')
            df = df.drop_duplicates(subset='customer_id', keep='first')
        else:
            # If 'join_date' is not present, just drop duplicates based on 'customer_id'
            df = df.drop_duplicates(subset='customer_id', keep='first')
    # If 'customer_id' column is missing, no deduplication can be performed based on it.

    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        # Use na=False to treat NaN emails as invalid
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        # If email column doesn't exist, create it with all False values and bool dtype
        df['email_is_valid'] = pd.Series(False, index=df.index, dtype=bool)

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Create dummy data for bronze.customers_raw if it doesn't exist
    # This makes the main() function runnable for testing purposes.
    con.execute('CREATE SCHEMA IF NOT EXISTS bronze;')
    dummy_raw_data = pd.DataFrame({
        'customer_id': [1.0, 2.0, 1.0],
        'name': ['  Test User  ', 'Another User', 'Test User Old'],
        'email': ['test@example.com', 'another@example.com', 'old@example.com'],
        'address': ['123 Main', '456 Oak', '123 Main'],
        'join_date': ['2023-01-01', '2023-02-01', '2022-12-01'],
        '_source_file': ['f1', 'f1', 'f1'],
        '_ingest_ts': ['ts1', 'ts1', 'ts1']
    })
    con.register('dummy_raw_data', dummy_raw_data)
    con.execute('CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM dummy_raw_data')

    # Load data from bronze.customers_raw
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Transform the DataFrame
    result_df = transform(df)

    # Write the result to silver.customers_cleaned
    con.execute('CREATE SCHEMA IF NOT EXISTS silver;')
    # Register the DataFrame for DuckDB to use in SQL
    con.register('result_df', result_df)
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result_df')

    con.close()

if __name__ == '__main__':
    main()