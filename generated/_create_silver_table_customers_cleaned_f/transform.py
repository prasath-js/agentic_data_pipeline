import pandas as pd
import duckdb

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
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id (ascending) and join_date (descending) to ensure
    # the latest join_date is kept when duplicates are dropped.
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        df = df.drop_duplicates(subset='customer_id', keep='first')
    elif 'customer_id' in df.columns: # If join_date is missing, just drop by customer_id
        df = df.drop_duplicates(subset='customer_id', keep='first')


    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        # Use na=False to treat NaN emails as invalid
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        # If email column doesn't exist, all emails are considered invalid
        df['email_is_valid'] = False

    return df

def main():
    DB_PATH = 'data/warehouse.duckdb' # Define DB_PATH inside main as per "No code at module level" rule

    con = duckdb.connect(DB_PATH)

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