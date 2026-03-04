import pandas as pd
import duckdb

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and standardizes account data from bronze.accounts_raw.

    The transformation steps include:
    1. Stripping leading/trailing whitespace from all string columns.
    2. Ensuring the 'account_id' column exists and dropping rows where it is null.
    3. Uppercasing the 'industry' column if it is present.
    4. Dropping duplicate rows based on the 'account_id' column, keeping the first occurrence.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw account data.

    Returns:
        pd.DataFrame: The transformed DataFrame, ready for the silver layer.
    """
    # Create a copy to ensure the original DataFrame is not modified
    df_cleaned = df.copy()

    # 1. Strip whitespace from all string columns
    # Identify columns with 'object' dtype, which typically indicates strings
    for col in df_cleaned.select_dtypes(include='object').columns:
        # Apply .str.strip() to remove whitespace.
        # This method handles NaN values gracefully by returning NaN.
        df_cleaned[col] = df_cleaned[col].str.strip()

    # 2. Ensure 'account_id' column exists and drop rows where it is null
    if 'account_id' not in df_cleaned.columns:
        # 'account_id' is a critical identifier; raise an error if it's missing.
        raise ValueError("DataFrame must contain an 'account_id' column.")
    
    # Drop rows where 'account_id' is null to ensure data integrity.
    df_cleaned = df_cleaned.dropna(subset=['account_id'])

    # 3. Uppercase the 'industry' column if it exists
    if 'industry' in df_cleaned.columns:
        # Convert 'industry' values to uppercase for standardization.
        # .str.upper() also handles NaN values gracefully.
        df_cleaned['industry'] = df_cleaned['industry'].str.upper()

    # 4. Drop duplicate rows by 'account_id', keeping the first occurrence
    # This ensures that each 'account_id' is unique in the silver table.
    df_cleaned = df_cleaned.drop_duplicates(subset=['account_id'], keep='first')

    return df_cleaned

def main():
    # DB_PATH is expected to be pre-injected into the execution environment.
    con = duckdb.connect(DB_PATH)

    # Ensure the necessary schemas exist in the DuckDB database
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # For demonstration and testing purposes, create a dummy bronze.accounts_raw table.
    # In a real pipeline, this table would be populated by an upstream BRONZE layer process.
    con.execute("""
        CREATE OR REPLACE TABLE bronze.accounts_raw AS SELECT * FROM (VALUES
            (' ACC003 ', ' FinGroup-3 ', ' Tech ', ' USA ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (' ACC001 ', ' HealthCo-2 ', NULL, ' UK ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (' ACC003 ', ' HealthCo-2 ', ' Healthcare ', ' Germany ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (' ACC001 ', ' HealthCo-2 ', ' Tech ', ' Germany ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (' ACC002 ', ' Global Corporation ', ' Tech ', ' USA ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (NULL, 'Invalid Account', 'Finance', 'Canada', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00'),
            (' ACC004 ', ' Another Co ', ' Retail ', ' France ', 'accounts.csv', '2026-03-04T07:35:48.604685+00:00')
        ) AS t(account_id, account_name, industry, region, _source_file, _ingest_ts);
    """)

    # Load the raw data from the bronze layer into a pandas DataFrame
    df_raw = con.execute('SELECT * FROM bronze.accounts_raw').df()

    # Apply the defined pandas transformations
    df_cleaned = transform(df_raw)

    # Write the transformed DataFrame to the silver layer as accounts_cleaned
    # DuckDB can directly create tables from pandas DataFrames.
    con.execute('CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM df_cleaned')

    # Close the DuckDB connection
    con.close()

if __name__ == '__main__':
    main()