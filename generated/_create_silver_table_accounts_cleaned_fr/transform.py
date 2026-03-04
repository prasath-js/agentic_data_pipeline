import pandas as pd
import duckdb

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the accounts DataFrame.

    - Strips whitespace from all string columns.
    - Ensures 'account_id' column exists and drops rows where it is null.
    - Uppercases the 'industry' column if it exists.
    - Drops duplicate rows by 'account_id', keeping the first occurrence.

    Args:
        df (pd.DataFrame): The input DataFrame from bronze.accounts_raw.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    # Create a copy to avoid modifying the original DataFrame passed to the function
    df_cleaned = df.copy()

    # Strip whitespace from all string columns
    for col in df_cleaned.select_dtypes(include=['object', 'string']).columns:
        df_cleaned[col] = df_cleaned[col].str.strip()

    # Ensure account_id column exists and drop rows where it is null
    if 'account_id' not in df_cleaned.columns:
        raise ValueError("Column 'account_id' is missing from the DataFrame.")
    df_cleaned = df_cleaned.dropna(subset=['account_id'])

    # Uppercase the industry column if it exists
    if 'industry' in df_cleaned.columns:
        df_cleaned['industry'] = df_cleaned['industry'].str.upper()

    # Drop duplicate rows by account_id, keeping the first occurrence
    df_cleaned = df_cleaned.drop_duplicates(subset=['account_id'], keep='first')

    return df_cleaned

def main():
    """
    Main function to orchestrate data loading, transformation, and writing.
    """
    DB_PATH = "my_database.duckdb" # Define DB_PATH here

    con = duckdb.connect(database=DB_PATH, read_only=False)

    # Create dummy data for bronze.accounts_raw if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("""
        CREATE OR REPLACE TABLE bronze.accounts_raw AS SELECT * FROM (VALUES
            (' ACC001 ', '  Company A  ', ' tech ', 'USA'),
            ('ACC002', 'Company B', 'finance', 'UK'),
            (NULL, 'Company C', 'healthcare', 'Germany'),
            ('ACC004', 'Company D', ' healthcare ', 'France'),
            ('ACC001', 'Company A-dup', 'IT', 'USA')
        ) AS t(account_id, account_name, industry, region);
    """)

    # Load data from bronze.accounts_raw
    df_raw = con.execute('SELECT * FROM bronze.accounts_raw').df()

    # Apply transformations
    df_cleaned = transform(df_raw)

    # Register the cleaned DataFrame and write to silver.accounts_cleaned
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute('CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM df_cleaned')

    con.close()

if __name__ == '__main__':
    main()