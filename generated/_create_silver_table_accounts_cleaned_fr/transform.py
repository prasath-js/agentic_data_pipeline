import duckdb
import pandas as pd
import os

def transform(accounts_raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw accounts data.

    - Strips whitespace from all string columns.
    - Ensures 'account_id' column exists and drops rows where it is null.
    - Uppercases the 'industry' column if it exists.
    - Drops duplicate rows by 'account_id', keeping the first occurrence.
    """
    df = accounts_raw_df.copy()

    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].str.strip()

    # 2. Ensure account_id column exists and drop rows where it is null
    if 'account_id' not in df.columns:
        raise ValueError("The 'account_id' column is missing from the input DataFrame.")
    df.dropna(subset=['account_id'], inplace=True)

    # 3. Uppercase the industry column if it exists
    if 'industry' in df.columns:
        df['industry'] = df['industry'].str.upper()

    # 4. Drop duplicate rows by account_id, keeping the first occurrence
    df.drop_duplicates(subset=['account_id'], keep='first', inplace=True)

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Create schemas if they don't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # Load bronze.accounts_raw
    # In a real scenario, this would be populated by an ingestion process.
    # For demonstration, we'll create a dummy table if it doesn't exist.
    con.execute("""
        CREATE OR REPLACE TABLE bronze.accounts_raw AS SELECT * FROM (VALUES
            ('ACC003 ', ' FinGroup-3 ', ' Tech ', ' USA ', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00'),
            ('ACC001', ' HealthCo-2 ', NULL, ' UK ', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00'),
            ('ACC003', ' HealthCo-2 ', ' Healthcare ', ' Germany ', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00'),
            ('ACC001 ', ' HealthCo-2 ', ' Tech ', ' Germany ', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00'),
            ('ACC002', 'Global Corporation', 'Tech', 'USA', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00'),
            (NULL, 'Null Account', 'Other', 'Canada', 'accounts_1000.csv', '2026-03-03T18:31:49.720038+00:00')
        ) AS t(account_id, account_name, industry, region, _source_file, _ingest_ts);
    """)

    # 1. Load Data: Load bronze.accounts_raw into a pandas DataFrame
    accounts_raw_df = con.execute("SELECT * FROM bronze.accounts_raw").fetchdf()

    # Perform the transformation
    accounts_cleaned_df = transform(accounts_raw_df)

    # 6. Write to SILVER: Register the cleaned pandas DataFrame with DuckDB and write it to the silver.accounts_cleaned table.
    con.execute("CREATE OR REPLACE TABLE silver.accounts_cleaned AS SELECT * FROM accounts_cleaned_df;")

    print("Transformation complete. Data written to silver.accounts_cleaned.")
    print("Sample from silver.accounts_cleaned:")
    print(con.execute("SELECT * FROM silver.accounts_cleaned LIMIT 5").fetchdf())

    con.close()

if __name__ == "__main__":
    # Ensure DB_PATH and DASHBOARD are defined for standalone execution if needed
    if 'DB_PATH' not in locals():
        DB_PATH = "data/pipeline.duckdb"
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if 'DASHBOARD' not in locals():
        DASHBOARD = "dashboard_output"
        os.makedirs(DASHBOARD, exist_ok=True)
    main()