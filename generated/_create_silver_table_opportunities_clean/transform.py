import pandas as pd
import duckdb
import os

# Define DB_PATH at module level for main() to use.
# For a real pipeline, this might come from environment variables or a config file.
DB_PATH = 'my_pipeline.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a series of transformations to the opportunities DataFrame.

    - Strips whitespace from all string columns.
    - Drops rows where 'account_id' is null.
    - Drops rows where 'stage' is null.
    - Coerces the 'value' column to numeric, setting invalid parsing to NaN.
    """
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Drop rows where account_id is null
    df = df.dropna(subset=['account_id'])

    # Drop rows where stage is null
    df = df.dropna(subset=['stage'])

    # Coerce 'value' column to numeric (errors='coerce')
    # The prompt mentions 'opportunity_value (or value)', and the sample shows 'value'.
    # We'll use 'value' as per the sample.
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    return df

def main():
    # Connects to duckdb using DB_PATH
    con = duckdb.connect(DB_PATH)

    # Create dummy bronze data for demonstration if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("""
        CREATE OR REPLACE TABLE bronze.opportunities_raw AS SELECT * FROM (VALUES
            (' OPP001 ', ' ACC001 ', '100.50', '2024-01-01', ' Negotiation ', 'file1', 'ts1'),
            ('OPP002', 'ACC002', '200', '2024-01-02', 'Closed Won', 'file2', 'ts2'),
            ('OPP003', NULL, 'invalid', '2024-01-03', 'Prospecting', 'file3', 'ts3'),
            ('OPP004', 'ACC004', '300.75', '2024-01-04', NULL, 'file4', 'ts4'),
            ('OPP005', 'ACC005', NULL, '2024-01-05', 'Closed Lost', 'file5', 'ts5')
        ) AS t(opportunity_id, account_id, value, close_date, stage, _source_file, _ingest_ts);
    """)

    # reads source table into a DataFrame
    df = con.execute('SELECT * FROM bronze.opportunities_raw').df()

    # calls result = transform(df)
    result_df = transform(df)

    # writes result back to duckdb
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute('CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM result_df')

    # closes the connection
    con.close()
    print(f"Pipeline executed. Cleaned data written to {DB_PATH} in silver.opportunities_cleaned.")

if __name__ == '__main__':
    # Clean up previous DB file if it exists for a fresh run, useful for testing the main function.
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    main()