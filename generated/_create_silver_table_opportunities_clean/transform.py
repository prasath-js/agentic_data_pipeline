import pandas as pd
import duckdb

# Define DB_PATH as it's used in main()
# This path should point to your DuckDB database file.
DB_PATH = 'my_pipeline.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip()

    # Drop rows where account_id is null
    df = df.dropna(subset=['account_id'])

    # Drop rows where stage is null
    df = df.dropna(subset=['stage'])

    # Coerce opportunity_value (or value) column to numeric (errors='coerce')
    # Check for 'opportunity_value' first, then 'value'
    if 'opportunity_value' in df.columns:
        df['opportunity_value'] = pd.to_numeric(df['opportunity_value'], errors='coerce')
    elif 'value' in df.columns:
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Load BRONZE data
    # This assumes 'bronze.opportunities_raw' table exists in the DuckDB database.
    df = con.execute('SELECT * FROM bronze.opportunities_raw').df()

    # Apply transformations
    result = transform(df)

    # Write SILVER table
    # Ensure the silver schema exists
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute('CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()