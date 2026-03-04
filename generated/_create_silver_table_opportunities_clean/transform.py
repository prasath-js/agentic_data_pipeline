import pandas as pd
import duckdb

DB_PATH = "data.duckdb" # Define DB_PATH for duckdb connection

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].str.strip()

    # Drop rows where account_id is null
    df = df.dropna(subset=['account_id'])

    # Drop rows where stage is null
    df = df.dropna(subset=['stage'])

    # Coerce opportunity_value (or value) column to numeric (errors='coerce')
    # Check for both 'opportunity_value' and 'value'
    if 'opportunity_value' in df.columns:
        df['opportunity_value'] = pd.to_numeric(df['opportunity_value'], errors='coerce')
    elif 'value' in df.columns:
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    return df

def main():
    con = duckdb.connect(DB_PATH)
    
    # Load data from BRONZE
    df = con.execute('SELECT * FROM bronze.opportunities_raw').df()
    
    # Apply transformations
    result = transform(df)
    
    # Write result to SILVER
    con.execute('CREATE SCHEMA IF NOT EXISTS silver')
    con.execute('CREATE OR REPLACE TABLE silver.opportunities_cleaned AS SELECT * FROM result')
    
    con.close()

if __name__ == '__main__':
    main()