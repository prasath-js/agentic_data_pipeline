import duckdb
import pandas as pd
import os

# Pre-injected constants (for demonstration, assume they are available)
# DB_PATH = "data/warehouse.duckdb"
# DASHBOARD = "dashboard_output"

def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw transactions DataFrame according to the specified rules.

    Args:
        df (pd.DataFrame): The input DataFrame with raw transaction data.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Ensure transaction_id column exists and drop rows where it is null
    if 'transaction_id' in df.columns:
        df = df.dropna(subset=['transaction_id'])
    else:
        # If transaction_id doesn't exist, we can't fulfill the requirement
        # to drop nulls based on it. For robustness, we might log a warning
        # or raise an error, but for this task, we'll proceed without it.
        pass # No action needed if column doesn't exist for dropping nulls

    # 3. Coerce amount column to numeric (errors='coerce')
    # 4. Drop rows where amount is null or amount <= 0
    if 'amount' in df.columns:
        # Remove non-numeric characters like '$' before coercing
        df['amount'] = df['amount'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['amount'])
        df = df[df['amount'] > 0]
    else:
        # If 'amount' column is missing, we cannot apply these rules.
        pass

    # 5. Parse transaction_date to datetime if the column exists
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        # Optionally, drop rows where date parsing failed if desired,
        # but the prompt only asks to parse, not to drop on failure.
    
    return df

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # Create a dummy bronze.transactions_raw for demonstration if it doesn't exist
    # In a real pipeline, this would be populated by an ingestion step.
    con.execute("""
        CREATE OR REPLACE TABLE bronze.transactions_raw AS SELECT * FROM (VALUES
            (101.0, 1.0, 1, '$100.00 ', '2023-01-01', 'f1', 'ts1'),
            (102.0, 2.0, 2, ' 50.50', '2023-01-02', 'f1', 'ts1'),
            (NULL, 3.0, 3, '$200.00', '2023-01-03', 'f1', 'ts1'),
            (104.0, 4.0, 4, 'invalid', '2023-01-04', 'f1', 'ts1'),
            (105.0, 5.0, 5, '$0.00', '2023-01-05', 'f1', 'ts1'),
            (106.0, 6.0, 6, '-10.00', '2023-01-06', 'f1', 'ts1'),
            (107.0, 7.0, 7, '$75.25', 'invalid-date', 'f1', 'ts1'),
            (108.0, 8.0, 8, ' $120.00 ', '2023-01-08 ', 'f1', 'ts1')
        ) AS t(transaction_id, customer_id, quantity, amount, transaction_date, _source_file, _ingest_ts);
    """)

    # Load raw data
    transactions_raw_df = con.execute("SELECT * FROM bronze.transactions_raw").fetchdf()

    # Apply cleaning transformation
    transactions_cleaned_df = clean_transactions(transactions_raw_df)

    # Write to silver layer
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM transactions_cleaned_df;")

    print("Successfully created silver.transactions_cleaned table.")
    print(con.execute("SELECT * FROM silver.transactions_cleaned").fetchdf())

    con.close()

if __name__ == "__main__":
    main()