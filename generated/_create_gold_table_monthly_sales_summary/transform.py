import pandas as pd
import duckdb
import numpy as np

# Define DB_PATH as a global constant for the main function
# In a real application, this would likely come from environment variables or a config file.
DB_PATH = 'my_database.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a DataFrame of transactions into a monthly sales summary.

    Args:
        df (pd.DataFrame): Input DataFrame with 'transaction_date' and 'amount' columns.

    Returns:
        pd.DataFrame: A DataFrame summarized by year_month, including monthly_revenue,
                      transaction_count, and mom_growth.
    """
    # 1. Parse transaction_date to datetime if not already parsed
    # The input df from silver.transactions_cleaned is expected to have transaction_date as datetime64[us],
    # but this step ensures robustness against different input types.
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

    # Drop rows where transaction_date could not be parsed (NaT values)
    df = df.dropna(subset=['transaction_date'])

    # If after dropping NaT, the DataFrame is empty, return an empty summary DataFrame
    if df.empty:
        # Define the expected columns and their dtypes for an empty DataFrame
        return pd.DataFrame(columns=['year_month', 'monthly_revenue', 'transaction_count', 'mom_growth']) \
                 .astype({'year_month': str, 'monthly_revenue': float, 'transaction_count': int, 'mom_growth': float})

    # 2. Extract year_month as a string (e.g. 'YYYY-MM')
    df['year_month'] = df['transaction_date'].dt.strftime('%Y-%m')

    # Group by year_month: monthly_revenue = sum(amount), transaction_count = count
    summary_df = df.groupby('year_month').agg(
        monthly_revenue=('amount', 'sum'),
        transaction_count=('transaction_id', 'count') # Counting non-null transaction_ids for transaction_count
    ).reset_index()

    # 3. Sort by year_month ascending
    summary_df = summary_df.sort_values('year_month').reset_index(drop=True)

    # Compute mom_growth = percentage change in monthly_revenue month-over-month
    # Multiply by 100 to express as a percentage
    summary_df['mom_growth'] = summary_df['monthly_revenue'].pct_change() * 100

    return summary_df

def main():
    """
    Main function to connect to DuckDB, load data, transform it, and write back.
    """
    # Connect to duckdb using DB_PATH
    con = duckdb.connect(DB_PATH)

    try:
        # Ensure the silver schema exists for reading
        con.execute('CREATE SCHEMA IF NOT EXISTS silver;')
        # Create a dummy table for demonstration if it doesn't exist
        # In a real scenario, this table would already be populated.
        con.execute("""
            CREATE TABLE IF NOT EXISTS silver.transactions_cleaned (
                transaction_id INTEGER,
                customer_id INTEGER,
                quantity INTEGER,
                amount DOUBLE,
                transaction_date TIMESTAMP
            );
        """)
        # Insert some dummy data if the table is empty for testing the main function
        # In a real pipeline, this would not be needed as data would be pre-existing.
        if con.execute("SELECT COUNT(*) FROM silver.transactions_cleaned").fetchone()[0] == 0:
            con.execute("""
                INSERT INTO silver.transactions_cleaned VALUES
                (1, 101, 1, 10.0, '2023-01-05'),
                (2, 102, 2, 20.0, '2023-01-10'),
                (3, 101, 1, 15.0, '2023-02-01'),
                (4, 103, 3, 30.0, '2023-02-15'),
                (5, 102, 2, 25.0, '2023-03-01'),
                (6, 104, 1, 5.0, '2023-03-10');
            """)

        # Read source table into a DataFrame
        df = con.execute('SELECT * FROM silver.transactions_cleaned').df()

        # Call result = transform(df)
        result = transform(df)

        # Write result back to duckdb
        # Register the DataFrame as a DuckDB temporary table for writing
        con.create_table('result_temp_table', result)
        con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
        con.execute('CREATE OR REPLACE TABLE gold.monthly_sales_summary AS SELECT * FROM result_temp_table')
        con.execute('DROP TABLE result_temp_table') # Clean up the temporary table

    finally:
        # Close the connection
        con.close()

if __name__ == '__main__':
    main()