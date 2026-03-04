import pandas as pd
import duckdb
import numpy as np # Required for np.nan and np.inf in pytest suite

# Define DB_PATH at the module level as it's a configuration constant
DB_PATH = 'data/warehouse.duckdb'

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

    # Read source table into a DataFrame
    df = con.execute('SELECT * FROM silver.transactions_cleaned').df()

    # Call result = transform(df)
    result = transform(df)

    # Write result back to duckdb
    # Register the DataFrame as a DuckDB view/table for writing
    con.execute('CREATE OR REPLACE TABLE gold.monthly_sales_summary AS SELECT * FROM result')

    # Close the connection
    con.close()

if __name__ == '__main__':
    main()