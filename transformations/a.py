import duckdb
import pandas as pd
import os
import time

def run_transformation(con):
    """
    Performs the transformation to create the gold.monthly_sales_summary table.
    Accepts an active DuckDB connection.
    """
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Load data from silver.transactions_cleaned
    try:
        transactions_df = con.execute("SELECT * FROM silver.transactions_cleaned").df()
    except duckdb.CatalogException:
        print("Error: silver.transactions_cleaned table not found. Please ensure the silver layer is processed.")
        return

    # Initialize a DataFrame for rejected rows
    # It should contain all original columns from transactions_df plus 'rejection_reason'
    rejected_cols = transactions_df.columns.tolist() + ['rejection_reason']
    rejected_rows_df = pd.DataFrame(columns=rejected_cols)

    # --- Date Parsing and Year-Month Extraction ---
    # Create a copy to avoid SettingWithCopyWarning
    transactions_processed_df = transactions_df.copy()

    # Attempt to parse transaction_date
    transactions_processed_df['parsed_transaction_date'] = pd.to_datetime(
        transactions_processed_df['transaction_date'], errors='coerce'
    )

    # Extract year_month
    transactions_processed_df['year_month'] = transactions_processed_df['parsed_transaction_date'].dt.strftime('%Y-%m')

    # --- Rejection Handling for null year_month ---
    null_year_month_mask = transactions_processed_df['year_month'].isnull()
    if null_year_month_mask.any():
        rejected_null_year_month = transactions_processed_df[null_year_month_mask].copy()
        rejected_null_year_month['rejection_reason'] = 'transaction_date could not be parsed or resulted in null year_month'
        
        # Select only the columns that should go into the rejected_rows table
        # This implicitly drops 'parsed_transaction_date' and 'year_month' from rejected rows
        rejected_rows_df = pd.concat([rejected_rows_df, rejected_null_year_month[rejected_cols]], ignore_index=True)
        
        transactions_processed_df = transactions_processed_df[~null_year_month_mask]

    # --- Handle invalid amounts (coercing to NaN and dropping) ---
    # Ensure 'amount' is numeric before summing. Rows with non-numeric amounts
    # will become NaN and then be dropped from aggregation, but not explicitly
    # added to rejected.rejected_rows as per the prompt's specific rejection criteria.
    transactions_processed_df['amount'] = pd.to_numeric(transactions_processed_df['amount'], errors='coerce')
    transactions_processed_df.dropna(subset=['amount'], inplace=True)

    # --- Aggregation ---
    monthly_summary_df = transactions_processed_df.groupby('year_month').agg(
        monthly_revenue=('amount', 'sum'),
        transaction_count=('transaction_id', 'count')
    ).reset_index()

    # --- Sort by year_month ---
    monthly_summary_df = monthly_summary_df.sort_values(by='year_month', ascending=True)

    # --- Compute MoM Growth ---
    # Calculate percentage change, then multiply by 100 for percentage format
    monthly_summary_df['mom_growth'] = monthly_summary_df['monthly_revenue'].pct_change() * 100
    # For the first month, mom_growth will be NaN, which is correct.

    # --- Write to Gold Layer ---
    con.execute("CREATE OR REPLACE TABLE gold.monthly_sales_summary AS SELECT * FROM monthly_summary_df")

    # --- Write Rejected Rows ---
    if not rejected_rows_df.empty:
        # Check if rejected.rejected_rows table exists
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0]

        if table_exists == 0:
            # Create table if it doesn't exist
            con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_rows_df")
        else:
            # Append to existing table
            con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_rows_df")

def main():
    """
    Main function to connect to DuckDB and run the transformation.
    """
    con = duckdb.connect(DB_PATH)
    try:
        run_transformation(con)
    finally:
        con.close()
        # Small delay to help ensure file handle is released on Windows,
        # preventing "file in use" errors for subsequent connections.
        time.sleep(0.1) 

if __name__ == "__main__":
    main()