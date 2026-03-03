import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # Load data from silver.transactions_cleaned
    try:
        transactions_df = con.execute("SELECT * FROM silver.transactions_cleaned").df()
    except duckdb.CatalogException:
        print("Error: silver.transactions_cleaned table not found. Please ensure the silver layer is processed.")
        con.close()
        return

    # Initialize a DataFrame for rejected rows
    rejected_rows_df = pd.DataFrame(columns=transactions_df.columns.tolist() + ['rejection_reason'])

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
        rejected_rows_df = pd.concat([rejected_rows_df, rejected_null_year_month[rejected_rows_df.columns]], ignore_index=True)
        transactions_processed_df = transactions_processed_df[~null_year_month_mask]

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
    # For the first month, mom_growth should be NaN, which pct_change handles by default.

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

    con.close()

if __name__ == "__main__":
    main()