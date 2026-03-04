import pandas as pd
import duckdb
import numpy as np

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the spending tier for customers based on their total_spent.

    Args:
        df (pd.DataFrame): A DataFrame containing customer spending data,
                           including a 'total_spent' column.

    Returns:
        pd.DataFrame: The input DataFrame with an additional 'spending_tier' column.
    """
    conditions = [
        df['total_spent'] > 10000,
        df['total_spent'] > 1000
    ]
    choices = ['High', 'Medium']
    df['spending_tier'] = np.select(conditions, choices, default='Low')
    return df

def main():
    """
    Orchestrates the data loading, transformation, and storage for customer spending.
    """
    DB_PATH = "data/warehouse.duckdb" # Define DB_PATH for the main function
    con = duckdb.connect(DB_PATH)

    # 1. Load SILVER tables into pandas DataFrames
    # Ensure these tables exist in your DuckDB instance for the pipeline to run
    # For demonstration, we might need to create dummy tables if they don't exist
    # For the purpose of this refactoring, we assume they exist.
    customers_df = con.execute("SELECT * FROM silver.customers_cleaned").df()
    transactions_df = con.execute("SELECT * FROM silver.transactions_cleaned").df()

    # Register DataFrames with DuckDB for SQL operations
    con.register("customers_cleaned_df", customers_df)
    con.register("transactions_cleaned_df", transactions_df)

    # 2. Execute a DuckDB SQL query for joining and aggregation
    sql_query = """
    SELECT
        c.customer_id,
        c.name,
        c.email,
        c.address,
        c.join_date,
        COALESCE(SUM(t.amount), 0) AS total_spent,
        COUNT(t.transaction_id) AS transaction_count,
        COALESCE(SUM(t.amount), 0) / NULLIF(COUNT(t.transaction_id), 0) AS average_transaction_value
    FROM
        customers_cleaned_df AS c
    LEFT JOIN
        transactions_cleaned_df AS t
    ON
        c.customer_id = t.customer_id
    GROUP BY
        c.customer_id, c.name, c.email, c.address, c.join_date
    """
    df_aggregated = con.execute(sql_query).df()

    # 3. Apply pandas-based transformations (spending_tier calculation)
    result_df = transform(df_aggregated)

    # 4. Write the final DataFrame to the GOLD layer
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    # Register the result_df with DuckDB to write it
    con.register("result_df", result_df)
    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM result_df")

    con.close()

if __name__ == '__main__':
    main()