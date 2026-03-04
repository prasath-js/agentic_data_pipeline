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
    if df.empty:
        # Ensure 'spending_tier' column exists even for empty DataFrame
        df['spending_tier'] = pd.Series(dtype=str)
        return df

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
    # DB_PATH is a pre-injected constant in the execution environment.
    # For local testing, it's defined in the __main__ guard.
    con = duckdb.connect(DB_PATH)

    try:
        # 1. Load SILVER tables into pandas DataFrames
        # This assumes the 'silver' schema and its tables are accessible via DB_PATH
        df_customers = con.execute('SELECT * FROM silver.customers_cleaned').df()
        df_transactions = con.execute('SELECT * FROM silver.transactions_cleaned').df()

        # 2. Register both DataFrames with the DuckDB connection for the current session
        con.register('customers_cleaned_df', df_customers)
        con.register('transactions_cleaned_df', df_transactions)

        # 3. Execute a DuckDB SQL query to join, aggregate, and calculate average
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

        # 4. Apply pandas-based transformations (spending_tier calculation)
        result_df = transform(df_aggregated)

        # 5. Write the final DataFrame to the GOLD layer
        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        # Register the result_df to make it available for DuckDB SQL
        con.register('customer_spending_result_df', result_df)
        con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM customer_spending_result_df")

    finally:
        con.close()

if __name__ == '__main__':
    # Placeholder for DB_PATH for local execution.
    # In a real pipeline, this would be injected.
    DB_PATH = 'local_pipeline.duckdb'
    # Example: Create dummy silver tables for local testing if DB_PATH is new
    with duckdb.connect(DB_PATH) as con_test:
        con_test.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con_test.execute("""
            CREATE OR REPLACE TABLE silver.customers_cleaned (
                customer_id INTEGER,
                name VARCHAR,
                email VARCHAR,
                address VARCHAR,
                join_date DATE
            );
        """)
        con_test.execute("""
            INSERT INTO silver.customers_cleaned VALUES
            (1, 'Alice', 'alice@example.com', '123 Main St', '2021-01-01'),
            (2, 'Bob', 'bob@example.com', '456 Oak Ave', '2022-03-15'),
            (3, 'Charlie', 'charlie@example.com', '789 Pine Ln', '2023-07-20');
        """)
        con_test.execute("""
            CREATE OR REPLACE TABLE silver.transactions_cleaned (
                transaction_id INTEGER,
                customer_id INTEGER,
                amount DECIMAL(10, 2),
                transaction_date DATE
            );
        """)
        con_test.execute("""
            INSERT INTO silver.transactions_cleaned VALUES
            (101, 1, 12000.00, '2023-01-05'),
            (102, 1, 3000.00, '2023-02-10'),
            (103, 2, 500.00, '2023-03-20'),
            (104, 2, 700.00, '2023-04-25'),
            (105, 3, 100.00, '2023-08-01');
        """)
    main()
    # Optional: Verify output for local testing
    with duckdb.connect(DB_PATH) as con_test:
        print("\nGold table content after main() execution:")
        print(con_test.execute("SELECT * FROM gold.customer_spending").df())