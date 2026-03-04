import pandas as pd
import duckdb
import numpy as np
import os
from pandas.testing import assert_frame_equal

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates spending tier based on total_spent.

    Args:
        df (pd.DataFrame): DataFrame with customer_id, total_spent,
                           transaction_count, and average_transaction_value.

    Returns:
        pd.DataFrame: The input DataFrame with an added 'spending_tier' column.
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
    Main function to connect to DuckDB, perform SQL transformations,
    apply pandas transformations, and write the result back to DuckDB.
    """
    # Define DB_PATH for execution. In a real scenario, this might be injected.
    DB_PATH = "temp_pipeline.duckdb"
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)

    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    # --- Setup dummy silver tables for demonstration/execution ---
    customers_data = {
        'customer_id': [1, 2, 3, 4, 5, 6],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com', 'frank@example.com']
    }
    transactions_data = {
        'transaction_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
        'customer_id': [1, 1, 2, 3, 3, 3, 4, 5, 1, 2, 4],
        'amount': [100.50, 250.00, 50.75, 1200.00, 300.00, 5000.00, 150.25, 10.00, 10000.00, 500.00, 2000.00]
    }
    df_customers_cleaned = pd.DataFrame(customers_data)
    df_transactions_cleaned = pd.DataFrame(transactions_data)

    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM df_customers_cleaned;")
    con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM df_transactions_cleaned;")
    # --- End: Setup dummy silver tables ---

    sql_query = """
    SELECT
        c.customer_id,
        c.name,
        c.email,
        SUM(t.amount) AS total_spent,
        COUNT(t.transaction_id) AS transaction_count,
        SUM(t.amount) / COUNT(t.transaction_id) AS average_transaction_value
    FROM silver.customers_cleaned AS c
    LEFT JOIN silver.transactions_cleaned AS t
        ON c.customer_id = t.customer_id
    GROUP BY
        c.customer_id,
        c.name,
        c.email
    ORDER BY
        c.customer_id
    """

    df_aggregated = con.execute(sql_query).df()

    df_aggregated['total_spent'] = df_aggregated['total_spent'].fillna(0)
    df_aggregated['transaction_count'] = df_aggregated['transaction_count'].fillna(0).astype(int)

    result = transform(df_aggregated)

    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM result")

    con.close()

if __name__ == '__main__':
    main()