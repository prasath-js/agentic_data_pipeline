import pandas as pd
import duckdb
import numpy as np

DB_PATH = 'my_database.duckdb'

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
    con = duckdb.connect(DB_PATH)

    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

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
    """

    df_aggregated = con.execute(sql_query).df()

    df_aggregated['total_spent'] = df_aggregated['total_spent'].fillna(0)
    df_aggregated['transaction_count'] = df_aggregated['transaction_count'].fillna(0).astype(int)

    df_result = transform(df_aggregated)

    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM df_result")

    con.close()

if __name__ == '__main__':
    main()