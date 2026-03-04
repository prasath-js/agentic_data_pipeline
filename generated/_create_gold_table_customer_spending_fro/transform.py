import pandas as pd
import duckdb
import numpy as np
import pytest

# Define DB_PATH - this is a placeholder, adjust as needed for your environment
DB_PATH = "data/my_database.duckdb"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms customer and transaction data to calculate spending metrics and tiers.

    Args:
        df (pd.DataFrame): A DataFrame containing customer details, total_spent,
                           and transaction_count, typically from a join of
                           customers and transactions tables.

    Returns:
        pd.DataFrame: The transformed DataFrame with average_transaction_value
                      and spending_tier columns added.
    """
    # Calculate average_transaction_value, handling potential division by zero
    # If transaction_count is 0, average_transaction_value should be 0 or NaN
    df['average_transaction_value'] = df['total_spent'] / df['transaction_count']
    df['average_transaction_value'] = df['average_transaction_value'].replace([np.inf, -np.inf], np.nan).fillna(0)

    # Assign spending_tier based on total_spent
    conditions = [
        df['total_spent'] > 10000,
        df['total_spent'] > 1000
    ]
    choices = ['High', 'Medium']
    df['spending_tier'] = np.select(conditions, choices, default='Low')

    return df

def main():
    """
    Main function to connect to DuckDB, load data, perform transformations,
    and write the result back to DuckDB.
    """
    con = duckdb.connect(DB_PATH)

    # SQL query to join customers and transactions, aggregate spending,
    # and count transactions per customer.
    # It also handles the conversion of 'amount' from string to double.
    sql_query = """
    SELECT
        c.customer_id,
        c.name,
        c.email,
        c.address,
        c.join_date,
        SUM(CAST(REPLACE(t.amount, '$', '') AS DOUBLE)) AS total_spent,
        COUNT(t.transaction_id) AS transaction_count
    FROM
        silver.customers_cleaned AS c
    LEFT JOIN
        silver.transactions_cleaned AS t
        ON c.customer_id = t.customer_id
    GROUP BY
        c.customer_id,
        c.name,
        c.email,
        c.address,
        c.join_date
    """

    # Execute the SQL query and fetch the result into a pandas DataFrame
    initial_df = con.execute(sql_query).df()

    # Handle customers with no transactions (LEFT JOIN will result in NULLs for transaction aggregates)
    # For total_spent, SUM(NULL) is NULL, which becomes 0.0 in pandas.
    # For transaction_count, COUNT(t.transaction_id) will be 0 for customers with no transactions.
    initial_df['total_spent'] = initial_df['total_spent'].fillna(0)
    initial_df['transaction_count'] = initial_df['transaction_count'].fillna(0).astype(int)

    # Apply the pandas-based transformation
    result_df = transform(initial_df)

    # Create or replace the GOLD table in DuckDB
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM result_df")

    # Close the DuckDB connection
    con.close()

if __name__ == '__main__':
    main()