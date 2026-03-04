import pandas as pd
import duckdb
import numpy as np

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # ALL transformation logic here - pure pandas, no duckdb
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
    # If transaction_count is 0, average_transaction_value should be 0.
    # Using .replace([np.inf, -np.inf], np.nan).fillna(0) handles cases where total_spent is non-zero
    # but transaction_count is zero, resulting in inf, which is then converted to 0.
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
    # connects to duckdb using DB_PATH
    DB_PATH = './my_database.duckdb'
    con = duckdb.connect(DB_PATH)

    # reads source table into a DataFrame
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
    # For total_spent, SUM(NULL) is NULL, which pandas converts to NaN. Fill with 0.
    # For transaction_count, COUNT(t.transaction_id) will be 0 for customers with no transactions.
    initial_df['total_spent'] = initial_df['total_spent'].fillna(0)
    initial_df['transaction_count'] = initial_df['transaction_count'].fillna(0).astype(int)

    # calls result = transform(df)
    result_df = transform(initial_df)

    # writes result back to duckdb
    # Register the result DataFrame with DuckDB
    con.register('result_df_view', result_df)

    # Create or replace the GOLD table in DuckDB
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM result_df_view")

    # closes the connection
    con.close()

if __name__ == '__main__':
    main()