import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # --- Transformation Logic ---
    # Assume silver.customers_cleaned and silver.transactions_cleaned are already populated.

    # 1. Left-join transactions onto customers and identify rejected rows
    # Use a CTE to perform the join and identify valid/rejected rows.
    
    # First, identify rejected rows
    # Rows are rejected if transaction.customer_id does not have a match in customers_cleaned
    # or if transaction.customer_id itself is NULL.
    rejected_query = """
    WITH joined_data AS (
        SELECT
            t.transaction_id,
            t.customer_id AS original_transaction_customer_id,
            t.amount,
            t.transaction_date,
            c.customer_id AS matched_customer_id -- This will be NULL if no match
        FROM silver.transactions_cleaned AS t
        LEFT JOIN silver.customers_cleaned AS c
            ON t.customer_id = c.customer_id
    )
    SELECT
        transaction_id,
        original_transaction_customer_id,
        amount,
        transaction_date,
        'No matching customer_id or customer_id is NULL' AS rejection_reason
    FROM joined_data
    WHERE matched_customer_id IS NULL;
    """
    rejected_df = con.execute(rejected_query).fetchdf()

    if not rejected_df.empty:
        # Check if rejected.rejected_rows table exists
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows';").fetchone()[0] > 0

        if not table_exists:
            # Create table if it doesn't exist. Ensure types match expected input.
            con.execute("""
                CREATE TABLE rejected.rejected_rows (
                    transaction_id VARCHAR,
                    original_transaction_customer_id VARCHAR,
                    amount DOUBLE,
                    transaction_date DATE,
                    rejection_reason VARCHAR
                );
            """)
        
        # Insert or append rejected rows
        con.register("rejected_df_temp", rejected_df)
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df_temp;")
        con.unregister("rejected_df_temp")
    
    # 2. Process valid rows: aggregate, calculate, and assign spending tier
    gold_query = """
    WITH valid_transactions AS (
        SELECT
            c.customer_id,
            t.transaction_id,
            t.amount
        FROM silver.customers_cleaned AS c
        JOIN silver.transactions_cleaned AS t
            ON c.customer_id = t.customer_id
    ),
    aggregated_spending AS (
        SELECT
            customer_id,
            SUM(amount) AS total_spent,
            COUNT(transaction_id) AS transaction_count,
            SUM(amount) / COUNT(transaction_id) AS average_transaction_value
        FROM valid_transactions
        GROUP BY customer_id
    )
    SELECT
        customer_id,
        total_spent,
        transaction_count,
        average_transaction_value,
        CASE
            WHEN total_spent > 10000 THEN 'High'
            WHEN total_spent > 1000 THEN 'Medium'
            ELSE 'Low'
        END AS spending_tier
    FROM aggregated_spending;
    """
    
    customer_spending_df = con.execute(gold_query).fetchdf()

    # 3. Write result to GOLD as customer_spending
    con.execute("CREATE OR REPLACE TABLE gold.customer_spending AS SELECT * FROM customer_spending_df;")

    con.close()

if __name__ == "__main__":
    main()