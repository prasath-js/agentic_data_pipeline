import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    # 1. Load Silver Tables
    try:
        accounts_df = con.execute("SELECT * FROM silver.accounts_cleaned").df()
        opportunities_df = con.execute("SELECT * FROM silver.opportunities_cleaned").df()
    except duckdb.CatalogException as e:
        print(f"Error loading silver tables: {e}")
        print("Please ensure silver.accounts_cleaned and silver.opportunities_cleaned exist.")
        con.close()
        return

    # Ensure account_id is string type and handle potential 'nan' string values
    if 'account_id' in accounts_df.columns:
        accounts_df['account_id'] = accounts_df['account_id'].astype(str).replace('nan', None)
    if 'account_id' in opportunities_df.columns:
        opportunities_df['account_id'] = opportunities_df['account_id'].astype(str).replace('nan', None)

    # 2. Rejection Handling: Null account_id in accounts_cleaned
    rejected_accounts_df = accounts_df[accounts_df['account_id'].isnull()].copy()
    if not rejected_accounts_df.empty:
        rejected_accounts_df['rejection_reason'] = 'account_id is null'
        
        # Register the rejected DataFrame as a temporary table
        con.register("temp_rejected_accounts_df", rejected_accounts_df)
        
        # Create the rejected.rejected_rows table if it doesn't exist, inferring schema from the rejected_accounts_df
        # Use WHERE 1=0 to create table with schema but no data, then insert
        con.execute("CREATE TABLE IF NOT EXISTS rejected.rejected_rows AS SELECT * FROM temp_rejected_accounts_df WHERE 1=0;")
        
        # Insert rejected rows into the table
        con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM temp_rejected_accounts_df;")
        
        # Unregister the temporary view
        con.unregister("temp_rejected_accounts_df")
        
        print(f"Rejected {len(rejected_accounts_df)} rows from accounts_cleaned due to null account_id.")

    # Filter out rejected rows from accounts_df for further processing
    accounts_cleaned_df = accounts_df[accounts_df['account_id'].notnull()]

    # 3. Register DataFrames with DuckDB
    con.register("accounts_cleaned_df", accounts_cleaned_df)
    con.register("opportunities_df", opportunities_df)

    # 4. SQL Transformation
    # Left join opportunities onto accounts, aggregate, compute win_rate, and rank
    account_performance_query = """
    WITH AccountOpportunities AS (
        SELECT
            acc.account_id,
            acc.account_name,
            acc.industry,
            acc.region,
            opp.opportunity_id,
            opp.value AS opportunity_value,
            opp.stage
        FROM
            accounts_cleaned_df AS acc
        LEFT JOIN
            opportunities_df AS opp
            ON acc.account_id = opp.account_id
    ),
    AggregatedPerformance AS (
        SELECT
            account_id,
            account_name,
            industry,
            region,
            SUM(COALESCE(opportunity_value, 0)) AS pipeline_value,
            COUNT(opportunity_id) AS opportunity_count,
            CAST(SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(opportunity_id), 0) AS win_rate
        FROM
            AccountOpportunities
        GROUP BY
            account_id,
            account_name,
            industry,
            region
    )
    SELECT
        account_id,
        account_name,
        industry,
        region,
        pipeline_value,
        opportunity_count,
        COALESCE(win_rate, 0.0) AS win_rate, -- Replace NULL win_rate with 0.0 for accounts with no opportunities
        ROW_NUMBER() OVER (PARTITION BY industry ORDER BY pipeline_value DESC) AS rank_within_industry
    FROM
        AggregatedPerformance
    ORDER BY
        industry, pipeline_value DESC
    """

    account_performance_df = con.execute(account_performance_query).df()

    # Ensure correct data types for the output DataFrame
    if not account_performance_df.empty:
        account_performance_df['account_id'] = account_performance_df['account_id'].astype(str)
        account_performance_df['account_name'] = account_performance_df['account_name'].astype(str)
        account_performance_df['industry'] = account_performance_df['industry'].astype(str)
        account_performance_df['region'] = account_performance_df['region'].astype(str)
        account_performance_df['pipeline_value'] = account_performance_df['pipeline_value'].astype(float)
        account_performance_df['opportunity_count'] = account_performance_df['opportunity_count'].astype(int)
        account_performance_df['win_rate'] = account_performance_df['win_rate'].astype(float)
        account_performance_df['rank_within_industry'] = account_performance_df['rank_within_industry'].astype(int)


    # 5. Write result to GOLD as account_performance
    con.execute("CREATE OR REPLACE TABLE gold.account_performance AS SELECT * FROM account_performance_df;")
    print(f"Successfully created gold.account_performance with {len(account_performance_df)} rows.")

    # Unregister DataFrames
    con.unregister("accounts_cleaned_df")
    con.unregister("opportunities_df")

    # 6. Close connection
    con.close()

if __name__ == "__main__":
    main()