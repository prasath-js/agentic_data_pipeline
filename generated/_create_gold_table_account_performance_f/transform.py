import duckdb
import pandas as pd
import numpy as np

# Define DB_PATH at the module level for easy access
DB_PATH = 'data/pipeline.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a DataFrame containing joined account and opportunity data
    to calculate account performance metrics and rank.

    Args:
        df (pd.DataFrame): A DataFrame resulting from a left join of
                           accounts_cleaned and opportunities_cleaned.
                           Expected columns: 'account_id', 'account_name',
                           'industry', 'region', 'opportunity_id', 'value',
                           'stage'.

    Returns:
        pd.DataFrame: A DataFrame with account performance metrics including
                      pipeline_value, opportunity_count, win_rate, and
                      rank_in_industry.
    """
    # Rename 'value' column from opportunities to 'opportunity_value' for clarity
    df = df.rename(columns={'value': 'opportunity_value'})

    # Group by account details and aggregate performance metrics
    grouped = df.groupby(['account_id', 'account_name', 'industry', 'region']).agg(
        pipeline_value=('opportunity_value', 'sum'),
        opportunity_count=('opportunity_id', 'count'), # Count non-null opportunity_ids
        closed_won_count=('stage', lambda x: (x == 'Closed Won').sum())
    ).reset_index()

    # Calculate win_rate, handling division by zero
    grouped['win_rate'] = np.where(
        grouped['opportunity_count'] > 0,
        grouped['closed_won_count'] / grouped['opportunity_count'],
        0.0 # Assign 0.0 win rate if no opportunities
    )

    # Drop the intermediate closed_won_count column
    grouped = grouped.drop(columns=['closed_won_count'])

    # Rank accounts by pipeline_value descending within each industry
    # NaN industries will be treated as a separate group for ranking.
    grouped['rank_in_industry'] = grouped.groupby('industry')['pipeline_value'].rank(
        method='dense', ascending=False
    ).astype(int) # Convert rank to integer

    # Select and reorder columns for the final output
    output_df = grouped[[
        'account_id',
        'account_name',
        'industry',
        'region',
        'pipeline_value',
        'opportunity_count',
        'win_rate',
        'rank_in_industry'
    ]]

    return output_df

def main():
    """
    Main function to execute the data pipeline:
    1. Connects to DuckDB.
    2. Reads source tables.
    3. Joins data.
    4. Applies the transformation.
    5. Writes the transformed data back to DuckDB.
    6. Closes the connection.
    """
    con = duckdb.connect(DB_PATH)

    # Load silver tables into pandas DataFrames
    df_accounts = con.execute('SELECT account_id, account_name, industry, region FROM silver.accounts_cleaned').df()
    df_opportunities = con.execute('SELECT account_id, opportunity_id, value, stage FROM silver.opportunities_cleaned').df()

    # Perform the left join in pandas to prepare data for the transform function
    df_joined = pd.merge(df_accounts, df_opportunities, on='account_id', how='left')

    # Apply the transformation logic using the pure pandas function
    result_df = transform(df_joined)

    # Register the result DataFrame as a DuckDB table and write to GOLD layer
    con.execute('CREATE SCHEMA IF NOT EXISTS gold')
    con.execute('CREATE OR REPLACE TABLE gold.account_performance AS SELECT * FROM result_df')

    con.close()

if __name__ == '__main__':
    main()