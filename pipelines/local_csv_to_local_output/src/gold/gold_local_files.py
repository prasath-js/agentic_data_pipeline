# src/gold/gold_local_files.py
import os
import pandas as pd
import logging
from typing import Dict, Any
from src.db_connection.builder import ConnectionBuilder
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def gold_local_files(silver_df: pd.DataFrame) -> None:
    """
    Aggregates the silver layer DataFrame and writes the result to a local file.

    Args:
        silver_df (pd.DataFrame): The cleaned and transformed DataFrame from the silver layer.
    """
    if silver_df.empty:
        logger.warning("Silver DataFrame is empty, skipping gold layer aggregation and write.")
        return

    settings = Settings()
    output_path = settings.GOLD_OUTPUT_PATH

    if not output_path:
        logger.error("Gold output path is not configured. Cannot write gold data.")
        return

    logger.info(f"Starting gold layer aggregation and write to {output_path}.")

    # --- Aggregation Example ---
    # Aggregate total amount and quantity by account_id and close_date
    # Ensure columns exist before aggregation
    columns_to_group = ['account_id', 'close_date']
    columns_to_aggregate = ['amount', 'quantity', 'value']

    # Filter for columns that actually exist in the DataFrame
    existing_group_cols = [col for col in columns_to_group if col in silver_df.columns]
    existing_agg_cols = [col for col in columns_to_aggregate if col in silver_df.columns]

    if not existing_group_cols:
        logger.warning("No valid grouping columns found for gold aggregation. Aggregating by all rows.")
        # If no grouping columns, just sum all relevant numeric columns
        if existing_agg_cols:
            gold_df = pd.DataFrame(silver_df[existing_agg_cols].sum()).T
            gold_df.columns = [f'total_{col}' for col in gold_df.columns]
        else:
            logger.warning("No numeric columns to aggregate. Gold output will be empty.")
            gold_df = pd.DataFrame()
    elif not existing_agg_cols:
        logger.warning("No valid aggregation columns found for gold aggregation. Gold output will contain only grouping columns.")
        gold_df = silver_df[existing_group_cols].drop_duplicates()
    else:
        gold_df = silver_df.groupby(existing_group_cols, as_index=False)[existing_agg_cols].sum()
        gold_df.rename(columns={col: f'total_{col}' for col in existing_agg_cols}, inplace=True)
        logger.info(f"Aggregated {len(silver_df)} rows into {len(gold_df)} gold records.")

    if gold_df.empty:
        logger.warning("Gold DataFrame is empty after aggregation, skipping write operation.")
        return

    # --- Write to output target ---
    try:
        # Use the ConnectionBuilder to get the appropriate connector
        # The connector type is 'local_files', and it uses the output_path
        connector = ConnectionBuilder.get_connector(
            connector_type="local_files",
            config={"path": output_path}
        )
        # Assuming the connector's write method can handle pandas to_csv arguments
        connector.write(df=gold_df, write_method="csv", index=False)
        logger.info(f"Successfully wrote {len(gold_df)} rows to gold output: {output_path}.")
    except Exception as e:
        logger.error(f"Failed to write gold data to {output_path}: {e}", exc_info=True)