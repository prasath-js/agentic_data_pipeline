import pandas as pd
import logging
from typing import Dict, Any

from src.db_connection.builder import DBConnectorBuilder
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def generate_gold_data(silver_df: pd.DataFrame, settings: Settings) -> None:
    """
    Aggregates the silver DataFrame and writes the resulting gold data to a local file.

    Args:
        silver_df (pd.DataFrame): The transformed DataFrame from the silver layer.
        settings (Settings): Application settings containing configuration for gold output.

    Returns:
        None
    """
    logger.info("Starting Gold layer processing.")

    if silver_df.empty:
        logger.warning("Silver DataFrame is empty. Skipping Gold aggregation and output.")
        return

    # 1. Aggregate Silver data
    logger.info("Aggregating Silver data...")
    try:
        # For demonstration, let's aggregate by account and close date
        # Summing amount and quantity, and counting distinct opportunities
        gold_df = silver_df.groupby(['account_id', silver_df['close_date'].dt.to_period('M')]).agg(
            total_amount=('amount', 'sum'),
            total_quantity=('quantity', 'sum'),
            num_opportunities=('opportunity_id', 'nunique')
        ).reset_index()

        # Convert Period back to string for easier CSV output
        gold_df['close_date'] = gold_df['close_date'].astype(str)

        logger.info(f"Aggregation complete. Gold DataFrame has {len(gold_df)} rows.")
        logger.debug("Gold DataFrame head:\n%s", gold_df.head())

    except KeyError as e:
        logger.error(f"Missing expected column for aggregation: {e}. Gold aggregation aborted.")
        return
    except Exception as e:
        logger.error(f"An error occurred during Gold aggregation: {e}")
        return

    # 2. Write to output target
    output_config = {
        "type": "local_files",
        "path": settings.gold_settings.local_output_path,
        "format": "csv" # Or 'json', 'parquet' etc.
    }

    try:
        connector = DBConnectorBuilder.get_connector(output_config["type"])
        logger.info(f"Writing Gold data to local file: {output_config['path']}")
        connector.write(gold_df, output_config)
        logger.info("Gold data successfully written to output.")
    except Exception as e:
        logger.error(f"Failed to write Gold data to output target {output_config['path']}: {e}")
        raise

if __name__ == '__main__':
    # This block is for demonstration/testing purposes when running this file directly.
    from src.config.logging_config import setup_logging
    import os
    setup_logging()
    settings = Settings()

    # Create a dummy silver DataFrame for testing
    dummy_silver_data = {
        "opportunity_id": [1, 2, 3, 5, 6],
        "account_id": ["ACC001_masked", "ACC002_masked", "ACC001_masked", "ACC003_masked", "ACC001_masked"],
        "value": [1000, 2500, 1500, 500, 700],
        "close_date": pd.to_datetime(["2023-01-15", "2023-02-20", "2023-01-25", "2023-03-01", "2023-02-10"]),
        "stage": ["Closed Won", "Open", "Closed Lost", "Closed Won", "Open"],
        "transaction_id": ["T001", "T002", "T003", "T005", "T006"],
        "customer_id": ["CUST001_masked", "CUST002_masked", "CUST001_masked", "CUST003_masked", "CUST001_masked"],
        "quantity": [10, 5, 8, 2, 3],
        "amount": [950.50, 2400.00, 1400.75, 480.00, 650.00],
        "transaction_date": pd.to_datetime(["2023-01-10", "2023-02-18", "2023-01-22", "2023-04-01", "2023-02-05"])
    }
    silver_df_test = pd.DataFrame(dummy_silver_data)
    logger.info("Initial Silver DataFrame for Gold:\n%s", silver_df_test)

    # Ensure output directory exists for testing
    output_dir = os.path.dirname(settings.gold_settings.local_output_path)
    os.makedirs(output_dir, exist_ok=True)

    try:
        generate_gold_data(silver_df_test, settings)
        logger.info(f"Gold data generated and saved to {settings.gold_settings.local_output_path}")
        # Optionally, read back to verify
        gold_read_back = pd.read_csv(settings.gold_settings.local_output_path)
        logger.info("Gold data read back:\n%s", gold_read_back)
        os.remove(settings.gold_settings.local_output_path) # Clean up dummy file
        logger.info(f"Cleaned up dummy Gold output at {settings.gold_settings.local_output_path}")
    except Exception as e:
        logger.error(f"Test Gold generation failed: {e}")