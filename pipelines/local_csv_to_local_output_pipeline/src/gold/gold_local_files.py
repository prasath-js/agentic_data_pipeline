import pandas as pd
import os
import logging
from typing import Dict, Any

from src.db_connection.builder import ConnectionBuilder
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def gold_local_files(df_silver: pd.DataFrame) -> None:
    """
    Aggregates the silver layer data and writes it to a local file (e.g., CSV or Parquet)
    as the gold layer output.

    This function performs the following steps:
    1. Aggregates data by 'account_id' and 'close_date' to sum 'amount' and 'quantity'.
    2. Writes the aggregated DataFrame to a specified local file path.

    Args:
        df_silver (pd.DataFrame): The transformed DataFrame from the silver layer.

    Returns:
        None
    """
    settings = Settings()
    output_file_path = os.getenv("GOLD_LOCAL_OUTPUT_PATH")
    output_file_format = os.getenv("GOLD_LOCAL_OUTPUT_FORMAT", "csv").lower()

    if not output_file_path:
        logger.error("GOLD_LOCAL_OUTPUT_PATH environment variable not set.")
        raise ValueError("Gold output file path not configured.")

    logger.info("Starting gold layer aggregation and output.")

    # 1. Aggregate Silver data
    if {'account_id', 'amount', 'quantity', 'close_date'}.issubset(df_silver.columns):
        df_gold = df_silver.groupby(['account_id', 'close_date']).agg(
            total_amount=('amount', 'sum'),
            total_quantity=('quantity', 'sum'),
            opportunity_count=('opportunity_id', 'nunique')
        ).reset_index()
        logger.info(f"Aggregated silver data into {len(df_gold)} rows.")
    else:
        logger.warning("Required columns for aggregation (account_id, amount, quantity, close_date) not found. Skipping aggregation.")
        df_gold = df_silver.copy() # If aggregation cannot happen, output the silver data directly
        logger.info("Outputting silver data directly as gold layer due to missing aggregation columns.")


    # 2. Write to output target
    try:
        connector_config: Dict[str, Any] = {
            "type": "local_files",
            "path": output_file_path
        }
        connector = ConnectionBuilder.build_connector(connector_config)

        connector.write(df=df_gold, file_path=output_file_path, file_format=output_file_format)

        logger.info(f"Successfully wrote {len(df_gold)} rows to gold layer output: {output_file_path} (format: {output_file_format})")
    except Exception as e:
        logger.error(f"Error writing gold layer data to {output_file_path}: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    # Example usage for standalone testing
    # Create a dummy silver DataFrame
    data = {
        'opportunity_id': [1, 2, 3, 4],
        'account_id': [101, 102, 101, 103],
        'value': [1000.00, 500.00, 200.00, 750.00],
        'close_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-20'), pd.Timestamp('2023-01-15'), pd.Timestamp('2023-04-10')],
        'stage': ['Closed Won', 'Open', 'Closed Lost', 'Open'],
        'transaction_id': ['T1', 'T2', 'T3', 'T4'],
        'customer_id': ['C1', 'C2', 'C1', 'C3'],
        'quantity': [10, 5, 2, 8],
        'amount': [100.00, 50.00, 20.00, 75.00],
        'transaction_date': [pd.Timestamp('2023-01-10'), pd.Timestamp('2023-02-15'), pd.Timestamp('2023-02-28'), pd.Timestamp('2023-04-05')]
    }
    silver_df_test = pd.DataFrame(data)

    # Configure logging for testing
    os.environ["LOG_LEVEL"] = "INFO"
    from src.config.logging_config import configure_logging
    configure_logging()

    # Set up dummy output path
    dummy_output_path = "temp_gold_output.csv"
    os.environ["GOLD_LOCAL_OUTPUT_PATH"] = dummy_output_path
    os.environ["GOLD_LOCAL_OUTPUT_FORMAT"] = "csv"

    print("--- Silver DataFrame (Input for Gold) ---")
    print(silver_df_test)
    print(f"Input row count: {len(silver_df_test)}")

    try:
        gold_local_files(silver_df_test)
        print(f"\nGold output written to: {dummy_output_path}")

        # Verify output
        if os.path.exists(dummy_output_path):
            gold_df_read = pd.read_csv(dummy_output_path, parse_dates=['close_date'])
            print("\n--- Gold DataFrame (Read from output) ---")
            print(gold_df_read)
            assert len(gold_df_read) == 3, f"Expected 3 aggregated rows, got {len(gold_df_read)}"
            assert gold_df_read[gold_df_read['account_id'] == 101]['total_amount'].iloc[0] == 120.0, "Aggregation error for account 101"
            print("\nAssertions passed for test data.")
        else:
            print("Error: Gold output file not found.")

    except Exception as e:
        print(f"Failed to generate gold layer: {e}")
    finally:
        if os.path.exists(dummy_output_path):
            os.remove(dummy_output_path)
            print(f"Cleaned up {dummy_output_path}")