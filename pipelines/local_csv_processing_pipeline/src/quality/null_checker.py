import pandas as pd
import logging
from typing import Dict, List

# Configure logging for this module
logger = logging.getLogger(__name__)

def check_for_nulls(df: pd.DataFrame, columns: List[str]) -> Dict[str, int]:
    """
    Checks specified columns in a DataFrame for null values and logs warnings.

    Args:
        df (pd.DataFrame): The DataFrame to check.
        columns (List[str]): A list of column names to check for nulls.

    Returns:
        Dict[str, int]: A dictionary where keys are column names and values are
                        the count of nulls found in that column.
    """
    null_counts: Dict[str, int] = {}
    for col in columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(
                    f"Null values detected in column '{col}': {null_count} nulls."
                )
            null_counts[col] = null_count
        else:
            logger.warning(
                f"Column '{col}' not found in DataFrame. Skipping null check for this column."
            )
            null_counts[col] = 0  # Indicate 0 nulls for non-existent column

    return null_counts

if __name__ == "__main__":
    # Example usage (for testing purposes only)
    import sys
    from config.logging_config import configure_logging

    configure_logging()
    logger.info("Running null_checker.py example...")

    # Create a sample DataFrame
    data = {
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": [101, 102, None, 104, 105],
        "customer_name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "email": ["alice@example.com", None, "charlie@example.com", "diana@example.com", "eve@example.com"],
        "amount": [100.0, 150.5, 200.0, None, 50.0],
        "status": ["completed", "pending", "completed", "cancelled", "completed"],
        "region": ["east", "west", "north", "south", "east"],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023", "04/01/2023", "05/01/2023"],
    }
    sample_df = pd.DataFrame(data)

    # Columns to check for nulls
    columns_to_check = ["order_id", "customer_id", "customer_name", "email", "amount", "non_existent_col"]

    logger.info(f"Checking DataFrame for nulls in columns: {columns_to_check}")
    nulls_found = check_for_nulls(sample_df, columns_to_check)

    logger.info(f"Null counts by column: {null_found}")

    # Expected output:
    # WARNING:__main__:Null values detected in column 'customer_id': 1 nulls.
    # WARNING:__main__:Null values detected in column 'customer_name': 1 nulls.
    # WARNING:__main__:Null values detected in column 'email': 1 nulls.
    # WARNING:__main__:Null values detected in column 'amount': 1 nulls.
    # WARNING:__main__:Column 'non_existent_col' not found in DataFrame. Skipping null check for this column.
    # INFO:__main__:Null counts by column: {'order_id': 0, 'customer_id': 1, 'customer_name': 1, 'email': 1, 'amount': 1, 'non_existent_col': 0}