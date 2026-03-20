# src/quality/row_count_validator.py
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def validate_row_count(
    df: pd.DataFrame,
    min_rows: Optional[int] = None,
    max_rows: Optional[int] = None,
    expected_rows: Optional[int] = None,
    tolerance: float = 0.1
) -> bool:
    """
    Validates the row count of a DataFrame against specified criteria.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        min_rows (Optional[int]): The minimum expected number of rows.
        max_rows (Optional[int]): The maximum expected number of rows.
        expected_rows (Optional[int]): The exact expected number of rows (with tolerance).
        tolerance (float): The percentage tolerance for `expected_rows` validation (e.g., 0.1 for 10%).

    Returns:
        bool: True if the row count passes all validations, False otherwise.
    """
    current_rows = len(df)
    validation_passed = True

    logger.info(f"Validating row count: current_rows={current_rows}")

    if min_rows is not None:
        if current_rows < min_rows:
            logger.error(f"Row count validation failed: {current_rows} rows is less than minimum expected {min_rows}.")
            validation_passed = False
        else:
            logger.info(f"Row count ({current_rows}) meets minimum threshold ({min_rows}).")

    if max_rows is not None:
        if current_rows > max_rows:
            logger.error(f"Row count validation failed: {current_rows} rows is greater than maximum expected {max_rows}.")
            validation_passed = False
        else:
            logger.info(f"Row count ({current_rows}) is within maximum threshold ({max_rows}).")

    if expected_rows is not None:
        lower_bound = expected_rows * (1 - tolerance)
        upper_bound = expected_rows * (1 + tolerance)
        if not (lower_bound <= current_rows <= upper_bound):
            logger.error(f"Row count validation failed: {current_rows} rows is not within "
                         f"{tolerance*100:.0f}% tolerance of expected {expected_rows} "
                         f"({lower_bound:.0f}-{upper_bound:.0f}).")
            validation_passed = False
        else:
            logger.info(f"Row count ({current_rows}) is within {tolerance*100:.0f}% tolerance of expected ({expected_rows}).")

    if validation_passed:
        logger.info("Row count validation successful.")
    else:
        logger.warning("Row count validation failed for one or more checks.")

    return validation_passed