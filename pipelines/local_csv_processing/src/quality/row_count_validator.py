import logging

# Get a logger instance
logger = logging.getLogger(__name__)

def validate_row_counts(bronze_count: int, silver_count: int, gold_count: int) -> bool:
    """
    Validates row counts across different pipeline stages (Bronze, Silver, Gold).

    Logs a warning if the Silver layer row count is less than 80% of the Bronze layer.
    This function primarily serves as a high-level data quality check for significant
    data loss between stages.

    Args:
        bronze_count (int): The number of rows processed in the Bronze layer.
        silver_count (int): The number of rows processed in the Silver layer.
        gold_count (int): The number of rows processed in the Gold layer.

    Returns:
        bool: True if all specified checks pass (or no critical failures are detected).
              Currently, it returns True after logging warnings for potential issues,
              as no hard stop conditions are defined for these checks.
    """
    logger.info(f"Starting row count validation:")
    logger.info(f"  Bronze layer row count: {bronze_count}")
    logger.info(f"  Silver layer row count: {silver_count}")
    logger.info(f"  Gold layer row count: {gold_count}")

    # Check for significant row count drop from Bronze to Silver
    if bronze_count > 0:
        silver_bronze_ratio = silver_count / bronze_count
        if silver_bronze_ratio < 0.8:
            logger.warning(
                f"Silver layer row count ({silver_count}) is less than 80% "
                f"of Bronze layer row count ({bronze_count}). "
                f"Ratio: {silver_bronze_ratio:.2f}. "
                f"Investigate potential excessive filtering or data loss."
            )
        else:
            logger.info(
                f"Silver layer row count ({silver_count}) is within acceptable "
                f"range compared to Bronze ({bronze_count}). Ratio: {silver_bronze_ratio:.2f}."
            )
    elif silver_count > 0:
        logger.warning(
            f"Bronze layer row count is zero, but Silver layer has {silver_count} rows. "
            f"This might indicate an issue in the Bronze ingestion or count."
        )
    else:
        logger.info("Both Bronze and Silver layer row counts are zero. No data processed.")

    # Additional checks can be added here, e.g., Silver vs Gold, etc.
    if silver_count < gold_count:
        logger.warning(
            f"Gold layer row count ({gold_count}) is greater than Silver layer row count ({silver_count}). "
            f"This is usually unexpected unless aggregation logic expands rows (e.g., joins). "
            f"Please verify gold layer transformation logic."
        )

    logger.info("Row count validation completed.")
    return True