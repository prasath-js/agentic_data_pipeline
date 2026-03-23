import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

def validate_row_counts(bronze_count: int, silver_count: int, gold_count: int) -> bool:
    """
    Validates row counts across different pipeline stages (Bronze, Silver, Gold).

    Logs a warning if the Silver layer row count is less than 80% of the Bronze
    layer row count, indicating a significant data loss during transformation.
    This function currently does not enforce specific thresholds for Gold, but
    it can be extended to do so.

    Args:
        bronze_count (int): The number of rows ingested in the Bronze layer.
        silver_count (int): The number of rows after Silver layer transformations.
        gold_count (int): The number of rows after Gold layer aggregations.

    Returns:
        bool: True if all row count checks pass, False otherwise.
              Currently, it only returns False if the silver_count
              is less than 80% of bronze_count.
    """
    all_checks_pass = True

    logger.info(f"Bronze layer row count: {bronze_count}")
    logger.info(f"Silver layer row count: {silver_count}")
    logger.info(f"Gold layer row count: {gold_count}")

    # Check for significant row count drop from Bronze to Silver
    if bronze_count > 0:
        silver_bronze_ratio = (silver_count / bronze_count) * 100
        if silver_bronze_ratio < 80.0:
            logger.warning(
                f"Silver layer row count ({silver_count}) is less than 80% "
                f"of Bronze layer row count ({bronze_count}). Ratio: {silver_bronze_ratio:.2f}%. "
                "This may indicate significant data loss during Silver layer processing."
            )
            all_checks_pass = False
        else:
            logger.info(
                f"Silver layer row count ({silver_count}) is {silver_bronze_ratio:.2f}% "
                f"of Bronze layer row count ({bronze_count}). Within acceptable limits."
            )
    elif silver_count > 0:
        logger.warning(
            "Bronze layer had 0 rows but Silver layer has rows. This is unexpected."
        )
        all_checks_pass = False

    # Additional checks can be added here, for example:
    # - gold_count must be <= silver_count
    # - gold_count must be > 0 if silver_count > 0 (unless specific aggregation logic allows 0)

    if all_checks_pass:
        logger.info("All row count validation checks passed.")
    else:
        logger.error("Some row count validation checks failed.")

    return all_checks_pass
