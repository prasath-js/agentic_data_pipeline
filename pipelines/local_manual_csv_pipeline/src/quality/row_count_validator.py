import logging

logger = logging.getLogger(__name__)

def validate_row_counts(bronze_count: int, silver_count: int, gold_count: int) -> bool:
    """
    Validates row counts across the Bronze, Silver, and Gold layers of the pipeline.
    Warns if the Silver layer row count is less than 80% of the Bronze layer row count.

    Args:
        bronze_count (int): Total number of rows extracted into the Bronze layer.
        silver_count (int): Total number of rows processed into the Silver layer.
        gold_count (int): Total number of rows aggregated into the Gold layer.

    Returns:
        bool: True if the validation executes successfully.
    """
    logger.info("Starting row count validation across medallion layers.")
    logger.info("Counts - Bronze: %d, Silver: %d, Gold: %d", bronze_count, silver_count, gold_count)

    if bronze_count == 0:
        logger.warning("Bronze row count is 0. Pipeline extracted no data.")
        return False

    silver_retention_ratio = silver_count / bronze_count

    if silver_retention_ratio < 0.80:
        logger.warning(
            "Data drop warning: Silver row count (%d) is less than 80%% of Bronze row count (%d). "
            "Actual retention: %.2f%%.",
            silver_count,
            bronze_count,
            silver_retention_ratio * 100
        )
    else:
        logger.info(
            "Silver retention is healthy at %.2f%%.",
            silver_retention_ratio * 100
        )

    if gold_count > silver_count:
        logger.warning(
            "Data inflation warning: Gold row count (%d) is unexpectedly higher than Silver row count (%d).",
            gold_count,
            silver_count
        )

    logger.info("Row count validation completed.")
    return True