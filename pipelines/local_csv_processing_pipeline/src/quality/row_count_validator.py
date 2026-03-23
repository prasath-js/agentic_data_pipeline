import logging

logger = logging.getLogger(__name__)

def validate_row_counts(bronze_count: int, silver_count: int, gold_count: int) -> bool:
    """
    Validates the row counts across the Bronze, Silver, and Gold layers.
    
    Logs a warning if the Silver layer row count is less than 80% of the 
    Bronze layer row count.
    
    Args:
        bronze_count (int): Number of rows in the Bronze layer DataFrame.
        silver_count (int): Number of rows in the Silver layer DataFrame.
        gold_count (int): Number of rows in the Gold layer DataFrame.
        
    Returns:
        bool: True if row count validation passes, False if counts are logically invalid.
    """
    logger.info("Starting row count validation.")
    
    if bronze_count < 0 or silver_count < 0 or gold_count < 0:
        logger.error("Invalid row counts detected. Counts cannot be negative.")
        return False
        
    logger.info(f"Pipeline row counts -> Bronze: {bronze_count}, Silver: {silver_count}, Gold: {gold_count}")
    
    if bronze_count > 0:
        retention_rate = silver_count / bronze_count
        if retention_rate < 0.80:
            logger.warning(
                "Data loss warning: Silver layer row count is less than 80 percent of Bronze layer. "
                f"Bronze: {bronze_count}, Silver: {silver_count} (Retention: {retention_rate:.2%})"
            )
    else:
        if silver_count > 0 or gold_count > 0:
            logger.error("Bronze count is 0, but downstream layers have data. This indicates an anomaly.")
            return False

    logger.info("Row count validation completed successfully.")
    return True