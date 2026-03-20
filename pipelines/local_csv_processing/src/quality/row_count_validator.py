import logging

# Configure logging for the module
logger = logging.getLogger(__name__)

def validate_row_counts(bronze_count: int, silver_count: int, gold_count: int) -> bool:
    """
    Validates row counts across Bronze, Silver, and Gold layers to detect potential data loss or empty outputs.

    Args:
        bronze_count (int): The number of rows in the Bronze layer DataFrame.
        silver_count (int): The number of rows in the Silver layer DataFrame.
        gold_count (int): The number of rows in the Gold layer DataFrame.

    Returns:
        bool: True if all row count checks pass, False otherwise.
    """
    all_checks_pass = True
    data_loss_threshold = 0.80  # Silver count should be at least 80% of Bronze

    logger.info(f"Bronze layer row count: {bronze_count}")
    logger.info(f"Silver layer row count: {silver_count}")
    logger.info(f"Gold layer row count: {gold_count}")

    # Check for unexpected data loss from Bronze to Silver
    if bronze_count > 0 and silver_count < (bronze_count * data_loss_threshold):
        logger.warning(
            f"Potential unexpected data loss detected: Silver layer has {silver_count} rows, "
            f"which is less than {data_loss_threshold*100}% of Bronze layer's {bronze_count} rows."
        )
        all_checks_pass = False
    elif bronze_count == 0 and silver_count > 0:
        logger.warning(
            "Bronze layer had 0 rows but Silver layer has rows. This is an unexpected scenario."
        )
        all_checks_pass = False
    elif bronze_count > 0 and silver_count == 0:
        logger.warning(
            "Bronze layer had rows but Silver layer has 0 rows. All data might have been filtered out."
        )
        all_checks_pass = False
    elif bronze_count == 0 and silver_count == 0:
        logger.info("Both Bronze and Silver layers are empty. No data to process.")

    # Check if Gold layer is empty
    if gold_count == 0:
        logger.warning("Gold layer has 0 rows. The final output is empty.")
        all_checks_pass = False

    if all_checks_pass:
        logger.info("All row count validation checks passed successfully.")
    else:
        logger.error("One or more row count validation checks failed.")

    return all_checks_pass