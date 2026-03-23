import logging
import pandas as pd

logger = logging.getLogger(__name__)

def transform_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw Bronze data into clean Silver data.
    
    Operations performed:
    - Masks PII columns ('customer_name', 'email') with '***MASKED***'.
    - Standardizes 'order_date' from 'DD/MM/YYYY' to ISO format ('YYYY-MM-DD').
    - Removes rows with null values in critical columns.
    - Filters out invalid rows where 'amount' is 0 or negative.
    
    Args:
        df (pd.DataFrame): The raw dataframe from the Bronze layer.
        
    Returns:
        pd.DataFrame: The cleaned and transformed Silver dataframe.
    """
    logger.info("Starting Silver layer transformations.")
    
    # Create a copy to avoid SettingWithCopyWarning
    transformed_df = df.copy()
    
    # 1. Mask PII Columns
    pii_columns = ["customer_name", "email"]
    for col in pii_columns:
        if col in transformed_df.columns:
            transformed_df[col] = "***MASKED***"
            logger.info("Masked PII column: %s", col)
            
    # 2. Fix Date Format
    if "order_date" in transformed_df.columns:
        logger.info("Converting 'order_date' from DD/MM/YYYY to ISO format.")
        # Coerce errors to NaT to be handled by the null drop later
        transformed_df["order_date"] = pd.to_datetime(
            transformed_df["order_date"], 
            format="%d/%m/%Y", 
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        
    # 3. Filter invalid amounts
    if "amount" in transformed_df.columns:
        logger.info("Filtering rows where 'amount' <= 0.")
        # Ensure numeric type for comparison
        transformed_df["amount"] = pd.to_numeric(transformed_df["amount"], errors="coerce")
        initial_rows = len(transformed_df)
        transformed_df = transformed_df[transformed_df["amount"] > 0]
        logger.info("Removed %d rows with invalid amounts.", initial_rows - len(transformed_df))

    # 4. Remove rows with nulls in critical columns
    critical_columns = ["order_id", "customer_id", "amount", "order_date"]
    existing_critical = [col for col in critical_columns if col in transformed_df.columns]
    
    if existing_critical:
        initial_rows = len(transformed_df)
        transformed_df = transformed_df.dropna(subset=existing_critical)
        dropped_rows = initial_rows - len(transformed_df)
        if dropped_rows > 0:
            logger.info("Dropped %d rows with null values in critical columns.", dropped_rows)

    logger.info("Silver layer transformations completed successfully.")
    return transformed_df