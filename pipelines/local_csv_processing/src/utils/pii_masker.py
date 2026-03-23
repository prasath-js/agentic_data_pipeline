import pandas as pd
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

class PIIMasker:
    """
    Utility class for masking PII (Personally Identifiable Information) in a DataFrame.
    """

    @staticmethod
    def mask_pii_columns(df: pd.DataFrame, columns_to_mask: list[str]) -> pd.DataFrame:
        """
        Masks specified PII columns in a DataFrame by replacing their values with '***MASKED***'.

        Args:
            df (pd.DataFrame): The input DataFrame containing PII.
            columns_to_mask (list[str]): A list of column names to be masked.

        Returns:
            pd.DataFrame: A new DataFrame with the specified PII columns masked.
        """
        if not isinstance(df, pd.DataFrame):
            logger.error("Input must be a pandas DataFrame.")
            raise TypeError("Input must be a pandas DataFrame.")

        if not isinstance(columns_to_mask, list):
            logger.error("Columns to mask must be a list of strings.")
            raise TypeError("Columns to mask must be a list of strings.")

        df_masked = df.copy()
        masked_count = 0
        for col in columns_to_mask:
            if col in df_masked.columns:
                original_values_count = df_masked[col].count()
                if original_values_count > 0:
                    df_masked[col] = "***MASKED***"
                    masked_count += 1
                    logger.info(f"Column '{col}' was masked successfully.")
                else:
                    logger.warning(f"Column '{col}' exists but contains no non-null values to mask.")
            else:
                logger.warning(f"Column '{col}' not found in DataFrame. Skipping masking for this column.")

        if masked_count > 0:
            logger.info(f"Successfully masked PII in {masked_count} out of {len(columns_to_mask)} specified columns.")
        else:
            logger.info("No PII columns were masked based on the provided list and DataFrame content.")

        return df_masked

# Example usage (for testing purposes, not part of production pipeline execution)
if __name__ == "__main__":
    # Configure basic logging for console output
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Sample DataFrame
    data = {
        'order_id': [1, 2, 3, 4],
        'customer_id': ['C101', 'C102', 'C103', 'C104'],
        'customer_name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Diana Miller'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com'],
        'amount': [100.50, 200.00, 150.75, 300.25],
        'status': ['completed', 'pending', 'completed', 'failed'],
        'region': ['North', 'South', 'East', 'West'],
        'order_date': ['01/01/2023', '02/01/2023', '03/01/2023', '04/01/2023']
    }
    sample_df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(sample_df)

    # Columns to mask as per the prompt
    pii_columns = ["customer_name", "email"]

    # Mask PII
    masked_df = PIIMasker.mask_pii_columns(sample_df.copy(), pii_columns)

    print("\nMasked DataFrame:")
    print(masked_df)

    # Test with a non-existent column
    print("\nTesting with a non-existent column:")
    masked_df_test = PIIMasker.mask_pii_columns(sample_df.copy(), ["customer_name", "phone_number"])
    print(masked_df_test)

    # Test with empty DataFrame
    print("\nTesting with an empty DataFrame:")
    empty_df = pd.DataFrame(columns=['col1', 'col2'])
    masked_empty_df = PIIMasker.mask_pii_columns(empty_df, ['col1'])
    print(masked_empty_df)

    # Test with an invalid input type
    print("\nTesting with invalid input type (not a DataFrame):")
    try:
        PIIMasker.mask_pii_columns("not a dataframe", ["customer_name"])
    except TypeError as e:
        logger.error(f"Caught expected error: {e}")

    # Test with an invalid columns_to_mask type
    print("\nTesting with invalid columns_to_mask type (not a list):")
    try:
        PIIMasker.mask_pii_columns(sample_df.copy(), "customer_name")
    except TypeError as e:
        logger.error(f"Caught expected error: {e}")