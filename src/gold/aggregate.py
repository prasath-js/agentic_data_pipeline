import os
import logging
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoldLayer:
    """
    Gold layer processing for the sales pipeline.

    Reads silver layer parquet files, applies business aggregations, and writes
    the aggregated data to a local file.
    """

    def __init__(self, silver_data_path: str, output_path: str):
        """
        Initializes the GoldLayer processor.

        Args:
            silver_data_path (str): The file path to the silver layer parquet data.
            output_path (str): The directory path where the gold layer output will be written.
        """
        self.silver_data_path = silver_data_path
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)
        logger.info(f"GoldLayer initialized with silver_data_path: {self.silver_data_path}, output_path: {self.output_path}")

    def _read_silver_data(self) -> pd.DataFrame:
        """
        Reads the silver layer parquet data.

        Returns:
            pd.DataFrame: A DataFrame containing the silver layer data.

        Raises:
            FileNotFoundError: If the silver data file does not exist.
            Exception: For other errors during file reading.
        """
        logger.info(f"Attempting to read silver layer data from: {self.silver_data_path}")
        try:
            df = pd.read_parquet(self.silver_data_path)
            logger.info(f"Successfully read silver layer data. Rows: {len(df)}")
            return df
        except FileNotFoundError:
            logger.error(f"Silver layer data file not found at: {self.silver_data_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading silver layer parquet file: {e}")
            raise

    def _aggregate_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes sales data from the silver layer.
        This method assumes the input DataFrame from the silver layer already contains
        pre-aggregated 'total_sales' and 'total_quantity' by 'order_date' and 'region'.
        This gold layer step primarily ensures data types and filters based on 'order_date',
        passing through the already aggregated data.

        Args:
            df (pd.DataFrame): The input DataFrame from the silver layer.

        Returns:
            pd.DataFrame: A DataFrame with processed sales data, retaining the silver layer's aggregation.
        """
        logger.info("Starting gold layer data preparation (type conversion, assuming pre-aggregation)...")
        try:
            # Ensure 'order_date' is in datetime format for consistency and potential downstream use
            if 'order_date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['order_date']):
                df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

            # Filter out rows where order_date is NaT due to coercion errors
            df_filtered = df.dropna(subset=['order_date'])

            # As per requirements, the silver layer already provides aggregated 'total_sales'
            # and 'total_quantity' by 'order_date' and 'region'.
            # Therefore, no further aggregation is needed in the gold layer for these metrics.
            logger.info(f"Sales data prepared successfully, consuming pre-aggregated silver layer data. Rows: {len(df_filtered)}")
            return df_filtered
        except Exception as e:
            logger.error(f"Error during sales data preparation in gold layer: {e}")
            raise

    def _write_gold_data(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Writes the gold layer aggregated data to a local CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            file_name (str): The name of the output file.
        """
        output_file_path = os.path.join(self.output_path, file_name)
        logger.info(f"Attempting to write gold layer data to: {output_file_path}")
        try:
            df.to_csv(output_file_path, index=False)
            logger.info(f"Successfully wrote gold layer data to: {output_file_path}. Rows: {len(df)}")
        except Exception as e:
            logger.error(f"Error writing gold layer data to CSV: {e}")
            raise

    def run(self) -> None:
        """
        Executes the gold layer processing: reads silver data, aggregates it,
        and writes the result to the output destination.
        """
        logger.info("Starting gold layer processing...")
        try:
            silver_df = self._read_silver_data()
            if silver_df.empty:
                logger.warning("Silver layer data is empty. Skipping aggregation and output.")
                return

            gold_df = self._aggregate_sales_data(silver_df)

            output_file_name = f"sales_daily_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            self._write_gold_data(gold_df, output_file_name)

            logger.info("Gold layer processing completed successfully.")
        except Exception as e:
            logger.critical(f"Gold layer processing failed: {e}")
            raise

def main() -> None:
    """
    Main function to run the Gold layer processing.
    Retrieves configuration from environment variables.
    """

    silver_staging_dir = os.getenv("SILVER_STAGING_DIR", "data/silver")
    gold_output_dir = os.getenv("GOLD_OUTPUT_DIR", "data/gold")
    # The silver layer (`src/silver/transform.py`) writes to `sales_silver.parquet`.
    # We use this consistent filename here.
    SILVER_FILENAME = "sales_silver.parquet"

    silver_parquet_file = os.path.join(silver_staging_dir, SILVER_FILENAME)

    # Create dummy silver data for local testing if it doesn't exist
    if not os.path.exists(silver_parquet_file):
        logger.warning(f"Silver parquet file not found at {silver_parquet_file}. Generating dummy data for testing.")
        os.makedirs(silver_staging_dir, exist_ok=True)
        dummy_data = {
            'order_id': ['1', '2', '3', '4', '5'],
            'customer_id': ['cust1', 'cust2', 'cust1', 'cust3', 'cust2'],
            'customer_name': ['hashed_name_A', 'hashed_name_B', 'hashed_name_A', 'hashed_name_C', 'hashed_name_B'],
            'customer_email': ['hashed_email_A', 'hashed_email_B', 'hashed_email_A', 'hashed_email_C', 'hashed_email_B'],
            'product_id': ['prod1', 'prod2', 'prod1', 'prod3', 'prod2'],
            'product_name': ['hashed_prod_A', 'hashed_prod_B', 'hashed_prod_A', 'hashed_prod_C', 'hashed_prod_B'],
            'quantity': [1, 2, 1, 3, 1],
            'unit_price': [10.0, 20.0, 10.0, 5.0, 20.0],
            'total_amount': [10.0, 40.0, 10.0, 15.0, 20.0],
            'order_date': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03'],
            'region': ['East', 'West', 'East', 'North', 'West'],
            'status': ['Completed', 'Completed', 'Pending', 'Completed', 'Completed']
        }
        dummy_df = pd.DataFrame(dummy_data)
        # Ensure order_date is datetime for consistency
        dummy_df['order_date'] = pd.to_datetime(dummy_df['order_date'])
        # Add pre-aggregated total_sales and total_quantity as expected from silver layer
        # For dummy data, we'll just rename total_amount to total_sales and sum quantity
        # In a real scenario, silver would provide these directly based on its aggregation.
        dummy_df['total_sales'] = dummy_df['total_amount'] # Assuming total_amount from silver is already total_sales for this row/group
        dummy_df['total_quantity'] = dummy_df['quantity'] # Assuming quantity from silver is already total_quantity for this row/group
        
        # To simulate the silver layer's actual aggregation, we need to pre-aggregate the dummy data.
        # This is for the dummy data creation ONLY, to make it consistent with the expectation of the gold layer.
        pre_aggregated_dummy_df = dummy_df.groupby(['order_date', 'region']).agg(
            total_sales=('total_amount', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()

        pre_aggregated_dummy_df.to_parquet(silver_parquet_file, index=False)
        logger.info(f"Dummy silver parquet file (pre-aggregated) created at: {silver_parquet_file}")

    gold_layer = GoldLayer(
        silver_data_path=silver_parquet_file,
        output_path=gold_output_dir
    )
    gold_layer.run()

if __name__ == "__main__":
    main()
