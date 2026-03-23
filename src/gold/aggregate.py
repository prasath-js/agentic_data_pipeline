import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoldLayerProcessor:
    """
    Processes silver layer data to create aggregated gold layer data.
    Reads silver parquet files, applies business aggregations, and writes
    the results to a specified output destination.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the GoldLayerProcessor with configuration settings.

        Args:
            config (Dict[str, Any]): A dictionary containing configuration parameters
                                     like staging directories and output details.
        """
        self.silver_data_dir = config['silver_data_dir']
        self.gold_output_config = config['gold_output_config']

    def _read_silver_data(self) -> pd.DataFrame:
        """
        Reads silver layer data from a parquet file.

        Returns:
            pd.DataFrame: A DataFrame containing the silver layer data.
        
        Raises:
            FileNotFoundError: If the silver data file does not exist.
            Exception: For other errors during file reading.
        """
        silver_file_path = os.path.join(self.silver_data_dir, "sales_silver.parquet")
        logger.info(f"Attempting to read silver data from: {silver_file_path}")
        try:
            df = pd.read_parquet(silver_file_path)
            logger.info(f"Successfully read silver data. Rows read: {len(df)}")
            return df
        except FileNotFoundError:
            logger.error(f"Silver data file not found: {silver_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading silver data from {silver_file_path}: {e}")
            raise

    def _apply_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Splies business aggregations to the silver layer data.
        Aggregates total sales and quantity by order_date and region.

        Args:
            df (pd.DataFrame): The silver layer DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with aggregated data.
        """
        logger.info("Applying business aggregations: total sales and quantity by order_date.")
        
        # Ensure order_date is datetime for proper grouping
        df['order_date'] = pd.to_datetime(df['order_date'])

        # Aggregate total sales and quantity by order_date
        aggregated_df = df.groupby(['order_date']).agg(
            total_sales=('total_amount', 'sum'),
            total_quantity=('quantity', 'sum'),
            number_of_orders=('order_id', 'nunique')
        ).reset_index()

        logger.info(f"Aggregations applied. Resulting rows: {len(aggregated_df)}")
        return aggregated_df

    def _write_gold_data(self, df: pd.DataFrame) -> None:
        """
        Writes the aggregated gold layer data to the specified output destination.

        Args:
            df (pd.DataFrame): The DataFrame containing gold layer data.
        
        Raises:
            ValueError: If an unsupported output type is specified.
            Exception: For other errors during data writing.
        """
        output_type = self.gold_output_config['type']
        
        if output_type == 'local_file':
            output_dir = os.getenv('GOLD_LOCAL_FILE_PATH', 'data/gold')
            os.makedirs(output_dir, exist_ok=True)
            output_file_name = f"sales_pipeline_aggregated_sales_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
            output_path = os.path.join(output_dir, output_file_name)
            
            logger.info(f"Writing gold data to local file: {output_path}")
            try:
                df.to_parquet(output_path, index=False)
                logger.info(f"Successfully wrote {len(df)} rows of gold data to {output_path}")
            except Exception as e:
                logger.error(f"Error writing gold data to local file {output_path}: {e}")
                raise
        else:
            logger.error(f"Unsupported gold output type: {output_type}")
            raise ValueError(f"Unsupported gold output type: {output_type}")

    def run(self) -> None:
        """
        Executes the gold layer processing pipeline.
        """
        logger.info("Starting Gold Layer processing for sales_pipeline.")
        try:
            silver_df = self._read_silver_data()
            if silver_df.empty:
                logger.warning("Silver DataFrame is empty. No aggregations to perform.")
                return

            gold_df = self._apply_aggregations(silver_df)
            self._write_gold_data(gold_df)
            logger.info("Gold Layer processing completed successfully.")
        except Exception as e:
            logger.error(f"Gold Layer processing failed: {e}", exc_info=True)
            raise

def main() -> None:
    """
    Main function to run the Gold Layer processor.
    """
    # Configuration could come from a YAML file or environment variables
    # For this example, we define it directly or from environment variables
    config = {
        'silver_data_dir': os.getenv('SILVER_DATA_DIR', 'data/silver'),
        'gold_output_config': {
            'type': os.getenv('GOLD_OUTPUT_TYPE', 'local_file'),
            # Add other output specific configurations if needed
        }
    }

    processor = GoldLayerProcessor(config)
    processor.run()

if __name__ == "__main__":
    main()
