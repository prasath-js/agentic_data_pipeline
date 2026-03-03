import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS rejected;")

    try:
        # Load bronze.transactions_raw
        transactions_raw_df = con.execute("SELECT * FROM bronze.transactions_raw").df()

        if transactions_raw_df.empty:
            print("bronze.transactions_raw is empty. No transformations to perform.")
            # Ensure silver and rejected tables are created as empty with correct schema
            # Define schema for silver.transactions_cleaned
            empty_silver_df = pd.DataFrame(columns=[
                "transaction_id", "customer_id", "quantity", "amount", "transaction_date", "_source_file", "_ingest_ts"
            ]).astype({
                "transaction_id": 'float64', "customer_id": 'float64', "quantity": 'float64',
                "amount": 'float64', "transaction_date": 'string', "_source_file": 'string', "_ingest_ts": 'string'
            })
            con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM empty_silver_df;", parameters={'empty_silver_df': empty_silver_df})

            # Define schema for rejected.rejected_rows (original columns + rejection_reason)
            empty_rejected_df = transactions_raw_df.head(0).copy()
            empty_rejected_df['rejection_reason'] = pd.Series(dtype='string')
            con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM empty_rejected_df;", parameters={'empty_rejected_df': empty_rejected_df})
            return

        # Create a working DataFrame for validation and cleaning
        validation_df = transactions_raw_df.copy()

        # Strip whitespace from all string columns
        string_cols = validation_df.select_dtypes(include=['object', 'string']).columns
        for col in string_cols:
            validation_df[col] = validation_df[col].astype(str).str.strip()

        # Coerce amount column to numeric (errors='coerce')
        # Create a temporary cleaned amount column for validation
        if 'amount' in validation_df.columns:
            # Remove currency symbols and commas before converting to numeric
            validation_df['amount_cleaned'] = validation_df['amount'].astype(str).str.replace(r'[^\d\.-]', '', regex=True)
            validation_df['amount_cleaned'] = pd.to_numeric(validation_df['amount_cleaned'], errors='coerce')
        else:
            validation_df['amount_cleaned'] = pd.Series(dtype='float64', index=validation_df.index) # Handle missing amount column gracefully

        # Identify Rejections
        # Rows with null transaction_id are REJECTED
        is_null_id = validation_df['transaction_id'].isnull()

        # Rows with null or zero or negative amount are REJECTED
        is_invalid_amount = validation_df['amount_cleaned'].isnull() | (validation_df['amount_cleaned'] <= 0)

        rejected_mask = is_null_id | is_invalid_amount

        # Separate & Assign Reasons
        # Valid rows come from the cleaned/coerced validation_df
        valid_df = validation_df[~rejected_mask].copy()
        # Drop the temporary 'amount_cleaned' column and rename it back to 'amount'
        if 'amount_cleaned' in valid_df.columns:
            valid_df['amount'] = valid_df['amount_cleaned']
            valid_df = valid_df.drop(columns=['amount_cleaned'])

        # Prepare rejected_df with rejection reasons
        rejected_df = transactions_raw_df[rejected_mask].copy()
        if not rejected_df.empty:
            # Initialize rejection_reason column in validation_df for all rows
            validation_df['rejection_reason'] = ''

            # Assign reasons based on the masks
            validation_df.loc[is_null_id, 'rejection_reason'] += 'Null transaction_id; '
            validation_df.loc[is_invalid_amount, 'rejection_reason'] += 'Invalid amount (null, zero, or negative); '

            # Clean up trailing '; '
            validation_df['rejection_reason'] = validation_df['rejection_reason'].str.strip('; ')

            # Transfer the rejection reasons to the rejected_df, aligning by index
            rejected_df['rejection_reason'] = validation_df.loc[rejected_mask, 'rejection_reason']

        # Write SILVER table: transactions_cleaned
        con.execute("CREATE OR REPLACE TABLE silver.transactions_cleaned AS SELECT * FROM valid_df;", parameters={'valid_df': valid_df})

        # Write REJECTED rows
        if not rejected_df.empty:
            # Check if rejected.rejected_rows table exists
            table_exists = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows';"
            ).fetchone()[0] > 0

            if table_exists:
                # Append to existing table
                con.execute("INSERT INTO rejected.rejected_rows SELECT * FROM rejected_df;", parameters={'rejected_df': rejected_df})
            else:
                # Create new table
                con.execute("CREATE TABLE rejected.rejected_rows AS SELECT * FROM rejected_df;", parameters={'rejected_df': rejected_df})
        else:
            # If no rejections, ensure the rejected table schema is defined but empty
            temp_rejected_schema_df = transactions_raw_df.head(0).copy()
            temp_rejected_schema_df['rejection_reason'] = pd.Series(dtype='string')
            con.execute("CREATE OR REPLACE TABLE rejected.rejected_rows AS SELECT * FROM temp_rejected_schema_df;", parameters={'temp_rejected_schema_df': temp_rejected_schema_df})

    finally:
        con.close()

if __name__ == "__main__":
    main()