import duckdb
import pandas as pd
import os

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    if 'customer_id' in df.columns:
        if 'join_date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['join_date']):
            # Sort by customer_id (ascending) and join_date (descending)
            # This ensures that for each customer_id, the row with the latest join_date comes first
            df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False]).reset_index(drop=True)
            df = df.drop_duplicates(subset='customer_id', keep='first')
        else:
            # If join_date is missing or not datetime, just drop duplicates on customer_id
            df = df.drop_duplicates(subset='customer_id', keep='first')

    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        # Convert to string to handle potential non-string types (e.g., NaN, numbers)
        df['email_is_valid'] = df['email'].astype(str).str.contains('@', na=False)
    else:
        # If 'email' column doesn't exist, create 'email_is_valid' as False for all rows
        df['email_is_valid'] = False

    return df

def main():
    con = duckdb.connect(DB_PATH)

    # Read from BRONZE
    df = con.execute("SELECT * FROM bronze.customers_raw").df()

    # Transform data
    result = transform(df)

    # Write to SILVER
    con.execute("CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result")

    con.close()

if __name__ == "__main__":
    main()