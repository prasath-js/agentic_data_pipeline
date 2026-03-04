import pandas as pd
import duckdb

# Define DB_PATH as a constant
DB_PATH = 'my_database.duckdb'

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw customer data by cleaning string columns,
    parsing dates, handling duplicates, and validating emails.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw customer data.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    # Strip whitespace from all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id and join_date (descending) to keep the latest date
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        df = df.drop_duplicates(subset='customer_id', keep='first')
    elif 'customer_id' in df.columns: # If join_date is missing, just drop duplicates on customer_id
        df = df.drop_duplicates(subset='customer_id', keep='first')


    # Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].str.contains('@', na=False)
    else:
        df['email_is_valid'] = False # Default to False if email column is missing

    return df

def main():
    """
    Main function to connect to DuckDB, load data, transform it,
    and write the result back to DuckDB.
    """
    con = duckdb.connect(DB_PATH)

    # Ensure the bronze schema exists for reading
    con.execute('CREATE SCHEMA IF NOT EXISTS bronze')
    # Create a dummy table for demonstration if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.customers_raw (
            customer_id INTEGER,
            name VARCHAR,
            email VARCHAR,
            address VARCHAR,
            join_date VARCHAR
        );
    """)
    # Insert some dummy data if the table is empty
    if con.execute('SELECT COUNT(*) FROM bronze.customers_raw').fetchone()[0] == 0:
        con.execute("""
            INSERT INTO bronze.customers_raw VALUES
            (1, ' John Doe ', 'john.doe@example.com', '123 Main St ', '2023-01-01'),
            (2, 'Jane Smith', 'invalid-email', '456 Oak Ave', '2023-01-02'),
            (3, '  Peter Pan  ', 'peter.pan@domain.co.uk ', '789 Pine Ln', '2023-01-03');
        """)


    # Load data from bronze.customers_raw
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Apply transformations
    result = transform(df)

    # Write the result to silver.customers_cleaned
    con.execute('CREATE OR REPLACE SCHEMA silver')
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()