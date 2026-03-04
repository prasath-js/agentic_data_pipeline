import pandas as pd
import duckdb

# Define DB_PATH for the main function
DB_PATH = "my_database.duckdb"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw customers DataFrame by cleaning string columns,
    parsing dates, handling duplicates, and validating emails.

    Args:
        df (pd.DataFrame): The input DataFrame containing raw customer data.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    # 1. Strip whitespace from all string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # 2. Parse join_date to datetime if the column exists
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

    # 3. Drop duplicate rows by customer_id keeping the one with the latest join_date
    # Sort by customer_id (ascending) and join_date (descending) to keep the latest date
    if 'customer_id' in df.columns and 'join_date' in df.columns:
        df = df.sort_values(by=['customer_id', 'join_date'], ascending=[True, False])
        df = df.drop_duplicates(subset='customer_id', keep='first')
    elif 'customer_id' in df.columns: # Fallback if join_date is missing for duplicate handling
        df = df.drop_duplicates(subset='customer_id', keep='first')


    # 4. Create a boolean column email_is_valid (True when email contains '@')
    if 'email' in df.columns:
        df['email_is_valid'] = df['email'].astype(str).str.contains('@', na=False)
    else:
        df['email_is_valid'] = False # Default to False if email column is missing

    return df

def main():
    """
    Main function to orchestrate data loading, transformation, and writing.
    """
    con = duckdb.connect(DB_PATH)

    # Ensure schema exists
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    # Create a dummy bronze table for demonstration if it doesn't exist
    con.execute("""
        CREATE OR REPLACE TABLE bronze.customers_raw AS SELECT * FROM (VALUES
            (1, ' John Doe ', 'john.doe@example.com', '2023-01-01'),
            (2, 'Jane Smith ', 'jane.smith@example.com', '2023-01-02'),
            (1, 'John Doe Updated', 'john.doe.new@example.com', '2023-01-05'),
            (3, 'Alice', 'alice@example.com', '2023-01-03')
        ) AS t(customer_id, name, email, join_date);
    """)

    # Load Data
    df = con.execute('SELECT * FROM bronze.customers_raw').df()

    # Transform Data
    result = transform(df)

    # Write to SILVER
    con.execute('CREATE OR REPLACE TABLE silver.customers_cleaned AS SELECT * FROM result')

    con.close()

if __name__ == '__main__':
    main()