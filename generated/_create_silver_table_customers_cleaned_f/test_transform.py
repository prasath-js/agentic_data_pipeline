import pandas as pd
from transform import transform # Assuming this file is saved as 'transform.py'

def test_transform_string_stripping():
    data = {
        'customer_id': [1, 2],
        'name': ['  John Doe  ', ' Jane Smith '],
        'email': ['john@example.com', 'jane@example.com'],
        'address': [' 123 Main St ', '456 Oak Ave'],
        'join_date': ['2023-01-01', '2023-01-02']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    assert transformed_df['name'].iloc[0] == 'John Doe'
    assert transformed_df['name'].iloc[1] == 'Jane Smith'
    assert transformed_df['address'].iloc[0] == '123 Main St'
    assert transformed_df['address'].iloc[1] == '456 Oak Ave'

def test_transform_date_parsing_duplicates_and_email_validation():
    data = {
        'customer_id': [101, 102, 101, 103, 102],
        'name': ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'],
        'email': ['alice@example.com', 'bob@example.com', 'invalid-alice', 'charlie@test.com', 'bob.dup@example.com'],
        'address': ['1A', '2B', '1A', '3C', '2B'],
        'join_date': ['2023-03-10', '2023-03-05', '2023-03-15', '2023-03-01', '2023-03-07']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # Check date parsing
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date'])
    # After sorting by customer_id and then join_date descending, and dropping duplicates
    # The order will be 101 (2023-03-15), 102 (2023-03-07), 103 (2023-03-01)
    assert transformed_df[transformed_df['customer_id'] == 101]['join_date'].iloc[0] == pd.Timestamp('2023-03-15')
    assert transformed_df[transformed_df['customer_id'] == 102]['join_date'].iloc[0] == pd.Timestamp('2023-03-07')
    assert transformed_df[transformed_df['customer_id'] == 103]['join_date'].iloc[0] == pd.Timestamp('2023-03-01')


    # Check duplicate dropping (keeping latest join_date)
    assert len(transformed_df) == 3
    # The dataframe is sorted by customer_id in the transform function
    assert transformed_df['customer_id'].tolist() == [101, 102, 103]

    # Check email validation
    assert transformed_df[transformed_df['customer_id'] == 101]['email_is_valid'].iloc[0] == False # 'invalid-alice'
    assert transformed_df[transformed_df['customer_id'] == 102]['email_is_valid'].iloc[0] == True # 'bob.dup@example.com'
    assert transformed_df[transformed_df['customer_id'] == 103]['email_is_valid'].iloc[0] == True # 'charlie@test.com'

def test_transform_missing_columns():
    data = {
        'customer_id': [1, 2],
        'name': ['Test1', 'Test2']
    }
    df = pd.DataFrame(data)
    transformed_df = transform(df)

    # join_date and email should not exist, so email_is_valid should be False
    assert 'join_date' not in transformed_df.columns
    assert 'email' not in transformed_df.columns
    assert 'email_is_valid' in transformed_df.columns
    assert all(transformed_df['email_is_valid'] == False)

def test_transform_empty_dataframe():
    df = pd.DataFrame(columns=['customer_id', 'name', 'email', 'address', 'join_date'])
    transformed_df = transform(df)

    assert transformed_df.empty
    assert 'email_is_valid' in transformed_df.columns
    # Check if join_date column exists and is of datetime type, even if empty
    assert 'join_date' in transformed_df.columns
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['join_date']) or transformed_df['join_date'].empty