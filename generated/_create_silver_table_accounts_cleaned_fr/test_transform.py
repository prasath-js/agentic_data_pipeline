import pandas as pd
import pytest
from transform import transform

def test_strip_whitespace_and_uppercase_industry():
    data = {
        'account_id': [' ACC001 ', 'ACC002', 'ACC003 '],
        'account_name': ['  Company A  ', 'Company B', 'Company C'],
        'industry': [' tech ', '  finance  ', ' retail '],
        'region': ['USA', 'UK', 'Germany']
    }
    df = pd.DataFrame(data)
    
    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Company A', 'Company B', 'Company C'],
        'industry': ['TECH', 'FINANCE', 'RETAIL'],
        'region': ['USA', 'UK', 'Germany']
    }
    expected_df = pd.DataFrame(expected_data)
    
    result_df = transform(df)
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_drop_null_account_id_and_duplicates():
    data = {
        'account_id': ['ACC001', None, 'ACC002', 'ACC001', 'ACC003', None],
        'account_name': ['Comp A', 'Comp B', 'Comp C', 'Comp A Dup', 'Comp D', 'Comp E'],
        'industry': ['Tech', 'Finance', 'Retail', 'Tech', 'Manufacturing', 'Services'],
        'region': ['USA', 'UK', 'Germany', 'USA', 'France', 'Spain']
    }
    df = pd.DataFrame(data)
    
    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Comp A', 'Comp C', 'Comp D'],
        'industry': ['TECH', 'RETAIL', 'MANUFACTURING'],
        'region': ['USA', 'Germany', 'France']
    }
    expected_df = pd.DataFrame(expected_data)
    
    result_df = transform(df)
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_missing_industry_column():
    data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'region': ['USA', 'UK']
    }
    df = pd.DataFrame(data)

    expected_data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'region': ['USA', 'UK']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = transform(df)
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_empty_dataframe():
    df = pd.DataFrame(columns=['account_id', 'account_name', 'industry', 'region'])
    expected_df = pd.DataFrame(columns=['account_id', 'account_name', 'industry', 'region'])
    
    result_df = transform(df)
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_account_id_column_missing_raises_error():
    data = {
        'account_name': ['Company A'],
        'industry': ['Tech']
    }
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match="Column 'account_id' is missing from the DataFrame."):
        transform(df)