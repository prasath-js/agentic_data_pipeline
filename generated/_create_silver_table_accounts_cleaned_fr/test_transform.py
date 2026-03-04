import pandas as pd
# The transform function is defined in the same file, so it's directly accessible.
# For pytest, if this were in a separate file named 'transform.py',
# 'from transform import transform' would be correct.
# In a single file, it's implicitly available, but keeping the import
# makes the test suite self-contained and explicit about its dependency.
from transform import transform

def test_strip_whitespace_and_uppercase_industry():
    data = {
        'account_id': ['ACC001 ', ' ACC002', 'ACC003'],
        'account_name': ['  Company A  ', 'Company B', 'Company C '],
        'industry': [' tech ', '  finance  ', 'retail'],
        'region': ['USA', 'UK', 'Germany']
    }
    df_input = pd.DataFrame(data)
    df_expected = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Company A', 'Company B', 'Company C'],
        'industry': ['TECH', 'FINANCE', 'RETAIL'],
        'region': ['USA', 'UK', 'Germany']
    })
    df_output = transform(df_input)
    pd.testing.assert_frame_equal(df_output.reset_index(drop=True), df_expected.reset_index(drop=True))

def test_drop_null_account_id_and_duplicates():
    data = {
        'account_id': ['ACC001', None, 'ACC002', 'ACC001', 'ACC003', None],
        'account_name': ['Comp A', 'Comp B', 'Comp C', 'Comp A Old', 'Comp D', 'Comp E'],
        'industry': ['Tech', 'Finance', 'Retail', 'Tech', 'Manufacturing', 'Services'],
        'region': ['USA', 'UK', 'Germany', 'USA', 'France', 'Spain']
    }
    df_input = pd.DataFrame(data)
    df_expected = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Comp A', 'Comp C', 'Comp D'], # Keep first for ACC001
        'industry': ['TECH', 'RETAIL', 'MANUFACTURING'], # Uppercased
        'region': ['USA', 'Germany', 'France']
    })
    df_output = transform(df_input)
    # Sort both dataframes by account_id before comparison to handle potential order differences
    df_output_sorted = df_output.sort_values(by='account_id').reset_index(drop=True)
    df_expected_sorted = df_expected.sort_values(by='account_id').reset_index(drop=True)
    pd.testing.assert_frame_equal(df_output_sorted, df_expected_sorted)

def test_empty_dataframe():
    df_input = pd.DataFrame(columns=['account_id', 'account_name', 'industry', 'region'])
    df_output = transform(df_input)
    pd.testing.assert_frame_equal(df_output, df_input)

def test_no_industry_column():
    data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Comp A', 'Comp B'],
        'region': ['USA', 'UK']
    }
    df_input = pd.DataFrame(data)
    df_expected = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Comp A', 'Comp B'],
        'region': ['USA', 'UK']
    })
    df_output = transform(df_input)
    pd.testing.assert_frame_equal(df_output, df_expected)

def test_industry_with_nan():
    data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Comp A', 'Comp B'],
        'industry': [' tech ', None],
        'region': ['USA', 'UK']
    }
    df_input = pd.DataFrame(data)
    df_expected = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Comp A', 'Comp B'],
        'industry': ['TECH', None],
        'region': ['USA', 'UK']
    })
    df_output = transform(df_input)
    pd.testing.assert_frame_equal(df_output, df_expected)