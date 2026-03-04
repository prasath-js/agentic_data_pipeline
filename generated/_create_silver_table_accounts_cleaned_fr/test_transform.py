import pandas as pd
import pytest
from transform import transform

def test_transform_basic_cleaning():
    """
    Test basic cleaning: whitespace stripping, industry uppercasing, and null account_id dropping.
    """
    data = {
        'account_id': [' ACC001 ', 'ACC002', None, 'ACC004'],
        'account_name': ['  Company A  ', 'Company B', 'Company C', 'Company D'],
        'industry': [' tech ', 'finance', None, ' healthcare '],
        'region': ['USA', 'UK', 'Germany', 'France']
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC004'],
        'account_name': ['Company A', 'Company B', 'Company D'],
        'industry': ['TECH', 'FINANCE', 'HEALTHCARE'],
        'region': ['USA', 'UK', 'France']
    }
    df_expected = pd.DataFrame(expected_data)

    df_output = transform(df_input)

    pd.testing.assert_frame_equal(df_output.reset_index(drop=True), df_expected.reset_index(drop=True))

def test_transform_duplicate_handling():
    """
    Test duplicate account_id handling, keeping the first occurrence.
    """
    data = {
        'account_id': ['ACC001', 'ACC002', 'ACC001', 'ACC003', 'ACC002'],
        'account_name': ['Company A', 'Company B', 'Company A-dup', 'Company C', 'Company B-dup'],
        'industry': ['Tech', 'Finance', 'Healthcare', 'Retail', 'Manufacturing'],
        'region': ['USA', 'UK', 'Germany', 'France', 'Canada']
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Company A', 'Company B', 'Company C'],
        'industry': ['TECH', 'FINANCE', 'RETAIL'],
        'region': ['USA', 'UK', 'France']
    }
    df_expected = pd.DataFrame(expected_data)

    df_output = transform(df_input)

    pd.testing.assert_frame_equal(df_output.reset_index(drop=True), df_expected.reset_index(drop=True))

def test_transform_missing_industry_column():
    """
    Test that the transformation works correctly when the 'industry' column is missing.
    """
    data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'region': ['USA', 'UK']
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'region': ['USA', 'UK']
    }
    df_expected = pd.DataFrame(expected_data)

    df_output = transform(df_input)

    pd.testing.assert_frame_equal(df_output.reset_index(drop=True), df_expected.reset_index(drop=True))

def test_transform_empty_dataframe():
    """
    Test with an empty DataFrame.
    """
    data = {
        'account_id': [],
        'account_name': [],
        'industry': [],
        'region': []
    }
    df_input = pd.DataFrame(data, dtype=str) # Ensure string type for columns

    df_expected = pd.DataFrame(data, dtype=str)

    df_output = transform(df_input)

    pd.testing.assert_frame_equal(df_output.reset_index(drop=True), df_expected.reset_index(drop=True))

def test_transform_account_id_missing_raises_error():
    """
    Test that a ValueError is raised if 'account_id' column is missing.
    """
    data = {
        'account_name': ['Company A'],
        'industry': ['Tech']
    }
    df_input = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'account_id' is missing from the DataFrame."):
        transform(df_input)