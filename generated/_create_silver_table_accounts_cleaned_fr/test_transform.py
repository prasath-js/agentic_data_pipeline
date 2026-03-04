import pandas as pd
import pytest
from transform import transform

def test_transform_basic_cleaning_and_deduplication():
    """
    Tests whitespace stripping, industry uppercasing, null account_id dropping,
    and duplicate account_id removal (keeping the first).
    """
    data = {
        'account_id': [' ACC001 ', ' ACC002 ', ' ACC001 ', None, ' ACC003 '],
        'account_name': ['  Account One  ', 'Account Two', 'Account One Dup', 'Null Account', 'Account Three'],
        'industry': [' tech ', ' finance ', ' healthcare ', ' retail ', None],
        'region': ['USA', 'UK', 'USA', 'Canada', 'Germany'],
        'other_string': ['  data1  ', 'data2', 'data3', 'data4', 'data5']
    }
    df_input = pd.DataFrame(data)

    # Expected output after transformations
    # - ' ACC001 ' (index 0) is kept, ' ACC001 ' (index 2) is dropped.
    # - None account_id (index 3) is dropped.
    # - Whitespace stripped from all string columns.
    # - Industry uppercased.
    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Account One', 'Account Two', 'Account Three'],
        'industry': ['TECH', 'FINANCE', None],
        'region': ['USA', 'UK', 'Germany'],
        'other_string': ['data1', 'data2', 'data5']
    }
    df_expected = pd.DataFrame(expected_data)

    df_result = transform(df_input)

    # Sort both DataFrames by 'account_id' to ensure consistent comparison regardless of row order
    df_result = df_result.sort_values(by='account_id').reset_index(drop=True)
    df_expected = df_expected.sort_values(by='account_id').reset_index(drop=True)

    # Use check_dtype=False because pandas might infer different dtypes (e.g., object vs. float for None/NaN)
    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=False)

def test_transform_missing_industry_column():
    """
    Tests that the transformation works correctly when the 'industry' column is absent.
    Other operations (stripping, null drop, deduplication) should still apply.
    """
    data = {
        'account_id': [' ACC001 ', ' ACC002 ', ' ACC001 '],
        'account_name': ['  Account One  ', 'Account Two', 'Account One Dup'],
        'region': ['USA', 'UK', 'USA']
    }
    df_input = pd.DataFrame(data)

    expected_data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Account One', 'Account Two'],
        'region': ['USA', 'UK']
    }
    df_expected = pd.DataFrame(expected_data)

    df_result = transform(df_input)

    df_result = df_result.sort_values(by='account_id').reset_index(drop=True)
    df_expected = df_expected.sort_values(by='account_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(df_result, df_expected)

def test_transform_all_null_account_ids():
    """
    Tests that if all 'account_id' values are null, the resulting DataFrame is empty.
    """
    data = {
        'account_id': [None, None, None],
        'account_name': ['A', 'B', 'C'],
        'industry': ['Tech', 'Finance', 'Retail'],
        'region': ['USA', 'UK', 'Germany']
    }
    df_input = pd.DataFrame(data)

    df_result = transform(df_input)
    assert df_result.empty
    # Ensure columns are preserved even if no rows remain
    assert list(df_result.columns) == list(df_input.columns)

def test_transform_empty_dataframe():
    """
    Tests the transformation with an empty input DataFrame.
    Should return an empty DataFrame with the same columns.
    """
    df_input = pd.DataFrame(columns=['account_id', 'account_name', 'industry', 'region'])
    df_result = transform(df_input)
    assert df_result.empty
    assert list(df_result.columns) == list(df_input.columns)

def test_transform_account_id_missing_raises_error():
    """
    Tests that a ValueError is raised if the 'account_id' column is entirely missing.
    """
    data = {
        'account_name': ['Account One'],
        'industry': ['Tech']
    }
    df_input = pd.DataFrame(data)
    with pytest.raises(ValueError, match="DataFrame must contain an 'account_id' column."):
        transform(df_input)