import pandas as pd
import pytest
from transform import transform

def test_strip_whitespace_and_uppercase_industry():
    """
    Test that whitespace is stripped from string columns and industry is uppercased.
    """
    data = {
        'account_id': [' ACC001 ', 'ACC002'],
        'account_name': [' Company A ', 'Company B'],
        'industry': [' tech ', ' finance '],
        'region': [' USA ', 'UK'],
        '_source_file': ['file1.csv', 'file2.csv'],
        '_ingest_ts': ['ts1', 'ts2']
    }
    df_raw = pd.DataFrame(data)
    df_cleaned = transform(df_raw)

    expected_data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'industry': ['TECH', 'FINANCE'],
        'region': ['USA', 'UK'],
        '_source_file': ['file1.csv', 'file2.csv'],
        '_ingest_ts': ['ts1', 'ts2']
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df_cleaned.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_drop_null_account_id_and_duplicates():
    """
    Test that rows with null account_id are dropped and duplicates are handled.
    """
    data = {
        'account_id': ['ACC001', None, 'ACC002', 'ACC001', 'ACC003'],
        'account_name': ['Company A', 'Null Co', 'Company B', 'Company A Dup', 'Company C'],
        'industry': ['Tech', 'Other', 'Finance', 'Healthcare', 'Retail'],
        'region': ['USA', 'UK', 'Germany', 'USA', 'France'],
        '_source_file': ['f1', 'f1', 'f2', 'f1', 'f3'],
        '_ingest_ts': ['ts1', 'ts1', 'ts2', 'ts1', 'ts3']
    }
    df_raw = pd.DataFrame(data)
    df_cleaned = transform(df_raw)

    expected_data = {
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'account_name': ['Company A', 'Company B', 'Company C'],
        'industry': ['TECH', 'FINANCE', 'RETAIL'],
        'region': ['USA', 'Germany', 'France'],
        '_source_file': ['f1', 'f2', 'f3'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3']
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df_cleaned.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_missing_industry_column():
    """
    Test that the transform function handles cases where the 'industry' column is missing
    without raising an error, and other operations still apply.
    """
    data = {
        'account_id': [' ACC001 ', 'ACC002', 'ACC001'],
        'account_name': [' Company A ', 'Company B', 'Company A Dup'],
        'region': [' USA ', 'UK', 'USA'],
        '_source_file': ['file1.csv', 'file2.csv', 'file1.csv'],
        '_ingest_ts': ['ts1', 'ts2', 'ts1']
    }
    df_raw = pd.DataFrame(data)
    df_cleaned = transform(df_raw)

    expected_data = {
        'account_id': ['ACC001', 'ACC002'],
        'account_name': ['Company A', 'Company B'],
        'region': ['USA', 'UK'],
        '_source_file': ['file1.csv', 'file2.csv'],
        '_ingest_ts': ['ts1', 'ts2']
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df_cleaned.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_account_id_missing_raises_error():
    """
    Test that a ValueError is raised if 'account_id' column is missing.
    """
    data = {
        'account_name': ['Company A'],
        'industry': ['Tech']
    }
    df_raw = pd.DataFrame(data)
    with pytest.raises(ValueError, match="The 'account_id' column is missing"):
        transform(df_raw)