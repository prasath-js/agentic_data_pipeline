import pandas as pd
import pytest

from transform import transform

def test_strip_whitespace_and_null_drops():
    # Test case 1: Basic functionality - whitespace stripping, null drops, numeric coercion
    data = {
        'opportunity_id': [' OPP001 ', 'OPP002', 'OPP003', 'OPP004', 'OPP005'],
        'account_id': [' ACC001 ', 'ACC002', None, 'ACC004', 'ACC005'],
        'value': ['100.50', '200', 'invalid', '300.75', None],
        'close_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'stage': [' Negotiation ', 'Closed Won', 'Prospecting', None, 'Closed Lost'],
        '_source_file': ['file1', 'file2', 'file3', 'file4', 'file5'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3', 'ts4', 'ts5']
    }
    df_raw = pd.DataFrame(data)
    df_cleaned = transform(df_raw)

    # Expected DataFrame after transformations
    # Rows with account_id=None (OPP003) and stage=None (OPP004) should be dropped.
    expected_data = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP005'],
        'account_id': ['ACC001', 'ACC002', 'ACC005'],
        'value': [100.50, 200.0, None],
        'close_date': ['2024-01-01', '2024-01-02', '2024-01-05'],
        'stage': ['Negotiation', 'Closed Won', 'Closed Lost'],
        '_source_file': ['file1', 'file2', 'file5'],
        '_ingest_ts': ['ts1', 'ts2', 'ts5']
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['value'] = expected_df['value'].astype(float) # Ensure float type for value column

    pd.testing.assert_frame_equal(df_cleaned.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_empty_and_all_null_cases():
    # Test case 2: Empty DataFrame
    empty_df = pd.DataFrame(columns=['opportunity_id', 'account_id', 'value', 'close_date', 'stage', '_source_file', '_ingest_ts'])
    cleaned_empty_df = transform(empty_df)
    pd.testing.assert_frame_equal(cleaned_empty_df, empty_df)

    # Test case 3: DataFrame with all nulls in critical columns
    all_null_data = {
        'opportunity_id': ['OPP001', 'OPP002'],
        'account_id': [None, None],
        'value': ['100', '200'],
        'close_date': ['2024-01-01', '2024-01-02'],
        'stage': ['Negotiation', None],
        '_source_file': ['file1', 'file2'],
        '_ingest_ts': ['ts1', 'ts2']
    }
    df_all_null = pd.DataFrame(all_null_data)
    cleaned_all_null_df = transform(df_all_null)
    # Expected: Both rows should be dropped because account_id is null in both,
    # and stage is null in the second.
    expected_all_null_df = pd.DataFrame(columns=['opportunity_id', 'account_id', 'value', 'close_date', 'stage', '_source_file', '_ingest_ts'])
    expected_all_null_df['value'] = expected_all_null_df['value'].astype(float) # Ensure float type for value column
    pd.testing.assert_frame_equal(cleaned_all_null_df.reset_index(drop=True), expected_all_null_df.reset_index(drop=True))

def test_value_column_coercion():
    # Test case 4: Specific test for value column coercion
    data = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003', 'OPP004'],
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004'],
        'value': ['123.45', '678', 'not_a_number', None],
        'close_date': ['2024-01-01'] * 4,
        'stage': ['Open'] * 4,
        '_source_file': ['file1'] * 4,
        '_ingest_ts': ['ts1'] * 4
    }
    df_raw = pd.DataFrame(data)
    df_cleaned = transform(df_raw)

    # When 'not_a_number' is coerced, it becomes NaN. None also becomes NaN in a float column.
    expected_values = pd.Series([123.45, 678.0, float('nan'), float('nan')], dtype=float)
    pd.testing.assert_series_equal(df_cleaned['value'].reset_index(drop=True), expected_values.reset_index(drop=True), check_names=False)