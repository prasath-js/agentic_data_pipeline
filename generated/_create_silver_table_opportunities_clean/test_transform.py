import pandas as pd
from pandas.testing import assert_frame_equal
from transform import transform

def test_strip_whitespace_and_null_drops():
    # Test data with leading/trailing spaces and nulls
    data = {
        'opportunity_id': [' OPP001 ', 'OPP002', 'OPP003', 'OPP004'],
        'account_id': [' ACC001 ', 'ACC002', None, 'ACC004'],
        'value': [100.0, '200.5', 'invalid', 400.0],
        'close_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
        'stage': [' Negotiation ', 'Closed Won', 'Prospecting', None],
        '_source_file': ['file1', 'file2', 'file3', 'file4'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3', 'ts4']
    }
    df_raw = pd.DataFrame(data)

    # Expected data after transformations
    expected_data = {
        'opportunity_id': ['OPP001', 'OPP002'],
        'account_id': ['ACC001', 'ACC002'],
        'value': [100.0, 200.5],
        'close_date': ['2023-01-01', '2023-01-02'],
        'stage': ['Negotiation', 'Closed Won'],
        '_source_file': ['file1', 'file2'],
        '_ingest_ts': ['ts1', 'ts2']
    }
    df_expected = pd.DataFrame(expected_data)
    df_expected['value'] = df_expected['value'].astype(float) # Ensure correct dtype

    df_transformed = transform(df_raw)

    # Sort both dataframes by a common column to ensure consistent order for comparison
    df_transformed = df_transformed.sort_values(by='opportunity_id').reset_index(drop=True)
    df_expected = df_expected.sort_values(by='opportunity_id').reset_index(drop=True)

    assert_frame_equal(df_transformed, df_expected)

def test_value_column_coercion_and_missing_value_column():
    # Test data with 'opportunity_value' and mixed types
    data_opp_value = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'opportunity_value': [100, '200.5', 'not_a_number'],
        'close_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'stage': ['Negotiation', 'Closed Won', 'Prospecting'],
        '_source_file': ['file1', 'file2', 'file3'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3']
    }
    df_raw_opp_value = pd.DataFrame(data_opp_value)

    expected_data_opp_value = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'opportunity_value': [100.0, 200.5, pd.NA], # pd.NA for coerced errors
        'close_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'stage': ['Negotiation', 'Closed Won', 'Prospecting'],
        '_source_file': ['file1', 'file2', 'file3'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3']
    }
    df_expected_opp_value = pd.DataFrame(expected_data_opp_value)
    df_expected_opp_value['opportunity_value'] = df_expected_opp_value['opportunity_value'].astype(float)

    df_transformed_opp_value = transform(df_raw_opp_value)
    assert_frame_equal(df_transformed_opp_value, df_expected_opp_value)

    # Test data without 'value' or 'opportunity_value'
    data_no_value_col = {
        'opportunity_id': ['OPP001'],
        'account_id': ['ACC001'],
        'close_date': ['2023-01-01'],
        'stage': ['Negotiation'],
        '_source_file': ['file1'],
        '_ingest_ts': ['ts1']
    }
    df_raw_no_value_col = pd.DataFrame(data_no_value_col)
    df_transformed_no_value_col = transform(df_raw_no_value_col)
    assert_frame_equal(df_transformed_no_value_col, df_raw_no_value_col) # Should remain unchanged