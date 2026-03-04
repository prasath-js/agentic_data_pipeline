import pandas as pd
import pytest
from transform import transform

def test_strip_whitespace():
    data = {
        'opportunity_id': [' OPP001 ', 'OPP002 ', ' OPP003'],
        'account_id': [' ACC001', 'ACC002', 'ACC003'],
        'value': [100, 200, 300],
        'close_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'stage': [' Negotiation ', 'Closed Won', ' Prospecting'],
        '_source_file': ['file1', 'file2', 'file3'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3']
    }
    df = pd.DataFrame(data)
    cleaned_df = transform(df)

    expected_data = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003'],
        'account_id': ['ACC001', 'ACC002', 'ACC003'],
        'value': [100, 200, 300],
        'close_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'stage': ['Negotiation', 'Closed Won', 'Prospecting'],
        '_source_file': ['file1', 'file2', 'file3'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3']
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(cleaned_df, expected_df)

def test_drop_nulls_and_coerce_numeric():
    data = {
        'opportunity_id': ['OPP001', 'OPP002', 'OPP003', 'OPP004', 'OPP005'],
        'account_id': ['ACC001', None, 'ACC003', 'ACC004', 'ACC005'],
        'value': ['100', '200.5', 'invalid', 400, None],
        'close_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'stage': ['Negotiation', 'Closed Won', None, 'Prospecting', 'Closed Lost'],
        '_source_file': ['file1', 'file2', 'file3', 'file4', 'file5'],
        '_ingest_ts': ['ts1', 'ts2', 'ts3', 'ts4', 'ts5']
    }
    df = pd.DataFrame(data)
    cleaned_df = transform(df)

    expected_data = {
        'opportunity_id': ['OPP001', 'OPP004', 'OPP005'],
        'account_id': ['ACC001', 'ACC004', 'ACC005'],
        'value': [100.0, 400.0, None],
        'close_date': ['2024-01-01', '2024-01-04', '2024-01-05'],
        'stage': ['Negotiation', 'Prospecting', 'Closed Lost'],
        '_source_file': ['file1', 'file4', 'file5'],
        '_ingest_ts': ['ts1', 'ts4', 'ts5']
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['value'] = expected_df['value'].astype(float) # Ensure correct dtype after coercion

    pd.testing.assert_frame_equal(cleaned_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_value_column_name_flexibility():
    data_opportunity_value = {
        'opportunity_id': ['OPP001'],
        'account_id': ['ACC001'],
        'opportunity_value': ['100'],
        'close_date': ['2024-01-01'],
        'stage': ['Negotiation'],
        '_source_file': ['file1'],
        '_ingest_ts': ['ts1']
    }
    df_op_value = pd.DataFrame(data_opportunity_value)
    cleaned_df_op_value = transform(df_op_value)
    assert cleaned_df_op_value['opportunity_value'].dtype == float
    assert cleaned_df_op_value['opportunity_value'].iloc[0] == 100.0

    data_value = {
        'opportunity_id': ['OPP001'],
        'account_id': ['ACC001'],
        'value': ['200'],
        'close_date': ['2024-01-01'],
        'stage': ['Negotiation'],
        '_source_file': ['file1'],
        '_ingest_ts': ['ts1']
    }
    df_value = pd.DataFrame(data_value)
    cleaned_df_value = transform(df_value)
    assert cleaned_df_value['value'].dtype == float
    assert cleaned_df_value['value'].iloc[0] == 200.0

    # Test with neither column present (should not error, no change to non-existent column)
    data_no_value_col = {
        'opportunity_id': ['OPP001'],
        'account_id': ['ACC001'],
        'amount': ['300'], # Different column name
        'close_date': ['2024-01-01'],
        'stage': ['Negotiation'],
        '_source_file': ['file1'],
        '_ingest_ts': ['ts1']
    }
    df_no_value = pd.DataFrame(data_no_value_col)
    cleaned_df_no_value = transform(df_no_value)
    assert 'amount' in cleaned_df_no_value.columns
    assert cleaned_df_no_value['amount'].dtype == object # Should remain object/string
    assert cleaned_df_no_value['amount'].iloc[0] == '300'