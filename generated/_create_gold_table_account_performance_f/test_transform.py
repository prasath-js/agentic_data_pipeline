import pandas as pd
import numpy as np
import pytest
from transform import transform # Assuming 'transform' function is in a module named 'transform.py' or accessible

def test_transform_basic_functionality():
    """
    Tests basic aggregation, win rate calculation, and ranking across multiple industries.
    """
    # Input DataFrame simulating the result of a left join
    data = {
        'account_id': ['A1', 'A1', 'A2', 'A2', 'A3', 'A4', 'A4', 'A5'],
        'account_name': ['Acc1', 'Acc1', 'Acc2', 'Acc2', 'Acc3', 'Acc4', 'Acc4', 'Acc5'],
        'industry': ['Tech', 'Tech', 'Retail', 'Retail', 'Tech', 'Finance', 'Finance', 'Tech'],
        'region': ['USA', 'USA', 'EU', 'EU', 'USA', 'USA', 'USA', 'EU'],
        'opportunity_id': ['OP1', 'OP2', 'OP3', 'OP4', 'OP5', 'OP6', 'OP7', 'OP8'],
        'value': [100.0, 200.0, 50.0, 150.0, 300.0, 1000.0, 500.0, 50.0],
        'stage': ['Closed Won', 'Closed Lost', 'Closed Won', 'Negotiation', 'Closed Won', 'Closed Won', 'Closed Lost', 'Closed Won']
    }
    df_input = pd.DataFrame(data)

    # Expected output DataFrame
    expected_data = {
        'account_id': ['A1', 'A2', 'A3', 'A4', 'A5'],
        'account_name': ['Acc1', 'Acc2', 'Acc3', 'Acc4', 'Acc5'],
        'industry': ['Tech', 'Retail', 'Tech', 'Finance', 'Tech'],
        'region': ['USA', 'EU', 'USA', 'USA', 'EU'],
        'pipeline_value': [300.0, 200.0, 300.0, 1500.0, 50.0],
        'opportunity_count': [2, 2, 1, 2, 1],
        'win_rate': [0.5, 0.5, 1.0, 0.5, 1.0],
        'rank_in_industry': [1, 1, 1, 1, 2] # Tech: A1=300 (rank 1), A3=300 (rank 1), A5=50 (rank 2)
    }
    df_expected = pd.DataFrame(expected_data)

    df_result = transform(df_input)

    # Sort both DataFrames for consistent comparison
    df_result = df_result.sort_values(by='account_id').reset_index(drop=True)
    df_expected = df_expected.sort_values(by='account_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=True)

def test_transform_edge_cases():
    """
    Tests edge cases like accounts with no opportunities, all lost/won opportunities,
    NaN industry, and single-account industries.
    """
    # Input DataFrame with edge cases
    data = {
        'account_id': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
        'account_name': ['BAcc1', 'BAcc2', 'BAcc3', 'BAcc4', 'BAcc5', 'BAcc6', 'BAcc7'],
        'industry': ['Health', 'Health', 'Edu', 'Edu', np.nan, 'Health', 'Solo'],
        'region': ['UK', 'UK', 'USA', 'USA', 'EU', 'UK', 'USA'],
        'opportunity_id': ['OP1', 'OP2', 'OP3', 'OP4', 'OP5', None, 'OP6'], # B6 has no opportunities
        'value': [100.0, 200.0, 50.0, 150.0, 300.0, np.nan, 75.0],
        'stage': ['Closed Lost', 'Closed Won', 'Closed Won', 'Negotiation', 'Closed Won', np.nan, 'Closed Won']
    }
    df_input = pd.DataFrame(data)

    # Expected output DataFrame
    expected_data = {
        'account_id': ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'],
        'account_name': ['BAcc1', 'BAcc2', 'BAcc3', 'BAcc4', 'BAcc5', 'BAcc6', 'BAcc7'],
        'industry': ['Health', 'Health', 'Edu', 'Edu', np.nan, 'Health', 'Solo'],
        'region': ['UK', 'UK', 'USA', 'USA', 'EU', 'UK', 'USA'],
        'pipeline_value': [100.0, 200.0, 50.0, 150.0, 300.0, 0.0, 75.0],
        'opportunity_count': [1, 1, 1, 1, 1, 0, 1],
        'win_rate': [0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0],
        'rank_in_industry': [2, 1, 2, 1, 1, 3, 1] # Health: B2=200 (1), B1=100 (2), B6=0 (3)
                                                  # Edu: B4=150 (1), B3=50 (2)
                                                  # NaN: B5=300 (1)
                                                  # Solo: B7=75 (1)
    }
    df_expected = pd.DataFrame(expected_data)

    df_result = transform(df_input)

    # Sort both DataFrames for consistent comparison
    df_result = df_result.sort_values(by='account_id').reset_index(drop=True)
    df_expected = df_expected.sort_values(by='account_id').reset_index(drop=True)

    pd.testing.assert_frame_equal(df_result, df_expected, check_dtype=True)