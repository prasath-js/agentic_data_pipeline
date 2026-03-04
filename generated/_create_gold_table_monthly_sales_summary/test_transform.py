import pandas as pd
import numpy as np
from transform import transform

def test_basic_monthly_summary():
    # Test case 1: Basic aggregation and sorting for multiple months
    data = {
        'transaction_id': [1, 2, 3, 4, 5, 6],
        'customer_id': [101, 102, 101, 103, 102, 104],
        'quantity': [1, 2, 1, 3, 2, 1],
        'amount': [10.0, 20.0, 15.0, 30.0, 25.0, 5.0],
        'transaction_date': ['2023-01-05', '2023-01-10', '2023-02-01', '2023-02-15', '2023-03-01', '2023-03-10']
    }
    df = pd.DataFrame(data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    result = transform(df)

    # Expected calculations:
    # 2023-01: revenue=10+20=30, count=2, mom_growth=NaN
    # 2023-02: revenue=15+30=45, count=2, mom_growth=(45-30)/30 * 100 = 50.0
    # 2023-03: revenue=25+5=30, count=2, mom_growth=(30-45)/45 * 100 = -33.33...

    expected_data = {
        'year_month': ['2023-01', '2023-02', '2023-03'],
        'monthly_revenue': [30.0, 45.0, 30.0],
        'transaction_count': [2, 2, 2],
        'mom_growth': [np.nan, 50.0, (30.0 - 45.0) / 45.0 * 100]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['mom_growth'] = expected_df['mom_growth'].astype(float) # Ensure float type for NaN comparison

    pd.testing.assert_frame_equal(result, expected_df, check_dtype=True, check_exact=False, rtol=1e-6)

def test_mom_growth_calculation_with_zeros_and_inf():
    # Test case 2: More complex MoM growth including zero revenue and division by zero scenarios
    data = {
        'transaction_id': [1, 2, 3, 4, 5, 6],
        'customer_id': [1, 2, 1, 3, 2, 4],
        'quantity': [1, 1, 1, 1, 1, 1],
        'amount': [100.0, 150.0, 50.0, 200.0, 0.0, 300.0], # 0.0 for a month
        'transaction_date': [
            '2023-01-10', '2023-01-20', # Jan: 250
            '2023-02-05',               # Feb: 50
            '2023-03-15',               # Mar: 200
            '2023-04-01',               # Apr: 0
            '2023-05-10'                # May: 300
        ]
    }
    df = pd.DataFrame(data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    result = transform(df)

    # Expected calculations:
    # Jan: revenue=250, count=2, mom_growth=NaN
    # Feb: revenue=50, count=1, mom_growth=(50-250)/250 * 100 = -80.0
    # Mar: revenue=200, count=1, mom_growth=(200-50)/50 * 100 = 300.0
    # Apr: revenue=0, count=1, mom_growth=(0-200)/200 * 100 = -100.0
    # May: revenue=300, count=1, mom_growth=(300-0)/0 * 100 = inf (pandas handles this as inf)

    expected_data = {
        'year_month': ['2023-01', '2023-02', '2023-03', '2023-04', '2023-05'],
        'monthly_revenue': [250.0, 50.0, 200.0, 0.0, 300.0],
        'transaction_count': [2, 1, 1, 1, 1],
        'mom_growth': [np.nan, -80.0, 300.0, -100.0, np.inf]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['mom_growth'] = expected_df['mom_growth'].astype(float)

    pd.testing.assert_frame_equal(result, expected_df, check_dtype=True, check_exact=False, rtol=1e-6)

def test_empty_dataframe_input():
    # Test case 3: Empty input DataFrame
    df = pd.DataFrame(columns=['transaction_id', 'customer_id', 'quantity', 'amount', 'transaction_date'])
    # Ensure 'transaction_date' column is of datetime type, even if empty
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    result = transform(df)

    expected_df = pd.DataFrame(columns=['year_month', 'monthly_revenue', 'transaction_count', 'mom_growth'])
    # Explicitly set dtypes for the expected empty DataFrame to match the transform function's output
    expected_df['year_month'] = expected_df['year_month'].astype(str)
    expected_df['monthly_revenue'] = expected_df['monthly_revenue'].astype(float)
    expected_df['transaction_count'] = expected_df['transaction_count'].astype(int)
    expected_df['mom_growth'] = expected_df['mom_growth'].astype(float)

    pd.testing.assert_frame_equal(result, expected_df, check_dtype=True)

def test_dataframe_with_unparseable_dates():
    # Test case 4: DataFrame with dates that cannot be parsed
    data = {
        'transaction_id': [1, 2, 3, 4],
        'customer_id': [101, 102, 103, 104],
        'quantity': [1, 2, 3, 4],
        'amount': [10.0, 20.0, 30.0, 40.0],
        'transaction_date': ['2023-01-01', 'invalid-date', '2023-01-15', '2023-02-01']
    }
    df = pd.DataFrame(data)

    result = transform(df)

    # Only valid dates should be processed
    # 2023-01: revenue=10.0 + 30.0 = 40.0, count=2
    # 2023-02: revenue=40.0, count=1
    expected_data = {
        'year_month': ['2023-01', '2023-02'],
        'monthly_revenue': [40.0, 40.0],
        'transaction_count': [2, 1],
        'mom_growth': [np.nan, (40.0 - 40.0) / 40.0 * 100] # 0.0
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['mom_growth'] = expected_df['mom_growth'].astype(float)

    pd.testing.assert_frame_equal(result, expected_df, check_dtype=True, check_exact=False, rtol=1e-6)

def test_single_month_data():
    # Test case 5: Data for only a single month
    data = {
        'transaction_id': [1, 2],
        'customer_id': [101, 102],
        'quantity': [1, 2],
        'amount': [100.0, 200.0],
        'transaction_date': ['2023-01-01', '2023-01-15']
    }
    df = pd.DataFrame(data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    result = transform(df)

    expected_data = {
        'year_month': ['2023-01'],
        'monthly_revenue': [300.0],
        'transaction_count': [2],
        'mom_growth': [np.nan]
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df['mom_growth'] = expected_df['mom_growth'].astype(float)

    pd.testing.assert_frame_equal(result, expected_df, check_dtype=True, check_exact=False, rtol=1e-6)