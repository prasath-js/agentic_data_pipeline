import pytest
import duckdb
import pandas as pd
import os

DB_PATH = os.environ.get("DB_PATH", "data/pipeline.duckdb")

def test_opportunities_cleaned_table_exists_and_not_empty():
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT * FROM silver.opportunities_cleaned").df()
        assert not df.empty, "silver.opportunities_cleaned should not be empty"
    finally:
        con.close()

def test_no_nulls_in_key_columns_opportunities_cleaned():
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT account_id, stage FROM silver.opportunities_cleaned").df()
        assert df['account_id'].isnull().sum() == 0, "account_id should not have nulls in silver.opportunities_cleaned"
        assert df['stage'].isnull().sum() == 0, "stage should not have nulls in silver.opportunities_cleaned"
    finally:
        con.close()

def test_opportunity_value_is_numeric_opportunities_cleaned():
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT value FROM silver.opportunities_cleaned").df()
        assert pd.api.types.is_numeric_dtype(df['value']), "value column should be numeric in silver.opportunities_cleaned"
    finally:
        con.close()

def test_string_columns_stripped_opportunities_cleaned():
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute("SELECT opportunity_id, stage FROM silver.opportunities_cleaned").df()
        # Check a few string columns for leading/trailing whitespace
        # Convert to string type explicitly to handle potential None/NaN values gracefully before regex
        assert not df['opportunity_id'].astype(str).str.contains(r'^\s|\s$').any(), "opportunity_id should be stripped"
        assert not df['stage'].astype(str).str.contains(r'^\s|\s$').any(), "stage should be stripped"
    finally:
        con.close()

def test_rejected_rows_table_exists():
    con = duckdb.connect(DB_PATH)
    try:
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0
        assert table_exists, "rejected.rejected_rows table should exist if there were rejections"
    finally:
        con.close()

def test_rejected_rows_content_and_reason():
    con = duckdb.connect(DB_PATH)
    try:
        # Only run this test if the rejected_rows table actually exists and has data
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0

        if table_exists:
            rejected_df = con.execute("SELECT account_id, stage, rejection_reason FROM rejected.rejected_rows").df()
            assert not rejected_df.empty, "rejected.rejected_rows should not be empty if the table exists"

            # Ensure all rejected rows have a null in account_id or stage
            assert rejected_df.apply(lambda row: pd.isna(row['account_id']) or pd.isna(row['stage']), axis=1).all(), \
                "All rejected rows must have null account_id or null stage"

            # Ensure rejection_reason is populated and not empty
            assert rejected_df['rejection_reason'].notna().all(), "rejection_reason should be populated for all rejected rows"
            assert (rejected_df['rejection_reason'].str.len() > 0).all(), "rejection_reason should not be empty strings"
        else:
            pytest.skip("rejected.rejected_rows table does not exist, skipping content test.")
    finally:
        con.close()

def test_total_row_counts():
    con = duckdb.connect(DB_PATH)
    try:
        raw_count = con.execute("SELECT count(*) FROM bronze.opportunities_raw").fetchone()[0]
        cleaned_count = con.execute("SELECT count(*) FROM silver.opportunities_cleaned").fetchone()[0]
        
        # Check if rejected.rejected_rows table exists before querying its count
        rejected_table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'rejected' AND table_name = 'rejected_rows'"
        ).fetchone()[0] > 0
        
        rejected_count = 0
        if rejected_table_exists:
            rejected_count = con.execute("SELECT count(*) FROM rejected.rejected_rows").fetchone()[0]

        assert raw_count == (cleaned_count + rejected_count), "Total rows in raw should equal cleaned + rejected"
    finally:
        con.close()