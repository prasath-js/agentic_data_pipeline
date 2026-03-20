```python
import pytest
import pandas as pd
from pathlib import Path
import os
import logging

# Configure logging for tests to suppress actual output during testing,
# or direct it to a test-specific log.
# For unit tests, we generally don't want actual logging output to console unless debugging.
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("src").setLevel(logging.INFO) # Set a higher level for pipeline logs if desired

# Mock os.getenv for environment variables if the gold layer were to use them directly
# For local_files, it's less likely for the Gold layer itself, but good practice.
# In this specific case, gold_local_files doesn't seem to need env vars for its core logic.
# If it were writing to S3 or a DB, this mock would be crucial.
@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocks environment variables that the gold layer might use."""
    # Example: If gold layer needed an output bucket or similar
    # monkeypatch.setenv("OUTPUT_BUCKET", "test-output-bucket")
    pass

# Assuming the gold layer function is in src/gold/gold_local_files.py
# and named 'gold_local_files'
try:
    from src.gold.gold_local_files import gold_local_files
except ImportError:
    pytest.fail("Could not import gold_local_files. Ensure src/gold/gold_local_files.py exists and defines gold_local_files.")


@pytest.fixture
def sample_silver_df() -> pd.DataFrame:
    """
    Provides a sample Silver layer DataFrame for testing the Gold layer.
    This DataFrame simulates the output of the Silver layer.
    """
    data = {
        "opportunity_id": [1, 1, 2, 3, 3],
        "account_id": ["A1", "A1", "A2", "A1", "A3"],
        "value": [100.0, 200.0, 150.0, 50.0, 300.0],
        "close_date": ["2023-01-15", "2023-01-15", "2023-02-01", "2023-03-10", "2023-03-10"],
        "stage": ["Closed Won", "Closed Lost", "Open", "Closed Won", "Open"],
        "transaction_id": ["T1", "T2", "T3", "T4", "T5"],
        "customer_id": ["C1", "C1", "C2", "C1", "C3"],
        "quantity": [1, 2, 1, 1, 3],
        "amount":