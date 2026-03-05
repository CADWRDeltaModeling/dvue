"""Test configuration and fixtures for dvue tests."""

import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'station': ['A', 'B', 'C', 'D'],
        'value': [10.5, 20.3, 15.7, 30.2],
        'category': ['type1', 'type2', 'type1', 'type2']
    })


@pytest.fixture
def sample_timeseries():
    """Create a sample time series DataFrame."""
    dates = pd.date_range('2020-01-01', periods=100, freq='D')
    return pd.DataFrame({
        'datetime': dates,
        'station_A': np.random.randn(100).cumsum(),
        'station_B': np.random.randn(100).cumsum()
    })
