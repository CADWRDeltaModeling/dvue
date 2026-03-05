"""Tests for dvue utility functions."""

import pytest
from dvue.utils import get_unique_short_names


def test_get_unique_short_names_no_conflict():
    """Test that unique names are preserved."""
    paths = ['/path/to/file1.txt', '/other/path/file2.txt']
    result = get_unique_short_names(paths)
    assert result == ['file1.txt', 'file2.txt']


def test_get_unique_short_names_with_conflict():
    """Test that conflicts are resolved by adding parent directories."""
    paths = [
        '/path/to/data.csv',
        '/other/path/data.csv',
        '/another/location/data.csv'
    ]
    result = get_unique_short_names(paths)
    # Should differentiate by adding parent directories
    assert len(result) == 3
    assert len(set(result)) == 3  # All unique
    # Each should contain the basename
    for r in result:
        assert 'data.csv' in r


def test_get_unique_short_names_single_path():
    """Test with a single path."""
    paths = ['/path/to/file.txt']
    result = get_unique_short_names(paths)
    assert result == ['file.txt']


def test_get_unique_short_names_empty():
    """Test with empty input."""
    paths = []
    result = get_unique_short_names(paths)
    assert result == []
