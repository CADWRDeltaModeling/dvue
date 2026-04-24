"""Tests for TimeSeriesDataUIManager FILE_NUM column logic."""

import pandas as pd
import pytest

from dvue.catalog import DataCatalog, DataReference, InMemoryDataReferenceReader
from dvue.tsdataui import TimeSeriesDataUIManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reader():
    """Shared flyweight reader returning a trivial DataFrame."""
    return InMemoryDataReferenceReader(pd.DataFrame({"value": [1.0, 2.0]}))


def _build_catalog(filenames):
    """Build a DataCatalog with one DataReference per filename entry.

    Each reference carries DSS-style attributes (A–F) plus *filename*.
    """
    reader = _make_reader()
    cat = DataCatalog()
    for i, fn in enumerate(filenames):
        cat.add(
            DataReference(
                reader=reader,
                name=f"ref_{i}",
                A="AREA",
                B=f"STA{i:03d}",
                C="EC",
                D="01JAN2020-31DEC2020",
                E="1HOUR",
                F="VER1",
                filename=fn,
            )
        )
    return cat


class _StubManager(TimeSeriesDataUIManager):
    """Minimal concrete subclass for unit-testing."""

    def __init__(self, catalog, filename_column="filename", **kwargs):
        self._test_catalog = catalog
        super().__init__(filename_column=filename_column, **kwargs)

    @property
    def data_catalog(self):
        return self._test_catalog

    def _get_table_column_width_map(self):
        return {
            "A": "15%",
            "B": "15%",
            "C": "15%",
            "E": "10%",
            "F": "15%",
            "D": "20%",
        }

    def get_table_filters(self):
        return {}

    def get_tooltips(self):
        return []

    def get_map_color_columns(self):
        return ["C"]

    def get_map_marker_columns(self):
        return ["C"]

    def get_name_to_color(self):
        return {}

    def get_name_to_marker(self):
        return {}

    def get_time_range(self, dfcat):
        return (pd.Timestamp("2020-01-01"), pd.Timestamp("2020-12-31"))


# ---------------------------------------------------------------------------
# Tests — single file (no FILE_NUM)
# ---------------------------------------------------------------------------


class TestSingleFile:
    def test_display_fileno_is_false(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        assert mgr.display_fileno is False

    def test_file_num_not_in_table_columns(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        assert "FILE_NUM" not in mgr.get_table_columns()

    def test_file_num_not_in_catalog_df(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "FILE_NUM" not in df.columns


# ---------------------------------------------------------------------------
# Tests — multiple files (FILE_NUM must exist)
# ---------------------------------------------------------------------------


class TestMultipleFiles:
    def test_display_fileno_is_true(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        assert mgr.display_fileno is True

    def test_file_num_in_table_columns(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        assert "FILE_NUM" in mgr.get_table_columns()

    def test_file_num_in_catalog_df(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "FILE_NUM" in df.columns

    def test_file_num_values_correct(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        # First file → 0, second file → 1
        assert list(df["FILE_NUM"]) == [0, 1]

    def test_get_data_catalog_consistent_across_calls(self):
        """Regression: second call must also include FILE_NUM."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df1 = mgr.get_data_catalog()
        df2 = mgr.get_data_catalog()
        assert "FILE_NUM" in df1.columns
        assert "FILE_NUM" in df2.columns
        assert list(df1["FILE_NUM"]) == list(df2["FILE_NUM"])


# ---------------------------------------------------------------------------
# Tests — table columns are a subset of catalog columns (regression guard)
# ---------------------------------------------------------------------------


class TestTableColumnsSubsetOfCatalog:
    """The KeyError that triggered this fix: get_table_columns() returned
    a column name that did not exist in the DataFrame from get_data_catalog()."""

    def test_single_file(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"

    def test_multiple_files(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"


# ---------------------------------------------------------------------------
# Tests — no filename column at all
# ---------------------------------------------------------------------------


class TestNoFilenameColumn:
    def test_display_fileno_false_when_no_column(self):
        reader = _make_reader()
        cat = DataCatalog()
        cat.add(DataReference(reader=reader, name="r0", X="val"))
        mgr = _StubManager(cat, filename_column="nonexistent")
        assert mgr.display_fileno is False

    def test_file_number_column_name_is_none(self):
        reader = _make_reader()
        cat = DataCatalog()
        cat.add(DataReference(reader=reader, name="r0", X="val"))
        mgr = _StubManager(cat, filename_column="nonexistent")
        assert mgr.file_number_column_name is None


# ---------------------------------------------------------------------------
# Tests — ClearCacheAction registered in get_data_actions
# ---------------------------------------------------------------------------


class TestClearCacheActionRegistered:
    def test_clear_cache_action_in_actions(self):
        from dvue.actions import ClearCacheAction
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        names = [a["name"] for a in mgr.get_data_actions()]
        assert "Clear Cache" in names

    def test_clear_cache_action_last(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        actions = mgr.get_data_actions()
        assert actions[-1]["name"] == "Clear Cache"

    def test_clear_cache_action_clears_catalog(self):
        """ClearCacheAction.callback must call invalidate_all_caches."""
        from dvue.actions import ClearCacheAction
        from unittest.mock import MagicMock, patch

        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)

        # Warm up the cache
        for ref in cat.list():
            ref.getData()
        assert any(ref._cached_data for ref in cat.list())

        # Build a fake dataui with the manager attached
        fake_dataui = MagicMock()
        fake_dataui._dataui_manager = mgr

        with patch("panel.state") as mock_state:
            mock_state.notifications = None  # suppress Panel notification
            ClearCacheAction().callback(None, fake_dataui)

        assert all(not ref._cached_data for ref in cat.list())
