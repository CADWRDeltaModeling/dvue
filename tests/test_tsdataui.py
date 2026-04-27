"""Tests for TimeSeriesDataUIManager url_num column logic."""

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

    def __init__(self, catalog, url_column="filename", **kwargs):
        self._test_catalog = catalog
        super().__init__(url_column=url_column, **kwargs)

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
# Tests — single file (no url_num)
# ---------------------------------------------------------------------------


class TestSingleFile:
    def test_display_url_num_is_false(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        assert mgr.display_url_num is False

    def test_url_num_not_in_table_columns(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        assert "url_num" not in mgr.get_table_columns()

    def test_url_num_not_in_catalog_df(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "url_num" not in df.columns


# ---------------------------------------------------------------------------
# Tests — multiple files (url_num must exist)
# ---------------------------------------------------------------------------


class TestMultipleFiles:
    def test_display_url_num_is_true(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        assert mgr.display_url_num is True

    def test_url_num_in_table_columns(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        assert "url_num" in mgr.get_table_columns()

    def test_url_num_in_catalog_df(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "url_num" in df.columns

    def test_url_num_values_correct(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        # First file → 0, second file → 1
        assert list(df["url_num"]) == [0, 1]

    def test_get_data_catalog_consistent_across_calls(self):
        """Regression: second call must also include url_num."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df1 = mgr.get_data_catalog()
        df2 = mgr.get_data_catalog()
        assert "url_num" in df1.columns
        assert "url_num" in df2.columns
        assert list(df1["url_num"]) == list(df2["url_num"])


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
    def test_display_url_num_false_when_no_column(self):
        reader = _make_reader()
        cat = DataCatalog()
        cat.add(DataReference(reader=reader, name="r0", X="val"))
        mgr = _StubManager(cat, url_column="nonexistent")
        assert mgr.display_url_num is False

    def test_url_num_column_is_none(self):
        reader = _make_reader()
        cat = DataCatalog()
        cat.add(DataReference(reader=reader, name="r0", X="val"))
        mgr = _StubManager(cat, url_column="nonexistent")
        assert mgr.url_num_column is None


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


# ---------------------------------------------------------------------------
# Tests — url_num dynamic metadata searchable in catalog
# ---------------------------------------------------------------------------


class TestUrlNumSearchable:
    """After _apply_url_num with a catalog, refs get url/url_num dynamic metadata
    so that catalog.search(url_num=0) correctly filters by source file."""

    def test_single_file_no_dynamic_metadata_injected(self):
        """Single-file catalog: no url_num dynamic metadata needed."""
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()  # triggers _apply_url_num
        ref = cat.get("ref_0")
        # No url_num should be set (only 1 file, display_url_num=False)
        assert ref.get_dynamic_metadata("url_num") is None

    def test_url_num_dynamic_metadata_injected_on_multi_file(self):
        """Multi-file catalog: refs must carry url_num as dynamic metadata."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()
        assert cat.get("ref_0").get_dynamic_metadata("url_num") == 0
        assert cat.get("ref_1").get_dynamic_metadata("url_num") == 1

    def test_url_dynamic_metadata_injected(self):
        """url dynamic metadata must also be set to the actual filename value."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()
        assert cat.get("ref_0").get_dynamic_metadata("url") == "file_a.dss"
        assert cat.get("ref_1").get_dynamic_metadata("url") == "file_b.dss"

    def test_catalog_search_by_url_num(self):
        """catalog.search(url_num=0) must return only refs from the first file."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()  # inject dynamic metadata
        results = cat.search(url_num=0)
        assert len(results) == 1
        assert results[0].name == "ref_0"

    def test_catalog_search_by_url_num_1(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()
        results = cat.search(url_num=1)
        assert len(results) == 1
        assert results[0].name == "ref_1"

    def test_math_ref_search_map_filters_by_url_num(self):
        """A MathDataReference with search_map url_num: 0 must resolve to file-0 refs."""
        from dvue.math_reference import MathDataReference
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        mgr.get_data_catalog()  # inject dynamic metadata
        ref = MathDataReference(
            expression="x * 2",
            search_map={"x": {"C": "EC", "url_num": 0}},
        )
        ref.set_catalog(cat)
        variables = ref._resolve_variables()
        assert "x" in variables
        # Only the file_a.dss ref should be matched (url_num=0)
        matched_ref = cat.search(C="EC", url_num=0)
        assert len(matched_ref) == 1
        assert matched_ref[0].name == "ref_0"
