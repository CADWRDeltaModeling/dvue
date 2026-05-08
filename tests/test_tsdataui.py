"""Tests for TimeSeriesDataUIManager source_num column logic."""

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
    Uses primary_key=["name"] so explicit names are used as pk.
    """
    reader = _make_reader()
    cat = DataCatalog(primary_key=["name"])
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
                source=fn,
            )
        )
    return cat


class _StubManager(TimeSeriesDataUIManager):
    """Minimal concrete subclass for unit-testing."""

    def __init__(self, catalog, **kwargs):
        self._test_catalog = catalog
        super().__init__(**kwargs)

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
# Tests — single source (no source_num)
# ---------------------------------------------------------------------------


class TestSingleSource:
    def test_source_num_not_in_catalog_df(self):
        """Single-source catalog: source_num column NOT injected."""
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "source_num" not in df.columns

    def test_table_columns_subset_of_catalog_single(self):
        cat = _build_catalog(["file_a.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"


# ---------------------------------------------------------------------------
# Tests — multiple sources (source_num injected automatically)
# ---------------------------------------------------------------------------


class TestMultipleSources:
    def test_source_num_in_catalog_df(self):
        """Multi-source catalog: source_num column injected automatically."""
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert "source_num" in df.columns

    def test_source_num_values_correct(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        assert list(df["source_num"]) == [0, 1]

    def test_get_data_catalog_consistent_across_calls(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df1 = mgr.get_data_catalog()
        df2 = mgr.get_data_catalog()
        assert "source_num" in df1.columns
        assert list(df1["source_num"]) == list(df2["source_num"])

    def test_table_columns_subset_of_catalog_multi(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        mgr = _StubManager(cat)
        df = mgr.get_data_catalog()
        missing = set(mgr.get_table_columns()) - set(df.columns)
        assert missing == set(), f"Columns {missing} not in catalog DataFrame"


# ---------------------------------------------------------------------------
# Tests — catalog search by source_num
# ---------------------------------------------------------------------------


class TestSourceNumSearchable:
    def test_catalog_search_by_source_num_0(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        results = cat.search(source_num=0)
        assert len(results) == 1
        assert results[0].name == "ref_0"

    def test_catalog_search_by_source_num_1(self):
        cat = _build_catalog(["file_a.dss", "file_b.dss"])
        results = cat.search(source_num=1)
        assert len(results) == 1
        assert results[0].name == "ref_1"


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
# Helpers for transform tests
# ---------------------------------------------------------------------------

def _make_hourly_series(n_hours=72, start="2020-01-01"):
    """Return a single-column DataFrame with hourly DatetimeIndex."""
    idx = pd.date_range(start=start, periods=n_hours, freq="1h")
    return pd.DataFrame({"value": range(n_hours)}, index=idx, dtype="float64")


def _make_daily_series(n_days=30, start="2020-01-01"):
    idx = pd.date_range(start=start, periods=n_days, freq="1D")
    return pd.DataFrame({"value": range(n_days)}, index=idx, dtype="float64")


class _TransformManager(_StubManager):
    """Stub with is_irregular always False (regular data)."""

    def is_irregular(self, r):
        return False

    def get_data_reference(self, row):
        cat = self._test_catalog
        return cat.get(row["name"])


def _manager_with_defaults():
    cat = _build_catalog(["file_a.dss"])
    return _TransformManager(cat)


def _run(mgr, data, row=None):
    """Invoke _process_curve_data with a wide-open time range."""
    if row is None:
        row = pd.Series({"name": "ref_0"})
    start = data.index[0]
    end = data.index[-1]
    return mgr._process_curve_data(data.copy(), row, (start, end))


# ---------------------------------------------------------------------------
# Tests — _process_curve_data: resample
# ---------------------------------------------------------------------------

class TestResampleTransform:
    def test_resample_daily_mean(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = "1D"
        mgr.resample_agg = "mean"
        data = _make_hourly_series(48)
        result = _run(mgr, data)
        # 48 hours → 2 daily rows
        assert len(result) == 2
        # Daily mean of hours 0-23 = 11.5
        assert abs(result.iloc[0, 0] - 11.5) < 1e-9

    def test_resample_daily_max(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = "1D"
        mgr.resample_agg = "max"
        data = _make_hourly_series(48)
        result = _run(mgr, data)
        assert abs(result.iloc[0, 0] - 23.0) < 1e-9

    def test_resample_daily_min(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = "1D"
        mgr.resample_agg = "min"
        data = _make_hourly_series(48)
        result = _run(mgr, data)
        assert abs(result.iloc[0, 0] - 0.0) < 1e-9

    def test_resample_disabled_when_empty_string(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = ""
        data = _make_hourly_series(48)
        result = _run(mgr, data)
        assert len(result) == 48

    def test_resample_invalid_period_no_crash(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = "NOTVALID"
        data = _make_hourly_series(24)
        # Should not raise; returns original data
        result = _run(mgr, data)
        assert len(result) == 24

    def test_resample_sum(self):
        mgr = _manager_with_defaults()
        mgr.resample_period = "1D"
        mgr.resample_agg = "sum"
        data = _make_hourly_series(24)
        result = _run(mgr, data)
        assert len(result) == 1
        # sum(0..23) = 276
        assert abs(result.iloc[0, 0] - 276.0) < 1e-9


# ---------------------------------------------------------------------------
# Tests — _process_curve_data: rolling
# ---------------------------------------------------------------------------

class TestRollingTransform:
    def test_rolling_mean(self):
        mgr = _manager_with_defaults()
        mgr.rolling_window = "3h"
        mgr.rolling_agg = "mean"
        data = _make_hourly_series(24)
        result = _run(mgr, data)
        # Time-based rolling always produces a value (uses all data within window).
        # At t=0: window contains only [0] → mean = 0.0
        # At t=2h: window contains [0, 1, 2] → mean = 1.0
        assert len(result) == 24
        assert abs(result.iloc[0, 0] - 0.0) < 1e-9
        assert abs(result.iloc[2, 0] - 1.0) < 1e-9

    def test_rolling_max(self):
        mgr = _manager_with_defaults()
        mgr.rolling_window = "3h"
        mgr.rolling_agg = "max"
        data = _make_hourly_series(24)
        result = _run(mgr, data)
        assert abs(result.iloc[2, 0] - 2.0) < 1e-9

    def test_rolling_disabled_when_empty_string(self):
        mgr = _manager_with_defaults()
        mgr.rolling_window = ""
        data = _make_hourly_series(24)
        result = _run(mgr, data)
        assert len(result) == 24
        # No NaN introduced by rolling
        assert not result.iloc[:, 0].isna().any()

    def test_rolling_invalid_window_no_crash(self):
        mgr = _manager_with_defaults()
        mgr.rolling_window = "NOTVALID"
        data = _make_hourly_series(24)
        result = _run(mgr, data)
        assert len(result) == 24


# ---------------------------------------------------------------------------
# Tests — _process_curve_data: differencing
# ---------------------------------------------------------------------------

class TestDiffTransform:
    def test_diff_default_period(self):
        mgr = _manager_with_defaults()
        mgr.do_diff = True
        data = _make_daily_series(5)
        result = _run(mgr, data)
        assert pd.isna(result.iloc[0, 0])  # first element is NaN after diff
        # All subsequent diffs of consecutive integers are 1.0
        for i in range(1, 5):
            assert abs(result.iloc[i, 0] - 1.0) < 1e-9

    def test_diff_periods_2(self):
        mgr = _manager_with_defaults()
        mgr.do_diff = True
        mgr.diff_periods = 2
        data = _make_daily_series(5)
        result = _run(mgr, data)
        assert pd.isna(result.iloc[0, 0])
        assert pd.isna(result.iloc[1, 0])
        assert abs(result.iloc[2, 0] - 2.0) < 1e-9

    def test_diff_disabled(self):
        mgr = _manager_with_defaults()
        mgr.do_diff = False
        data = _make_daily_series(5)
        result = _run(mgr, data)
        assert abs(result.iloc[0, 0] - 0.0) < 1e-9


# ---------------------------------------------------------------------------
# Tests — _process_curve_data: cumsum
# ---------------------------------------------------------------------------

class TestCumsumTransform:
    def test_cumsum(self):
        mgr = _manager_with_defaults()
        mgr.do_cumsum = True
        data = _make_daily_series(5)
        result = _run(mgr, data)
        expected = [0, 1, 3, 6, 10]
        for i, exp in enumerate(expected):
            assert abs(result.iloc[i, 0] - exp) < 1e-9

    def test_cumsum_disabled(self):
        mgr = _manager_with_defaults()
        mgr.do_cumsum = False
        data = _make_daily_series(5)
        result = _run(mgr, data)
        assert abs(result.iloc[0, 0] - 0.0) < 1e-9
        assert abs(result.iloc[4, 0] - 4.0) < 1e-9


# ---------------------------------------------------------------------------
# Tests — _process_curve_data: scale factor
# ---------------------------------------------------------------------------

class TestScaleFactorTransform:
    def test_scale_by_2(self):
        mgr = _manager_with_defaults()
        mgr.scale_factor = 2.0
        data = _make_daily_series(5)
        result = _run(mgr, data)
        for i in range(5):
            assert abs(result.iloc[i, 0] - i * 2.0) < 1e-9

    def test_scale_1_is_noop(self):
        mgr = _manager_with_defaults()
        mgr.scale_factor = 1.0
        data = _make_daily_series(5)
        result = _run(mgr, data)
        for i in range(5):
            assert abs(result.iloc[i, 0] - float(i)) < 1e-9

    def test_scale_negative(self):
        mgr = _manager_with_defaults()
        mgr.scale_factor = -1.0
        data = _make_daily_series(3)
        result = _run(mgr, data)
        assert abs(result.iloc[1, 0] - (-1.0)) < 1e-9


# ---------------------------------------------------------------------------
# Tests — TransformToCatalogAction expression builder
# ---------------------------------------------------------------------------

class TestTransformToCatalogActionExpressionBuilder:
    """Unit-test the static expression/tag builder without a live Panel session."""

    def _mgr(self, **kwargs):
        mgr = _manager_with_defaults()
        for k, v in kwargs.items():
            setattr(mgr, k, v)
        return mgr

    def test_no_transforms_gives_empty_tag(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr()
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert expr == "x"
        assert tag == ""

    def test_resample_only(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(resample_period="1D", resample_agg="mean")
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert "resample('1D')" in expr
        assert ".mean()" in expr
        assert tag == "1D_mean"

    def test_rolling_only(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(rolling_window="24H", rolling_agg="mean")
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert "rolling('24H')" in expr
        assert tag == "r24H_mean"

    def test_diff_only(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(do_diff=True, diff_periods=1)
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert ".diff(1)" in expr
        assert tag == "diff"

    def test_diff_multiple_periods(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(do_diff=True, diff_periods=3)
        _, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert tag == "diff3"

    def test_cumsum_only(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(do_cumsum=True)
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert ".cumsum()" in expr
        assert "cumsum" in tag

    def test_scale_only(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(scale_factor=2.0)
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert "* 2.0" in expr
        assert tag == "x2.0"

    def test_scale_1_excluded(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(scale_factor=1.0)
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert "scale" not in tag

    def test_chained_resample_and_scale(self):
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(resample_period="1D", resample_agg="mean", scale_factor=0.3048)
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        assert "resample" in expr
        assert "0.3048" in expr
        assert "1D_mean" in tag
        assert "x0.3048" in tag

    def test_tidal_filter_before_resample_in_expression(self):
        """cosine_lanczos must wrap x before resample chains on the result."""
        from dvue.actions import TransformToCatalogAction
        mgr = self._mgr(do_tidal_filter=True, resample_period="1D", resample_agg="mean")
        expr, tag = TransformToCatalogAction._build_expression_and_tag(mgr)
        # cosine_lanczos(x, '40h').resample('1D').mean() — filter index < resample index
        assert expr.index("cosine_lanczos") < expr.index("resample")
        assert "tf" in tag
        assert "1D_mean" in tag

    def test_build_ref_name_replaces_awkward_chars(self):
        """_build_ref_name must produce a name with only safe chars."""
        from dvue.actions import TransformToCatalogAction
        reader = _make_reader()
        cat = DataCatalog(primary_key=["B", "C"])
        cat.add(DataReference(reader=reader, name="ref_0", B="STA 001", C="EC"))
        mgr = _TransformManager(cat)
        orig_ref = cat.get(B="STA 001", C="EC")
        name = TransformToCatalogAction._build_ref_name(orig_ref, "1D_mean", mgr)
        assert " " not in name

    def test_transform_to_catalog_action_registered(self):
        """'Transform → Ref' widget must appear in the Transform tab (not the action bar)."""
        cat = _build_catalog(["file_a.dss"])
        mgr = _TransformManager(cat)
        # Action bar should NOT contain 'Transform → Ref' — it was moved to the Transform tab.
        action_names = [a["name"] for a in mgr.get_data_actions()]
        assert "Transform → Ref" not in action_names
        # The Transform tab widgets must contain the button.
        widgets = mgr.get_widgets()
        transform_col = widgets.get("Transform")
        assert transform_col is not None, "Transform tab missing from get_widgets()"
        # Collect all button names recursively in the Transform Column
        def _collect_button_names(obj, found=None):
            if found is None:
                found = []
            import panel as pn
            if isinstance(obj, (pn.widgets.Button, pn.widgets.MenuButton)):
                found.append(obj.name)
            if hasattr(obj, "objects"):
                for child in obj.objects:
                    _collect_button_names(child, found)
            return found
        btn_names = _collect_button_names(transform_col)
        assert any("Transform" in n or "Catalog" in n for n in btn_names), (
            f"No Transform→Ref button found in Transform tab. Buttons found: {btn_names}"
        )


# ---------------------------------------------------------------------------
# Tests — TransformToCatalogAction attribute inheritance
# ---------------------------------------------------------------------------

class TestTransformToCatalogAttributeInheritance:
    """Verify that the new MathDataReference inherits the original ref's
    attributes and that the transform tag is recorded correctly."""

    def _add_ref_and_run(self, extra_attrs=None, **transform_params):
        """Helper: build a catalog with one ref, activate transforms, call the
        action callback, and return the newly added MathDataReference."""
        from dvue.actions import TransformToCatalogAction
        from unittest.mock import MagicMock, patch

        reader = _make_reader()
        cat = DataCatalog(primary_key=["name"])
        attrs = dict(A="AREA", B="STA001", C="FLOW", D="2020", E="1HOUR",
                     F="STUDY_V1", filename="file_a.dss")
        if extra_attrs:
            attrs.update(extra_attrs)
        cat.add(DataReference(reader=reader, name="ref_0", **attrs))

        mgr = _TransformManager(cat)
        for k, v in transform_params.items():
            setattr(mgr, k, v)

        dfcat = mgr.get_data_catalog()

        fake_dataui = MagicMock()
        fake_dataui._dataui_manager = mgr
        fake_dataui.display_table.selection = [0]
        fake_dataui._dfcat = dfcat

        with patch("panel.state") as mock_state:
            mock_state.notifications = None
            TransformToCatalogAction().callback(None, fake_dataui)

        # Find the newly added math ref
        math_refs = [r for r in cat.list() if getattr(r, "ref_type", "raw") != "raw"]
        assert len(math_refs) == 1, f"Expected 1 math ref, got {len(math_refs)}"
        return math_refs[0]

    def test_inherits_B_attribute(self):
        new_ref = self._add_ref_and_run(resample_period="1D", resample_agg="mean")
        assert new_ref.get_attribute("B") == "STA001"

    def test_inherits_C_attribute(self):
        new_ref = self._add_ref_and_run(resample_period="1D", resample_agg="mean")
        assert new_ref.get_attribute("C") == "FLOW"

    def test_does_not_inherit_source(self):
        """'source' (the file path) must not be copied to the math ref."""
        new_ref = self._add_ref_and_run(resample_period="1D", resample_agg="mean")
        # source defaults to None/empty for MathDataReference — not the original filename
        assert new_ref.get_attribute("source") != "file_a.dss"

    def test_F_attribute_not_modified(self):
        """F attribute must be inherited unchanged — not tagged with the transform."""
        new_ref = self._add_ref_and_run(resample_period="1D", resample_agg="mean")
        # F should be the original value, untouched
        assert new_ref.get_attribute("F") == "STUDY_V1"

    def test_tag_attribute_always_set(self):
        """'tag' attribute is always set to the short transform tag."""
        new_ref = self._add_ref_and_run(resample_period="1D", resample_agg="mean")
        assert new_ref.get_attribute("tag") == "1D_mean"

    def test_tag_attribute_set_when_no_F(self):
        """'tag' attribute is set even when the original ref has no F attribute."""
        from dvue.actions import TransformToCatalogAction
        from unittest.mock import MagicMock, patch

        reader = _make_reader()
        cat = DataCatalog(primary_key=["name"])
        cat.add(DataReference(reader=reader, name="ref_0", B="STA001", C="FLOW"))

        mgr = _TransformManager(cat)
        mgr.do_cumsum = True
        dfcat = mgr.get_data_catalog()

        fake_dataui = MagicMock()
        fake_dataui._dataui_manager = mgr
        fake_dataui.display_table.selection = [0]
        fake_dataui._dfcat = dfcat

        with patch("panel.state") as mock_state:
            mock_state.notifications = None
            TransformToCatalogAction().callback(None, fake_dataui)

        math_refs = [r for r in cat.list() if getattr(r, "ref_type", "raw") != "raw"]
        assert len(math_refs) == 1
        assert math_refs[0].get_attribute("tag") == "cumsum"

    def test_empty_F_still_sets_tag(self):
        """An empty string F: tag attribute is still set; F is not modified."""
        new_ref = self._add_ref_and_run(extra_attrs={"F": ""}, scale_factor=2.0)
        assert new_ref.get_attribute("tag") == "x2.0"
        # F was empty so it stays empty (not tagged)
        assert new_ref.get_attribute("F") == ""


# ---------------------------------------------------------------------------
# Tests — TransformToCatalogAction naming (_build_ref_name)
# ---------------------------------------------------------------------------

class TestTransformToCatalogNaming:
    """Verify the clean short-name scheme:
        [s{source_num}_]{pk_values}__{transform_tag}
    """

    def _make_ref(self, cat, name="ref_0", source="a.dss", **attrs):
        reader = _make_reader()
        ref = DataReference(reader=reader, name=name, source=source, **attrs)
        cat.add(ref)
        return ref

    # -- Single-source: no s{n}_ prefix (primary_key with pk cols) --------

    def test_single_source_no_prefix(self):
        from dvue.actions import TransformToCatalogAction
        cat = DataCatalog(primary_key=["B", "C"])
        self._make_ref(cat, name="ref_0", B="RSAC054", C="FLOW", F="V1", source="a.dss")
        mgr = _TransformManager(cat)
        orig_ref = cat.get(B="RSAC054", C="FLOW")
        name = TransformToCatalogAction._build_ref_name(orig_ref, "1D_mean", mgr)
        assert name == "RSAC054_FLOW__1D_mean"

    def test_single_source_no_s_prefix(self):
        """Single-source catalog: no s0_ prefix."""
        from dvue.actions import TransformToCatalogAction
        cat = DataCatalog(primary_key=["B", "C"])
        self._make_ref(cat, name="ref_0", B="RSAC054", C="FLOW", source="a.dss")
        mgr = _TransformManager(cat)
        orig_ref = cat.get(B="RSAC054", C="FLOW")
        name = TransformToCatalogAction._build_ref_name(orig_ref, "tf", mgr)
        assert not name.startswith("s")
        assert name == "RSAC054_FLOW__tf"

    # -- Multi-source: s{n}_ prefix (primary_key includes source_num) -----

    def test_multi_source_adds_prefix(self):
        """Multi-source catalog: s0_ prefix for source 0, s1_ for source 1."""
        from dvue.actions import TransformToCatalogAction
        cat = DataCatalog(primary_key=["source_num", "B", "C"])
        reader = _make_reader()
        ref0 = DataReference(reader=reader, name="ref_0", B="STA000", C="EC", source="file_a.dss")
        ref1 = DataReference(reader=reader, name="ref_1", B="STA000", C="EC", source="file_b.dss")
        cat.add(ref0)
        cat.add(ref1)
        mgr = _TransformManager(cat)
        name0 = TransformToCatalogAction._build_ref_name(ref0, "1D_mean", mgr)
        name1 = TransformToCatalogAction._build_ref_name(ref1, "1D_mean", mgr)
        assert name0.startswith("s0_")
        assert name1.startswith("s1_")

    def test_multi_source_full_name_shape(self):
        from dvue.actions import TransformToCatalogAction
        cat = DataCatalog(primary_key=["source_num", "B", "C"])
        reader = _make_reader()
        ref0 = DataReference(reader=reader, name="ref_0", B="STA000", C="EC", source="file_a.dss")
        ref1 = DataReference(reader=reader, name="ref_1", B="STA000", C="EC", source="file_b.dss")
        cat.add(ref0)
        cat.add(ref1)
        mgr = _TransformManager(cat)
        name = TransformToCatalogAction._build_ref_name(ref0, "1D_mean", mgr)
        assert name == "s0_STA000_EC__1D_mean"

    def test_no_pk_cols_falls_back_to_ref_name(self):
        from dvue.actions import TransformToCatalogAction
        # primary_key=["name"] means no pk-value columns; falls back to ref.name
        cat = DataCatalog(primary_key=["name"])
        reader = _make_reader()
        ref = DataReference(reader=reader, name="ref_0", B="RSAC054", C="FLOW", source="a.dss")
        cat.add(ref)
        mgr = _TransformManager(cat)
        name = TransformToCatalogAction._build_ref_name(ref, "1D_mean", mgr)
        assert "__1D_mean" in name
        assert "ref_0" in name


# ---------------------------------------------------------------------------
# Tests — NaN suppression in _build_ref_name
# ---------------------------------------------------------------------------


class TestRefKeyNaNSuppression:
    """NaN-valued float attributes must not appear in _build_ref_name output."""

    def test_nan_attrs_excluded_from_name(self):
        """Ref with NaN attributes should produce a clean name."""
        from dvue.actions import TransformToCatalogAction
        reader = _make_reader()
        cat = DataCatalog(primary_key=["station", "variable"])
        ref = DataReference(
            reader=reader,
            name="r",
            station="RSAC054",
            variable="flow",
            area=float("nan"),
        )
        cat.add(ref)
        mgr = _TransformManager(cat)
        name = TransformToCatalogAction._build_ref_name(ref, "tf", mgr)
        assert "nan" not in name
        assert "RSAC054" in name
        assert "flow" in name

    def test_build_catalog_from_dataframe(self):
        """build_catalog_from_dataframe with primary_key works."""
        import pandas as pd
        from dvue.catalog import build_catalog_from_dataframe

        reader = _make_reader()
        df = pd.DataFrame([
            {"station": "A", "variable": "flow", "area": float("nan"), "filename": "f.dss"},
            {"station": "B", "variable": "EC",   "area": float("nan"), "filename": "f.dss"},
        ])
        cat = build_catalog_from_dataframe(
            df, reader,
            ref_name_fn=lambda row: f'{row["station"]}_{row["variable"]}',
            primary_key=["station", "variable"],
        )
        refs = cat.list()
        assert len(refs) == 2
        for ref in refs:
            assert ref.name in ("A_flow", "B_EC")
