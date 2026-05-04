"""Tests for SourceCompareAction helpers in dvue.actions.

These tests cover pure logic functions (no Panel dependency) and a
catalog-level integration test for _create_compare_refs().
"""

import pandas as pd
import pytest

from dvue.actions import (
    _detect_varying_columns,
    _group_by_identity,
    _build_compare_name,
    _create_compare_refs,
)
from dvue.catalog import DataCatalog, DataReference, InMemoryDataReferenceReader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ts(value=1.0):
    idx = pd.date_range("2020-01-01", periods=4, freq="h")
    return pd.DataFrame({"value": [value] * 4}, index=idx)


def _make_ref(name, station, variable, source, data_value=1.0, **extra):
    reader = InMemoryDataReferenceReader(_make_ts(data_value))
    ref = DataReference(source=source, reader=reader, name=name, station=station, variable=variable, **extra)
    return ref


# ---------------------------------------------------------------------------
# _detect_varying_columns
# ---------------------------------------------------------------------------


class TestDetectVaryingColumns:
    def test_basic(self):
        df = pd.DataFrame({
            "station": ["A", "A", "B", "B"],
            "variable": ["flow", "flow", "flow", "flow"],
            "source": ["obs", "model", "obs", "model"],
        })
        result = _detect_varying_columns(df, identity_cols=["station", "variable"])
        assert result == ["source"]

    def test_nothing_varies(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "variable": ["flow", "flow"],
            "source": ["obs", "obs"],
        })
        result = _detect_varying_columns(df, identity_cols=["station", "variable"])
        assert result == []

    def test_multiple_varying(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "source": ["obs", "model"],
            "run": ["r1", "r2"],
        })
        result = _detect_varying_columns(df, identity_cols=["station"])
        assert set(result) == {"source", "run"}

    def test_metadata_columns_excluded(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "variable": ["flow", "flow"],
            "source": ["obs", "model"],
            "ref_type": ["raw", "math"],       # metadata — should be excluded
            "expression": ["", "x - base"],    # metadata — should be excluded
        })
        result = _detect_varying_columns(df, identity_cols=["station", "variable"])
        assert result == ["source"]

    def test_nan_values_ignored(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "source": ["obs", float("nan")],
        })
        result = _detect_varying_columns(df, identity_cols=["station"])
        # only one non-NaN unique value, should not appear as varying
        assert result == []

    def test_empty_identity_cols(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "source": ["obs", "model"],
        })
        result = _detect_varying_columns(df, identity_cols=[])
        assert "source" in result


# ---------------------------------------------------------------------------
# _group_by_identity
# ---------------------------------------------------------------------------


class TestGroupByIdentity:
    def test_two_stations_two_sources(self):
        df = pd.DataFrame({
            "station": ["A", "A", "B", "B"],
            "variable": ["flow", "flow", "flow", "flow"],
            "source": ["obs", "model", "obs", "model"],
            "name": ["A_flow_obs", "A_flow_model", "B_flow_obs", "B_flow_model"],
        })
        groups = _group_by_identity(df, identity_cols=["station", "variable"], vary_by_col="source")
        assert len(groups) == 2
        keys = [k for k in groups]
        assert ("A", "flow") in keys
        assert ("B", "flow") in keys

    def test_empty_identity_cols_uses_all_other_columns(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "source": ["obs", "model"],
            "name": ["r1", "r2"],
        })
        groups = _group_by_identity(df, identity_cols=[], vary_by_col="source")
        # "station" and "source" are the only non-metadata cols; source is excluded
        # so groups are formed by station alone
        assert len(groups) == 1

    def test_single_group(self):
        df = pd.DataFrame({
            "station": ["A", "A"],
            "source": ["obs", "model"],
        })
        groups = _group_by_identity(df, identity_cols=["station"], vary_by_col="source")
        assert len(groups) == 1
        assert ("A",) in groups


# ---------------------------------------------------------------------------
# _build_compare_name
# ---------------------------------------------------------------------------


class TestBuildCompareName:
    def test_diff(self):
        assert _build_compare_name("sac_flow", "model", "Diff") == "sac_flow__diff_model"

    def test_ratio(self):
        assert _build_compare_name("sac_flow", "obs", "Ratio") == "sac_flow__ratio_obs"

    def test_sanitises_spaces(self):
        assert _build_compare_name("sac_flow", "my model run", "Diff") == "sac_flow__diff_my_model_run"

    def test_sanitises_special_chars(self):
        assert _build_compare_name("sac_flow", "run/2024", "Ratio") == "sac_flow__ratio_run_2024"

    def test_case_insensitive_operation(self):
        assert _build_compare_name("x", "b", "diff") == "x__diff_b"
        assert _build_compare_name("x", "b", "RATIO") == "x__ratio_b"


# ---------------------------------------------------------------------------
# _create_compare_refs — integration with a real DataCatalog
# ---------------------------------------------------------------------------


class _FakeManager:
    """Minimal manager stub for _create_compare_refs tests."""

    identity_key_columns = ["station", "variable"]

    def __init__(self, catalog):
        self._catalog = catalog

    def get_data_reference(self, row):
        return self._catalog.get(row["name"])


def _build_catalog_with_sources():
    """Two stations × two sources — 4 raw refs in total."""
    catalog = DataCatalog()
    for station in ("A", "B"):
        for source in ("obs", "model"):
            name = f"{station}_flow_{source}"
            ref = _make_ref(name=name, station=station, variable="flow", source=source)
            ref.set_key_attributes(["station", "variable"])
            catalog.add(ref)
    return catalog


def _build_dfselected(catalog):
    df = catalog.to_dataframe().reset_index()
    return df


class TestCreateCompareRefs:
    def test_diff_creates_correct_refs(self):
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        n = _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        assert n == 2
        assert "A_flow__diff_model" in [r.name for r in catalog.list()]
        assert "B_flow__diff_model" in [r.name for r in catalog.list()]
        # source column should be "diff" for easy filtering
        ref = catalog.get("A_flow__diff_model")
        assert ref.get_attribute("source") == "diff"
        assert ref.get_attribute("compare_op") == "diff"
        assert ref.get_attribute("compare_source") == "model"

    def test_ratio_creates_correct_refs(self):
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        n = _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Ratio",
        )
        assert n == 2
        assert "A_flow__ratio_model" in [r.name for r in catalog.list()]
        ref = catalog.get("A_flow__ratio_model")
        assert ref.get_attribute("source") == "ratio"
        assert ref.get_attribute("compare_op") == "ratio"
        assert ref.get_attribute("compare_source") == "model"

    def test_diff_expression_is_correct(self):
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        ref = catalog.get("A_flow__diff_model")
        assert ref.get_attribute("expression") == "x - base"

    def test_ratio_expression_is_correct(self):
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Ratio",
        )
        ref = catalog.get("A_flow__ratio_model")
        assert ref.get_attribute("expression") == "x / base"

    def test_partial_group_skipped_silently(self):
        """Station C has only obs (no model) — should be skipped without error."""
        catalog = _build_catalog_with_sources()
        # Add a ref that has no pair
        lone = _make_ref(name="C_flow_obs", station="C", variable="flow", source="obs")
        lone.set_key_attributes(["station", "variable"])
        catalog.add(lone)

        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        n = _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        # Still creates 2 (A + B); C is skipped
        assert n == 2
        assert "C_flow__diff_model" not in [r.name for r in catalog.list()]

    def test_idempotent(self):
        """Running twice should replace, not duplicate."""
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        n_refs_after_first = sum(1 for r in catalog.list())

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        assert sum(1 for r in catalog.list()) == n_refs_after_first

    def test_diff_data_values(self):
        """getData() on a diff ref should return model - obs element-wise."""
        catalog = DataCatalog()
        ref_obs = _make_ref("A_flow_obs", station="A", variable="flow", source="obs", data_value=10.0)
        ref_obs.set_key_attributes(["station", "variable"])
        ref_model = _make_ref("A_flow_model", station="A", variable="flow", source="model", data_value=15.0)
        ref_model.set_key_attributes(["station", "variable"])
        catalog.add(ref_obs)
        catalog.add(ref_model)

        df = catalog.to_dataframe().reset_index()
        manager = _FakeManager(catalog)

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=df,
            vary_by_col="source",
            base_value="obs",
            operation="Diff",
        )
        diff_ref = catalog.get("A_flow__diff_model")
        result = diff_ref.getData()
        assert (result.iloc[:, 0] == 5.0).all()

    def test_ratio_data_values(self):
        """getData() on a ratio ref should return model / obs element-wise."""
        catalog = DataCatalog()
        ref_obs = _make_ref("A_flow_obs", station="A", variable="flow", source="obs", data_value=4.0)
        ref_obs.set_key_attributes(["station", "variable"])
        ref_model = _make_ref("A_flow_model", station="A", variable="flow", source="model", data_value=12.0)
        ref_model.set_key_attributes(["station", "variable"])
        catalog.add(ref_obs)
        catalog.add(ref_model)

        df = catalog.to_dataframe().reset_index()
        manager = _FakeManager(catalog)

        _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=df,
            vary_by_col="source",
            base_value="obs",
            operation="Ratio",
        )
        ratio_ref = catalog.get("A_flow__ratio_model")
        result = ratio_ref.getData()
        assert (result.iloc[:, 0] == 3.0).all()

    def test_no_base_in_selection_returns_zero(self):
        """If none of the selected rows have the specified base value, return 0."""
        catalog = _build_catalog_with_sources()
        dfselected = _build_dfselected(catalog)
        manager = _FakeManager(catalog)

        n = _create_compare_refs(
            manager=manager,
            catalog=catalog,
            dfselected=dfselected,
            vary_by_col="source",
            base_value="nonexistent",
            operation="Diff",
        )
        assert n == 0
