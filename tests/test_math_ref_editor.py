"""Tests for MathRefEditorAction helpers and TimeSeriesDataUIManager math-ref utilities.

Tests that do NOT require a running Panel server:
- _enrich_catalog_with_math_ref_hints (base-class helper in tsdataui)
- YAML download callback produces the canonical save_math_refs format
  (i.e. the format that MathDataCatalogReader.build() can parse back)
"""

from __future__ import annotations

import io
import yaml
import pandas as pd
import pytest

from dvue.catalog import (
    DataCatalog,
    DataReference,
    InMemoryDataReferenceReader,
    MathDataReference,
    MathDataCatalogReader,
)
from dvue.math_reference import save_math_refs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw_catalog() -> DataCatalog:
    cat = DataCatalog()
    df = pd.DataFrame({"v": [1.0, 2.0]})
    cat.add(DataReference(
        source="", reader=InMemoryDataReferenceReader(df),
        name="flow_upstream", variable="flow", unit="cfs"
    ))
    cat.add(DataReference(
        source="", reader=InMemoryDataReferenceReader(df),
        name="flow_downstream", variable="flow", unit="cfs"
    ))
    return cat


def _make_mixed_catalog() -> DataCatalog:
    cat = _make_raw_catalog()
    m = MathDataReference(
        "flow_upstream + flow_downstream",
        name="total_flow",
        catalog=cat,
        variable="flow",
        unit="cfs",
    )
    cat.add(m)
    return cat


# ---------------------------------------------------------------------------
# _enrich_catalog_with_math_ref_hints
# ---------------------------------------------------------------------------

class TestEnrichCatalogWithMathRefHints:
    """Tests for TimeSeriesDataUIManager._enrich_catalog_with_math_ref_hints."""

    def _get_helper(self):
        """Return the base-class helper without instantiating the full manager."""
        from dvue.tsdataui import TimeSeriesDataUIManager
        # Access the unbound method directly to avoid NotImplementedError from __init__.
        return TimeSeriesDataUIManager._enrich_catalog_with_math_ref_hints

    def test_no_expression_column_noop(self):
        helper = self._get_helper()
        df = pd.DataFrame({"name": ["r1", "r2"], "variable": ["flow", "stage"]})
        result = helper(None, df)
        assert "expression" not in result.columns

    def test_fills_blank_expression_with_name(self):
        helper = self._get_helper()
        df = pd.DataFrame({
            "name": ["raw_ref", "math_ref"],
            "expression": [None, "raw_ref * 2"],
        })
        result = helper(None, df)
        assert result.loc[result["name"] == "raw_ref", "expression"].iloc[0] == "raw_ref"
        assert result.loc[result["name"] == "math_ref", "expression"].iloc[0] == "raw_ref * 2"

    def test_fills_whitespace_only_expression_with_name(self):
        helper = self._get_helper()
        df = pd.DataFrame({
            "name": ["r1"],
            "expression": ["   "],
        })
        result = helper(None, df)
        assert result.loc[0, "expression"] == "r1"

    def test_does_not_overwrite_existing_expression(self):
        helper = self._get_helper()
        df = pd.DataFrame({
            "name": ["math_ref"],
            "expression": ["A + B"],
        })
        result = helper(None, df)
        assert result.loc[0, "expression"] == "A + B"

    def test_integration_with_to_dataframe(self):
        """_enrich works on the actual output of DataCatalog.to_dataframe()."""
        helper = self._get_helper()
        cat = _make_mixed_catalog()
        df = cat.to_dataframe().reset_index()
        result = helper(None, df)
        assert "expression" in result.columns
        # raw refs should have their catalog name in expression column
        raw_rows = result[result["ref_type"] == "raw"]
        assert (raw_rows["expression"] == raw_rows["name"]).all()
        # math ref should keep its formula
        math_rows = result[result["ref_type"] == "math"]
        assert math_rows["expression"].iloc[0] == "flow_upstream + flow_downstream"


# ---------------------------------------------------------------------------
# YAML download format (bug fix: must match MathDataCatalogReader format)
# ---------------------------------------------------------------------------

class TestYamlDownloadFormat:
    """The download callback must produce YAML that MathDataCatalogReader can load."""

    def _build_download_bytes(self, catalog: DataCatalog) -> bytes:
        """Simulate _yaml_download_callback() by calling save_math_refs to a BytesIO."""
        import tempfile, os
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", delete=False, mode="w", encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name
        try:
            save_math_refs(catalog, tmp_path)
            with open(tmp_path, "rb") as fh:
                return fh.read()
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def test_download_yaml_parses_back(self):
        cat = _make_mixed_catalog()
        data = self._build_download_bytes(cat)
        parsed = yaml.safe_load(io.BytesIO(data).read())
        assert isinstance(parsed, list)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "total_flow"
        assert "expression" in parsed[0]

    def test_download_yaml_no_nested_criteria_key(self):
        """Old bug: download wrote {search_map: {var: {criteria: {...}}}} — must not exist."""
        cat = _make_raw_catalog()
        m = MathDataReference(
            "obs * 2",
            name="doubled",
            catalog=cat,
            search_map={"obs": {"variable": "flow"}},
        )
        cat.add(m)
        data = self._build_download_bytes(cat)
        parsed = yaml.safe_load(io.BytesIO(data).read())
        obs_crit = parsed[0]["search_map"]["obs"]
        # Must be flat {attr: val} — no nested 'criteria' key
        assert "criteria" not in obs_crit
        assert "variable" in obs_crit

    def test_download_yaml_no_nested_attributes_key(self):
        """Old bug: download wrote top-level 'attributes: {}' nested dict — must not exist."""
        cat = _make_raw_catalog()
        m = MathDataReference(
            "obs * 2",
            name="doubled",
            catalog=cat,
            search_map={"obs": {"variable": "flow"}},
            station_id="S1",
        )
        cat.add(m)
        data = self._build_download_bytes(cat)
        parsed = yaml.safe_load(io.BytesIO(data).read())
        entry = parsed[0]
        # station_id must be at top level, not inside an 'attributes' sub-dict
        assert "attributes" not in entry
        assert entry.get("station_id") == "S1"

    def test_download_then_upload_round_trip(self):
        """Full round-trip: save_math_refs → MathDataCatalogReader.build() preserves all data."""
        import tempfile, os
        cat = _make_raw_catalog()
        m = MathDataReference(
            "obs - model",
            name="bias",
            catalog=cat,
            search_map={
                "obs": {"variable": "flow"},
                "model": {"variable": "flow"},
            },
            search_require_single={"obs": True, "model": False},
            unit="cfs",
        )
        cat.add(m)
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", delete=False, mode="w", encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name
        try:
            save_math_refs(cat, tmp_path)
            refs = MathDataCatalogReader(parent_catalog=cat).build(tmp_path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        assert len(refs) == 1
        r = refs[0]
        assert r.name == "bias"
        assert r.expression == "obs - model"
        assert r.get_attribute("unit") == "cfs"
        assert "obs" in r._search_map
        assert "_require_single" not in r._search_map["obs"]
        assert r._search_require_single.get("obs") is True
        assert r._search_require_single.get("model") is False
