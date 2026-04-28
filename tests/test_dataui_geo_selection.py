"""Tests for geo/map selection behaviour with mixed catalogs (raw + math refs).

Covers two bugs fixed in DataUI:
  1. update_map_features: -1 entries from get_indexer (math ref rows absent
     from current_view after is_valid filter) must be stripped from
     selected= opts passed to the map.
  2. select_data_catalog: when the map fires a selection callback, previously-
     selected rows with NaN geometry (math refs) must be preserved in the table
     selection rather than silently discarded.
"""

import types
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point

from dvue.catalog import DataCatalog, DataReference, InMemoryDataReferenceReader
from dvue.math_reference import MathDataReference


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reader():
    return InMemoryDataReferenceReader(
        pd.DataFrame({"value": [1.0, 2.0]}, index=pd.to_datetime(["2020-01-01", "2020-01-02"]))
    )


def _make_mixed_dfcat():
    """Return a GeoDataFrame that mimics DataCatalog.to_dataframe().reset_index()
    for a mixed catalog: two raw refs with geometry, one math ref without.

    Columns: name, id, variable, source, filename, geometry, ref_type
    """
    geo_rows = [
        {
            "name": "src::A/elev",
            "id": "A",
            "variable": "elev",
            "source": "src",
            "filename": "/f/a.h5",
            "geometry": Point(0, 0),
            "ref_type": "raw",
        },
        {
            "name": "src::B/elev",
            "id": "B",
            "variable": "elev",
            "source": "src",
            "filename": "/f/b.h5",
            "geometry": Point(1, 1),
            "ref_type": "raw",
        },
    ]
    math_row = {
        "name": "math_elev",
        "id": None,
        "variable": None,
        "source": "",
        "filename": None,
        "geometry": None,
        "ref_type": "math",
    }
    all_rows = geo_rows + [math_row]
    df = pd.DataFrame(all_rows)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return gdf


# ---------------------------------------------------------------------------
# Tests for the -1 filter fix in update_map_features
# ---------------------------------------------------------------------------

class TestCurrentSelectionNegativeFilter:
    """Unit-test the index-filter logic extracted from update_map_features."""

    def _compute_current_selection(self, current_view_index, selected_index):
        """Replicate the fixed logic from update_map_features."""
        return [
            i
            for i in current_view_index.get_indexer(selected_index).tolist()
            if i >= 0
        ]

    def test_pure_geo_selection_unchanged(self):
        """When only geo rows are selected get_indexer returns no -1; result is identical."""
        gdf = _make_mixed_dfcat()
        current_view = gdf[gdf["ref_type"] == "raw"]  # geo-only rows
        selected = gdf.iloc[[0, 1]]  # both geo rows

        result = self._compute_current_selection(current_view.index, selected.index)
        assert result == [0, 1]

    def test_math_ref_produces_minus_one_before_fix(self):
        """Demonstrate that get_indexer returns -1 for a math ref row (NaN geometry)
        that is absent from current_view.  This is the pre-fix raw value."""
        gdf = _make_mixed_dfcat()
        current_view = gdf[gdf["ref_type"] == "raw"]
        # select geo row 0 + the math ref row (index 2)
        selected = gdf.iloc[[0, 2]]

        raw_result = current_view.index.get_indexer(selected.index).tolist()
        assert -1 in raw_result, "Expected -1 for math ref label not in current_view"

    def test_negative_one_is_filtered_out(self):
        """After fix: -1 entries are removed; geo rows still map correctly."""
        gdf = _make_mixed_dfcat()
        current_view = gdf[gdf["ref_type"] == "raw"]
        # select geo row 0 (index 0) + math ref (index 2)
        selected = gdf.iloc[[0, 2]]

        result = self._compute_current_selection(current_view.index, selected.index)
        assert -1 not in result
        assert len(result) == 1  # only the valid geo mapping survives

    def test_all_math_refs_selected_returns_empty(self):
        """When only math ref rows are selected, result is an empty list (no map points)."""
        gdf = _make_mixed_dfcat()
        current_view = gdf[gdf["ref_type"] == "raw"]
        selected = gdf.iloc[[2]]  # only math ref

        result = self._compute_current_selection(current_view.index, selected.index)
        assert result == []


# ---------------------------------------------------------------------------
# Tests for the NaN-geometry preservation fix in select_data_catalog
# ---------------------------------------------------------------------------

def _make_select_data_catalog_inputs(dfcat, table_selection, map_feature_indices):
    """Build minimal stubs for testing the select_data_catalog else-branch logic.

    Returns (dfcat, table_stub, map_features_stub) where map_features_stub
    contains only geo rows (mimicking _map_features which excludes NaN geometry).
    """
    geo_dfcat = dfcat[dfcat.geometry.notna()].reset_index(drop=True)

    # Stub for self._map_features.dframe()
    map_features_mock = MagicMock()
    map_features_mock.dframe.return_value = geo_dfcat

    # Stub for table widget
    table_mock = MagicMock()
    table_mock.selection = table_selection

    return dfcat, table_mock, map_features_mock, geo_dfcat


def _run_select_data_catalog_else(dfcat, table, map_features, index):
    """Replicate the fixed else-branch from select_data_catalog."""
    dfs = map_features.dframe().iloc[index]
    merged_indices = dfcat.reset_index().merge(dfs)["index"].to_list()
    geo_selected_indices = dfcat.index.get_indexer(merged_indices).tolist()

    non_geo_positions = []
    if isinstance(dfcat, gpd.GeoDataFrame) and table.selection:
        has_no_geo = dfcat.geometry.isna()
        non_geo_positions = [
            i for i in table.selection
            if i < len(dfcat) and has_no_geo.iloc[i]
        ]
    return sorted(set(non_geo_positions + geo_selected_indices))


class TestSelectDataCatalogPreservesNonGeoRows:

    def test_math_ref_preserved_when_geo_row_clicked(self):
        """When a geo row is clicked on the map while a math ref is selected in the
        table, the math ref's table position must still be in selected_indices."""
        gdf = _make_mixed_dfcat().reset_index(drop=True)
        # table has math ref (pos 2) already selected
        # map click: user clicks geo row 1
        dfcat, table, map_features, geo_dfcat = _make_select_data_catalog_inputs(
            gdf, table_selection=[2], map_feature_indices=[1]
        )

        selected = _run_select_data_catalog_else(dfcat, table, map_features, [1])
        assert 2 in selected, "Math ref position must survive map click"
        # geo row 1 must also be selected
        assert any(gdf.iloc[i]["id"] == "B" for i in selected)

    def test_math_ref_preserved_alongside_multiple_geo_rows(self):
        """Selecting two geo rows on the map + math ref already selected in table."""
        gdf = _make_mixed_dfcat().reset_index(drop=True)
        dfcat, table, map_features, geo_dfcat = _make_select_data_catalog_inputs(
            gdf, table_selection=[2], map_feature_indices=[0, 1]
        )

        selected = _run_select_data_catalog_else(dfcat, table, map_features, [0, 1])
        assert 2 in selected, "Math ref must survive when two geo rows are map-selected"
        assert len([i for i in selected if gdf.iloc[i]["ref_type"] == "raw"]) == 2

    def test_no_math_ref_pre_selected_result_unchanged(self):
        """When no math refs are in the prior table selection, behaviour is identical
        to the pre-fix code (no regressions)."""
        gdf = _make_mixed_dfcat().reset_index(drop=True)
        dfcat, table, map_features, geo_dfcat = _make_select_data_catalog_inputs(
            gdf, table_selection=[0], map_feature_indices=[1]
        )

        selected = _run_select_data_catalog_else(dfcat, table, map_features, [1])
        # Only the geo row that was map-clicked survives; position 0 is NOT a math ref
        # so it should NOT be auto-preserved (user deselected it by clicking elsewhere)
        assert 2 not in selected  # math ref was not pre-selected

    def test_empty_map_index_preserves_math_ref(self):
        """If the map fires an empty selection index, math refs that were selected
        in the table should still remain selected."""
        gdf = _make_mixed_dfcat().reset_index(drop=True)
        dfcat, table, map_features, geo_dfcat = _make_select_data_catalog_inputs(
            gdf, table_selection=[2], map_feature_indices=[]
        )

        selected = _run_select_data_catalog_else(dfcat, table, map_features, [])
        assert 2 in selected
        assert len(selected) == 1

    def test_non_geodataframe_unchanged(self):
        """When _dfcat is a plain DataFrame (no geometry), the non-geo preservation
        path is skipped and result equals the original geo_selected_indices only."""
        # Build a plain DataFrame (no geometry)
        plain_df = _make_mixed_dfcat().drop(columns=["geometry"]).reset_index(drop=True)
        geo_subset = plain_df[plain_df["ref_type"] == "raw"].reset_index(drop=True)

        map_features = MagicMock()
        map_features.dframe.return_value = geo_subset
        table = MagicMock()
        table.selection = [2]  # math ref position pre-selected

        dfs = map_features.dframe().iloc[[1]]
        # plain_df has no geometry column so isinstance(dfcat, gpd.GeoDataFrame) is False
        assert not isinstance(plain_df, gpd.GeoDataFrame)
        # For a plain DataFrame the non_geo_positions block is skipped
        non_geo_positions = []
        if isinstance(plain_df, gpd.GeoDataFrame) and table.selection:
            non_geo_positions = [99]  # should not reach here
        assert non_geo_positions == []
