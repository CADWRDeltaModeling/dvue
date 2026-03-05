"""Tests for dvue.catalog – DataReference, CatalogView, MathDataReference,
DataCatalog, CSVDirectoryReader, and PatternCSVDirectoryReader."""

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dvue.catalog import (
    CSVDirectoryReader,
    CatalogView,
    DataCatalog,
    DataCatalogReader,
    DataReference,
    MathDataReference,
    PatternCSVDirectoryReader,
    _pattern_to_regex,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_df():
    return pd.DataFrame(
        {
            "temperature": [20.0, 25.0, 30.0, 35.0],
            "season": ["spring", "summer", "summer", "summer"],
        }
    )


@pytest.fixture()
def multi_col_df():
    return pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [10.0, 20.0, 30.0]})


@pytest.fixture()
def csv_dir(tmp_path):
    """Directory with three CSV files, one unmatched file, and one non-CSV file."""
    files = {
        "flow__STA001__USGS.csv": pd.DataFrame({"value": [1.0, 2.0, 3.0]}),
        "stage__STA002__CDEC.csv": pd.DataFrame({"value": [4.0, 5.0, 6.0]}),
        "temperature__STA001__CDEC.csv": pd.DataFrame({"value": [7.0, 8.0, 9.0]}),
        "unmatched_file.csv": pd.DataFrame({"value": [99.0]}),
        "notes.txt": None,  # non-CSV — ignored by glob
    }
    for filename, df in files.items():
        p = tmp_path / filename
        if df is not None:
            df.to_csv(p, index=False)
        else:
            p.write_text("just a text file")
    return tmp_path


# ===========================================================================
# DataReference
# ===========================================================================


class TestDataReference:
    def test_from_dataframe(self, simple_df):
        ref = DataReference(simple_df, name="climate")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["temperature", "season"]

    def test_from_dataframe_returns_copy(self, simple_df):
        ref = DataReference(simple_df, name="r")
        result = ref.getData()
        result["new_col"] = 0
        # Original not mutated
        assert "new_col" not in ref.getData().columns

    def test_from_series_wrapped_in_dataframe(self):
        s = pd.Series([1, 2, 3], name="x")
        ref = DataReference(s, name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert "x" in result.columns

    def test_from_callable(self, simple_df):
        ref = DataReference(lambda: simple_df.copy(), name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4

    def test_from_callable_returning_series(self):
        ref = DataReference(lambda: pd.Series([1, 2, 3], name="v"), name="r")
        assert isinstance(ref.getData(), pd.DataFrame)

    def test_from_csv_path(self, tmp_path, simple_df):
        p = tmp_path / "data.csv"
        simple_df.to_csv(p, index=False)
        ref = DataReference(str(p), name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["temperature", "season"]

    def test_from_pathlib_path(self, tmp_path, simple_df):
        p = tmp_path / "data.csv"
        simple_df.to_csv(p, index=False)
        ref = DataReference(p, name="r")
        assert isinstance(ref.getData(), pd.DataFrame)

    def test_unsupported_extension_raises(self, tmp_path):
        p = tmp_path / "data.xyz"
        p.write_text("x")
        ref = DataReference(str(p), name="r")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            ref.getData()

    def test_cache_enabled_by_default(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(loader, name="r")
        ref.getData()
        ref.getData()
        assert call_count["n"] == 1

    def test_cache_disabled(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(loader, name="r", cache=False)
        ref.getData()
        ref.getData()
        assert call_count["n"] == 2

    def test_invalidate_cache(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(loader, name="r")
        ref.getData()
        ref.invalidate_cache()
        ref.getData()
        assert call_count["n"] == 2

    def test_invalidate_cache_is_chainable(self, simple_df):
        ref = DataReference(simple_df, name="r")
        assert ref.invalidate_cache() is ref

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def test_attributes_stored(self, simple_df):
        ref = DataReference(simple_df, name="r", variable="temperature", unit="degC")
        assert ref.get_attribute("variable") == "temperature"
        assert ref.get_attribute("unit") == "degC"

    def test_get_attribute_default(self, simple_df):
        ref = DataReference(simple_df, name="r")
        assert ref.get_attribute("missing") is None
        assert ref.get_attribute("missing", "fallback") == "fallback"

    def test_has_attribute(self, simple_df):
        ref = DataReference(simple_df, name="r", tag="test")
        assert ref.has_attribute("tag")
        assert not ref.has_attribute("nope")

    # ------------------------------------------------------------------
    # ref_key
    # ------------------------------------------------------------------

    def test_ref_key_default_joins_attribute_values(self, simple_df):
        ref = DataReference(simple_df, name="r", station="A", variable="wind", interval="hourly")
        assert ref.ref_key() == "A_wind_hourly"

    def test_ref_key_sanitizes_spaces(self, simple_df):
        ref = DataReference(simple_df, name="r", station_name="Station A")
        assert ref.ref_key() == "Station_A"

    def test_ref_key_sanitizes_special_chars(self, simple_df):
        ref = DataReference(simple_df, name="r", unit="m/s")
        assert ref.ref_key() == "m_s"

    def test_ref_key_includes_numeric_attributes(self, simple_df):
        ref = DataReference(simple_df, name="r", year=2020)
        assert ref.ref_key() == "2020"

    def test_ref_key_skips_complex_types(self, simple_df):
        class _Blob:
            pass

        ref = DataReference(simple_df, name="r", station="A", blob=_Blob())
        assert ref.ref_key() == "A"

    def test_ref_key_empty_when_no_attributes(self, simple_df):
        ref = DataReference(simple_df, name="r")
        assert ref.ref_key() == ""

    def test_ref_key_override_in_subclass(self, simple_df):
        class CustomRef(DataReference):
            def ref_key(self) -> str:
                return self.get_attribute("station", "") + "_custom"

        ref = CustomRef(simple_df, name="r", station="A")
        assert ref.ref_key() == "A_custom"

    def test_set_attribute_chainable(self, simple_df):
        ref = DataReference(simple_df, name="r")
        result = ref.set_attribute("a", 1).set_attribute("b", 2)
        assert result is ref
        assert ref.get_attribute("a") == 1
        assert ref.get_attribute("b") == 2

    def test_attributes_returns_copy(self, simple_df):
        ref = DataReference(simple_df, name="r", x=1)
        d = ref.attributes
        d["y"] = 2
        assert not ref.has_attribute("y")

    def test_matches_exact(self, simple_df):
        ref = DataReference(simple_df, name="r", variable="T", unit="K")
        assert ref.matches(variable="T")
        assert ref.matches(variable="T", unit="K")
        assert not ref.matches(variable="T", unit="degC")

    def test_matches_callable_predicate(self, simple_df):
        ref = DataReference(simple_df, name="r", year=2020)
        assert ref.matches(year=lambda y: y >= 2019)
        assert not ref.matches(year=lambda y: y >= 2021)

    # ------------------------------------------------------------------
    # Operator overloading
    # ------------------------------------------------------------------

    def test_add_two_refs(self):
        a = DataReference(pd.DataFrame({"v": [1.0, 2.0]}), name="A")
        b = DataReference(pd.DataFrame({"v": [10.0, 20.0]}), name="B")
        result = (a + b).getData()
        assert list(result.iloc[:, 0]) == [11.0, 22.0]

    def test_mul_ref_by_scalar(self):
        a = DataReference(pd.DataFrame({"v": [2.0, 4.0]}), name="A")
        result = (a * 3).getData()
        assert list(result.iloc[:, 0]) == [6.0, 12.0]

    def test_scalar_mul_ref(self):
        a = DataReference(pd.DataFrame({"v": [2.0, 4.0]}), name="A")
        result = (3 * a).getData()
        assert list(result.iloc[:, 0]) == [6.0, 12.0]

    def test_neg_ref(self):
        a = DataReference(pd.DataFrame({"v": [1.0, -2.0]}), name="A")
        result = (-a).getData()
        assert list(result.iloc[:, 0]) == [-1.0, 2.0]

    def test_sub_scalar(self):
        a = DataReference(pd.DataFrame({"v": [5.0, 10.0]}), name="A")
        result = (a - 2).getData()
        assert list(result.iloc[:, 0]) == [3.0, 8.0]

    def test_div_ref(self):
        a = DataReference(pd.DataFrame({"v": [6.0, 9.0]}), name="A")
        b = DataReference(pd.DataFrame({"v": [2.0, 3.0]}), name="B")
        result = (a / b).getData()
        assert list(result.iloc[:, 0]) == [3.0, 3.0]

    def test_pow_ref(self):
        a = DataReference(pd.DataFrame({"v": [2.0, 3.0]}), name="A")
        result = (a**2).getData()
        assert list(result.iloc[:, 0]) == [4.0, 9.0]

    # ------------------------------------------------------------------
    # Repr / str
    # ------------------------------------------------------------------

    def test_repr_contains_name(self, simple_df):
        ref = DataReference(simple_df, name="my_ref")
        assert "my_ref" in repr(ref)

    def test_str_contains_name(self, simple_df):
        ref = DataReference(simple_df, name="my_ref")
        assert "my_ref" in str(ref)


# ===========================================================================
# CatalogView
# ===========================================================================


@pytest.fixture()
def hydro_catalog():
    """A catalog with three DataReferences covering two stations and two sources.

    Note: 'source' is a reserved DataReference constructor param, so we use
    set_attribute() to store it as metadata.
    """
    cat = DataCatalog()
    cat.add(
        DataReference(
            pd.DataFrame({"value": [1.0, 2.0]}), name="flow", stationid="STA001", variable="flow"
        ).set_attribute("source", "USGS")
    )
    cat.add(
        DataReference(
            pd.DataFrame({"value": [3.0, 4.0]}), name="stage", stationid="STA002", variable="stage"
        ).set_attribute("source", "CDEC")
    )
    cat.add(
        DataReference(
            pd.DataFrame({"value": [5.0, 6.0]}),
            name="temp",
            stationid="STA001",
            variable="temperature",
        ).set_attribute("source", "USGS")
    )
    return cat


class TestCatalogView:
    # ------------------------------------------------------------------
    # Construction and basic filtering
    # ------------------------------------------------------------------

    def test_no_selection_returns_all_refs(self, hydro_catalog):
        view = CatalogView(hydro_catalog)
        assert len(view) == 3

    def test_dict_selection_exact(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert len(view) == 2
        assert set(view.list_names()) == {"flow", "temp"}

    def test_dict_selection_multiple_criteria(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS", "stationid": "STA001"})
        assert len(view) == 2

    def test_dict_selection_callable_predicate(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"stationid": lambda s: s.endswith("001")})
        assert set(view.list_names()) == {"flow", "temp"}

    def test_callable_selection(self, hydro_catalog):
        view = CatalogView(
            hydro_catalog, selection=lambda ref: ref.get_attribute("source") == "CDEC"
        )
        assert view.list_names() == ["stage"]

    def test_invalid_selection_type_raises(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection=42)
        with pytest.raises(TypeError, match="Unsupported selection type"):
            view.list()

    # ------------------------------------------------------------------
    # Dict-like interface (read-only)
    # ------------------------------------------------------------------

    def test_contains(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert "flow" in view
        assert "stage" not in view

    def test_len(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert len(view) == 2

    def test_iter(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "CDEC"})
        names = [r.name for r in view]
        assert names == ["stage"]

    def test_getitem(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        ref = view["flow"]
        assert ref.name == "flow"

    def test_get_nonexistent_raises(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        with pytest.raises(KeyError):
            view.get("stage")  # stage is CDEC, excluded by view

    def test_list_names(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert set(view.list_names()) == {"flow", "temp"}

    def test_list(self, hydro_catalog):
        view = CatalogView(hydro_catalog)
        assert len(view.list()) == 3
        assert all(isinstance(r, DataReference) for r in view.list())

    # ------------------------------------------------------------------
    # select() – chaining with AND semantics
    # ------------------------------------------------------------------

    def test_select_returns_new_instance(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        narrower = view.select({"variable": "flow"})
        assert narrower is not view

    def test_select_narrows_view(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        narrower = view.select({"variable": "flow"})
        assert narrower.list_names() == ["flow"]

    def test_select_accumulates_and_semantics(self, hydro_catalog):
        # source=USGS gives 2 refs; adding stationid=STA001 still gives same 2
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        chained = view.select({"stationid": "STA001"})
        assert set(chained.list_names()) == {"flow", "temp"}

    def test_select_can_produce_empty_view(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        empty = view.select({"source": "CDEC"})  # contradicts parent selection
        assert len(empty) == 0

    def test_select_with_callable(self, hydro_catalog):
        view = CatalogView(hydro_catalog)
        result = view.select(lambda r: "a" in r.name)
        assert set(result.list_names()) == {"stage"}  # only "stage" contains 'a'

    # ------------------------------------------------------------------
    # search() within the view
    # ------------------------------------------------------------------

    def test_search_within_view(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        results = view.search(variable="flow")
        assert len(results) == 1
        assert results[0].name == "flow"

    def test_search_respects_view_boundary(self, hydro_catalog):
        """search() on a view cannot see refs excluded by the view's selection."""
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert view.search(source="CDEC") == []

    # ------------------------------------------------------------------
    # to_dataframe()
    # ------------------------------------------------------------------

    def test_to_dataframe_only_contains_matched_refs(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        df = view.to_dataframe()
        assert set(df.index) == {"flow", "temp"}

    def test_to_dataframe_empty_view(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "NOPE"})
        df = view.to_dataframe()
        assert df.empty

    def test_to_dataframe_columns(self, hydro_catalog):
        view = CatalogView(hydro_catalog)
        df = view.to_dataframe()
        assert "source" in df.columns
        assert "stationid" in df.columns
        assert "variable" in df.columns

    # ------------------------------------------------------------------
    # Schema map
    # ------------------------------------------------------------------

    def test_schema_map_inherited_from_source(self):
        cat = DataCatalog(schema_map={"stationid": "station"})
        cat.add(DataReference(pd.DataFrame({"v": [1]}), name="r", stationid="S01"))
        view = CatalogView(cat, selection={"stationid": "S01"})
        assert len(view) == 1
        # Canonical name works in search
        assert view.search(station="S01") == [view["r"]]

    def test_to_dataframe_applies_schema_map(self):
        cat = DataCatalog(schema_map={"stationid": "station"})
        cat.add(DataReference(pd.DataFrame({"v": [1]}), name="r", stationid="S01"))
        view = CatalogView(cat)
        df = view.to_dataframe()
        assert "station" in df.columns
        assert "stationid" not in df.columns

    # ------------------------------------------------------------------
    # Live update: changes to source catalog are reflected
    # ------------------------------------------------------------------

    def test_live_update_new_ref_appears(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert len(view) == 2
        hydro_catalog.add(
            DataReference(
                pd.DataFrame({"value": [7.0]}),
                name="salinity",
                stationid="STA003",
                variable="salinity",
            ).set_attribute("source", "USGS")
        )
        assert len(view) == 3  # new ref now visible

    def test_live_update_removed_ref_disappears(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        assert len(view) == 2
        hydro_catalog.remove("flow")
        assert len(view) == 1
        assert "flow" not in view

    # ------------------------------------------------------------------
    # Read-only mutations raise TypeError
    # ------------------------------------------------------------------

    def test_add_raises_type_error(self, hydro_catalog, simple_df):
        view = CatalogView(hydro_catalog)
        with pytest.raises(TypeError, match="read-only"):
            view.add(DataReference(simple_df, name="x"))

    def test_remove_raises_type_error(self, hydro_catalog):
        view = CatalogView(hydro_catalog)
        with pytest.raises(TypeError, match="read-only"):
            view.remove("flow")

    def test_add_source_raises_type_error(self, hydro_catalog, tmp_path):
        view = CatalogView(hydro_catalog)
        with pytest.raises(TypeError, match="read-only"):
            view.add_source(str(tmp_path))

    # ------------------------------------------------------------------
    # Integration with MathDataReference
    # ------------------------------------------------------------------

    def test_math_ref_resolves_from_catalog_view(self, hydro_catalog):
        """MathDataReference can use a CatalogView as its catalog."""
        usgs_view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        # "flow" and "temp" are in the view
        expr = MathDataReference("flow + temp", catalog=usgs_view)
        result = expr.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.iloc[:, 0]) == [6.0, 8.0]  # [1+5, 2+6]

    def test_math_ref_cannot_see_excluded_refs(self, hydro_catalog):
        """Variables excluded by the view are not resolved."""
        usgs_view = CatalogView(hydro_catalog, selection={"source": "USGS"})
        expr = MathDataReference("stage", catalog=usgs_view)  # stage is CDEC
        with pytest.raises(ValueError, match="No variables could be resolved"):
            expr.getData()

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def test_repr(self, hydro_catalog):
        view = CatalogView(hydro_catalog, selection={"source": "USGS"}, name="usgs_view")
        r = repr(view)
        assert "CatalogView" in r
        assert "usgs_view" in r


# ===========================================================================
# MathDataReference
# ===========================================================================


@pytest.fixture()
def ab_refs():
    a = DataReference(pd.DataFrame({"v": [1.0, 2.0, 3.0]}), name="A")
    b = DataReference(pd.DataFrame({"v": [10.0, 20.0, 30.0]}), name="B")
    return a, b


class TestMathDataReference:
    def test_basic_expression(self, ab_refs):
        a, b = ab_refs
        m = MathDataReference("A + B * 2", variable_map={"A": a, "B": b})
        result = m.getData()
        assert list(result.iloc[:, 0]) == [21.0, 42.0, 63.0]

    def test_returns_dataframe(self, ab_refs):
        a, b = ab_refs
        m = MathDataReference("A + B", variable_map={"A": a, "B": b})
        assert isinstance(m.getData(), pd.DataFrame)

    def test_multi_column_dataframe_expression(self):
        df = pd.DataFrame({"x": [1.0, 2.0], "y": [3.0, 4.0]})
        ref = DataReference(df, name="M")
        m = MathDataReference("M * 2", variable_map={"M": ref})
        result = m.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["x", "y"]
        assert list(result["x"]) == [2.0, 4.0]

    def test_numpy_function_in_expression(self, ab_refs):
        a, b = ab_refs
        m = MathDataReference("sqrt(A**2 + B**2)", variable_map={"A": a, "B": b})
        expected = list(np.sqrt(np.array([1, 4, 9]) + np.array([100, 400, 900])))
        assert list(m.getData().iloc[:, 0]) == pytest.approx(expected)

    def test_numpy_constants(self):
        a = DataReference(pd.DataFrame({"v": [0.0]}), name="A")
        m = MathDataReference("A + pi", variable_map={"A": a})
        import math

        assert m.getData().iloc[0, 0] == pytest.approx(math.pi)

    def test_expression_with_scalar(self, ab_refs):
        a, _ = ab_refs
        m = MathDataReference("A * 5 + 1", variable_map={"A": a})
        assert list(m.getData().iloc[:, 0]) == [6.0, 11.0, 16.0]

    def test_variables_resolved_from_catalog(self):
        df_x = pd.DataFrame({"v": [1.0, 2.0]})
        df_y = pd.DataFrame({"v": [3.0, 4.0]})
        cat = DataCatalog()
        cat.add(DataReference(df_x, name="X"))
        cat.add(DataReference(df_y, name="Y"))
        m = MathDataReference("X + Y", catalog=cat)
        assert list(m.getData().iloc[:, 0]) == [4.0, 6.0]

    def test_variable_map_takes_priority_over_catalog(self):
        df_local = pd.DataFrame({"v": [100.0]})
        df_cat = pd.DataFrame({"v": [1.0]})
        cat = DataCatalog()
        cat.add(DataReference(df_cat, name="X"))
        m = MathDataReference(
            "X",
            variable_map={"X": DataReference(df_local, name="X_local")},
            catalog=cat,
        )
        assert m.getData().iloc[0, 0] == 100.0

    def test_no_variables_raises(self):
        m = MathDataReference("1 + 1")
        with pytest.raises(ValueError, match="No variables could be resolved"):
            m.getData()

    def test_bad_expression_raises(self):
        a = DataReference(pd.DataFrame({"v": [1.0]}), name="A")
        m = MathDataReference("A / 0", variable_map={"A": a})
        # Division by zero on integers raises; float gives inf (no error)
        # Use a syntax error to guarantee the ValueError path
        m2 = MathDataReference("A +* B", variable_map={"A": a})
        with pytest.raises(ValueError, match="Failed to evaluate expression"):
            m2.getData()

    def test_set_catalog_chainable(self, ab_refs):
        a, _ = ab_refs
        cat = DataCatalog()
        cat.add(a)
        m = MathDataReference("A * 2")
        result = m.set_catalog(cat)
        assert result is m
        assert list(m.getData().iloc[:, 0]) == [2.0, 4.0, 6.0]

    def test_set_variable_chainable(self, ab_refs):
        a, _ = ab_refs
        m = MathDataReference("A * 3")
        result = m.set_variable("A", a)
        assert result is m
        assert list(m.getData().iloc[:, 0]) == [3.0, 6.0, 9.0]

    def test_cache_disabled_by_default(self, ab_refs):
        a, b = ab_refs
        call_count = {"n": 0}
        original_load = a._load_data.__func__ if hasattr(a._load_data, "__func__") else None

        data_store = {"v": [1.0, 2.0, 3.0]}

        def loader():
            call_count["n"] += 1
            return pd.DataFrame(data_store)

        a2 = DataReference(loader, name="A2", cache=False)  # must not cache
        m = MathDataReference("A2 * 2", variable_map={"A2": a2})
        m.getData()
        data_store["v"] = [9.0, 8.0, 7.0]
        second = m.getData()
        # Both MathDataReference (cache=False) and a2 (cache=False) re-evaluate
        assert call_count["n"] == 2

    # ------------------------------------------------------------------
    # Operator overloading on MathDataReference
    # ------------------------------------------------------------------

    def test_chain_operators(self, ab_refs):
        a, b = ab_refs
        expr = a + b * 2  # MathDataReference
        assert isinstance(expr, MathDataReference)
        assert list(expr.getData().iloc[:, 0]) == [21.0, 42.0, 63.0]

    def test_neg_math_ref(self, ab_refs):
        a, _ = ab_refs
        m = MathDataReference("A", variable_map={"A": a})
        neg = -m
        assert list(neg.getData().iloc[:, 0]) == [-1.0, -2.0, -3.0]

    def test_complex_chain(self):
        x = DataReference(pd.DataFrame({"v": [3.0, 4.0]}), name="X")
        y = DataReference(pd.DataFrame({"v": [4.0, 3.0]}), name="Y")
        mag = (x**2 + y**2) ** 0.5
        result = list(mag.getData().iloc[:, 0])
        assert result == pytest.approx([5.0, 5.0])

    def test_repr(self, ab_refs):
        a, b = ab_refs
        m = MathDataReference("A + B", name="sum", variable_map={"A": a, "B": b})
        assert "sum" in repr(m)
        assert "A + B" in repr(m)


# ===========================================================================
# DataCatalog
# ===========================================================================


class TestDataCatalog:
    def test_add_and_get(self, simple_df):
        cat = DataCatalog()
        ref = DataReference(simple_df, name="r1")
        cat.add(ref)
        assert cat.get("r1") is ref

    def test_getitem(self, simple_df):
        cat = DataCatalog()
        ref = DataReference(simple_df, name="r")
        cat.add(ref)
        assert cat["r"] is ref

    def test_add_requires_name(self, simple_df):
        cat = DataCatalog()
        with pytest.raises(ValueError, match="non-empty name"):
            cat.add(DataReference(simple_df))

    def test_contains(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r"))
        assert "r" in cat
        assert "other" not in cat

    def test_len(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r1"))
        cat.add(DataReference(simple_df.copy(), name="r2"))
        assert len(cat) == 2

    def test_iter(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="a"))
        cat.add(DataReference(simple_df.copy(), name="b"))
        names = [r.name for r in cat]
        assert set(names) == {"a", "b"}

    def test_remove(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r"))
        cat.remove("r")
        assert "r" not in cat

    def test_remove_nonexistent_raises(self):
        cat = DataCatalog()
        with pytest.raises(KeyError):
            cat.remove("nope")

    def test_get_nonexistent_raises(self):
        cat = DataCatalog()
        with pytest.raises(KeyError):
            cat.get("nope")

    def test_list_names(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="a"))
        cat.add(DataReference(simple_df.copy(), name="b"))
        assert cat.list_names() == ["a", "b"]

    def test_list(self, simple_df):
        cat = DataCatalog()
        r = DataReference(simple_df, name="r")
        cat.add(r)
        assert r in cat.list()

    # ------------------------------------------------------------------
    # Duplicate detection
    # ------------------------------------------------------------------

    def test_exact_duplicate_same_object_raises(self, simple_df):
        cat = DataCatalog()
        r = DataReference(simple_df, name="r", unit="K")
        cat.add(r)
        with pytest.raises(ValueError, match="same source and identical metadata"):
            cat.add(r)

    def test_exact_duplicate_equal_dataframe_raises(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r", unit="K"))
        with pytest.raises(ValueError, match="same source and identical metadata"):
            cat.add(DataReference(simple_df.copy(), name="r", unit="K"))

    def test_same_name_different_attrs_replaces(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r", unit="K"))
        cat.add(DataReference(simple_df, name="r", unit="degC"))
        assert cat["r"].get_attribute("unit") == "degC"

    def test_same_name_different_source_data_replaces(self, simple_df):
        cat = DataCatalog()
        r1 = DataReference(simple_df, name="r", unit="K")
        cat.add(r1)
        df2 = pd.DataFrame({"temperature": [0.0], "season": ["winter"]})
        r2 = DataReference(df2, name="r", unit="K")
        cat.add(r2)
        assert cat["r"] is r2

    def test_add_is_chainable(self, simple_df):
        cat = DataCatalog()
        result = cat.add(DataReference(simple_df, name="r"))
        assert result is cat

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def test_search_scalar(self, simple_df):
        cat = DataCatalog()
        r1 = DataReference(simple_df, name="r1", variable="T", unit="K")
        r2 = DataReference(simple_df.copy(), name="r2", variable="P", unit="Pa")
        cat.add(r1)
        cat.add(r2)
        assert cat.search(variable="T") == [r1]
        assert cat.search(variable="P") == [r2]

    def test_search_multiple_criteria(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r1", variable="T", unit="K"))
        cat.add(DataReference(simple_df.copy(), name="r2", variable="T", unit="Pa"))
        cat.add(DataReference(simple_df.copy(), name="r3", variable="P", unit="K"))
        result = cat.search(variable="T", unit="K")
        assert len(result) == 1
        assert result[0].name == "r1"

    def test_search_callable_predicate(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r1", year=2020))
        cat.add(DataReference(simple_df.copy(), name="r2", year=2018))
        result = cat.search(year=lambda y: y >= 2019)
        assert len(result) == 1
        assert result[0].name == "r1"

    def test_search_no_match_returns_empty(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r", variable="T"))
        assert cat.search(variable="X") == []

    # ------------------------------------------------------------------
    # Schema map
    # ------------------------------------------------------------------

    def test_search_with_canonical_name(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param", "unit": "units"})
        ref = DataReference(simple_df, name="r", variable="temperature", unit="degC")
        cat.add(ref)
        # Search using canonical name
        assert cat.search(param="temperature") == [ref]
        assert cat.search(units="degC") == [ref]

    def test_search_raw_name_still_works(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        ref = DataReference(simple_df, name="r", variable="temperature")
        cat.add(ref)
        # Raw name works too (it's not in the canonical→raw reverse map,
        # so treated as-is)
        assert cat.search(variable="temperature") == [ref]

    def test_get_canonical_attribute(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        cat.add(DataReference(simple_df, name="r", variable="T"))
        assert cat.get_canonical_attribute("r", "param") == "T"

    def test_set_schema_map_chainable(self, simple_df):
        cat = DataCatalog()
        result = cat.set_schema_map({"variable": "param"})
        assert result is cat

    # ------------------------------------------------------------------
    # to_dataframe
    # ------------------------------------------------------------------

    def test_to_dataframe_structure(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r1", variable="T", unit="K"))
        cat.add(DataReference(simple_df.copy(), name="r2", variable="P", unit="Pa"))
        df = cat.to_dataframe()
        assert set(df.index) == {"r1", "r2"}
        assert "variable" in df.columns
        assert "unit" in df.columns

    def test_to_dataframe_applies_schema_map(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        cat.add(DataReference(simple_df, name="r", variable="T"))
        df = cat.to_dataframe()
        assert "param" in df.columns
        assert "variable" not in df.columns

    def test_to_dataframe_empty_catalog(self):
        cat = DataCatalog()
        df = cat.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    # ------------------------------------------------------------------
    # Reader registration
    # ------------------------------------------------------------------

    def test_add_reader_and_add_source(self, csv_dir):
        reader = PatternCSVDirectoryReader("{name}__{stationid}__{source}")
        cat = DataCatalog().add_reader(reader)
        cat.add_source(str(csv_dir))
        # 3 matched, 1 unmatched (skipped)
        assert len(cat) == 3

    def test_add_source_no_reader_raises(self, tmp_path):
        cat = DataCatalog()
        with pytest.raises(ValueError, match="No registered DataCatalogReader"):
            cat.add_source(str(tmp_path))

    def test_register_reader_is_global(self, csv_dir, monkeypatch):
        """register_reader() affects all new DataCatalog instances."""
        reader = PatternCSVDirectoryReader("{name}__{stationid}__{source}")
        # Patch the global list so we don't pollute other tests
        original = DataCatalog._global_readers[:]
        try:
            DataCatalog.register_reader(reader)
            cat = DataCatalog()
            cat.add_source(str(csv_dir))
            assert len(cat) == 3
        finally:
            DataCatalog._global_readers[:] = original

    def test_add_reader_is_instance_local(self, csv_dir):
        """Instance readers do not bleed into sibling catalogs."""
        reader = PatternCSVDirectoryReader("{name}__{stationid}__{source}")
        cat1 = DataCatalog().add_reader(reader)
        cat2 = DataCatalog()  # no reader added
        cat1.add_source(str(csv_dir))
        assert len(cat1) == 3
        with pytest.raises(ValueError):
            cat2.add_source(str(csv_dir))

    def test_add_reader_chainable(self):
        reader = CSVDirectoryReader()
        cat = DataCatalog()
        result = cat.add_reader(reader)
        assert result is cat

    # ------------------------------------------------------------------
    # Repr / str
    # ------------------------------------------------------------------

    def test_repr(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(simple_df, name="r"))
        assert "DataCatalog" in repr(cat)
        assert "1" in repr(cat)


# ===========================================================================
# CSVDirectoryReader
# ===========================================================================


class TestCSVDirectoryReader:
    def test_can_handle_directory(self, tmp_path):
        reader = CSVDirectoryReader()
        assert reader.can_handle(str(tmp_path))

    def test_can_handle_csv_file(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("a,b\n1,2\n")
        assert CSVDirectoryReader().can_handle(str(p))

    def test_cannot_handle_non_csv_file(self, tmp_path):
        p = tmp_path / "data.json"
        p.write_text("{}")
        assert not CSVDirectoryReader().can_handle(str(p))

    def test_cannot_handle_non_path(self):
        assert not CSVDirectoryReader().can_handle(42)

    def test_reads_all_csvs(self, tmp_path):
        for name in ("a.csv", "b.csv", "c.csv"):
            pd.DataFrame({"v": [1]}).to_csv(tmp_path / name, index=False)
        refs = CSVDirectoryReader().read(str(tmp_path))
        assert sorted(r.name for r in refs) == ["a", "b", "c"]

    def test_each_ref_is_data_reference(self, tmp_path):
        pd.DataFrame({"v": [1]}).to_csv(tmp_path / "x.csv", index=False)
        refs = CSVDirectoryReader().read(str(tmp_path))
        assert all(isinstance(r, DataReference) for r in refs)

    def test_file_path_attribute_set(self, tmp_path):
        p = tmp_path / "x.csv"
        pd.DataFrame({"v": [1]}).to_csv(p, index=False)
        refs = CSVDirectoryReader().read(str(tmp_path))
        assert refs[0].get_attribute("file_path") == str(p)

    def test_default_attributes_applied(self, tmp_path):
        pd.DataFrame({"v": [1]}).to_csv(tmp_path / "x.csv", index=False)
        refs = CSVDirectoryReader(project="climate").read(str(tmp_path))
        assert refs[0].get_attribute("project") == "climate"

    def test_get_data_from_ref(self, tmp_path):
        df = pd.DataFrame({"val": [10, 20, 30]})
        df.to_csv(tmp_path / "data.csv", index=False)
        refs = CSVDirectoryReader().read(str(tmp_path))
        result = refs[0].getData()
        assert list(result["val"]) == [10, 20, 30]

    def test_empty_directory_returns_empty_list(self, tmp_path):
        refs = CSVDirectoryReader().read(str(tmp_path))
        assert refs == []

    def test_single_file_mode(self, tmp_path):
        p = tmp_path / "sensor.csv"
        pd.DataFrame({"v": [1, 2]}).to_csv(p, index=False)
        refs = CSVDirectoryReader().read(str(p))
        assert len(refs) == 1
        assert refs[0].name == "sensor"


# ===========================================================================
# PatternCSVDirectoryReader – _pattern_to_regex helper
# ===========================================================================


class TestPatternToRegex:
    def test_basic_double_underscore(self):
        rx = _pattern_to_regex("{name}__{stationid}__{source}")
        m = rx.match("flow__STA001__USGS")
        assert m is not None
        assert m.group("name") == "flow"
        assert m.group("stationid") == "STA001"
        assert m.group("source") == "USGS"

    def test_field_with_underscores_in_value(self):
        rx = _pattern_to_regex("{name}__{stationid}__{source}")
        m = rx.match("flow_rate__STA001__USGS_NWIS")
        assert m is not None
        assert m.group("name") == "flow_rate"
        assert m.group("stationid") == "STA001"
        assert m.group("source") == "USGS_NWIS"

    def test_single_hyphen_separator(self):
        rx = _pattern_to_regex("{source}-{stationid}-{name}")
        m = rx.match("USGS-STA001-flow")
        assert m is not None
        assert m.group("source") == "USGS"
        assert m.group("stationid") == "STA001"
        assert m.group("name") == "flow"

    def test_no_match_returns_none(self):
        rx = _pattern_to_regex("{name}__{stationid}__{source}")
        assert rx.match("unmatched") is None
        assert rx.match("only__two") is None

    def test_single_field_pattern(self):
        rx = _pattern_to_regex("{name}")
        m = rx.match("anything_goes_here")
        assert m is not None
        assert m.group("name") == "anything_goes_here"

    def test_anchored_matching(self):
        rx = _pattern_to_regex("{name}__{stationid}__{source}")
        # Extra trailing characters should not match
        assert rx.match("a__b__c__extra") is None


# ===========================================================================
# PatternCSVDirectoryReader
# ===========================================================================


class TestPatternCSVDirectoryReader:
    PATTERN = "{name}__{stationid}__{source}"

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def test_invalid_pattern_no_placeholders_raises(self):
        with pytest.raises(ValueError, match="no \\{field\\} placeholders"):
            PatternCSVDirectoryReader("no_placeholders_here")

    def test_pattern_property(self):
        reader = PatternCSVDirectoryReader(self.PATTERN)
        assert reader.pattern == self.PATTERN

    def test_fields_property(self):
        reader = PatternCSVDirectoryReader(self.PATTERN)
        assert reader.fields == ["name", "stationid", "source"]

    def test_repr(self):
        reader = PatternCSVDirectoryReader(self.PATTERN)
        assert self.PATTERN in repr(reader)

    # ------------------------------------------------------------------
    # can_handle
    # ------------------------------------------------------------------

    def test_can_handle_existing_directory(self, tmp_path):
        assert PatternCSVDirectoryReader(self.PATTERN).can_handle(str(tmp_path))

    def test_cannot_handle_nonexistent_path(self, tmp_path):
        assert not PatternCSVDirectoryReader(self.PATTERN).can_handle(str(tmp_path / "nope"))

    def test_cannot_handle_csv_file(self, tmp_path):
        p = tmp_path / "x.csv"
        p.write_text("a\n1\n")
        # PatternCSVDirectoryReader requires a directory, not a single file
        assert not PatternCSVDirectoryReader(self.PATTERN).can_handle(str(p))

    def test_cannot_handle_non_path_type(self):
        assert not PatternCSVDirectoryReader(self.PATTERN).can_handle(42)

    # ------------------------------------------------------------------
    # read – reference count and names
    # ------------------------------------------------------------------

    def test_matched_files_are_read(self, csv_dir):
        reader = PatternCSVDirectoryReader(self.PATTERN)
        refs = reader.read(str(csv_dir))
        assert len(refs) == 3

    def test_unmatched_file_is_skipped(self, csv_dir, caplog):
        import logging

        reader = PatternCSVDirectoryReader(self.PATTERN)
        with caplog.at_level(logging.WARNING):
            refs = reader.read(str(csv_dir))
        assert "unmatched_file.csv" in caplog.text
        assert len(refs) == 3

    def test_empty_directory_returns_empty(self, tmp_path):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(tmp_path))
        assert refs == []

    def test_names_come_from_name_field(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        names = {r.name for r in refs}
        assert names == {"flow", "stage", "temperature"}

    # ------------------------------------------------------------------
    # read – metadata extraction
    # ------------------------------------------------------------------

    def test_stationid_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        by_name = {r.name: r for r in refs}
        assert by_name["flow"].get_attribute("stationid") == "STA001"
        assert by_name["stage"].get_attribute("stationid") == "STA002"
        assert by_name["temperature"].get_attribute("stationid") == "STA001"

    def test_source_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        by_name = {r.name: r for r in refs}
        assert by_name["flow"].get_attribute("source") == "USGS"
        assert by_name["stage"].get_attribute("source") == "CDEC"
        assert by_name["temperature"].get_attribute("source") == "CDEC"

    def test_file_path_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        for r in refs:
            assert r.get_attribute("file_path") is not None
            assert r.get_attribute("file_path").endswith(".csv")

    def test_format_metadata_is_csv(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        assert all(r.get_attribute("format") == "csv" for r in refs)

    def test_default_attributes_applied(self, csv_dir):
        reader = PatternCSVDirectoryReader(self.PATTERN, project="hydro", version=2)
        refs = reader.read(str(csv_dir))
        assert all(r.get_attribute("project") == "hydro" for r in refs)
        assert all(r.get_attribute("version") == 2 for r in refs)

    def test_name_field_not_in_metadata(self, csv_dir):
        """The 'name' placeholder is consumed as the reference name, not stored as an attribute."""
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        for r in refs:
            # 'name' should NOT appear as an extra attribute
            assert not r.has_attribute("name")

    # ------------------------------------------------------------------
    # read – getData() round-trip
    # ------------------------------------------------------------------

    def test_getData_returns_csv_contents(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        by_name = {r.name: r for r in refs}
        result = by_name["flow"].getData()
        assert isinstance(result, pd.DataFrame)
        assert "value" in result.columns
        assert list(result["value"]) == [1.0, 2.0, 3.0]

    # ------------------------------------------------------------------
    # Integration with DataCatalog
    # ------------------------------------------------------------------

    def test_catalog_search_by_stationid(self, csv_dir):
        cat = DataCatalog().add_reader(PatternCSVDirectoryReader(self.PATTERN))
        cat.add_source(str(csv_dir))
        results = cat.search(stationid="STA001")
        assert len(results) == 2
        assert {r.name for r in results} == {"flow", "temperature"}

    def test_catalog_search_by_source(self, csv_dir):
        cat = DataCatalog().add_reader(PatternCSVDirectoryReader(self.PATTERN))
        cat.add_source(str(csv_dir))
        assert len(cat.search(source="CDEC")) == 2
        assert len(cat.search(source="USGS")) == 1

    def test_catalog_search_combined_criteria(self, csv_dir):
        cat = DataCatalog().add_reader(PatternCSVDirectoryReader(self.PATTERN))
        cat.add_source(str(csv_dir))
        results = cat.search(stationid="STA001", source="CDEC")
        assert len(results) == 1
        assert results[0].name == "temperature"

    def test_catalog_search_with_schema_map(self, csv_dir):
        cat = DataCatalog(schema_map={"stationid": "station", "source": "provider"})
        cat.add_reader(PatternCSVDirectoryReader(self.PATTERN))
        cat.add_source(str(csv_dir))
        results = cat.search(station="STA001")
        assert len(results) == 2
        results2 = cat.search(provider="USGS")
        assert len(results2) == 1

    def test_catalog_to_dataframe(self, csv_dir):
        cat = DataCatalog().add_reader(PatternCSVDirectoryReader(self.PATTERN))
        cat.add_source(str(csv_dir))
        df = cat.to_dataframe()
        assert set(df.index) == {"flow", "stage", "temperature"}
        assert "stationid" in df.columns
        assert "source" in df.columns

    def test_alternate_separator_pattern(self, tmp_path):
        """Reader works with a hyphen separator instead of double underscore."""
        pattern = "{source}-{stationid}-{name}"
        files = {
            "USGS-STA001-flow.csv": [1.0, 2.0],
            "CDEC-STA002-stage.csv": [3.0, 4.0],
        }
        for fname, vals in files.items():
            pd.DataFrame({"value": vals}).to_csv(tmp_path / fname, index=False)

        reader = PatternCSVDirectoryReader(pattern)
        refs = reader.read(str(tmp_path))
        by_name = {r.name: r for r in refs}

        assert set(by_name.keys()) == {"flow", "stage"}
        assert by_name["flow"].get_attribute("source") == "USGS"
        assert by_name["flow"].get_attribute("stationid") == "STA001"
        assert by_name["stage"].get_attribute("source") == "CDEC"

    def test_extra_field_in_pattern(self, tmp_path):
        """Additional fields beyond name/stationid/source are stored as metadata."""
        pattern = "{name}__{stationid}__{source}__{variable}"
        pd.DataFrame({"v": [1.0]}).to_csv(
            tmp_path / "flow__STA001__USGS__discharge.csv", index=False
        )
        refs = PatternCSVDirectoryReader(pattern).read(str(tmp_path))
        assert len(refs) == 1
        assert refs[0].get_attribute("variable") == "discharge"

    def test_references_sorted_by_filename(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).read(str(csv_dir))
        names = [r.name for r in refs]
        assert names == sorted(names)
