"""Tests for dvue.catalog – DataReference, CatalogView, MathDataReference,
DataCatalog, CSVDirectoryReader, and PatternCSVDirectoryReader."""

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from dvue.catalog import (
    CSVDirectoryReader,
    CSVDirectoryBuilder,
    CatalogView,
    CatalogBuilder,
    DataCatalog,
    DataCatalogReader,
    DataReference,
    DataReferenceReader,
    InMemoryDataReferenceReader,
    CallableDataReferenceReader,
    FileDataReferenceReader,
    MathDataReference,
    MathDataCatalogReader,
    PatternCSVDirectoryReader,
    PatternCSVDirectoryBuilder,
    _pattern_to_regex,
    build_catalog_from_dataframe,
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
    def test_source_in_attributes(self, simple_df):
        ref = DataReference(source="/data/flow.csv", reader=InMemoryDataReferenceReader(simple_df), name="r")
        assert ref.get_attribute("source") == "/data/flow.csv"
        assert "source" in ref.attributes

    def test_source_searchable_via_catalog(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="s1.csv", reader=InMemoryDataReferenceReader(simple_df), name="a"))
        cat.add(DataReference(source="s2.csv", reader=InMemoryDataReferenceReader(simple_df.copy()), name="b"))
        assert cat.search(source="s1.csv") == [cat["a"]]
        assert cat.search(source="s2.csv") == [cat["b"]]

    def test_source_in_to_dataframe(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="/x.csv", reader=InMemoryDataReferenceReader(simple_df), name="r"))
        df = cat.to_dataframe()
        assert "source" in df.columns
        assert df.loc["r", "source"] == "/x.csv"

    def test_source_not_in_default_ref_key(self, simple_df):
        """source should not appear in ref_key() by default."""
        ref = DataReference(source="/data/flow.csv", reader=InMemoryDataReferenceReader(simple_df),
                            name="r", station="A", variable="T")
        assert ref.ref_key() == "A_T"  # source excluded

    def test_source_not_in_default_key_attributes(self, simple_df):
        ref = DataReference(source="s.csv", reader=InMemoryDataReferenceReader(simple_df),
                            name="r", station="A")
        assert "source" not in ref.get_key_attributes()

    def test_source_included_when_set_as_key_attribute(self, simple_df):
        ref = DataReference(source="s.csv", reader=InMemoryDataReferenceReader(simple_df),
                            name="r", station="A")
        ref.set_key_attributes(["source", "station"])
        assert "source_csv" in ref.ref_key() or "s_csv" in ref.ref_key()

    def test_from_dataframe(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="climate")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["temperature", "season"]

    def test_from_dataframe_returns_copy(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        result = ref.getData()
        result["new_col"] = 0
        # Original not mutated
        assert "new_col" not in ref.getData().columns

    def test_from_series_wrapped_in_dataframe(self):
        s = pd.Series([1, 2, 3], name="x")
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(s), name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert "x" in result.columns

    def test_from_callable(self, simple_df):
        ref = DataReference(source="", reader=CallableDataReferenceReader(lambda: simple_df.copy()), name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4

    def test_from_callable_returning_series(self):
        ref = DataReference(source="", reader=CallableDataReferenceReader(lambda: pd.Series([1, 2, 3], name="v")), name="r")
        assert isinstance(ref.getData(), pd.DataFrame)

    def test_from_csv_path(self, tmp_path, simple_df):
        p = tmp_path / "data.csv"
        simple_df.to_csv(p, index=False)
        ref = DataReference(source=str(p), reader="dvue.catalog.FileDataReferenceReader", name="r")
        result = ref.getData()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["temperature", "season"]

    def test_from_pathlib_path(self, tmp_path, simple_df):
        p = tmp_path / "data.csv"
        simple_df.to_csv(p, index=False)
        ref = DataReference(source=str(p), reader="dvue.catalog.FileDataReferenceReader", name="r")
        assert isinstance(ref.getData(), pd.DataFrame)

    def test_unsupported_extension_raises(self, tmp_path):
        p = tmp_path / "data.xyz"
        p.write_text("x")
        ref = DataReference(source=str(p), reader="dvue.catalog.FileDataReferenceReader", name="r")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            ref.getData()

    def test_cache_enabled_by_default(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(source="", reader=CallableDataReferenceReader(loader), name="r")
        ref.getData()
        ref.getData()
        assert call_count["n"] == 1

    def test_cache_disabled(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(source="", reader=CallableDataReferenceReader(loader), name="r", cache=False)
        ref.getData()
        ref.getData()
        assert call_count["n"] == 2

    def test_invalidate_cache(self, simple_df):
        call_count = {"n": 0}

        def loader():
            call_count["n"] += 1
            return simple_df.copy()

        ref = DataReference(source="", reader=CallableDataReferenceReader(loader), name="r")
        ref.getData()
        ref.invalidate_cache()
        ref.getData()
        assert call_count["n"] == 2

    def test_invalidate_cache_is_chainable(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        assert ref.invalidate_cache() is ref

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def test_attributes_stored(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="temperature", unit="degC")
        assert ref.get_attribute("variable") == "temperature"
        assert ref.get_attribute("unit") == "degC"

    def test_get_attribute_default(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        assert ref.get_attribute("missing") is None
        assert ref.get_attribute("missing", "fallback") == "fallback"

    def test_has_attribute(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", tag="test")
        assert ref.has_attribute("tag")
        assert not ref.has_attribute("nope")

    # ------------------------------------------------------------------
    # ref_key
    # ------------------------------------------------------------------

    def test_ref_key_default_joins_attribute_values(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind", interval="hourly")
        assert ref.ref_key() == "A_wind_hourly"

    def test_ref_key_sanitizes_spaces(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station_name="Station A")
        assert ref.ref_key() == "Station_A"

    def test_ref_key_sanitizes_special_chars(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="m/s")
        assert ref.ref_key() == "m_s"

    def test_ref_key_includes_numeric_attributes(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", year=2020)
        assert ref.ref_key() == "_2020"  # prefixed with _ to form a valid Python identifier

    def test_ref_key_never_starts_with_digit(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station_id="1", variable="flow")
        key = ref.ref_key()
        assert key[0].isalpha() or key[0] == "_", f"ref_key {key!r} starts with a digit"

    def test_ref_key_skips_complex_types(self, simple_df):
        class _Blob:
            pass

        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", blob=_Blob())
        assert ref.ref_key() == "A"

    def test_ref_key_empty_when_no_attributes(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        assert ref.ref_key() == ""

    def test_ref_key_override_in_subclass(self, simple_df):
        class CustomRef(DataReference):
            def ref_key(self) -> str:
                return self.get_attribute("station", "") + "_custom"

        ref = CustomRef(simple_df, name="r", station="A")
        assert ref.ref_key() == "A_custom"

    # ------------------------------------------------------------------
    # Key attributes
    # ------------------------------------------------------------------

    def test_get_key_attributes_default_returns_all_attr_names(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind")
        assert ref.get_key_attributes() == ["station", "variable"]

    def test_get_key_attributes_returns_set_names(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind", interval="hourly")
        ref.set_key_attributes(["station", "variable"])
        assert ref.get_key_attributes() == ["station", "variable"]

    def test_set_key_attributes_is_chainable(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A")
        result = ref.set_key_attributes(["station"])
        assert result is ref

    def test_ref_key_respects_key_attributes_subset(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind", interval="hourly")
        ref.set_key_attributes(["station", "variable"])
        assert ref.ref_key() == "A_wind"

    def test_ref_key_respects_key_attributes_order(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind", interval="hourly")
        ref.set_key_attributes(["variable", "station"])
        assert ref.ref_key() == "wind_A"

    def test_ref_key_single_key_attribute(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind")
        ref.set_key_attributes(["variable"])
        assert ref.ref_key() == "wind"

    def test_ref_key_empty_key_attributes_returns_empty(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind")
        ref.set_key_attributes([])
        assert ref.ref_key() == ""

    def test_ref_key_unknown_key_attribute_skipped(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A")
        ref.set_key_attributes(["station", "nonexistent"])
        assert ref.ref_key() == "A"

    def test_ref_key_without_key_attributes_uses_all(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind", interval="hourly")
        assert ref.ref_key() == "A_wind_hourly"

    def test_get_key_attributes_returns_copy(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A")
        ref.set_key_attributes(["station"])
        keys = ref.get_key_attributes()
        keys.append("mutated")
        assert ref.get_key_attributes() == ["station"]

    def test_set_key_attributes_can_be_updated(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", station="A", variable="wind")
        ref.set_key_attributes(["station"])
        assert ref.ref_key() == "A"
        ref.set_key_attributes(["variable"])
        assert ref.ref_key() == "wind"

    def test_set_attribute_chainable(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        result = ref.set_attribute("a", 1).set_attribute("b", 2)
        assert result is ref
        assert ref.get_attribute("a") == 1
        assert ref.get_attribute("b") == 2

    def test_attributes_returns_copy(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", x=1)
        d = ref.attributes
        d["y"] = 2
        assert not ref.has_attribute("y")

    def test_matches_exact(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="T", unit="K")
        assert ref.matches(variable="T")
        assert ref.matches(variable="T", unit="K")
        assert not ref.matches(variable="T", unit="degC")

    def test_matches_callable_predicate(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", year=2020)
        assert ref.matches(year=lambda y: y >= 2019)
        assert not ref.matches(year=lambda y: y >= 2021)

    # ------------------------------------------------------------------
    # Operator overloading
    # ------------------------------------------------------------------

    def test_add_two_refs(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0, 2.0]})), name="A")
        b = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [10.0, 20.0]})), name="B")
        result = (a + b).getData()
        assert list(result.iloc[:, 0]) == [11.0, 22.0]

    def test_mul_ref_by_scalar(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [2.0, 4.0]})), name="A")
        result = (a * 3).getData()
        assert list(result.iloc[:, 0]) == [6.0, 12.0]

    def test_scalar_mul_ref(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [2.0, 4.0]})), name="A")
        result = (3 * a).getData()
        assert list(result.iloc[:, 0]) == [6.0, 12.0]

    def test_neg_ref(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0, -2.0]})), name="A")
        result = (-a).getData()
        assert list(result.iloc[:, 0]) == [-1.0, 2.0]

    def test_sub_scalar(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [5.0, 10.0]})), name="A")
        result = (a - 2).getData()
        assert list(result.iloc[:, 0]) == [3.0, 8.0]

    def test_div_ref(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [6.0, 9.0]})), name="A")
        b = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [2.0, 3.0]})), name="B")
        result = (a / b).getData()
        assert list(result.iloc[:, 0]) == [3.0, 3.0]

    def test_pow_ref(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [2.0, 3.0]})), name="A")
        result = (a**2).getData()
        assert list(result.iloc[:, 0]) == [4.0, 9.0]

    # ------------------------------------------------------------------
    # Repr / str
    # ------------------------------------------------------------------

    def test_repr_contains_name(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="my_ref")
        assert "my_ref" in repr(ref)

    def test_str_contains_name(self, simple_df):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="my_ref")
        assert "my_ref" in str(ref)


# ===========================================================================
# DataReferenceReader subclasses
# ===========================================================================


class TestInMemoryDataReferenceReader:
    def test_load_returns_dataframe(self):
        df = pd.DataFrame({"v": [1.0, 2.0]})
        reader = InMemoryDataReferenceReader(df)
        result = reader.load()
        pd.testing.assert_frame_equal(result, df)

    def test_load_returns_copy(self):
        df = pd.DataFrame({"v": [1.0, 2.0]})
        reader = InMemoryDataReferenceReader(df)
        result = reader.load()
        result["extra"] = 99
        assert "extra" not in reader.load().columns

    def test_load_wraps_series_in_dataframe(self):
        s = pd.Series([1, 2, 3], name="x")
        reader = InMemoryDataReferenceReader(s)
        result = reader.load()
        assert isinstance(result, pd.DataFrame)
        assert "x" in result.columns

    def test_load_ignores_extra_attributes(self):
        df = pd.DataFrame({"v": [1.0]})
        reader = InMemoryDataReferenceReader(df)
        result = reader.load(name="ignored", unit="K")
        pd.testing.assert_frame_equal(result, df)


class TestCallableDataReferenceReader:
    def test_load_calls_callable(self):
        df = pd.DataFrame({"v": [42.0]})
        reader = CallableDataReferenceReader(lambda: df.copy())
        result = reader.load()
        pd.testing.assert_frame_equal(result, df)

    def test_load_wraps_series(self):
        reader = CallableDataReferenceReader(lambda: pd.Series([1, 2], name="y"))
        result = reader.load()
        assert isinstance(result, pd.DataFrame)
        assert "y" in result.columns

    def test_callable_called_each_time_when_no_cache(self):
        calls = []

        def fn():
            calls.append(1)
            return pd.DataFrame({"v": [len(calls)]})

        reader = CallableDataReferenceReader(fn)
        ref = DataReference(source="", reader=reader, name="r", cache=False)
        ref.getData()
        ref.getData()
        assert len(calls) == 2

    def test_cache_prevents_repeated_calls(self):
        calls = []

        def fn():
            calls.append(1)
            return pd.DataFrame({"v": [1]})

        reader = CallableDataReferenceReader(fn)
        ref = DataReference(source="", reader=reader, name="r", cache=True)
        ref.getData()
        ref.getData()
        assert len(calls) == 1


class TestFileDataReferenceReader:
    def test_load_csv(self, tmp_path):
        p = tmp_path / "data.csv"
        pd.DataFrame({"val": [10, 20]}).to_csv(p, index=False)
        reader = FileDataReferenceReader()
        result = reader.load(file_path=str(p), format="csv")
        assert list(result["val"]) == [10, 20]

    def test_missing_file_path_raises(self):
        reader = FileDataReferenceReader()
        with pytest.raises((KeyError, ValueError)):
            reader.load(format="csv")

    def test_flyweight_shared_across_refs(self, tmp_path):
        p1 = tmp_path / "a.csv"
        p2 = tmp_path / "b.csv"
        pd.DataFrame({"v": [1]}).to_csv(p1, index=False)
        pd.DataFrame({"v": [2]}).to_csv(p2, index=False)
        reader = FileDataReferenceReader()
        r1 = DataReference(source="", reader=reader, name="a", file_path=str(p1), format="csv")
        r2 = DataReference(source="", reader=reader, name="b", file_path=str(p2), format="csv")
        assert r1._reader_instance is r2._reader_instance
        assert list(r1.getData()["v"]) == [1]
        assert list(r2.getData()["v"]) == [2]



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
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"value": [1.0, 2.0]})), name="flow", stationid="STA001", variable="flow"
        ).set_attribute("source", "USGS")
    )
    cat.add(
        DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"value": [3.0, 4.0]})), name="stage", stationid="STA002", variable="stage"
        ).set_attribute("source", "CDEC")
    )
    cat.add(
        DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"value": [5.0, 6.0]})), name="temp",
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
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1]})), name="r", stationid="S01"))
        view = CatalogView(cat, selection={"stationid": "S01"})
        assert len(view) == 1
        # Canonical name works in search
        assert view.search(station="S01") == [view["r"]]

    def test_to_dataframe_applies_schema_map(self):
        cat = DataCatalog(schema_map={"stationid": "station"})
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1]})), name="r", stationid="S01"))
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
                source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"value": [7.0]})),
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
            view.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="x"))

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
    a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0, 2.0, 3.0]})), name="A")
    b = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [10.0, 20.0, 30.0]})), name="B")
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
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(df), name="M")
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
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [0.0]})), name="A")
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
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(df_x), name="X"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(df_y), name="Y"))
        m = MathDataReference("X + Y", catalog=cat)
        assert list(m.getData().iloc[:, 0]) == [4.0, 6.0]

    def test_variable_map_takes_priority_over_catalog(self):
        df_local = pd.DataFrame({"v": [100.0]})
        df_cat = pd.DataFrame({"v": [1.0]})
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(df_cat), name="X"))
        m = MathDataReference(
            "X",
            variable_map={"X": DataReference(source="", reader=InMemoryDataReferenceReader(df_local), name="X_local")},
            catalog=cat,
        )
        assert m.getData().iloc[0, 0] == 100.0

    def test_no_variables_raises(self):
        m = MathDataReference("1 + 1")
        with pytest.raises(ValueError, match="No variables could be resolved"):
            m.getData()

    def test_bad_expression_raises(self):
        a = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]})), name="A")
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

        a2 = DataReference(source="", reader=CallableDataReferenceReader(loader), name="A2", cache=False)  # must not cache
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
        x = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [3.0, 4.0]})), name="X")
        y = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [4.0, 3.0]})), name="Y")
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
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1")
        cat.add(ref)
        assert cat.get("r1") is ref

    def test_getitem(self, simple_df):
        cat = DataCatalog()
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        cat.add(ref)
        assert cat["r"] is ref

    def test_add_requires_name(self, simple_df):
        cat = DataCatalog()
        with pytest.raises(ValueError, match="non-empty name"):
            cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df)))

    def test_contains(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r"))
        assert "r" in cat
        assert "other" not in cat

    def test_len(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r2"))
        assert len(cat) == 2

    def test_iter(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="a"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="b"))
        names = [r.name for r in cat]
        assert set(names) == {"a", "b"}

    def test_remove(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r"))
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
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="a"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="b"))
        assert cat.list_names() == ["a", "b"]

    def test_list(self, simple_df):
        cat = DataCatalog()
        r = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r")
        cat.add(r)
        assert r in cat.list()

    # ------------------------------------------------------------------
    # Duplicate detection
    # ------------------------------------------------------------------

    def test_exact_duplicate_same_object_raises(self, simple_df):
        cat = DataCatalog()
        r = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K")
        cat.add(r)
        with pytest.raises(ValueError, match="same reader, source, and"):
            cat.add(r)

    def test_exact_duplicate_same_reader_instance_raises(self, simple_df):
        cat = DataCatalog()
        reader = InMemoryDataReferenceReader(simple_df)
        cat.add(DataReference(source="", reader=reader, name="r", unit="K"))
        with pytest.raises(ValueError, match="same reader, source, and"):
            cat.add(DataReference(source="", reader=reader, name="r", unit="K"))

    def test_different_reader_instances_same_name_replaces(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K"))
        r2 = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r", unit="K")
        cat.add(r2)  # different reader instance — should replace, not raise
        assert cat["r"] is r2

    def test_same_name_different_attrs_replaces(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="degC"))
        assert cat["r"].get_attribute("unit") == "degC"

    def test_same_name_different_source_data_replaces(self, simple_df):
        cat = DataCatalog()
        r1 = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K")
        cat.add(r1)
        df2 = pd.DataFrame({"temperature": [0.0], "season": ["winter"]})
        r2 = DataReference(source="", reader=InMemoryDataReferenceReader(df2), name="r", unit="K")
        cat.add(r2)
        assert cat["r"] is r2

    def test_add_is_chainable(self, simple_df):
        cat = DataCatalog()
        result = cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r"))
        assert result is cat

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def test_search_scalar(self, simple_df):
        cat = DataCatalog()
        r1 = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1", variable="T", unit="K")
        r2 = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r2", variable="P", unit="Pa")
        cat.add(r1)
        cat.add(r2)
        assert cat.search(variable="T") == [r1]
        assert cat.search(variable="P") == [r2]

    def test_search_multiple_criteria(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1", variable="T", unit="K"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r2", variable="T", unit="Pa"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r3", variable="P", unit="K"))
        result = cat.search(variable="T", unit="K")
        assert len(result) == 1
        assert result[0].name == "r1"

    def test_search_callable_predicate(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1", year=2020))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r2", year=2018))
        result = cat.search(year=lambda y: y >= 2019)
        assert len(result) == 1
        assert result[0].name == "r1"

    def test_search_no_match_returns_empty(self, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="T"))
        assert cat.search(variable="X") == []

    # ------------------------------------------------------------------
    # Schema map
    # ------------------------------------------------------------------

    def test_search_with_canonical_name(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param", "unit": "units"})
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="temperature", unit="degC")
        cat.add(ref)
        # Search using canonical name
        assert cat.search(param="temperature") == [ref]
        assert cat.search(units="degC") == [ref]

    def test_search_raw_name_still_works(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="temperature")
        cat.add(ref)
        # Raw name works too (it's not in the canonical→raw reverse map,
        # so treated as-is)
        assert cat.search(variable="temperature") == [ref]

    def test_get_canonical_attribute(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="T"))
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
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r1", variable="T", unit="K"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df.copy()), name="r2", variable="P", unit="Pa"))
        df = cat.to_dataframe()
        assert set(df.index) == {"r1", "r2"}
        assert "variable" in df.columns
        assert "unit" in df.columns

    def test_to_dataframe_applies_schema_map(self, simple_df):
        cat = DataCatalog(schema_map={"variable": "param"})
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", variable="T"))
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
        original = DataCatalog._global_builders[:]
        try:
            DataCatalog.register_reader(reader)
            cat = DataCatalog()
            cat.add_source(str(csv_dir))
            assert len(cat) == 3
        finally:
            DataCatalog._global_builders[:] = original

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
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r"))
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
        refs = CSVDirectoryReader().build(str(tmp_path))
        assert sorted(r.name for r in refs) == ["a", "b", "c"]

    def test_each_ref_is_data_reference(self, tmp_path):
        pd.DataFrame({"v": [1]}).to_csv(tmp_path / "x.csv", index=False)
        refs = CSVDirectoryReader().build(str(tmp_path))
        assert all(isinstance(r, DataReference) for r in refs)

    def test_file_path_attribute_set(self, tmp_path):
        p = tmp_path / "x.csv"
        pd.DataFrame({"v": [1]}).to_csv(p, index=False)
        refs = CSVDirectoryReader().build(str(tmp_path))
        assert refs[0].get_attribute("file_path") == str(p)

    def test_default_attributes_applied(self, tmp_path):
        pd.DataFrame({"v": [1]}).to_csv(tmp_path / "x.csv", index=False)
        refs = CSVDirectoryReader(project="climate").build(str(tmp_path))
        assert refs[0].get_attribute("project") == "climate"

    def test_get_data_from_ref(self, tmp_path):
        df = pd.DataFrame({"val": [10, 20, 30]})
        df.to_csv(tmp_path / "data.csv", index=False)
        refs = CSVDirectoryReader().build(str(tmp_path))
        result = refs[0].getData()
        assert list(result["val"]) == [10, 20, 30]

    def test_empty_directory_returns_empty_list(self, tmp_path):
        refs = CSVDirectoryReader().build(str(tmp_path))
        assert refs == []

    def test_single_file_mode(self, tmp_path):
        p = tmp_path / "sensor.csv"
        pd.DataFrame({"v": [1, 2]}).to_csv(p, index=False)
        refs = CSVDirectoryReader().build(str(p))
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
        refs = reader.build(str(csv_dir))
        assert len(refs) == 3

    def test_unmatched_file_is_skipped(self, csv_dir, caplog):
        import logging

        reader = PatternCSVDirectoryReader(self.PATTERN)
        with caplog.at_level(logging.WARNING):
            refs = reader.build(str(csv_dir))
        assert "unmatched_file.csv" in caplog.text
        assert len(refs) == 3

    def test_empty_directory_returns_empty(self, tmp_path):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(tmp_path))
        assert refs == []

    def test_names_come_from_name_field(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        names = {r.name for r in refs}
        assert names == {"flow", "stage", "temperature"}

    # ------------------------------------------------------------------
    # read – metadata extraction
    # ------------------------------------------------------------------

    def test_stationid_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        by_name = {r.name: r for r in refs}
        assert by_name["flow"].get_attribute("stationid") == "STA001"
        assert by_name["stage"].get_attribute("stationid") == "STA002"
        assert by_name["temperature"].get_attribute("stationid") == "STA001"

    def test_source_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        by_name = {r.name: r for r in refs}
        assert by_name["flow"].get_attribute("source") == "USGS"
        assert by_name["stage"].get_attribute("source") == "CDEC"
        assert by_name["temperature"].get_attribute("source") == "CDEC"

    def test_file_path_metadata(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        for r in refs:
            assert r.get_attribute("file_path") is not None
            assert r.get_attribute("file_path").endswith(".csv")

    def test_format_metadata_is_csv(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        assert all(r.get_attribute("format") == "csv" for r in refs)

    def test_default_attributes_applied(self, csv_dir):
        reader = PatternCSVDirectoryReader(self.PATTERN, project="hydro", version=2)
        refs = reader.build(str(csv_dir))
        assert all(r.get_attribute("project") == "hydro" for r in refs)
        assert all(r.get_attribute("version") == 2 for r in refs)

    def test_name_field_not_in_metadata(self, csv_dir):
        """The 'name' placeholder is consumed as the reference name, not stored as an attribute."""
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        for r in refs:
            # 'name' should NOT appear as an extra attribute
            assert not r.has_attribute("name")

    # ------------------------------------------------------------------
    # read – getData() round-trip
    # ------------------------------------------------------------------

    def test_getData_returns_csv_contents(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
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
        refs = reader.build(str(tmp_path))
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
        refs = PatternCSVDirectoryReader(pattern).build(str(tmp_path))
        assert len(refs) == 1
        assert refs[0].get_attribute("variable") == "discharge"

    def test_references_sorted_by_filename(self, csv_dir):
        refs = PatternCSVDirectoryReader(self.PATTERN).build(str(csv_dir))
        names = [r.name for r in refs]
        assert names == sorted(names)


# ===========================================================================
# MathDataCatalogReader
# ===========================================================================


class TestMathDataCatalogReader:
    """Tests for MathDataCatalogReader.build() — YAML loading of MathDataReference."""

    @pytest.fixture()
    def base_catalog(self):
        cat = DataCatalog()
        df_a = pd.DataFrame({"v": [1.0, 2.0, 3.0]})
        df_b = pd.DataFrame({"v": [10.0, 20.0, 30.0]})
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(df_a), name="ref_a", variable="temp", unit="K"))
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(df_b), name="ref_b", variable="temp", unit="K"))
        return cat

    @pytest.fixture()
    def simple_yaml(self, tmp_path):
        content = """
- name: derived
  expression: ref_a + ref_b
  variable: derived_temp
  unit: K
"""
        p = tmp_path / "math_refs.yaml"
        p.write_text(content)
        return p

    @pytest.fixture()
    def search_map_yaml(self, tmp_path):
        content = """
- name: converted
  expression: obs * 2.0
  variable: scaled
  unit: degC
  search_map:
    obs:
      variable: temp
      unit: K
      _require_single: false
"""
        p = tmp_path / "search_map.yaml"
        p.write_text(content)
        return p

    @pytest.fixture()
    def dict_wrapped_yaml(self, tmp_path):
        content = """
math_refs:
  - name: derived2
    expression: ref_a
    variable: copy
    unit: K
"""
        p = tmp_path / "wrapped.yaml"
        p.write_text(content)
        return p

    # ------------------------------------------------------------------
    # can_handle
    # ------------------------------------------------------------------

    def test_can_handle_yaml_path(self, tmp_path):
        p = tmp_path / "refs.yaml"
        p.touch()
        assert MathDataCatalogReader().can_handle(str(p))

    def test_can_handle_yml_extension(self, tmp_path):
        p = tmp_path / "refs.yml"
        p.touch()
        assert MathDataCatalogReader().can_handle(str(p))

    def test_cannot_handle_non_yaml(self, tmp_path):
        p = tmp_path / "data.csv"
        p.touch()
        assert not MathDataCatalogReader().can_handle(str(p))

    def test_cannot_handle_non_path(self):
        assert not MathDataCatalogReader().can_handle(42)

    # ------------------------------------------------------------------
    # build — basic YAML loading
    # ------------------------------------------------------------------

    def test_build_returns_math_data_references(self, simple_yaml):
        refs = MathDataCatalogReader().build(str(simple_yaml))
        assert len(refs) == 1
        assert isinstance(refs[0], MathDataReference)

    def test_build_sets_name(self, simple_yaml):
        refs = MathDataCatalogReader().build(str(simple_yaml))
        assert refs[0].name == "derived"

    def test_build_sets_expression(self, simple_yaml):
        refs = MathDataCatalogReader().build(str(simple_yaml))
        assert refs[0].expression == "ref_a + ref_b"

    def test_build_stores_extra_attrs(self, simple_yaml):
        refs = MathDataCatalogReader().build(str(simple_yaml))
        assert refs[0].get_attribute("variable") == "derived_temp"
        assert refs[0].get_attribute("unit") == "K"

    def test_build_accepts_path_object(self, simple_yaml):
        refs = MathDataCatalogReader().build(Path(simple_yaml))
        assert len(refs) == 1

    def test_build_dict_wrapped_yaml(self, dict_wrapped_yaml):
        """YAML with a top-level 'math_refs' key is also accepted."""
        refs = MathDataCatalogReader().build(str(dict_wrapped_yaml))
        assert len(refs) == 1
        assert refs[0].name == "derived2"

    def test_build_empty_yaml_returns_empty_list(self, tmp_path):
        p = tmp_path / "empty.yaml"
        p.write_text("\n")
        refs = MathDataCatalogReader().build(str(p))
        assert refs == []

    # ------------------------------------------------------------------
    # build — parent_catalog wiring
    # ------------------------------------------------------------------

    def test_build_wires_catalog(self, simple_yaml, base_catalog):
        refs = MathDataCatalogReader(parent_catalog=base_catalog).build(str(simple_yaml))
        assert refs[0]._catalog is base_catalog

    def test_build_no_catalog_leaves_catalog_none(self, simple_yaml):
        refs = MathDataCatalogReader().build(str(simple_yaml))
        assert refs[0]._catalog is None

    def test_with_catalog_chainable(self, simple_yaml, base_catalog):
        reader = MathDataCatalogReader().with_catalog(base_catalog)
        refs = reader.build(str(simple_yaml))
        assert refs[0]._catalog is base_catalog

    # ------------------------------------------------------------------
    # build — search_map parsing
    # ------------------------------------------------------------------

    def test_build_parses_search_map(self, search_map_yaml):
        refs = MathDataCatalogReader().build(str(search_map_yaml))
        ref = refs[0]
        assert ref._search_map is not None
        assert "obs" in ref._search_map

    def test_build_search_map_strips_require_single(self, search_map_yaml):
        """_require_single must not appear inside the cleaned criteria dict."""
        refs = MathDataCatalogReader().build(str(search_map_yaml))
        criteria = refs[0]._search_map["obs"]
        assert "_require_single" not in criteria

    def test_build_search_map_captures_require_single(self, search_map_yaml):
        refs = MathDataCatalogReader().build(str(search_map_yaml))
        # _require_single: false → stored as False in _search_require_single
        assert refs[0]._search_require_single.get("obs") is False

    # ------------------------------------------------------------------
    # build — default_attrs
    # ------------------------------------------------------------------

    def test_default_attrs_applied(self, simple_yaml):
        refs = MathDataCatalogReader(project="test").build(str(simple_yaml))
        assert refs[0].get_attribute("project") == "test"

    def test_per_entry_attrs_override_defaults(self, tmp_path):
        p = tmp_path / "refs.yaml"
        p.write_text("- name: r\n  expression: x\n  unit: override\n")
        refs = MathDataCatalogReader(unit="default").build(str(p))
        assert refs[0].get_attribute("unit") == "override"

    # ------------------------------------------------------------------
    # Integration: build → getData() via DataCatalog
    # ------------------------------------------------------------------

    def test_getData_evaluates_expression(self, simple_yaml, base_catalog):
        refs = MathDataCatalogReader(parent_catalog=base_catalog).build(str(simple_yaml))
        base_catalog.add(refs[0])
        result = base_catalog["derived"].getData()
        # MathDataReference renames the result column to the ref name
        assert list(result.iloc[:, 0]) == [11.0, 22.0, 33.0]


# ===========================================================================
# DataReferenceReader.fqcn() and _resolve_class
# ===========================================================================

from dvue.catalog import _resolve_class  # noqa: E402


class TestFqcnAndResolveClass:
    def test_fqcn_returns_qualified_name(self):
        assert InMemoryDataReferenceReader.fqcn() == "dvue.catalog.InMemoryDataReferenceReader"

    def test_fqcn_file_reader(self):
        assert FileDataReferenceReader.fqcn() == "dvue.catalog.FileDataReferenceReader"

    def test_resolve_class_returns_type(self):
        cls = _resolve_class("dvue.catalog.FileDataReferenceReader")
        assert cls is FileDataReferenceReader

    def test_resolve_class_bad_module_raises(self):
        with pytest.raises((ImportError, ModuleNotFoundError)):
            _resolve_class("dvue.nonexistent_module.SomeClass")

    def test_resolve_class_bad_attribute_raises(self):
        with pytest.raises(AttributeError):
            _resolve_class("dvue.catalog.NoSuchClass")

    def test_resolve_class_no_module_raises(self):
        with pytest.raises(ImportError):
            _resolve_class("JustAName")


# ===========================================================================
# DataReference.to_dict() and DataCatalog.to_csv / from_csv
# ===========================================================================


class TestDataReferenceSerialization:
    def test_to_dict_contains_name_source_reader(self, simple_df):
        reader = InMemoryDataReferenceReader(simple_df)
        ref = DataReference(source="", reader=reader, name="r", unit="K")
        d = ref.to_dict()
        assert d["name"] == "r"
        assert d["source"] == ""
        assert d["reader"] == "dvue.catalog.InMemoryDataReferenceReader"

    def test_to_dict_contains_all_attributes(self, simple_df):
        reader = InMemoryDataReferenceReader(simple_df)
        ref = DataReference(source="", reader=reader, name="r", variable="T", unit="K", year=2020)
        d = ref.to_dict()
        assert d["variable"] == "T"
        assert d["unit"] == "K"
        assert d["year"] == 2020

    def test_to_dict_fqcn_string_reader(self, tmp_path, simple_df):
        simple_df.to_csv(tmp_path / "f.csv", index=False)
        ref = DataReference(
            source=str(tmp_path / "f.csv"),
            reader="dvue.catalog.FileDataReferenceReader",
            name="r",
        )
        d = ref.to_dict()
        assert d["source"] == str(tmp_path / "f.csv")
        assert d["reader"] == "dvue.catalog.FileDataReferenceReader"

    def test_reader_property_returns_fqcn(self, simple_df):
        reader = InMemoryDataReferenceReader(simple_df)
        ref = DataReference(source="", reader=reader, name="r")
        assert ref.reader == "dvue.catalog.InMemoryDataReferenceReader"

    def test_reader_property_from_fqcn_string(self):
        ref = DataReference(source="/x.csv", reader="dvue.catalog.FileDataReferenceReader", name="r")
        assert ref.reader == "dvue.catalog.FileDataReferenceReader"


class TestDataCatalogCSV:
    def test_to_csv_creates_file(self, tmp_path, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K"))
        p = tmp_path / "catalog.csv"
        cat.to_csv(p)
        assert p.exists()

    def test_to_csv_columns(self, tmp_path, simple_df):
        cat = DataCatalog()
        cat.add(DataReference(source="", reader=InMemoryDataReferenceReader(simple_df), name="r", unit="K"))
        p = tmp_path / "catalog.csv"
        cat.to_csv(p)
        df = pd.read_csv(p)
        assert "name" in df.columns
        assert "source" in df.columns
        assert "reader" in df.columns
        assert "unit" in df.columns

    def test_to_csv_round_trips_name_source_reader(self, tmp_path, simple_df):
        p_data = tmp_path / "data.csv"
        simple_df.to_csv(p_data, index=False)
        cat = DataCatalog()
        cat.add(DataReference(
            source=str(p_data),
            reader="dvue.catalog.FileDataReferenceReader",
            name="flow",
            unit="cfs",
        ))
        p_cat = tmp_path / "catalog.csv"
        cat.to_csv(p_cat)

        cat2 = DataCatalog.from_csv(p_cat)
        assert "flow" in cat2
        ref = cat2["flow"]
        assert ref.source == str(p_data)
        assert ref.reader == "dvue.catalog.FileDataReferenceReader"
        assert ref.get_attribute("unit") == "cfs"

    def test_from_csv_lazy_reader_loads_data(self, tmp_path, simple_df):
        p_data = tmp_path / "data.csv"
        simple_df.to_csv(p_data, index=False)
        cat = DataCatalog()
        cat.add(DataReference(
            source=str(p_data),
            reader="dvue.catalog.FileDataReferenceReader",
            name="climate",
        ))
        p_cat = tmp_path / "catalog.csv"
        cat.to_csv(p_cat)

        cat2 = DataCatalog.from_csv(p_cat)
        result = cat2["climate"].getData()
        assert list(result.columns) == ["temperature", "season"]
        assert len(result) == 4

    def test_to_csv_empty_catalog(self, tmp_path):
        cat = DataCatalog()
        p = tmp_path / "empty.csv"
        cat.to_csv(p)
        assert p.exists()
        df = pd.read_csv(p)
        assert list(df.columns) == ["name", "source", "reader"]
        assert len(df) == 0

    def test_from_csv_multiple_refs(self, tmp_path, simple_df):
        for name in ("a", "b", "c"):
            simple_df.to_csv(tmp_path / f"{name}.csv", index=False)
        cat = DataCatalog()
        for name in ("a", "b", "c"):
            cat.add(DataReference(
                source=str(tmp_path / f"{name}.csv"),
                reader="dvue.catalog.FileDataReferenceReader",
                name=name,
            ))
        p_cat = tmp_path / "catalog.csv"
        cat.to_csv(p_cat)
        cat2 = DataCatalog.from_csv(p_cat)
        assert set(cat2.list_names()) == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# Tests for DataCatalog.invalidate_all_caches
# ---------------------------------------------------------------------------


class TestInvalidateAllCaches:
    def _make_cached_catalog(self):
        """Two refs, each with a warm cache entry."""
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0, 2.0]}))
        cat = DataCatalog()
        for name in ("ref_a", "ref_b"):
            ref = DataReference(reader=reader, name=name, cache=True)
            ref.getData()          # populate cache
            cat.add(ref)
        return cat

    def test_caches_warm_before_clear(self):
        cat = self._make_cached_catalog()
        for ref in cat.list():
            assert ref._cached_data, "Cache should be warm before invalidation"

    def test_invalidate_all_clears_every_ref(self):
        cat = self._make_cached_catalog()
        result = cat.invalidate_all_caches()
        for ref in cat.list():
            assert not ref._cached_data, "Cache should be empty after invalidation"

    def test_invalidate_all_returns_self(self):
        cat = self._make_cached_catalog()
        assert cat.invalidate_all_caches() is cat

    def test_data_reloaded_after_invalidation(self):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [42.0]}))
        ref = DataReference(reader=reader, name="r", cache=True)
        cat = DataCatalog()
        cat.add(ref)
        df1 = ref.getData()
        cat.invalidate_all_caches()
        df2 = ref.getData()
        pd.testing.assert_frame_equal(df1, df2)


# ---------------------------------------------------------------------------
# Tests for build_catalog_from_dataframe
# ---------------------------------------------------------------------------


class TestBuildCatalogFromDataframe:
    def _make_df(self, filenames):
        rows = []
        for i, fn in enumerate(filenames):
            rows.append({"station": f"STA{i:03d}", "variable": "EC", "filename": fn})
        return pd.DataFrame(rows)

    @staticmethod
    def _ref_name(row):
        return f'{row["filename"]}::{row["station"]}/{row["variable"]}'

    def test_catalog_length_matches_df(self):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        df = self._make_df(["f1.dss", "f2.dss"])
        cat = build_catalog_from_dataframe(df, reader, self._ref_name)
        assert len(cat) == 2

    def test_same_pathname_different_files_both_present(self):
        """Entries with same station/variable but different filenames must both appear."""
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        rows = [
            {"station": "STA000", "variable": "EC", "filename": "f1.dss"},
            {"station": "STA000", "variable": "EC", "filename": "f2.dss"},
        ]
        df = pd.DataFrame(rows)
        cat = build_catalog_from_dataframe(df, reader, self._ref_name)
        assert len(cat) == 2  # different filename → different key → both stored

    def test_different_files_same_path_both_stored(self):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        rows = [
            {"station": "STA000", "variable": "EC", "filename": "f1.dss"},
            {"station": "STA000", "variable": "EC", "filename": "f2.dss"},
        ]
        df = pd.DataFrame(rows)
        cat = build_catalog_from_dataframe(df, reader, self._ref_name)
        assert len(cat) == 2

    def test_cache_enabled_on_all_refs(self):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        df = self._make_df(["f1.dss", "f2.dss"])
        cat = build_catalog_from_dataframe(df, reader, self._ref_name)
        for ref in cat.list():
            assert ref._cache_enabled

    def test_attributes_stored_on_ref(self):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        df = self._make_df(["f1.dss"])
        cat = build_catalog_from_dataframe(df, reader, self._ref_name)
        ref = cat.list()[0]
        assert ref.get_attribute("filename") == "f1.dss"
        assert ref.get_attribute("station") == "STA000"

    def test_geometry_excluded_from_attrs_stored_in_geometry(self):
        import shapely.geometry as sg
        import geopandas as gpd
        reader = InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]}))
        rows = [{"station": "STA000", "variable": "EC", "filename": "f1.dss",
                 "geometry": sg.Point(1.0, 2.0)}]
        gdf = gpd.GeoDataFrame(rows, geometry="geometry")
        cat = build_catalog_from_dataframe(gdf, reader, self._ref_name, crs="EPSG:4326")
        ref = cat.list()[0]
        # geometry stored as attribute (not excluded for geoviews use)
        assert ref.get_attribute("geometry") is not None


# ===========================================================================
# ref_type class attribute
# ===========================================================================


class TestRefType:
    def test_data_reference_default_ref_type(self):
        ref = DataReference(source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]})), name="r")
        assert ref.ref_type == "raw"

    def test_math_data_reference_ref_type(self):
        m = MathDataReference("A", variable_map={"A": DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]})), name="A"
        )})
        assert m.ref_type == "math"

    def test_subclass_can_override_ref_type(self):
        class CustomRef(DataReference):
            ref_type = "custom"

        ref = CustomRef(source="", name="c")
        assert ref.ref_type == "custom"

    def test_ref_type_readable_at_instance_level(self):
        ref = DataReference(source="", name="r")
        # Reading via the instance works even though it's a class attribute.
        assert ref.ref_type == "raw"

    def test_to_dataframe_includes_ref_type_column(self):
        cat = DataCatalog()
        cat.add(DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]})),
            name="raw_ref", variable="flow"
        ))
        m = MathDataReference("raw_ref", catalog=cat, name="math_ref", variable="flow_2")
        cat.add(m)
        df = cat.to_dataframe()
        assert "ref_type" in df.columns
        assert df.loc["raw_ref", "ref_type"] == "raw"
        assert df.loc["math_ref", "ref_type"] == "math"

    def test_to_dataframe_all_raw_shows_raw_type(self):
        cat = DataCatalog()
        cat.add(DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [1.0]})),
            name="r1"
        ))
        cat.add(DataReference(
            source="", reader=InMemoryDataReferenceReader(pd.DataFrame({"v": [2.0]})),
            name="r2"
        ))
        df = cat.to_dataframe()
        assert "ref_type" in df.columns
        assert (df["ref_type"] == "raw").all()


# ===========================================================================
# save_math_refs YAML round-trip
# ===========================================================================


from dvue.math_reference import save_math_refs  # noqa: E402


class TestSaveMathRefsRoundTrip:
    """save_math_refs → MathDataCatalogReader.build() must round-trip losslessly."""

    def _make_catalog(self) -> DataCatalog:
        cat = DataCatalog()
        df = pd.DataFrame({"v": [1.0, 2.0]})
        cat.add(DataReference(
            source="", reader=InMemoryDataReferenceReader(df),
            name="upstream", variable="flow", unit="cfs"
        ))
        cat.add(DataReference(
            source="", reader=InMemoryDataReferenceReader(df),
            name="downstream", variable="flow", unit="cfs"
        ))
        return cat

    def test_round_trip_name_and_expression(self, tmp_path):
        cat = self._make_catalog()
        m = MathDataReference("upstream + downstream", name="total", catalog=cat,
                               variable="flow", unit="cfs")
        cat.add(m)
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        refs = MathDataCatalogReader(parent_catalog=cat).build(p)
        assert len(refs) == 1
        assert refs[0].name == "total"
        assert refs[0].expression == "upstream + downstream"

    def test_round_trip_extra_attributes(self, tmp_path):
        cat = self._make_catalog()
        m = MathDataReference("upstream - downstream", name="diff", catalog=cat,
                               variable="flow_diff", unit="cfs", station_id="S1")
        cat.add(m)
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        refs = MathDataCatalogReader(parent_catalog=cat).build(p)
        assert refs[0].get_attribute("variable") == "flow_diff"
        assert refs[0].get_attribute("station_id") == "S1"

    def test_round_trip_search_map(self, tmp_path):
        cat = self._make_catalog()
        m = MathDataReference(
            "obs - model",
            name="bias",
            catalog=cat,
            search_map={
                "obs": {"variable": "flow", "unit": "cfs"},
                "model": {"variable": "flow", "unit": "cfs"},
            },
        )
        cat.add(m)
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        refs = MathDataCatalogReader(parent_catalog=cat).build(p)
        assert "obs" in refs[0]._search_map
        assert "model" in refs[0]._search_map
        # _require_single key must NOT appear in the criteria dict after a round-trip
        assert "_require_single" not in refs[0]._search_map["obs"]

    def test_round_trip_require_single_false(self, tmp_path):
        cat = self._make_catalog()
        m = MathDataReference(
            "ws.mean(axis=1)",
            name="mean_ws",
            catalog=cat,
            search_map={"ws": {"variable": "flow", "unit": "cfs"}},
            search_require_single={"ws": False},
        )
        cat.add(m)
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        refs = MathDataCatalogReader(parent_catalog=cat).build(p)
        assert refs[0]._search_require_single.get("ws") is False

    def test_round_trip_require_single_true_is_default(self, tmp_path):
        """require_single=True is the default — it should NOT be written to YAML."""
        import yaml
        cat = self._make_catalog()
        m = MathDataReference(
            "obs * 2",
            name="doubled",
            catalog=cat,
            search_map={"obs": {"variable": "flow"}},
            search_require_single={"obs": True},
        )
        cat.add(m)
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        raw = yaml.safe_load(p.read_text())
        # The flag should be absent when it's the default True value.
        obs_crit = raw[0]["search_map"]["obs"]
        assert "_require_single" not in obs_crit

    def test_raw_refs_not_included_in_yaml(self, tmp_path):
        cat = self._make_catalog()  # contains only raw refs
        p = tmp_path / "out.yaml"
        save_math_refs(cat, p)
        import yaml
        data = yaml.safe_load(p.read_text())
        assert data == [] or data is None

