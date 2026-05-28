"""Tests for RegistryUIManager dynamic table columns from reader metadata."""

import pandas as pd
import warnings

from dvue.catalog import DataReference, DataReferenceReader
from dvue.registry import ReaderRegistry
from dvue.registry_ui import RegistryUIManager


class _DriverReader(DataReferenceReader):
    """Stub reader that exposes custom metadata columns via scan()."""

    @classmethod
    def scan(cls, path: str):
        return [
            DataReference(
                source=path,
                reader=None,
                name=f"{path}::S1/flow",
                cache=True,
                station="S1",
                variable="flow",
                driver="demo_driver",
                domain="north",
                unit_hint="cfs",
            )
        ]

    def load(self, **attributes):
        return pd.DataFrame({"value": [1.0, 2.0]})


class _DSSLikeReader(DataReferenceReader):
    """Stub reader that mimics DSS metadata with optional empty columns."""

    @classmethod
    def scan(cls, path: str):
        return [
            DataReference(
                source=path,
                reader=None,
                name=f"{path}::S1/flow",
                cache=True,
                station="S1",
                variable="flow",
                A="AREA",
                B="STA001",
                C="FLOW",
                D="01JAN2000-31DEC2000",
                E="1HOUR",
                F="VER1",
                station_id=None,
            )
        ]

    def load(self, **attributes):
        return pd.DataFrame({"value": [1.0, 2.0]})


def _register_test_reader():
    ReaderRegistry.register("test_driver", _DriverReader, extensions=[".drvcols"])


def _register_dsslike_reader():
    ReaderRegistry.register("test_dsslike", _DSSLikeReader, extensions=[".dssx"])


def test_registry_ui_includes_driver_metadata_columns_in_table():
    _register_test_reader()
    mgr = RegistryUIManager(files=["example.drvcols"])

    df = mgr.get_data_catalog()
    assert "driver" in df.columns
    assert "domain" in df.columns
    assert "unit_hint" in df.columns

    table_cols = mgr.get_table_columns()
    assert "driver" in table_cols
    assert "domain" in table_cols
    assert "unit_hint" in table_cols


def test_registry_ui_filters_include_driver_metadata_columns():
    _register_test_reader()
    mgr = RegistryUIManager(files=["example2.drvcols"])

    filters = mgr.get_table_filters()
    assert "driver" in filters
    assert "domain" in filters
    assert "unit_hint" in filters


def test_registry_ui_dynamic_columns_have_stable_order():
    _register_test_reader()
    mgr = RegistryUIManager(files=["example3.drvcols"])

    table_cols = mgr.get_table_columns()
    assert table_cols.index("driver") < table_cols.index("domain")
    assert table_cols.index("domain") < table_cols.index("unit_hint")


def test_registry_ui_dynamic_columns_get_saner_widths():
    _register_test_reader()
    mgr = RegistryUIManager(files=["example4.drvcols"])

    widths = mgr.get_table_column_width_map()
    assert widths["driver"] == "10%"
    assert widths["domain"] == "10%"
    assert widths["unit_hint"] == "10%"


def test_registry_ui_init_has_no_param_pending_warning():
    _register_test_reader()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        RegistryUIManager(files=["example5.drvcols"])

    assert not any("ParamPendingDeprecationWarning" in str(w.message) for w in caught)


def test_registry_ui_shows_dss_a_to_f_columns():
    _register_dsslike_reader()
    mgr = RegistryUIManager(files=["hist.dssx"])

    table_cols = mgr.get_table_columns()
    for col in ["A", "B", "C", "D", "E", "F"]:
        assert col in table_cols


def test_registry_ui_hides_all_nan_columns():
    _register_dsslike_reader()
    mgr = RegistryUIManager(files=["hist2.dssx"])

    table_cols = mgr.get_table_columns()
    assert "station_id" not in table_cols
