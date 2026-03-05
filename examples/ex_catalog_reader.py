"""Example: Using DataCatalog and custom DataCatalogReader with DataUIManager.

This file demonstrates:

1. Building a DataCatalog from a directory of CSV files using the built-in
   PatternCSVDirectoryReader.
2. Writing a minimal custom DataCatalogReader for a new source type.
3. Connecting a DataCatalog to a DataUIManager (and DataUI) via the
   DataProvider.data_catalog property.
4. Using DataProvider standalone (no UI) for scripted data access.

Run individual sections with ``# %%`` in VS Code (Jupyter-style cells).
"""

# %% [1] ─── Imports ──────────────────────────────────────────────────────────
import pandas as pd
from pathlib import Path

from dvue.catalog import DataCatalog, DataCatalogReader, DataReference, CatalogView
from dvue.readers import CSVDirectoryReader, PatternCSVDirectoryReader
from dvue.dataui import DataProvider


# %% [2] ─── Build a catalog from a directory ─────────────────────────────────
# Suppose /data/hydro/ contains files like:
#   flow__STA001__USGS.csv
#   stage__STA001__USGS.csv
#   precip__STA002__CDEC.csv

DATA_DIR = Path("/data/hydro")  # ← change to an actual directory

if DATA_DIR.exists():
    catalog = (
        DataCatalog()
        .add_reader(PatternCSVDirectoryReader("{name}__{stationid}__{source}"))
        .add_source(str(DATA_DIR))
    )

    print(catalog)  # DataCatalog(3 references)
    print(catalog.to_dataframe())  # summary DataFrame
    print(catalog.search(source="USGS"))  # [DataReference(name='flow', ...), ...]

    # CatalogView: live filtered window
    usgs_view = CatalogView(catalog, selection={"source": "USGS"})
    print(usgs_view.list_names())  # ['flow', 'stage']

    # Retrieve actual data from a reference
    ref = catalog.get("flow")
    df = ref.getData()  # pd.DataFrame from the CSV
    print(df.head())


# %% [3] ─── Custom DataCatalogReader ─────────────────────────────────────────
# Template for connecting any new source type to the catalog system.


class InMemoryDataReader(DataCatalogReader):
    """A minimal reader that wraps an existing dict of DataFrames.

    This is a template – replace the source type (dict) and logic
    with whatever your data store requires (database connection,
    REST API client, cloud storage bucket, etc.).
    """

    def can_handle(self, source) -> bool:
        """Accept Python dicts mapping names → DataFrames."""
        return isinstance(source, dict) and all(
            isinstance(v, pd.DataFrame) for v in source.values()
        )

    def read(self, source: dict) -> list[DataReference]:
        """Wrap each DataFrame in a DataReference."""
        refs = []
        for name, df in source.items():
            ref = DataReference(df.copy(), name=name, source_type="in_memory")
            refs.append(ref)
        return refs


# Register globally so all new DataCatalog instances pick it up:
DataCatalog.register_reader(InMemoryDataReader())

# Usage:
data_store = {
    "flow": pd.DataFrame(
        {"datetime": pd.date_range("2020", periods=3, freq="h"), "value": [1.0, 2.0, 3.0]}
    ),
    "stage": pd.DataFrame(
        {"datetime": pd.date_range("2020", periods=3, freq="h"), "value": [0.5, 0.6, 0.7]}
    ),
}

mem_catalog = DataCatalog().add_source(data_store)
print(mem_catalog)  # DataCatalog(2 references)
print(mem_catalog["flow"].getData())  # DataFrame with flow data


# %% [4] ─── DataProvider standalone (no UI) ──────────────────────────────────
# Use DataProvider directly in scripts / notebooks without launching a Panel app.


class HydroProvider(DataProvider):
    """A DataProvider backed by an in-memory DataCatalog.

    In production, replace ``_catalog`` construction with your real source
    (directory reader, database reader, etc.).
    """

    def __init__(self, data_store: dict, **params):
        super().__init__(**params)
        self._cat = DataCatalog().add_source(data_store)

    @property
    def data_catalog(self) -> DataCatalog:
        return self._cat

    def build_station_name(self, r: pd.Series) -> str:
        return r.get("name", str(r.name))


provider = HydroProvider(data_store)

# get_data_catalog() → DataFrame (from catalog.to_dataframe().reset_index())
df_meta = provider.get_data_catalog()
print(df_meta)

# get_data_reference() → DataReference for a row
first_row = df_meta.iloc[0]
ref = provider.get_data_reference(first_row)
print(ref.getData())

# get_data() → generator of DataFrames for all selected rows
for ts_df in provider.get_data(df_meta):
    print(ts_df.shape)


# %% [5] ─── Full DataUIManager with catalog ──────────────────────────────────
# Extend DataUIManager (which extends DataProvider) for a Panel-powered DataUI.
# Only the view-layer methods need to be added on top of the catalog integration.

# NOTE: This class requires Panel / HoloViews / GeoViews dependencies.
# Uncomment and fill in the view methods when building a real application.

# import geopandas as gpd
# from dvue.dataui import DataUIManager
# from dvue import DataUI
#
# class HydroUIManager(DataUIManager):
#     """Full DataUIManager backed by a DataCatalog."""
#
#     def __init__(self, data_dir: str, **params):
#         super().__init__(**params)
#         self._catalog = (
#             DataCatalog()
#             .add_reader(PatternCSVDirectoryReader("{name}__{stationid}__{source}"))
#             .add_source(data_dir)
#         )
#
#     # ── DataProvider (data layer) ──────────────────────────────────────────
#     @property
#     def data_catalog(self) -> DataCatalog:
#         return self._catalog
#
#     def build_station_name(self, r):
#         return f"{r.get('stationid', '')} / {r.get('name', '')}"
#
#     def create_panel(self, df):
#         import panel as pn, holoviews as hv
#         curves = []
#         for _, row in df.iterrows():
#             data = self.get_data_reference(row).getData()
#             curves.append(hv.Curve(data, label=row.get("name", "")))
#         return pn.panel(hv.Overlay(curves))
#
#     # ── DataUIManager (view layer) ─────────────────────────────────────────
#     def get_table_column_width_map(self):
#         return {"name": "20%", "stationid": "20%", "source": "20%"}
#
#     def get_table_filters(self):
#         return {col: {"type": "input", "func": "like", "placeholder": f"Filter {col}"}
#                 for col in self.get_table_columns()}
#
#     def get_tooltips(self):
#         return [("Station", "@stationid"), ("Name", "@name"), ("Source", "@source")]
#
#     def get_map_color_columns(self):
#         return ["source"]
#
#     def get_name_to_color(self):
#         return {"USGS": "blue", "CDEC": "green"}
#
#     def get_map_marker_columns(self):
#         return ["source"]
#
#     def get_name_to_marker(self):
#         return {"USGS": "circle", "CDEC": "triangle"}
#
#     def append_to_title_map(self, title_map, unit, r):
#         title_map["station"] = r.get("stationid", "")
#
#     def create_title(self, title_map, unit, r):
#         return f"{title_map.get('station', '')} – {r.get('name', '')} [{unit}]"
#
# manager = HydroUIManager("/data/hydro")
# ui = DataUI(manager)
# template = ui.create_view(title="Hydro Data Explorer")
# template.servable()
