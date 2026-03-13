# %%
"""
Example: CatalogBuilder implementations, unified catalogs, and MathDataReferences.

This file is the canonical demonstration of how the dvue reader / catalog /
math-reference system fits together from end to end.  Every section is
self-contained and runnable — all sample data is generated in a temporary
directory so no external files are required.

Sections
--------
[1]  Generate sample time-series CSV data (written to examples/data/)
[2]  CSVDirectoryBuilder — load a plain directory of CSV files
[3]  PatternCSVDirectoryBuilder — extract structured metadata from filenames
[4]  Custom CatalogBuilder — wrap an arbitrary in-memory data source
[5]  Builder registration — global vs instance-scoped
[6]  Unified catalog — merge independently-sourced catalogs
[7]  MathDataReference — derived signals over the unified catalog
     [7a] Operator overload: unit conversion (m → ft)
     [7b] Operator overload: cross-source comparison (obs − model)
     [7c] Operator overload: cross-agency comparison
     [7d] Expression string with NumPy function (cumsum)
     [7e] Expression string resolved directly from the unified catalog
[8]  DataProvider standalone (scripted access, no Panel/UI)
[9a] Save MathDataReferences to YAML and reload via MathDataCatalogReader
[9b] DataUI — interactive catalog table view with Math Ref editor (Panel app)
"""

# %% -- [0] Imports -----------------------------------------------------------
from pathlib import Path

import numpy as np
import pandas as pd

from dvue.catalog import (
    CatalogView,
    CatalogBuilder,
    DataCatalog,
    DataCatalogReader,
    DataReference,
    DataReferenceReader,
    CallableDataReferenceReader,
    InMemoryDataReferenceReader,
    FileDataReferenceReader,
    MathDataReference,
    MathDataCatalogReader,
    save_math_refs,
)
from dvue.dataui import DataProvider, DataUIManager
from dvue.dataui import DataUI
from dvue.actions import MathRefEditorAction
from dvue.readers import CSVDirectoryBuilder, CSVDirectoryReader, PatternCSVDirectoryBuilder, PatternCSVDirectoryReader

# %% -- [1] Generate sample CSV data -----------------------------------------
#
# We create two structured directories (mimicking an "observed" USGS feed and
# a CDEC feed) plus a plain directory of tide-gauge files.  All series are
# synthetic hourly random-walks spanning 2023.
#
# File-naming conventions
# -----------------------
# hydro directory  :  {variable}__{stationid}__{agency}.csv
#                     e.g. water_level__RIO001__usgs.csv
# tide directory   :  tide_gauge_{site}.csv  (no structured metadata)

# Persist sample data under examples/data/ so it survives across runs and can
# be used by the DataUI example without regenerating each time.
DATA_DIR = Path(__file__).parent / "data"
HYDRO_DIR = DATA_DIR / "hydro"
TIDE_DIR = DATA_DIR / "tide"
HYDRO_DIR.mkdir(parents=True, exist_ok=True)
TIDE_DIR.mkdir(parents=True, exist_ok=True)

IDX = pd.date_range("2023-01-01", periods=8760, freq="h", name="datetime")
rng = np.random.default_rng(42)


def _rw(scale: float = 1.0) -> np.ndarray:
    """Cumulative random walk."""
    return np.cumsum(rng.standard_normal(len(IDX)) * scale)


def _write_ts(path: Path, values: np.ndarray) -> None:
    pd.DataFrame({"value": values}, index=IDX).to_csv(path)


# Only write files if the directories are empty — so data persists across runs.
if not any(HYDRO_DIR.iterdir()):
    _write_ts(HYDRO_DIR / "water_level__RIO001__usgs.csv", _rw(0.05))
    _write_ts(HYDRO_DIR / "flow__RIO001__usgs.csv", 100 + _rw(5.0))
    _write_ts(HYDRO_DIR / "water_level__SCR002__usgs.csv", _rw(0.08))
    _write_ts(HYDRO_DIR / "water_level__CSJ001__cdec.csv", _rw(0.06))
    _write_ts(HYDRO_DIR / "ec__CSJ001__cdec.csv", 400 + _rw(20.0))

if not any(TIDE_DIR.iterdir()):
    _write_ts(TIDE_DIR / "tide_gauge_Martinez.csv", _rw(0.3))
    _write_ts(TIDE_DIR / "tide_gauge_Mallard.csv", _rw(0.25))

print("Sample data directory:", DATA_DIR)
print(" hydro/:", sorted(p.name for p in HYDRO_DIR.iterdir()))
print(" tide/ :", sorted(p.name for p in TIDE_DIR.iterdir()))


# %% -- [2] CSVDirectoryReader -----------------------------------------------
#
# CSVDirectoryReader produces one DataReference per CSV file, named after the
# file stem.  Arbitrary keyword arguments supplied at construction become
# metadata attributes on every produced reference.
#
# Because the plain read_csv call returns a DataFrame with a string "datetime"
# column rather than a DatetimeIndex, we subclass the reader to inject a
# callable source that parses dates at load time.  This is the recommended
# approach whenever upstream CSV files have a timestamp index column.


class TideGaugeCsvBuilder(CSVDirectoryBuilder):
    """CSVDirectoryBuilder subclass that parses the first column as a DatetimeIndex."""

    def build(self, source) -> list[DataReference]:
        refs = super().build(source)
        for ref in refs:
            fp = ref.get_attribute("file_path")
            ref._reader = CallableDataReferenceReader(
                lambda p=fp: pd.read_csv(p, index_col=0, parse_dates=True)
            )
        return refs


TideGaugeCsvReader = TideGaugeCsvBuilder  # backward-compat alias


tide_catalog = (
    DataCatalog()
    .add_builder(TideGaugeCsvBuilder(source_type="tide_gauge", project="delta_calibration"))
    .add_source(str(TIDE_DIR))
)

print("\n── Tide catalog ──")
print(tide_catalog)
print(tide_catalog.to_dataframe()[["source_type", "project", "format"]])

# Spot-check: first column should be numeric, index should be DatetimeIndex
sample = tide_catalog.get("tide_gauge_Martinez").getData()
assert isinstance(sample.index, pd.DatetimeIndex), "Expected DatetimeIndex"
print("tide_gauge_Martinez shape:", sample.shape, "| dtype:", sample.dtypes["value"])


# %% -- [3] PatternCSVDirectoryReader ----------------------------------------
#
# PatternCSVDirectoryReader extracts structured metadata from filenames.
# When the pattern contains no {name} placeholder the full file stem becomes
# the reference name — guaranteeing uniqueness across agencies.
#
# Pattern: "{variable}__{stationid}__{agency}"
#   water_level__RIO001__usgs.csv  →  name="water_level__RIO001__usgs"
#                                     attrs: variable=water_level,
#                                            stationid=RIO001, agency=usgs
#
# We again subclass to add date parsing, and also to normalise whitespace
# from the parsed field values.


class HydroPatternBuilder(PatternCSVDirectoryBuilder):
    """PatternCSVDirectoryBuilder with DatetimeIndex parsing and agency tagging."""

    def __init__(self, **default_attrs):
        # No {name} placeholder: full stem becomes the catalog key.
        super().__init__("{variable}__{stationid}__{agency}", **default_attrs)

    def build(self, source) -> list[DataReference]:
        refs = super().build(source)
        for ref in refs:
            fp = ref.get_attribute("file_path")
            ref._reader = CallableDataReferenceReader(
                lambda p=fp: pd.read_csv(p, index_col=0, parse_dates=True)
            )
        return refs


HydroPatternReader = HydroPatternBuilder  # backward-compat alias


hydro_catalog = (
    DataCatalog()
    .add_builder(HydroPatternBuilder(network="delta"))
    .add_source(str(HYDRO_DIR))
)

print("\n── Hydro catalog ──")
print(hydro_catalog)
print(hydro_catalog.to_dataframe()[["variable", "stationid", "agency", "network"]])

usgs_refs = hydro_catalog.search(agency="usgs")
cdec_refs = hydro_catalog.search(agency="cdec")
print(f"\nUSGS references : {[r.name for r in usgs_refs]}")
print(f"CDEC references : {[r.name for r in cdec_refs]}")

# CatalogView: live filtered window into the hydro catalog
usgs_view = CatalogView(hydro_catalog, selection={"agency": "usgs"})
print("USGS view names :", usgs_view.list_names())


# %% -- [4] Custom CatalogBuilder -------------------------------------------
#
# CatalogBuilder is an abstract base class.  Implement can_handle() and
# build() to connect any data source to the catalog ecosystem.
#
# Here we model a "model output" store — a dict mapping ref names to
# DataFrames.  In a real application this could be a database connection,
# REST API client, cloud object store, etc.


class ModelOutputBuilder(CatalogBuilder):
    """Wraps a dict of {name: DataFrame} model-output results.

    This is a minimal template: replace the source type (dict) and loader
    logic with whatever your model data store requires.
    """

    def can_handle(self, source) -> bool:
        """Accept plain dicts whose values are DataFrames."""
        return isinstance(source, dict) and all(
            isinstance(v, pd.DataFrame) for v in source.values()
        )

    def build(self, source: dict) -> list[DataReference]:
        refs = []
        for name, df in source.items():
            ref = DataReference(
                InMemoryDataReferenceReader(df),
                name=name,
                source_type="model_output",
                model="hydrodynamic_v3",
            )
            refs.append(ref)
        return refs


ModelOutputReader = ModelOutputBuilder  # backward-compat alias


# Sample model output (same time index as the observed data)
model_outputs = {
    "model_RIO001": pd.DataFrame({"value": _rw(0.04)}, index=IDX),
    "model_CSJ001": pd.DataFrame({"value": _rw(0.05)}, index=IDX),
}

model_catalog = (
    DataCatalog()
    .add_builder(ModelOutputBuilder())
    .add_source(model_outputs)
)

print("\n── Model catalog ──")
print(model_catalog)
for ref in model_catalog.list():
    print(f"  {ref.name}  shape={ref.getData().shape}")


# %% -- [5] Builder registration — global vs instance-scoped ----------------
#
# Builders can be registered at two scopes:
#
#   DataCatalog.register_builder(builder)   ← global: all new catalogues inherit it
#   catalog.add_builder(builder)            ← instance: only this catalog is affected
#
# The global registry is useful for application-wide drivers (e.g. a
# DatastoreCatalogBuilder that handles any StationDatastore).  Instance
# registration is better when a builder is specific to one data source.

# Register ModelOutputBuilder globally so any new DataCatalog can accept a dict:
DataCatalog.register_builder(ModelOutputBuilder())

# Now an empty catalog can load model outputs without an explicit add_builder():
quick_catalog = DataCatalog().add_source(model_outputs)
print("\n── Global registry demo ──")
print(quick_catalog)  # DataCatalog(2 references) – reader was found globally


# %% -- [6] Unified catalog --------------------------------------------------
#
# Combine all three independently-sourced catalogs into one unified catalog.
#
# Approach A — manual iteration (always correct, most explicit):
#   Collect every DataReference from each sub-catalog and add them one by one.
unified = DataCatalog()
for cat in (tide_catalog, hydro_catalog, model_catalog):
    for ref in cat.list():
        unified.add(ref)

print("\n── Unified catalog ──")
print(unified)
print(f"Total refs: {len(unified.list())}")
print("All reference names:")
for name in sorted(unified.list_names()):
    print(f"  {name}")

# Approach B — one catalog, multiple reader types for non-conflicting sources:
#
# DataCatalog._find_reader() returns the most-recently-registered reader whose
# can_handle() returns True.  When two readers both accept directory paths
# (e.g. TideGaugeCsvReader and HydroPatternReader), registering both would
# cause the last-registered one to win for every directory source.
#
# The pattern therefore works best when readers target distinct Python types:
#   • HydroPatternReader  — str / Path  (directory of structured CSV files)
#   • ModelOutputReader   — dict        (in-memory DataFrames)
#   • TideGaugeCsvReader  — added separately (same source type as hydro reader)
#
unified_b = (
    DataCatalog()
    .add_builder(HydroPatternBuilder(network="delta"))   # handles str/Path dirs
    .add_builder(ModelOutputBuilder())                   # handles dicts
    .add_source(str(HYDRO_DIR))                          # → HydroPatternBuilder
    .add_source(model_outputs)                           # → ModelOutputBuilder
)
# Tide gauge refs (same source type as hydro) are merged from their own catalog:
for ref in tide_catalog.list():
    unified_b.add(ref)

print("\n── Unified catalog (approach B: single catalog + manual tide merge) ──")
print(unified_b)

# Both approaches produce the same set of reference names:
assert set(unified.list_names()) == set(unified_b.list_names()), (
    f"Sets differ: {set(unified.list_names()) ^ set(unified_b.list_names())}"
)
print("Both approaches produce the same reference names ✓")


# %% -- [7] MathDataReference over unified catalog ---------------------------
#
# MathDataReference evaluates an expression string in a NumPy namespace.
# Variables in the expression are resolved from:
#   a) an explicit variable_map={var: DataReference, ...}
#   b) a catalog passed via set_catalog() — ref names become variable names
#
# The arithmetic operators (+, -, *, /, **) on DataReference automatically
# construct MathDataReferences, merging variable maps as they go.

print("\n── MathDataReference examples ──")

rio_wl = unified.get("water_level__RIO001__usgs")
csj_wl = unified.get("water_level__CSJ001__cdec")
rio_flow = unified.get("flow__RIO001__usgs")
model_rio = unified.get("model_RIO001")


# [7a] Operator overload: unit conversion (m → ft) ---------------------------
# DataReference.__mul__ emits:  "water_level__RIO001__usgs * (3.28084)"
rio_wl_ft: MathDataReference = rio_wl * 3.28084
rio_wl_ft.name = "water_level__RIO001__usgs__ft"
rio_wl_ft.set_attribute("variable", "water_level")
rio_wl_ft.set_attribute("unit", "ft")
rio_wl_ft.set_attribute("stationid", "RIO001")

df_ft = rio_wl_ft.getData()
df_m = rio_wl.getData()
# Values should be ~3.28× larger in feet (use iloc[:,0] — column name is the ref name)
ratio = df_ft.iloc[:, 0].mean() / df_m.iloc[:, 0].mean()
print(f"\n[7a] Unit conversion (m→ft) | mean ratio = {abs(ratio):.5f} (expect 3.28084)")
print(f"     expression: {rio_wl_ft.expression!r}")


# [7b] Operator overload: obs − model (bias check) ---------------------------
# DataReference.__sub__ builds expression "lhs.name - rhs.name" and merges
# both variable maps automatically.
obs_minus_model: MathDataReference = rio_wl - model_rio
obs_minus_model.name = "bias_water_level__RIO001"
obs_minus_model.set_attribute("variable", "water_level_bias")
obs_minus_model.set_attribute("stationid", "RIO001")

df_bias = obs_minus_model.getData()
print(f"\n[7b] Obs − model (bias) | expression: {obs_minus_model.expression!r}")
print(f"     mean={df_bias.iloc[:, 0].mean():.4f}, "
      f"std={df_bias.iloc[:, 0].std():.4f}")


# [7c] Operator overload: cross-agency comparison ----------------------------
# Works across two different agencies; the unified catalog's refs can be
# freely composed since MathDataReference merges variable_maps transparently.
cross_agency: MathDataReference = rio_wl - csj_wl
cross_agency.name = "wl_diff__RIO001__CSJ001"
cross_agency.set_attribute("variable", "water_level_diff")

df_diff = cross_agency.getData()
print(f"\n[7c] Cross-agency wl diff | expression: {cross_agency.expression!r}")
print(f"     diff series shape: {df_diff.shape}")


# [7d] Expression string: cumulative flow ------------------------------------
# The expression string uses the ref name directly as the variable.
# The variable_map is provided explicitly here, but section [7e] shows how
# to use the catalog directly — so no variable_map is needed.
cumflow: MathDataReference = MathDataReference(
    expression="cumsum(flow__RIO001__usgs)",
    variable_map={"flow__RIO001__usgs": rio_flow},
    name="cumulative_flow__RIO001",
    variable="flow",
    unit="ft3",
    stationid="RIO001",
)
df_cum = cumflow.getData()
print(f"\n[7d] Cumulative flow (explicit variable_map)")
print(f"     expression: {cumflow.expression!r}")
print(f"     cumsum[-1] = {df_cum.iloc[-1, 0]:.2f}")


# [7e] Expression string resolved directly from the unified catalog ----------
# Pass the unified catalog to MathDataReference via set_catalog().  Any ref
# name in the expression is resolved automatically — no variable_map needed.
# This is the most convenient pattern when building expressions over a large
# shared catalog (e.g. anomaly = obs - model for multiple stations at once).
anomaly_expr = (
    "water_level__RIO001__usgs - model_RIO001 + "
    "0.5 * (water_level__CSJ001__cdec - model_CSJ001)"
)

multi_station_anomaly = (
    MathDataReference(
        expression=anomaly_expr,
        name="multi_station_anomaly",
        variable="water_level_anomaly",
    )
    .set_catalog(unified)
)

df_anom = multi_station_anomaly.getData()
print(f"\n[7e] Multi-station anomaly from catalog (no variable_map)")
print(f"     expression:\n       {anomaly_expr}")
print(f"     result shape: {df_anom.shape} | mean: {df_anom.iloc[:, 0].mean():.4f}")


# Add the derived references back into the unified catalog so subsequent
# analyses (and DataProvider) can access them by name.
for derived in (rio_wl_ft, obs_minus_model, cross_agency, cumflow, multi_station_anomaly):
    unified.add(derived)

print(f"\nUnified catalog with derived refs: {unified}")


# %% -- [8] DataProvider standalone ------------------------------------------
#
# DataProvider wraps a DataCatalog and exposes the standard dvue data-access
# API without requiring Panel or a running web server.  Use it in notebooks,
# scripts, or tests.


class HydroDataProvider(DataProvider):
    """DataProvider backed by the unified catalog.

    Override only the two abstract methods that are required by DataProvider:
    * data_catalog  (property)
    * build_station_name  (row → display label)
    """

    def __init__(self, catalog: DataCatalog, **params):
        super().__init__(**params)
        self._cat = catalog

    @property
    def data_catalog(self) -> DataCatalog:
        return self._cat

    def build_station_name(self, r: pd.Series) -> str:
        # Use stationid if present; fall back to the catalog key (r.name)
        return str(r.get("stationid", r.name))


provider = HydroDataProvider(unified)

# get_data_catalog() → GeoDataFrame-compatible DataFrame
dfcat = provider.get_data_catalog()
print("\n── DataProvider standalone ──")
print(f"Catalog DataFrame shape: {dfcat.shape}")
print(dfcat[["name", "stationid", "variable", "source_type"]].dropna(subset=["stationid"]).head(10).to_string(index=False))

# get_data_catalog() returns reset_index(), so 'name' is a regular column.
# Re-index by 'name' for convenient row lookups (each row's .name is then the
# reference string, which get_data_reference() resolves automatically).
dfcat_by_name = dfcat.set_index("name")

# get_data_reference() → DataReference for a specific row
row_rio_wl = dfcat_by_name.loc["water_level__RIO001__usgs"]
ref = provider.get_data_reference(row_rio_wl)
print(f"\nget_data_reference('water_level__RIO001__usgs') → {ref}")

# get_data() → generator of DataFrames for a selection of rows
selected = dfcat_by_name.loc[
    ["water_level__RIO001__usgs", "water_level__CSJ001__cdec", "bias_water_level__RIO001"]
]
print("\nget_data() generator output:")
for ts in provider.get_data(selected):
    print(f"  shape={ts.shape}, dtype={ts.dtypes.iloc[0]}, "
          f"index={ts.index.dtype}")


# %% -- [9a] Persist MathDataReferences to YAML and reload -------------------
#
# save_math_refs() writes every MathDataReference in the unified catalog to a
# YAML file.  MathDataCatalogReader loads them back, wiring each expression's
# variables to the parent catalog so lookups happen lazily at getData() time.
#
# This lets you:
#   • Commit derived signal definitions alongside source-reader configs.
#   • Build the raw catalog first, then overlay math refs from the YAML file.
#   • Edit the YAML manually to add / remove / modify expressions.

MATH_REFS_FILE = DATA_DIR / "math_refs.yaml"
save_math_refs(unified, MATH_REFS_FILE)
print("\n── Math refs persisted ──")
print(f"Written to: {MATH_REFS_FILE}")
# Print the file content so it's visible in the output:
print(MATH_REFS_FILE.read_text())

# Round-trip: build a fresh catalog with raw refs only, then load math refs.
fresh_catalog = DataCatalog()
for ref in unified.list():
    if not isinstance(ref, MathDataReference):
        fresh_catalog.add(ref)
fresh_catalog.add_builder(MathDataCatalogReader(parent_catalog=fresh_catalog))
fresh_catalog.add_source(str(MATH_REFS_FILE))
print(f"Reloaded catalog: {fresh_catalog}")
math_in_fresh = [r for r in fresh_catalog.list() if isinstance(r, MathDataReference)]
print(f"MathDataReferences loaded: {[r.name for r in math_in_fresh]}")
# Spot-check: the reloaded unit-conversion ref should yield the same values.
reloaded_ft = fresh_catalog.get("water_level__RIO001__usgs__ft")
assert reloaded_ft is not None, "reload failed for water_level__RIO001__usgs__ft"
assert abs(reloaded_ft.getData().iloc[:, 0].mean() / unified.get("water_level__RIO001__usgs").getData().iloc[:, 0].mean() - 3.28084) < 1e-4
print("Round-trip check passed ✓")


# %% -- [9b] DataUI — interactive catalog table view -------------------------
#
# DataUI pairs a Panel Tabulator table with (optionally) a map and an action
# panel.  The table is driven by a DataUIManager subclass that tells the UI:
#   • which columns to display and how wide they should be
#   • which columns get header filter widgets
#   • how to look up and plot data when the user clicks a row
#   • how to colour / mark points on the map (no geometry here → table only)
#
# DataUI is designed to be served with Panel:
#   panel serve examples/ex_readers.py --show
#
# In a notebook you can just call:  dataui.create_view().servable()

import panel as pn
pn.extension("tabulator", notifications=True, design="native")
pn.extension("gridstack")


class HydroDataUIManager(DataUIManager):
    """DataUIManager backed by the unified catalog.

    Implements the required view-layer methods for a simple table-only UI that
    lets users browse the catalog, filter rows, and download selected data.
    No geo-coordinates are present so the map panel is omitted automatically.
    """

    def __init__(self, catalog: DataCatalog, **params):
        self._cat = catalog
        super().__init__(**params)

    @property
    def data_catalog(self) -> DataCatalog:
        return self._cat

    def build_station_name(self, r: pd.Series) -> str:
        return str(r.get("stationid", r.get("name", r.name)))

    def get_data_catalog(self) -> pd.DataFrame:
        df = super().get_data_catalog()
        # Replace NaN with "" in string columns so the table shows blanks instead of "NaN".
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].fillna("")
        return df

    # -- table configuration --------------------------------------------------

    def get_table_column_width_map(self) -> dict:
        return {
            "name":        "20%",
            "variable":    "13%",
            "stationid":   "10%",
            "agency":      "8%",
            "source_type": "10%",
            "network":     "8%",
            "expression":  "31%",
        }

    def get_table_columns(self) -> list:
        # Only include columns that actually exist in the catalog DataFrame.
        dfcat = self.get_data_catalog()
        return [c for c in self.get_table_column_width_map() if c in dfcat.columns]

    def get_table_filters(self) -> dict:
        return {
            col: {"type": "input", "func": "like", "placeholder": f"Filter {col}…"}
            for col in self.get_table_columns()
        }

    # -- plot panel shown when the user selects rows and clicks "Plot" --------

    def create_panel(self, df: pd.DataFrame):
        """Return a panel with a simple time-series preview for each selected row."""
        import holoviews as hv
        hv.extension("bokeh")

        curves = []
        name_col = "name" if "name" in df.columns else None
        for _, row in df.iterrows():
            ref_name = row[name_col] if name_col else str(row.name)
            ref = self._cat.get(str(ref_name))
            if ref is None:
                continue
            ts = ref.getData()
            col = ts.columns[0]
            curve = hv.Curve(ts.reset_index(), kdims=["datetime"], vdims=[col], label=ref_name)
            curves.append(curve)

        if not curves:
            return pn.pane.Markdown("*No data available for selection.*")
        overlay = hv.Overlay(curves).opts(
            hv.opts.Curve(responsive=True, height=350, tools=["hover"]),
            hv.opts.Overlay(legend_position="top_right"),
        )
        return pn.pane.HoloViews(overlay, sizing_mode="stretch_width")

    # -- title helpers (called by TimeSeriesDataUIManager; minimal impl here) -

    def append_to_title_map(self, title_map: dict, unit: str, r: pd.Series) -> None:
        title_map["station"] = r.get("stationid", r.get("name", str(r.name)))
        title_map["variable"] = r.get("variable", "")

    def create_title(self, title_map: dict, unit: str, r: pd.Series) -> str:
        return f"{title_map.get('station', '')} – {title_map.get('variable', '')} ({unit})"

    # -- map configuration (no geometry — these are still required by DataUI) -

    def get_tooltips(self) -> list:
        return [("Name", "@name"), ("Variable", "@variable"), ("Station", "@stationid")]

    def get_map_color_columns(self) -> list:
        return ["variable", "agency"]

    def get_name_to_color(self) -> dict:
        return {
            "water_level":       "#1f77b4",
            "flow":              "#ff7f0e",
            "ec":                "#2ca02c",
            "water_level_bias":  "#d62728",
            "water_level_anomaly": "#9467bd",
        }

    def get_map_marker_columns(self) -> list:
        return ["agency", "source_type"]

    def get_name_to_marker(self) -> dict:
        return {
            "usgs":         "circle",
            "cdec":         "square",
            "tide_gauge":   "triangle",
            "model_output": "diamond",
        }

    def get_data_actions(self) -> list:
        actions = super().get_data_actions()
        math_action = MathRefEditorAction()
        actions.append(dict(
            name="Math Ref",
            button_type="warning",
            icon="math-function",
            action_type="display",
            callback=math_action.callback,
        ))
        return actions


manager = HydroDataUIManager(unified)
dataui  = DataUI(manager)

# Inspect the catalog table that DataUI will display:
dfview = manager.get_data_catalog()
print("\n── DataUI catalog view ──")
print(f"Total rows: {len(dfview)}")
cols = manager.get_table_columns()
print(f"Table columns: {cols}")
print(dfview[cols].fillna("").to_string(index=False))

# create_view() returns a Panel layout — servable as a Panel app.
# Launch with:  panel serve examples/ex_readers.py --show
# Or in a running notebook:  dataui.create_view().servable()
view = dataui.create_view(title="Hydro Data Catalog Demo")
print("\nDataUI view created:", type(view).__name__)
print("To launch as an app run:")
print("  panel serve examples/ex_readers.py --show")

# Make the view servable when this file is run via `panel serve`.
view.servable()
# %%
