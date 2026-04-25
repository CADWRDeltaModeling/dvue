# %%
"""
Example: TimeSeriesDataUIManager backed by DataCatalog and MathDataReference.

This file demonstrates:

1. Wrapping in-memory DataFrames in DataReference objects and indexing them
   in a DataCatalog -- one reference per station / variable / interval.

2. Loading derived series (MathDataReferences) from YAML files via
   MathDataCatalogReader.  Two YAML files are included:

   ``math_refs_tsdataui.yaml`` — direct name-lookup style:
     expressions reference catalog keys by their full name.

   ``math_refs_search_map.yaml`` — search_map / alias style:
     each expression variable is a short alias (``obs``, ``ws_a``, …)
     resolved at getData() time by catalog attribute criteria, making
     expressions portable and independent of naming conventions:

       a. Unit conversion:   ``obs * 2.23694``  (obs → wind_speed A hourly)
       b. Cross-station diff: ``ws_b - ws_a``    (each resolved by station attr)
       c. Normalised anomaly: ``(obs - obs.mean()) / obs.std()``
       d. Multi-station mean: ``ws.mean(axis=1)`` (ws → all wind_speed hourly,
          joined into a DataFrame with ``_require_single: false``)

3. Connecting the catalog to a TimeSeriesDataUIManager by:
   * Overriding the ``data_catalog`` property  -> returns the DataCatalog
   * Overriding ``get_data_catalog()``         -> reconstructs a GeoDataFrame
     from the metadata stored on each DataReference (so the map widget has
     geometry).
   * Overriding ``get_data_for_time_range()``  -> calls ``ref.getData()`` for
     the matching DataReference or MathDataReference, then slices to the
     selected time window.

4. Using MathRefEditorAction to create, edit, save, and **load** math refs
   interactively without restarting the kernel.
"""

# %% -- [1] Imports -----------------------------------------------------------
import numpy as np
import pandas as pd
import geopandas as gpd
import holoviews as hv
from pathlib import Path
from shapely.geometry import Point

from dvue import dataui, tsdataui
from dvue.catalog import DataCatalog, DataReference, MathDataReference, InMemoryDataReferenceReader
from dvue import MathDataCatalogReader

# %% -- [2] Station metadata and synthetic data generator ---------------------
STATIONS = [
    dict(station_id="1", station_name="Station A", lat=34.05, lon=-118.25),
    dict(station_id="2", station_name="Station B", lat=36.16, lon=-115.15),
    dict(station_id="3", station_name="Station C", lat=37.77, lon=-122.42),
]

VARIABLES = [
    ("precipitation", "mm"),
    ("wind_speed", "m/s"),
    ("flow", "cfs"),
]

INTERVALS = ["hourly", "daily"]


def create_smooth_tsdf(interval: str = "hourly", noise_scale: float = 1.0) -> pd.DataFrame:
    """Return a smooth random-walk time series covering 2020."""
    freq = {"hourly": "h", "daily": "d"}[interval]
    idx = pd.date_range("2020-01-01", "2021-01-01", freq=freq)
    return pd.DataFrame(
        np.cumsum(np.random.randn(len(idx)) * noise_scale),
        index=idx,
        columns=["value"],
    )


# %% -- [3] Build the DataCatalog ---------------------------------------------


class StationDataReference(DataReference):
    """DataReference subclass for meteorological station data.

    Overrides :meth:`~DataReference.ref_key` to produce a compact,
    human-readable identifier from the three most meaningful attributes:
    ``station_name``, ``variable``, and ``interval``.  Spaces in
    ``station_name`` are replaced with underscores so the result is a valid
    Python identifier suitable for use in :class:`MathDataReference`
    expression strings.

    Example::

        ref.ref_key()  # "Station_A__wind_speed__hourly"
    """
    OVERRIDE_DEFAULT_DEF=False
    def ref_key(self) -> str:
        if (self.OVERRIDE_DEFAULT_DEF):
            name = self.get_attribute("station_name", "").replace(" ", "_")
            variable = self.get_attribute("variable", "")
            interval = self.get_attribute("interval", "")
            return f"{name}__{variable}__{interval}"
        else:
            return super().ref_key()


catalog = DataCatalog()

# -- 3a. Raw DataReference objects --------------------------------------------
#
# Each DataReference wraps a synthetic DataFrame.  All display metadata
# (station_id, station_name, variable, unit, interval, geometry ...) is stored
# as DataReference attributes so that get_data_catalog() can reconstruct a
# GeoDataFrame from the catalog without any external data structure.

for stn in STATIONS:
    geom = Point(stn["lon"], stn["lat"])
    for variable, unit in VARIABLES:
        for interval in INTERVALS:
            ref = StationDataReference(
                reader=InMemoryDataReferenceReader(create_smooth_tsdf(interval=interval)),
                station_id=stn["station_id"],
                station_name=stn["station_name"],
                variable=variable,
                unit=unit,
                interval=interval,
                start_year="2020",
                max_year="2021",
                geometry=geom,
            )
            ref.set_key_attributes(["station_id", "variable", "interval", "unit"])  # for ref_key()
            ref.name = ref.ref_key()
            catalog.add(ref)


# -- Catalog summary (raw refs only -- math refs are loaded from YAML below) --
print(catalog)  # DataCatalog(N references)
print(catalog.to_dataframe()[["station_name", "variable", "unit", "interval"]].to_string())


# %% -- [4] ExampleTimeSeriesPlotAction + ExampleTimeSeriesDataUIManager ------


class ExampleTimeSeriesPlotAction(tsdataui.TimeSeriesPlotAction):
    """TimeSeriesPlotAction for the example station catalog.

    Overrides :meth:`create_curve` to add domain-specific axis labels and
    :meth:`append_to_title_map` / :meth:`create_title` to format overlay
    titles as ``station_ids(variables)``.
    """

    def create_curve(self, df, r, unit, file_index=None):
        label = f'{r["station_id"]}/{r["variable"]}/{r["interval"]}'
        crv = hv.Curve(df.iloc[:, [0]], label=label).redim(value=label)
        return crv.opts(
            xlabel="Time",
            ylabel=f'{r["variable"]} ({unit})',
            title=f'{r["variable"]} @ {r["station_name"]}',
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    def _append_value(self, new_value, value):
        new_value_str = str(new_value) if new_value is not None and str(new_value).lower() != "nan" else ""
        if new_value_str and new_value_str not in value:
            value += f'{", " if value else ""}{new_value_str}'
        return value

    def append_to_title_map(self, title_map, unit, r):
        value = title_map.get(unit, ["", ""])
        value[0] = self._append_value(r["variable"], value[0])
        value[1] = self._append_value(r["station_id"], value[1])
        title_map[unit] = value

    def create_title(self, v):
        return f"{v[1]}({v[0]})"


# %% -- [4] ExampleTimeSeriesDataUIManager ------------------------------------
class ExampleTimeSeriesDataUIManager(tsdataui.TimeSeriesDataUIManager):
    """TimeSeriesDataUIManager backed by a DataCatalog.

    Demonstrates the ``data_catalog`` / ``get_data_catalog()`` integration:

    * ``data_catalog`` property      -> returns the backing DataCatalog, making
      it available to DataProvider's default ``get_data_reference()`` helper.
    * ``get_data_catalog()``         -> reconstructs a GeoDataFrame from the
      metadata stored on each DataReference so the map widget has geometry.
    * ``get_data_for_time_range()``  -> calls ``ref.getData()`` for the named
      DataReference or MathDataReference, then slices to the time window.

    Both raw DataReferences and derived MathDataReferences are retrieved
    identically -- the caller does not need to distinguish them.
    """

    def __init__(self, catalog: DataCatalog):
        # Must be assigned before super().__init__() because
        # TimeSeriesDataUIManager.__init__ calls self.get_data_catalog() during
        # setup (to inspect column names and infer the time range).
        self._cat = catalog
        super().__init__()
        self.color_cycle_column = "station_name"
        self.dashed_line_cycle_column = "interval"
        self.marker_cycle_column = "variable"

    # -- DataProvider: expose the DataCatalog ---------------------------------
    @property
    def data_catalog(self) -> DataCatalog:
        """Expose the backing DataCatalog to DataProvider's default methods."""
        return self._cat

    # -- get_data_catalog: GeoDataFrame reconstructed from catalog metadata ---
    def get_data_catalog(self) -> gpd.GeoDataFrame:
        """Build a GeoDataFrame from the metadata attributes of each DataReference.

        ``reset_index()`` promotes the reference name (the catalog key) into a
        regular ``'name'`` column.  ``get_data_for_time_range()`` uses
        ``r["name"]`` to look up the corresponding DataReference at display time.

        :meth:`~dvue.tsdataui.TimeSeriesDataUIManager._enrich_catalog_with_math_ref_hints`
        fills blank ``expression`` cells for raw refs with their catalog name so
        users can see exactly what token to use in new expressions.
        """
        df = self._cat.to_dataframe().reset_index()  # 'name' becomes a regular column
        df = self._enrich_catalog_with_math_ref_hints(df)
        return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # -- get_data_for_time_range: delegate to DataReference.getData() ---------
    def get_data_for_time_range(self, r, time_range):
        """Load data via the DataReference (or MathDataReference) for this row.

        Uses ``r["name"]`` -- the catalog key placed into the DataFrame by
        ``get_data_catalog()`` -- to retrieve the correct reference.
        MathDataReferences evaluate their expression transparently on
        ``getData()``, so no special-casing is needed here.
        """
        ref = self._cat.get(r["name"])
        df = ref.getData()
        if time_range:
            df = df.loc[time_range[0] : time_range[1]]
        return df, r["unit"], "instantaneous"

    def get_time_range(self, dfcat):
        return pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01")

    def build_station_name(self, r):
        return str(r["station_name"])

    def is_irregular(self, r):
        return False

    # -- View layer -----------------------------------------------------------
    def get_table_column_width_map(self):
        widths = {
            "ref_type": "6%",
            "station_id": "6%",
            "station_name": "12%",
            "variable": "14%",
            "unit": "6%",
            "interval": "6%",
            "start_year": "6%",
            "max_year": "6%",
        }
        if self._has_math_refs():
            widths["expression"] = "44%"
        return widths

    def get_table_columns(self):
        # "name" is the DataCatalog lookup key used by get_data_for_time_range.
        # It must survive the DataUI.create_data_table() column trim even though
        # it has no entry in get_table_column_width_map() (not a visible column).
        return list(self.get_table_column_width_map().keys()) + ["name"]

    def get_table_filters(self):
        filterable = [
            "ref_type",
            "station_name",
            "station_id",
            "variable",
            "unit",
            "interval",
            "start_year",
            "max_year",
        ]
        if self._has_math_refs():
            filterable.append("expression")
        return {
            col: {"type": "input", "func": "like", "placeholder": f"Filter {col}"}
            for col in filterable
        }

    def get_tooltips(self):
        return [
            ("Station ID", "@station_id"),
            ("Name", "@station_name"),
            ("Variable", "@variable"),
            ("Unit", "@unit"),
            ("Interval", "@interval"),
        ]

    def get_map_color_columns(self):
        return ["variable", "interval"]

    def get_map_marker_columns(self):
        return ["variable"]

    def _make_plot_action(self):
        return ExampleTimeSeriesPlotAction()


# %% -- [5] Load math refs from YAML -----------------------------------------
#
# Two YAML files are included; both can be loaded independently or together.
#
# math_refs_tsdataui.yaml  -- direct name-lookup style (expression tokens are
#   full catalog key names like Station_A__wind_speed__hourly).
#
# math_refs_search_map.yaml  -- search_map / alias style (recommended):
#   expression tokens are short aliases like obs, ws_a, ws_b resolved by
#   catalog attribute criteria at getData() time.  Portable across catalogs.
#
# Both YAML paths are pre-filled in the Math Ref editor so the user can load
# either file with a single click.  Only one MATH_REFS_FILE is used as the
# default; change the value below to switch.

MATH_REFS_FILE = Path(__file__).parent / "data" / "math_refs_tsdataui.yaml"
MATH_REFS_SEARCH_MAP_FILE = Path(__file__).parent / "data" / "math_refs_search_map.yaml"

# Catalog starts with raw refs only.  Math refs are loaded on demand via the
# "Math Ref" editor action -- use "Load from YAML" to populate from either file.
catalog_from_yaml = DataCatalog()
for ref in catalog.list():
    catalog_from_yaml.add(ref)

print(f"Catalog: {catalog_from_yaml} (no math refs loaded yet)")


# %% -- [6] Launch the UI backed by the YAML catalog --------------------------
#
# MathRefEditorAction is included by default in TimeSeriesDataUIManager.
# Use the Math Ref button > "Load from YAML" to populate the catalog from
# either YAML file at runtime without restarting the kernel.
#
# To hide the button for a specific manager, set:
#   exmgr.show_math_ref_editor = False
#   -- or pass show_math_ref_editor=False to the constructor.

exmgr_editable = ExampleTimeSeriesDataUIManager(catalog_from_yaml)
ui = dataui.DataUI(exmgr_editable, station_id_column="station_id")
ui.create_view(
    title="Example Time Series Data UI (DataCatalog + MathDataReference from YAML)"
).servable()
# %%
