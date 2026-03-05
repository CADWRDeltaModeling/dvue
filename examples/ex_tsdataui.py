# %%
"""
Example: TimeSeriesDataUIManager backed by DataCatalog and MathDataReference.

This file demonstrates:

1. Wrapping in-memory DataFrames in DataReference objects and indexing them
   in a DataCatalog -- one reference per station / variable / interval.

2. Creating derived series via three MathDataReference patterns:

   a. Operator overloading  --  wind speed  m/s -> mph         (ref * 2.23694)
   b. Expression string     --  cumulative precipitation        (cumsum(...))
   c. Multi-var operators   --  cross-station wind anomaly      (ref_A - ref_C)

3. Connecting the catalog to a TimeSeriesDataUIManager by:
   * Overriding the ``data_catalog`` property  -> returns the DataCatalog
   * Overriding ``get_data_catalog()``         -> reconstructs a GeoDataFrame
     from the metadata stored on each DataReference (so the map widget has
     geometry).
   * Overriding ``get_data_for_time_range()``  -> calls ``ref.getData()`` for
     the matching DataReference or MathDataReference, then slices to the
     selected time window.

MathDataReference expressions are evaluated in a namespace that exposes NumPy
functions (``cumsum``, ``sqrt``, ``abs``, ``sin``, ``cos``, ``log``, ``exp``,
``clip``, ``where``, ``pi``, ``e``, and the full ``np`` / ``pd`` / ``math``
modules) alongside variables resolved from ``variable_map`` or a ``catalog``.
The arithmetic operators (``+``, ``-``, ``*``, ``/``, ``**``) automatically
construct MathDataReference objects so derived signals compose algebraically.
"""

# %% -- [1] Imports -----------------------------------------------------------
import numpy as np
import pandas as pd
import geopandas as gpd
import holoviews as hv
from shapely.geometry import Point

from dvue import dataui, tsdataui
from dvue.catalog import DataCatalog, DataReference, MathDataReference

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

    def ref_key(self) -> str:
        name = self.get_attribute("station_name", "").replace(" ", "_")
        variable = self.get_attribute("variable", "")
        interval = self.get_attribute("interval", "")
        return f"{name}__{variable}__{interval}"


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
                source=create_smooth_tsdf(interval=interval),
                station_id=stn["station_id"],
                station_name=stn["station_name"],
                variable=variable,
                unit=unit,
                interval=interval,
                start_year="2020",
                max_year="2021",
                geometry=geom,
            )
            ref.name = ref.ref_key()
            catalog.add(ref)


# -- 3b. MathDataReference -- operator overloading: m/s -> mph ---------------
#
# DataReference.__mul__ (and all other arithmetic operators) automatically
# constructs a MathDataReference whose expression string uses the operand's
# .name as the variable identifier.  Because StationDataReference.ref_key()
# produces valid Python identifiers, the generated expression string is
# syntactically correct without any further sanitisation.
#
# Generated expression: "Station_A__wind_speed__hourly * (2.23694)"

wind_a_h = catalog.search(station_name="Station A", variable="wind_speed", interval="hourly")[0]

wind_a_mph: MathDataReference = wind_a_h * 2.23694
wind_a_mph.name = "Station_A__wind_speed_mph__hourly"
for attr, val in [
    ("station_id", "1"),
    ("station_name", "Station A"),
    ("variable", "wind_speed_mph"),
    ("unit", "mph"),
    ("interval", "hourly"),
    ("start_year", "2020"),
    ("max_year", "2021"),
    ("geometry", Point(-118.25, 34.05)),
]:
    wind_a_mph.set_attribute(attr, val)
catalog.add(wind_a_mph)


# -- 3c. MathDataReference -- expression string: cumulative precipitation -----
#
# The expression is a Python string evaluated in a namespace that exposes
# NumPy functions: cumsum, sqrt, abs, sin, cos, log, log10, exp, clip, where,
# min, max, pi, e, and the full np / pd / math modules.
#
# Variable names in the expression must be valid Python identifiers that match
# keys in variable_map (or names in an attached catalog).  Because
# StationDataReference.ref_key() produces underscore-separated identifiers,
# ref.name can be embedded verbatim as the variable name.
#
# Generated expression: "cumsum(Station_B__precipitation__hourly)"

precip_b_h = catalog.search(station_name="Station B", variable="precipitation", interval="hourly")[
    0
]

precip_cumulative = MathDataReference(
    expression=f"cumsum({precip_b_h.name})",
    variable_map={precip_b_h.name: precip_b_h},
    name="Station_B__precip_cumulative__hourly",
    station_id="2",
    station_name="Station B",
    variable="precip_cumulative",
    unit="mm",
    interval="hourly",
    start_year="2020",
    max_year="2021",
    geometry=Point(-115.15, 36.16),
)
catalog.add(precip_cumulative)


# -- 3d. MathDataReference -- multi-variable: cross-station wind anomaly ------
#
# Operator overloading works across two DataReferences.  DataReference.__sub__
# builds the expression "lhs.name - rhs.name" and merges both variable maps
# automatically.  Any further arithmetic (scaling, abs, sqrt ...) can be
# chained without limit.
#
# Generated expression:
#   "Station_A__wind_speed__hourly - Station_C__wind_speed__hourly"

wind_c_h = catalog.search(station_name="Station C", variable="wind_speed", interval="hourly")[0]

wind_diff: MathDataReference = wind_a_h - wind_c_h
wind_diff.name = "wind_diff_A_minus_C__hourly"
for attr, val in [
    ("station_id", "1"),
    ("station_name", "Station A"),
    ("variable", "wind_diff_A_minus_C"),
    ("unit", "m/s"),
    ("interval", "hourly"),
    ("start_year", "2020"),
    ("max_year", "2021"),
    ("geometry", Point(-118.25, 34.05)),
]:
    wind_diff.set_attribute(attr, val)
catalog.add(wind_diff)


# -- Catalog summary ----------------------------------------------------------
print(catalog)  # DataCatalog(N references)
print(catalog.to_dataframe()[["station_name", "variable", "unit", "interval"]].to_string())


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
        """
        df = self._cat.to_dataframe().reset_index()  # 'name' becomes a regular column
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
        return {
            "station_id": "6%",
            "station_name": "14%",
            "variable": "18%",
            "unit": "8%",
            "interval": "8%",
            "start_year": "8%",
            "max_year": "8%",
        }

    def get_table_columns(self):
        # "name" is the DataCatalog lookup key used by get_data_for_time_range.
        # It must survive the DataUI.create_data_table() column trim even though
        # it has no entry in get_table_column_width_map() (not a visible column).
        return list(self.get_table_column_width_map().keys()) + ["name"]

    def get_table_filters(self):
        return {
            col: {"type": "input", "func": "like", "placeholder": f"Filter {col}"}
            for col in [
                "station_name",
                "station_id",
                "variable",
                "unit",
                "interval",
                "start_year",
                "max_year",
            ]
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
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    def append_to_title_map(self, title_map, unit, r):
        value = title_map.get(unit, ["", ""])
        value[0] = self._append_value(r["variable"], value[0])
        value[1] = self._append_value(r["station_id"], value[1])
        title_map[unit] = value

    def create_title(self, v):
        return f"{v[1]}({v[0]})"


# %% -- [5] Launch the UI -----------------------------------------------------
exmgr = ExampleTimeSeriesDataUIManager(catalog)
ui = dataui.DataUI(exmgr, station_id_column="station_id")
ui.create_view(title="Example Time Series Data UI (DataCatalog + MathDataReference)").servable()
# %%
