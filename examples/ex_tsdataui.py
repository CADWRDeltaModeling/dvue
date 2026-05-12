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

# %% -- [1b] Session persistence setup ----------------------------------------
#
# Must happen BEFORE pn.serve() / BokehServer.__init__(), which is why this
# example must be run as `python examples/ex_tsdataui.py`, not
# `panel serve examples/ex_tsdataui.py`.
#
# Two-layer approach:
#   Layer 1 — manager registry (in-memory):  UUID cookie → _MANAGER_REGISTRY
#     lookup → reuse existing param.Parameterized manager → fresh DataUI wraps
#     it.  All param state (time_range, catalog, math refs) is already live.
#   Layer 2 — diskcache (disk):  After a server restart the registry is empty;
#     diskcache restores picklable params into a newly created manager.

import panel as pn
from uuid import uuid4
from bokeh.server.urls import per_app_patterns
from panel.io.server import DocHandler


class _SessionAwareDocHandler(DocHandler):
    """Sets a persistent 'dvue_user_id' UUID cookie on first visit and injects
    it into the current request so session_key_func sees it immediately."""

    _COOKIE_NAME = "dvue_user_id"

    async def get(self, *args, **kwargs):
        user_id = self.get_cookie(self._COOKIE_NAME)
        if not user_id:
            user_id = uuid4().hex
            self.set_cookie(self._COOKIE_NAME, user_id, expires_days=365, path="/")
            # Inject into request.cookies (Tornado SimpleCookie) so
            # session_key_func can read it on the very first visit.
            self.request.cookies[self._COOKIE_NAME] = user_id
        await super().get(*args, **kwargs)


per_app_patterns[0] = (r"/?", _SessionAwareDocHandler)

# -- diskcache state store (server-restart fallback) -------------------------
#
# Panel already depends on diskcache (used by pn.state.as_cached), so it is
# always available in the Panel conda/pip environment.
#
# diskcache advantages over hand-rolled JSON:
#  - file-locking: safe for concurrent multi-user writes
#  - built-in TTL: entries expire automatically (no manual eviction needed)
#  - cleaner API: no tmp-file dance
#
# IMPORTANT: diskcache stores plain picklable Python objects (dicts, lists,
# datetimes).  It CANNOT store live Panel/HoloViews objects (pn.Tabs, hv.Overlay
# etc.) — those are not meaningfully picklable.  Only the *inputs* that produce
# plots (selection indices, time range) are stored here.

import diskcache

_CACHE_DIR = Path(__file__).parent.parent / ".session_cache"
_SESSION_CACHE = diskcache.Cache(str(_CACHE_DIR))
_TTL = 30 * 24 * 3600  # 30 days


def _load_state(user_id: str) -> dict:
    return _SESSION_CACHE.get(user_id, default={})


def _save_state(user_id: str, state: dict) -> None:
    _SESSION_CACHE.set(user_id, state, expire=_TTL)

# %% -- [2] Station metadata and synthetic data generator ---------------------
STATIONS = [
    dict(station_id="STA1", station_name="Station A", lat=34.05, lon=-118.25),
    dict(station_id="STA2", station_name="Station B", lat=36.16, lon=-115.15),
    dict(station_id="STA3", station_name="Station C", lat=37.77, lon=-122.42),
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
    """DataReference subclass for meteorological station data."""


catalog = DataCatalog(primary_key=["station_id", "variable", "interval", "unit"])

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


# %% -- [6] App registry ------------------------------------------------------
#
# user_id → {"mgr": manager, "ui": DataUI, "template": VanillaTemplate}
#
# Panel widgets (pn.Tabs, pn.Row, hv.HoloViews panes …) maintain their Python-
# level state (.objects, param values) independently of any Bokeh Document.
# When template.servable() is called in a new session, Panel creates fresh
# Bokeh models for the new Document that mirror the current Python state —
# including all plot tabs already in _display_panel.  No replay needed.
#
# Per-session work (URL/location sync) is re-registered via pn.state.onload
# because pn.state.location is bound to the current Bokeh Document.

_APP_REGISTRY: dict = {}  # user_id → {mgr, ui, template}


def _make_catalog():
    """Return a fresh per-user DataCatalog (same raw refs, independent dict)."""
    cat = DataCatalog(primary_key=["station_id", "variable", "interval", "unit"])
    for ref in catalog.list():
        cat.add(ref)
    return cat


def _snapshot(mgr, ui) -> dict:
    """Picklable snapshot for diskcache (server-restart fallback only)."""
    tr = getattr(mgr, "time_range", None)
    tbl = getattr(ui, "display_table", None)
    return {
        "time_range": (
            [pd.Timestamp(tr[0]).isoformat(), pd.Timestamp(tr[1]).isoformat()]
            if tr else None
        ),
        "selection": list(tbl.selection or []) if tbl is not None else [],
    }


def _restore(mgr, saved: dict) -> None:
    """Apply diskcache params to a freshly created manager."""
    tr = saved.get("time_range")
    if tr:
        try:
            mgr.time_range = (
                pd.Timestamp(tr[0]).to_pydatetime(),
                pd.Timestamp(tr[1]).to_pydatetime(),
            )
        except Exception:
            pass


# %% -- [7] App factory -------------------------------------------------------
#
# Called once per Bokeh session (every browser tab open / page load).
#
# Registry hit (server running, returning user):
#   Reuse existing mgr + ui + template.  Panel automatically mirrors all
#   widget state — display_panel tabs, table selection, etc. — into the new
#   Bokeh Document when template.servable() is called.  Only per-Document
#   setup (URL/location sync) is re-registered.
#
# No registry entry (new user or server restart):
#   Create fresh manager + DataUI.  Restore params from diskcache if present.
#   Re-trigger plot for the saved selection (diskcache case only).

def make_app():
    user_id = pn.state.cookies.get("dvue_user_id", "")

    if user_id and user_id in _APP_REGISTRY:
        # --- Registry hit: reuse existing objects --------------------------
        entry = _APP_REGISTRY[user_id]
        mgr   = entry["mgr"]
        ui    = entry["ui"]
        tmpl  = entry["template"]
        # Re-register per-Document setup (location/URL sync binds to curdoc).
        pn.state.onload(lambda: (ui.setup_location_sync(), ui.setup_url_sync()))
        tmpl.servable()
        return

    # --- No registry: new user or server restart ---------------------------
    mgr = ExampleTimeSeriesDataUIManager(_make_catalog())
    saved = _load_state(user_id) if user_id else {}
    if saved:
        _restore(mgr, saved)

    ui = dataui.DataUI(mgr, station_id_column="station_id")
    tmpl = ui.create_view(
        title="Example Time Series Data UI (DataCatalog + MathDataReference from YAML)"
    )
    tmpl.servable()

    if user_id:
        _APP_REGISTRY[user_id] = {"mgr": mgr, "ui": ui, "template": tmpl}

    # Restore selection + re-trigger plot (only meaningful after server restart
    # when diskcache had a saved selection; fresh users have no saved state).
    sel = saved.get("selection", [])

    def _on_load():
        if sel and hasattr(ui, "display_table") and hasattr(ui, "_registered_actions"):
            plot_cb = next(
                (a["callback"] for a in ui._registered_actions if a.get("name") == "Plot"),
                None,
            )
            if plot_cb:
                ui.display_table.selection = sel
                pn.state.curdoc.add_next_tick_callback(lambda: plot_cb(None, ui))

        # Wire live-persistence watchers.
        def _save(event=None):
            if user_id:
                _save_state(user_id, _snapshot(mgr, ui))

        mgr.param.watch(_save, "time_range")
        if hasattr(ui, "display_table"):
            ui.display_table.param.watch(_save, "selection")

    pn.state.onload(_on_load)


# %% -- [8] Programmatic launch -----------------------------------------------
#
# Run:  cd dvue && python examples/ex_tsdataui.py
# Do NOT use `panel serve` — the per_app_patterns patch in [1b] must execute
# before BokehServer.__init__().  pn.serve() receives make_app (a callable)
# so module-level code (catalog, class definitions) runs only once.

if __name__ == "__main__":
    pn.serve(
        {"tsdataui": make_app},
        port=5007,
        show=True,
        unused_session_lifetime_milliseconds=2_592_000_000,
    )
# %%
