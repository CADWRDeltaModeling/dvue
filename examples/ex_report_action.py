# %%
"""
Example: ReportAction — catalog-level report generation.

Demonstrates how to subclass ``ReportAction`` to produce a catalog-level
summary that runs without requiring any row selection.

Two report classes are shown:

1. ``CoverageReportAction``
   Summarises station coverage per variable (station count, year range) and
   renders a Markdown header + Tabulator table.

2. ``DataQualityReportAction``
   Loads time-series data for every catalog row and computes per-series
   statistics (count, mean, std, fraction missing).  Shows that ``generate()``
   can call ``manager.get_data_reference(row).getData()`` just like any other
   data-loading code.

Run with::

    panel serve examples/ex_report_action.py --show

Or as a script (no hot-reload)::

    python examples/ex_report_action.py
"""

# %% -- Imports ---------------------------------------------------------------
import numpy as np
import pandas as pd
import geopandas as gpd
import panel as pn
from pathlib import Path
from shapely.geometry import Point

from dvue import dataui, tsdataui
from dvue.actions import ReportAction
from dvue.catalog import DataCatalog, DataReference, InMemoryDataReferenceReader

pn.extension(notifications=True)

# %% -- Synthetic catalog -----------------------------------------------------

STATIONS = [
    dict(station_id="STA1", station_name="Station Alpha", lat=37.77, lon=-122.42),
    dict(station_id="STA2", station_name="Station Beta",  lat=36.16, lon=-115.15),
    dict(station_id="STA3", station_name="Station Gamma", lat=34.05, lon=-118.25),
]

VARIABLES = [
    ("flow",          "cfs"),
    ("stage",         "ft"),
    ("precipitation", "mm"),
]


def _make_ts(n_days=365, missing_frac=0.02, seed=None):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2020-01-01", periods=n_days, freq="D")
    values = np.cumsum(rng.standard_normal(n_days))
    # Introduce some NaNs so the quality report has something to show.
    mask = rng.random(n_days) < missing_frac
    values[mask] = np.nan
    return pd.DataFrame({"value": values}, index=idx)


catalog = DataCatalog(primary_key=["station_id", "variable", "unit"])

seed = 0
for stn in STATIONS:
    geom = Point(stn["lon"], stn["lat"])
    for variable, unit in VARIABLES:
        ref = DataReference(
            reader=InMemoryDataReferenceReader(_make_ts(seed=seed)),
            station_id=stn["station_id"],
            station_name=stn["station_name"],
            variable=variable,
            unit=unit,
            start_year=2020,
            end_year=2020,
            geometry=geom,
        )
        catalog.add(ref)
        seed += 1


# %% -- ReportAction subclasses -----------------------------------------------


class CoverageReportAction(ReportAction):
    """Summarise station coverage per variable — no data loading required."""

    def get_tab_label(self, tab_count):
        return f"Coverage R{tab_count}"

    def generate(self, catalog_df, manager):
        summary = (
            catalog_df.groupby("variable")
            .agg(
                stations=("station_id", "nunique"),
                min_year=("start_year", "min"),
                max_year=("end_year", "max"),
            )
            .reset_index()
        )
        table = pn.widgets.Tabulator(
            summary,
            show_index=False,
            sizing_mode="stretch_width",
            editors={col: None for col in summary.columns},
        )
        return pn.Column(
            pn.pane.Markdown("## Station Coverage by Variable"),
            table,
            sizing_mode="stretch_both",
        )


class DataQualityReportAction(ReportAction):
    """Compute per-series statistics by loading each series from the catalog."""

    def get_tab_label(self, tab_count):
        return f"Quality R{tab_count}"

    def generate(self, catalog_df, manager):
        rows = []
        for _, row in catalog_df.iterrows():
            try:
                ref = manager.get_data_reference(row)
                data = ref.getData()
            except Exception:
                rows.append({
                    "station_id": row.get("station_id", "?"),
                    "variable":   row.get("variable", "?"),
                    "unit":       row.get("unit", "?"),
                    "count":      0,
                    "mean":       float("nan"),
                    "std":        float("nan"),
                    "pct_missing": 100.0,
                })
                continue

            series = data.iloc[:, 0]
            n_total   = len(series)
            n_valid   = series.notna().sum()
            n_missing = n_total - n_valid
            rows.append({
                "station_id":   row.get("station_id", "?"),
                "variable":     row.get("variable", "?"),
                "unit":         row.get("unit", "?"),
                "count":        n_valid,
                "mean":         round(float(series.mean()), 3),
                "std":          round(float(series.std()), 3),
                "pct_missing":  round(100.0 * n_missing / n_total, 1) if n_total else 100.0,
            })

        df_report = pd.DataFrame(rows)
        table = pn.widgets.Tabulator(
            df_report,
            show_index=False,
            pagination="remote",
            page_size=20,
            sizing_mode="stretch_both",
            editors={col: None for col in df_report.columns},
        )
        return pn.Column(
            pn.pane.Markdown("## Data Quality Report"),
            table,
            sizing_mode="stretch_both",
        )


# %% -- Manager ---------------------------------------------------------------


class ExampleManager(tsdataui.TimeSeriesDataUIManager):
    def __init__(self, cat):
        self._cat = cat
        super().__init__()

    @property
    def data_catalog(self):
        return self._cat

    def get_data_catalog(self):
        df = self._cat.to_dataframe().reset_index()
        return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    def get_data_for_time_range(self, r, time_range):
        ref = self._cat.get(r["name"])
        df  = ref.getData()
        if time_range:
            df = df.loc[time_range[0]:time_range[1]]
        return df, r["unit"], "instantaneous"

    def get_time_range(self, dfcat):
        return pd.Timestamp("2020-01-01"), pd.Timestamp("2021-01-01")

    def build_station_name(self, r):
        return str(r["station_name"])

    def is_irregular(self, r):
        return False

    def get_table_column_width_map(self):
        return {
            "station_id":   "10%",
            "station_name": "20%",
            "variable":     "15%",
            "unit":         "8%",
            "start_year":   "8%",
            "end_year":     "8%",
        }

    def get_table_columns(self):
        return list(self.get_table_column_width_map().keys()) + ["name"]

    def get_tooltips(self):
        return [("Station", "@station_name"), ("Variable", "@variable")]

    def get_map_color_columns(self):
        return ["variable"]

    def get_map_marker_columns(self):
        return ["variable"]

    # -- Register the two report actions ------------------------------------
    def get_data_actions(self):
        actions = super().get_data_actions()
        coverage_action = CoverageReportAction()
        quality_action  = DataQualityReportAction()
        actions.append(dict(
            name="Coverage",
            button_type="warning",
            icon="report",
            action_type="display",
            callback=coverage_action.callback,
        ))
        actions.append(dict(
            name="Quality",
            button_type="warning",
            icon="list-check",
            action_type="display",
            callback=quality_action.callback,
        ))
        return actions


# %% -- App factory -----------------------------------------------------------


def make_app():
    mgr  = ExampleManager(catalog)
    ui   = dataui.DataUI(mgr, station_id_column="station_id")
    tmpl = ui.create_view(title="ReportAction Example")
    tmpl.servable()


make_app()

# %% -- Script launch ---------------------------------------------------------
if __name__ == "__main__":
    import subprocess, sys
    subprocess.run(
        [sys.executable, "-m", "panel", "serve", __file__, "--show", "--autoreload"],
        check=True,
    )
