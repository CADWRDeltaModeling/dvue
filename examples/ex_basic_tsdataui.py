"""
Basic Data UI Example

This example demonstrates how to use the TimeSeriesDataUIManager class to create
a simple time series data visualization dashboard without geographic information.

Based on the working ex_tsdataui.py example but simplified without geo data.
"""

import numpy as np
import pandas as pd
import panel as pn
import holoviews as hv
from dvue import tsdataui
from dvue import dataui

# Create sample catalog data (stations with time series data)
data = {
    "station_id": ["1", "2", "3"],
    "station_name": ["Station A", "Station B", "Station C"],
    "variable": ["temperature", "pressure", "humidity"],
    "unit": ["°C", "hPa", "%"],
    "interval": ["hourly", "daily", "hourly"],
    "start_year": ["2020", "2020", "2021"],
    "max_year": ["2024", "2024", "2024"],
}
df_catalog = pd.DataFrame(data)


# Create synthetic time series data for each station
def create_smooth_tsdf(interval="hourly", noise_scale=1.0):
    """Create a smooth random time series dataframe by cumulative sum of random noise."""
    if interval == "hourly":
        freq = "h"
    elif interval == "daily":
        freq = "d"
    else:
        freq = "h"

    date_rng = pd.date_range(start="1/1/2020", end="1/1/2021", freq=freq)
    # Generate smooth random walk
    values = np.cumsum(np.random.randn(len(date_rng)) * noise_scale)
    df = pd.DataFrame(values, index=date_rng, columns=["value"])
    return df


# Pre-generate time series data for each station
smooth_tsdfs = {
    row["station_name"]
    + row["unit"]
    + row["variable"]
    + row["interval"]: create_smooth_tsdf(interval=row["interval"], noise_scale=5.0)
    for _, row in df_catalog.iterrows()
}


class BasicTimeSeriesDataUIManager(tsdataui.TimeSeriesDataUIManager):
    """Simple time series data UI manager without geographic information"""

    def __init__(self, df):
        self.df = df
        super().__init__()

    def get_data_catalog(self):
        """Return the data catalog DataFrame"""
        return self.df

    def get_time_range(self, dfcat):
        """Return the time range for the data"""
        return pd.to_datetime("1/1/2020"), pd.to_datetime("1/1/2021")

    def build_station_name(self, r):
        """Build station identifier"""
        return f"{r['station_name']}"

    def get_table_column_width_map(self):
        """Define column widths for the table"""
        return {
            "station_id": "10%",
            "station_name": "20%",
            "variable": "15%",
            "unit": "10%",
            "interval": "10%",
            "start_year": "15%",
            "max_year": "15%",
        }

    def get_table_filters(self):
        """Define filters for each column"""
        return {
            "station_name": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "station_id": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "variable": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "unit": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "interval": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "start_year": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "max_year": {"type": "input", "func": "like", "placeholder": "Enter match"},
        }

    def is_irregular(self, r):
        """Check if data is irregular (all regular in this example)"""
        return False

    def get_data_for_time_range(self, r, time_range):
        """Fetch the actual time series data for a selected row"""
        key = r["station_name"] + r["unit"] + r["variable"] + r["interval"]
        return (
            smooth_tsdfs[key],
            r["unit"],
            "instantaneous",
        )

    def get_tooltips(self):
        """Return tooltips (empty for non-geo example)"""
        return []

    def get_map_color_columns(self):
        """Return columns for map coloring (required but not used without geo)"""
        return ["variable"]

    def get_map_marker_columns(self):
        """Return columns for markers (required but not used without geo)"""
        return ["variable"]

    def create_curve(self, df, r, unit, file_index=None):
        """Create a HoloViews curve for the time series data"""
        crvlabel = f'{r["station_id"]}/{r["variable"]}/{r["interval"]}'
        ylabel = f'{r["variable"]} ({unit})'
        title = f'{r["variable"]} @ {r["station_name"]}'
        crv = hv.Curve(df.iloc[:, [0]], label=crvlabel).redim(value=crvlabel)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            title=title,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    def _append_value(self, new_value, value):
        """Helper to append values to title map"""
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    def append_to_title_map(self, title_map, unit, r):
        """Append information to title map for display"""
        if unit in title_map:
            value = title_map[unit]
        else:
            value = ["", ""]
        value[0] = self._append_value(r["variable"], value[0])
        value[1] = self._append_value(r["station_id"], value[1])
        title_map[unit] = value

    def create_title(self, v):
        """Create title string for plot"""
        title = f"{v[1]} ({v[0]})"
        return title


# Initialize and serve
manager = BasicTimeSeriesDataUIManager(df_catalog)
ui = dataui.DataUI(manager)
ui.create_view(title="Basic Time Series Data UI (No Geo)").servable()

# To run this example:
# panel serve ex_basic_dataui.py --show
#
# Usage Instructions:
# 1. The table displays the data catalog with available time series
# 2. Select one or more rows by clicking on them (use Cmd/Ctrl+click for multiple)
# 3. Click the "Plot" button to display the time series plots in the panel below
# 4. The display panel will show interactive time series plots for the selected stations
