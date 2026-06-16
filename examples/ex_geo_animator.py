"""ex_geo_animator.py — GeoAnimatorManager demo with synthetic data.

Run with:
    panel serve examples/ex_geo_animator.py --show

The example shows all three supported geometry types side by side using
synthetic data from InMemorySlicingReader.

Subclassing guide (HDF5 example)
---------------------------------
To load data from an HDF5 file instead of memory, subclass SlicingReader::

    import h5py
    import numpy as np
    import pandas as pd
    from dvue.animator import SlicingReader

    class HDF5ConcentrationReader(SlicingReader):
        def __init__(self, filepath, constituent_index=0):
            import h5py, numpy as np, pandas as pd
            self._h5 = h5py.File(filepath, "r")
            ds = self._h5["output/channel concentration"]
            attrs = dict(ds.attrs)
            start = pd.Timestamp(attrs["start_time"][0].decode())
            freq  = pd.to_timedelta(attrs["interval"][0].decode())
            n_t   = ds.shape[0]
            idx   = pd.date_range(start=start, periods=n_t, freq=freq)
            self._ds   = ds
            self._ci   = constituent_index          # which constituent
            self._chans = self._h5["output/channel_number"][:]
            # Global range — careful with large files; consider chunked min/max
            sample = ds[:, constituent_index, :, :]  # (time, chan, loc)
            self._vmin = float(np.nanmin(sample))
            self._vmax = float(np.nanmax(sample))
            super().__init__(idx)

        @property
        def vmin(self): return self._vmin

        @property
        def vmax(self): return self._vmax

        def get_slice(self, timestamp):
            i = self._time_index.get_indexer([timestamp], method="nearest")[0]
            row = self._ds[i, self._ci, :, :]    # (n_chan, n_loc)
            values = row.mean(axis=1)             # average u/d
            return pd.Series(values, index=self._chans.tolist(), dtype=float)

Then:
    reader = HDF5ConcentrationReader("path/to/historical_gtm.h5")
    # Build GeoDataFrame from DSM2 channel shapefile (optionally buffer lines)
    gdf = gpd.read_file("DSM2_Flowline_Segments.shp").to_crs("EPSG:4326")
    gdf = gdf.rename(columns={"channel_nu": "geo_id"})
    # Buffer LineStrings → Polygons for filled colouring
    gdf["geometry"] = gdf.geometry.buffer(0.001)
    mgr = GeoAnimatorManager(reader, gdf, title="GTM EC")
    mgr.servable()
"""

# %%
import numpy as np
import pandas as pd
import geopandas as gpd
import panel as pn
from shapely.geometry import Point, Polygon, LineString

from dvue.animator import InMemorySlicingReader, GeoAnimatorManager, CURATED_COLORMAPS

pn.extension(throttled=True)

# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

N_IDS = 20
N_TIMES = 60
RNG = np.random.default_rng(0)


def _make_reader(seed: int = 0, freq: str = "D") -> InMemorySlicingReader:
    """Create a reader with a smooth random-walk signal."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2020-01-01", periods=N_TIMES, freq=freq)
    # Random walk — visually interesting animation
    values = rng.standard_normal((N_TIMES, N_IDS)).cumsum(axis=0) * 10 + 500
    df = pd.DataFrame(values, index=idx, columns=list(range(N_IDS)))
    return InMemorySlicingReader(df)


# ---------------------------------------------------------------------------
# 1. Points example — station-like observations
# ---------------------------------------------------------------------------

def _make_point_gdf() -> gpd.GeoDataFrame:
    lons = np.linspace(-122.5, -121.5, N_IDS)
    lats = np.linspace(37.8, 38.6, N_IDS)
    return gpd.GeoDataFrame(
        {"geo_id": list(range(N_IDS)),
         "label": [f"STN{i:03d}" for i in range(N_IDS)],
         "geometry": [Point(lon, lat) for lon, lat in zip(lons, lats)]},
        crs="EPSG:4326",
    )


reader_pts = _make_reader(seed=1, freq="6h")
gdf_pts = _make_point_gdf()

mgr_points = GeoAnimatorManager(
    reader_pts,
    gdf_pts,
    title="Synthetic Stations",
    colormap="plasma",
    map_width=700,
    map_height=450,
)

# ---------------------------------------------------------------------------
# 2. Polygons example — watershed sub-basins or grid cells
# ---------------------------------------------------------------------------

def _make_polygon_gdf() -> gpd.GeoDataFrame:
    def _cell(i):
        col, row = divmod(i, 4)
        x0 = -122.4 + col * 0.25
        y0 = 37.9 + row * 0.25
        return Polygon([(x0, y0), (x0 + 0.24, y0), (x0 + 0.24, y0 + 0.24), (x0, y0 + 0.24)])

    return gpd.GeoDataFrame(
        {"geo_id": list(range(N_IDS)),
         "zone": [f"Z{i:02d}" for i in range(N_IDS)],
         "geometry": [_cell(i) for i in range(N_IDS)]},
        crs="EPSG:4326",
    )


reader_poly = _make_reader(seed=2, freq="D")
gdf_poly = _make_polygon_gdf()

mgr_polygons = GeoAnimatorManager(
    reader_poly,
    gdf_poly,
    title="Synthetic Grid Cells",
    colormap="YlOrRd",
    map_width=700,
    map_height=450,
)

# ---------------------------------------------------------------------------
# 3. Lines example — channel network (like DSM2)
# ---------------------------------------------------------------------------

def _make_line_gdf() -> gpd.GeoDataFrame:
    """Fan-shaped channels radiating from a common node."""
    origin = (-122.0, 38.2)
    lines = []
    for i in range(N_IDS):
        angle = np.radians(-90 + i * (180 / (N_IDS - 1)))
        dx = 0.5 * np.cos(angle)
        dy = 0.5 * np.sin(angle)
        lines.append(LineString([origin, (origin[0] + dx, origin[1] + dy)]))
    return gpd.GeoDataFrame(
        {"geo_id": list(range(N_IDS)),
         "channel": [f"CH{i:03d}" for i in range(N_IDS)],
         "geometry": lines},
        crs="EPSG:4326",
    )


reader_lines = _make_reader(seed=3, freq="h")
gdf_lines = _make_line_gdf()

mgr_lines = GeoAnimatorManager(
    reader_lines,
    gdf_lines,
    title="Synthetic Channels",
    colormap="rainbow",
    map_width=700,
    map_height=450,
)

# ---------------------------------------------------------------------------
# Compose into a tabbed layout
# ---------------------------------------------------------------------------

tabs = pn.Tabs(
    ("Points", mgr_points),
    ("Polygons", mgr_polygons),
    ("Lines / Channels", mgr_lines),
    sizing_mode="stretch_width",
)

header = pn.pane.Markdown(
    "# GeoAnimatorManager Demo\n"
    "Use the **DateSlider** in each tab to animate the map. "
    "Adjust colour scale and colormap in the controls panel.\n\n"
    f"Available colormaps: `{'`, `'.join(CURATED_COLORMAPS)}`",
    sizing_mode="stretch_width",
)

app = pn.Column(header, tabs, sizing_mode="stretch_width")
app.servable(title="GeoAnimatorManager Demo")
