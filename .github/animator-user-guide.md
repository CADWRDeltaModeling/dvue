# dvue.animator — User Guide

Interactive geo-animation of time-varying spatial data on a tile-backed map.

---

## Quick Start

```python
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import panel as pn
import holoviews as hv

hv.extension("bokeh")
pn.extension()

from dvue.animator import InMemorySlicingReader, GeoAnimatorManager

# 1. Build a reader from any DataFrame (time × geo_id)
n_ids, n_steps = 20, 60
idx = pd.date_range("2020-01-01", periods=n_steps, freq="D")
data = pd.DataFrame(np.random.rand(n_steps, n_ids) * 1000,
                    index=idx, columns=range(n_ids))
reader = InMemorySlicingReader(data)

# 2. Build a GeoDataFrame with a "geo_id" column
gdf = gpd.GeoDataFrame(
    {"geo_id": range(n_ids),
     "geometry": [Point(-122.0 + i * 0.05, 38.0) for i in range(n_ids)]},
    crs="EPSG:4326",
)

# 3. Create and serve the animator
mgr = GeoAnimatorManager(reader, gdf, title="My Animation")
mgr.servable()
```

Or from the command line (DSM2 tidefiles):

```bash
dsm2ui animate hydro path/to/tidefile.h5
dsm2ui animate qual  path/to/qual_ec.h5 --constituent ec
```

---

## Core Concepts

### SlicingReader

A `SlicingReader` is the data source.  It must provide:

- A **regular** `pd.DatetimeIndex` (fixed time step — daily, hourly, 15-min, etc.)
- A `get_slice(timestamp)` method that returns a `pd.Series(index=geo_ids, values=float)`
- Global `vmin` / `vmax` for the initial colour scale

The built-in `InMemorySlicingReader` wraps any `pd.DataFrame` with a
`DatetimeIndex` as rows and geo-feature IDs as columns.

For HDF5 tidefiles, use the DSM2 readers from `dsm2ui.animate`:

```python
from dsm2ui.animate import (
    HydroH5FlowReader,
    HydroH5StageReader,
    HydroH5VelocityReader,
    QualH5ConcentrationReader,
)
```

### GeoDataFrame requirements

- Must have a **CRS** set (e.g. `crs="EPSG:4326"`).
- Must have a column that identifies each feature — passed as `geo_id_column`
  (default `"geo_id"`).  Values must match the column labels (index) of the
  `pd.Series` returned by `reader.get_slice()`.
- Supported geometry types: **Point**, **Polygon**, **LineString** (and Multi- variants).
  The correct Bokeh glyph (`scatter`, `patches`, `multi_line`) is chosen automatically.

---

## GeoAnimatorManager Parameters

| Parameter | Default | Description |
|---|---|---|
| `reader` | required | Any `SlicingReader` instance |
| `geodataframe` | required | `gpd.GeoDataFrame` with CRS |
| `geo_id_column` | `"geo_id"` | Column in GDF matching reader's geo IDs |
| `title` | `""` | Map title prefix (timestamp appended automatically) |
| `vmin` | `None` | Colour scale lower bound (None → reader.vmin) |
| `vmax` | `None` | Colour scale upper bound (None → reader.vmax) |
| `colormap` | `"viridis"` | Initial colormap (see curated list below) |
| `size` | `8.0` | Point radius (px) or line width (px) |
| `map_height` | `500` | Minimum map height (px) |
| `x2_callback` | `None` | Optional callable for an isohaline overlay |

---

## UI Controls

| Control | Description |
|---|---|
| **DiscretePlayer** | Play/pause/step/loop controls; `-`/`+` buttons adjust playback speed; loop policy radio (once / loop / reflect) |
| **Datetime label** | Shows the current timestamp above the player |
| **Color range (min, max)** | Enter two comma-separated values to fix the colour scale |
| **Colormap** | Dropdown to switch colormap |
| **Size** | Point radius or line width (hidden for polygon geometry) |
| **Show contours** | Toggle iso-value contour lines |
| **Contour levels** | Number of contour levels (2–30, shown when contours on) |
| **Contour smoothing** | Gaussian sigma for smoothing the contour raster (0=none) |
| **Contour level mode** | `nice` (round values), `linear` (equal spacing), `eq_hist` (quantile spacing) |
| **Label contours** | Toggle value labels on each contour level |
| **Show X2 line** | Toggle X2 isohaline (only shown if `x2_callback` was provided) |
| **X2 threshold** | EC threshold for X2 (µS/cm, shown when X2 on) |

**Hover tooltip** on channels: shows `Channel` (feature ID) and `Value` (current data value).  
**Hover tooltip** on contour lines: shows `Level` (the isovalue).

---

## Colormaps

The `colormap` parameter accepts any name from this curated list:

`viridis`, `plasma`, `inferno`, `magma`, `rainbow`, `coolwarm`, `RdBu_r`, `Blues`, `YlOrRd`, `turbo`

---

## Performance Tips

### Large tidefiles

Wrap any `SlicingReader` with `BufferedSlicingReader` to read HDF5 data in
200-step chunks instead of one step at a time:

```python
from dvue.animator import BufferedSlicingReader

reader = HydroH5FlowReader("tidefile.h5")
buffered = BufferedSlicingReader(reader, chunk_size=200)
mgr = GeoAnimatorManager(buffered, gdf, ...)
```

The `dsm2ui animate` CLI applies `BufferedSlicingReader` automatically.

### Simplify geometry

For complex channel centrelines (many survey vertices per channel), pass
`simplify_tolerance` in metres to `dsm2ui.animate.load_dsm2_channel_gdf()` or
via the CLI `--simplify` option.  50 m (default) removes redundant vertices
without visible quality loss.

### Initial colour scale

`vmin`/`vmax` are estimated from the first 20 time steps.  If your data has a
wide range not captured in those frames, set explicit values:

```bash
dsm2ui animate hydro tidefile.h5 --vmin 0 --vmax 50000
```

---

## DSM2 CLI Reference

### `dsm2ui animate hydro`

Animate a DSM2 HYDRO tidefile.

```
Usage: dsm2ui animate hydro [OPTIONS] H5FILE

Options:
  --variable [flow|stage|velocity]   Default: flow
  --location [both|upstream|downstream]  Default: both
  --port INTEGER          Web server port (0 = random)
  --desktop               Open in native window (requires pywebview)
  --shapefile FILE        Override bundled channel centreline GeoJSON
  --vmin FLOAT            Colour scale lower bound
  --vmax FLOAT            Colour scale upper bound
  --colormap NAME         Colormap (default: rainbow)
  --title TEXT            Map title
  --size FLOAT            Line width in pixels (default: 3.0)
  --simplify FLOAT        Geometry simplification tolerance in metres (default: 50)
  --log-level [debug|info|warning|error]  Default: warning
```

### `dsm2ui animate qual`

Animate a DSM2 QUAL or GTM tidefile.

```
Usage: dsm2ui animate qual [OPTIONS] H5FILE

Options:
  --constituent TEXT      Constituent name, e.g. ec (default: ec)
  --x2-threshold FLOAT    Enable X2 isohaline at this EC threshold (µS/cm)
  --port INTEGER
  --desktop
  --shapefile FILE
  --vmin FLOAT
  --vmax FLOAT
  --colormap NAME         (default: rainbow)
  --title TEXT
  --size FLOAT            (default: 3.0)
  --simplify FLOAT        (default: 50)
  --log-level [debug|info|warning|error]
```

**Example with X2 overlay:**

```bash
dsm2ui animate qual hist_qual_ec.h5 --constituent ec --x2-threshold 2700
```

---

## Extending: Custom SlicingReader

```python
from dvue.animator import SlicingReader
import pandas as pd
import numpy as np

class MyNetCDFReader(SlicingReader):
    def __init__(self, filepath):
        import xarray as xr
        self._ds = xr.open_dataset(filepath)
        self._var = self._ds["salinity"]  # (time, station)
        times = pd.DatetimeIndex(self._ds.time.values)
        self._ids = list(self._ds.station.values)
        # Sample first 20 steps for vmin/vmax
        sample = self._var.isel(time=slice(0, 20)).values
        self._vmin = float(np.nanmin(sample))
        self._vmax = float(np.nanmax(sample))
        super().__init__(times)

    @property
    def vmin(self): return self._vmin
    @property
    def vmax(self): return self._vmax

    def get_slice(self, timestamp):
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        vals = self._var.isel(time=i).values
        return pd.Series(vals, index=self._ids, dtype=float)

    def get_slice_range(self, start_idx, end_idx):
        block = self._var.isel(time=slice(start_idx, end_idx)).values
        return pd.DataFrame(block,
                            index=self._time_index[start_idx:end_idx],
                            columns=self._ids)
```

---

## Extending: Custom Isohaline (X2-like) Overlay

Any callable with signature `(step_idx: int, threshold: float) -> (xs, ys)` can
be passed as `x2_callback`.  `xs` and `ys` are lists of lists suitable for Bokeh
`multi_line`.

```python
def my_iso(step_idx, threshold):
    # compute crossing points as list of (x, y) tuples in EPSG:3857
    points = compute_crossings(step_idx, threshold)
    if len(points) < 2:
        return [], []
    return [[p[0] for p in points]], [[p[1] for p in points]]

mgr = GeoAnimatorManager(reader, gdf, x2_callback=my_iso)
```
