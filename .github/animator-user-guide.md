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
# Single file
dsm2ui animate hydro path/to/tidefile.h5
dsm2ui animate qual  path/to/qual_ec.h5 --constituent ec

# Two files — side-by-side
dsm2ui animate hydro study_a.h5 study_b.h5

# Two files — difference map (A − B)
dsm2ui animate hydro study_a.h5 study_b.h5 --diff
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
| `size` | `6.0` | Point radius (px) or line width (px) |
| `map_height` | `500` | Minimum map height (px) |
| `x2_callback` | `None` | Optional callable for an isohaline overlay |
| `transform_options` | `None` | Dict of `{"label": transform_fn}` shown in the Transform dropdown |
| `initial_transform` | `"none"` | Which transform to apply on startup |
| `buffer_chunk_size` | `200` | HDF5 read chunk size for `BufferedSlicingReader` |

---

## UI Controls

Controls are organised into **collapsible `pn.Card` sections** so the sidebar
stays compact.  Time controls are always visible above the sections.

### Always visible — Time

| Control | Description |
|---|---|
| **Timestamp label** | Current date/time shown above the player |
| **DiscretePlayer** | Play/pause/step/speed/loop controls |
| **Go to date/time** | Jump to any timestamp (snaps to nearest step) |

### Appearance card (open by default)

| Control | Description |
|---|---|
| **Color range (min, max)** | Two comma-separated values to fix the colour scale |
| **Colormap** | Dropdown to switch colormap |
| **Size / Line width** | Point radius or line width in pixels |
| **Show channels** | Toggle the channel data renderer on/off |
| **Show background map** | Toggle the WMTS tile basemap on/off |

### Contours card (collapsed; expands automatically when enabled)

| Control | Description |
|---|---|
| **Show contours** | Toggle iso-value contour lines |
| **Contour levels** | Number of levels when using auto mode (2–30) |
| **Contour smoothing** | Gaussian sigma applied to the raster before contouring (0 = none) |
| **Contour level mode** | `nice` (round values), `linear` (equal spacing), `eq_hist` (quantile spacing) |
| **Custom levels** | Comma-separated explicit levels, e.g. `500, 1000, 2000` — overrides count and mode when non-empty |
| **Color contours** | Colour lines using the active colormap (vs plain black) |
| **Label contours** | Show value labels on each level |

### Transform card (collapsed; shown only if transform options are provided)

| Control | Description |
|---|---|
| **Transform** | `none` / `Daily mean` / `Rolling 24 h` / `Rolling 14 D` / `Godin filter` |

### X2 isohaline card (collapsed; shown only if `x2_callback` was provided)

| Control | Description |
|---|---|
| **Show X2 line** | Toggle the X2 isohaline overlay |
| **X2 threshold** | EC threshold in µS/cm |

**Hover tooltip** on channels: shows `Channel` (feature ID) and `Value` (current data value).  
**Hover tooltip** on contour lines: shows `Level` (the isovalue).

---

## Time-Domain Transforms

The **Transform** dropdown in the controls panel switches between representations
of the same data without restarting the app.  The current playback position is
preserved (snapped to the nearest available timestamp in the new series).

| Transform | Effect | Timestep change? |
|---|---|---|
| `none` (default) | Raw data | No |
| `Daily mean` | `resample("D").mean()` | Yes — coarser (1 day) |
| `Rolling 24 h` | 24 h centred rolling mean | No |
| `Rolling 14 D` | 14-day centred rolling mean | No |
| `Godin filter` | Godin tidal filter (requires vtools3) | No |

> **Loading indicator** — Godin and rolling transforms require loading the full HDF5
> dataset before the first frame can be shown.  A **spinner overlay** appears on the
> map while the cache is being built, and the Transform selector is greyed out to
> prevent double-triggers.  The spinner clears automatically once the first frame is
> ready.

### Custom contour levels

Type explicit levels in the **Custom levels** text box to bypass the automatic
algorithm entirely:

```
Custom levels:  500, 1000, 2000, 3000
```

- Values are parsed as floats, sorted ascending, and used exactly as typed.
- The level count slider and mode selector are greyed out while custom levels are active.
- Clear the box to return to automatic level placement.
- Levels outside the current data range produce no visible line (normal contour behaviour).

### Applying transforms programmatically

All transform factories return a `TransformSpec` which is used automatically by
`GeoAnimatorManager._setup_reader()`.  You can also use it directly:

```python
from dsm2ui.animate import (
    HydroH5FlowReader,
    make_resample_transform,
    make_moving_average_transform,
    apply_godin,
)
from dvue.animator import StreamingTransformedSlicingReader, BufferedSlicingReader

raw = HydroH5FlowReader("hist_fc_mss.h5")

# Daily average (aggregate: no overlap needed; coarser time index)
daily_spec = make_resample_transform("D")
daily = StreamingTransformedSlicingReader(raw, daily_spec)

# 14-day rolling mean (convolution: 168-step overlap at 1-h data)
rolling_spec = make_moving_average_transform("14D")
rolling = StreamingTransformedSlicingReader(raw, rolling_spec)

# Godin tidal filter — convenience wrapper handles overlap automatically
godin = apply_godin(raw)    # returns StreamingTransformedSlicingReader

# Always buffer the output for smooth playback
buffered = BufferedSlicingReader(godin, chunk_size=200)
mgr = GeoAnimatorManager(buffered, gdf, title="Tidally filtered flow")
```

> **Large files** — `StreamingTransformedSlicingReader` never loads the
> full dataset.  For a 100-year hourly study (876 k steps × 519 channels),
> startup is near-instant: `time_index` is derived from metadata alone, and
> `vmin`/`vmax` are estimated from 200 frames at the centre of the file.
> Each 200-frame animation buffer refill reads ≈200–270 raw steps from HDF5.

> **Legacy callable** — passing a bare `callable` (not a `TransformSpec`)
> to `TransformedSlicingReader` still works; it loads the full dataset on
> first access.  Use `TransformSpec` for any new code.

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

## Multi-File Comparison

Two tidefiles can be compared side-by-side or as a difference map without writing
any Python — both modes are accessible from the CLI and the Python API.

### Side-by-side

```python
from dsm2ui.animate import animate_hydro_multi

mgr = animate_hydro_multi(
    "study_a.h5", "study_b.h5",
    variable="flow",
    title_a="Calibration", title_b="Alternative",
)
mgr.servable()
```

The `MultiGeoAnimatorManager` renders two maps next to each other with a shared
`DiscretePlayer` and `DatetimePicker`.  All controls (colormap, size, transform)
apply to both maps simultaneously.

### Difference map

Pass `show_diff=True` (Python) or `--diff` (CLI) to show `A − B` in a single map
using a **diverging colourmap** (default `coolwarm`) centred on zero.  A **"Show
diff (A − B)"** checkbox in the UI lets you toggle between side-by-side and diff
mode at runtime.

```python
mgr = animate_hydro_multi(
    "study_a.h5", "study_b.h5",
    variable="flow",
    show_diff=True,
    diff_colormap="coolwarm",
)
```

### DiffSlicingReader (low-level)

Use `DiffSlicingReader` directly when you need a diff data source without the UI:

```python
from dvue.animator import DiffSlicingReader, BufferedSlicingReader

diff = DiffSlicingReader(reader_a, reader_b)
# vmin/vmax are symmetric around 0 (auto-estimated)

buffered = BufferedSlicingReader(diff, chunk_size=200)

# Use like any other reader
frame = buffered.get_slice(pd.Timestamp("2016-01-15"))
```

**Mismatched time indices** are handled automatically:
- The common time index is the intersection of both readers' time ranges.
- The coarser of the two frequencies is used.
- A `ValueError` is raised if there is no temporal overlap.

### MultiGeoAnimatorManager parameters

| Parameter | Default | Description |
|---|---|---|
| `reader_a`, `reader_b` | required | Two `SlicingReader` instances |
| `gdf_a`, `gdf_b` | required | GeoDataFrames (may be the same object) |
| `title_a`, `title_b` | `""` | Per-map title |
| `colormap` | `"rainbow"` | Applied to both side-by-side maps |
| `diff_colormap` | `"coolwarm"` | Applied when diff mode is active |
| `show_diff` | `False` | Start in diff mode |
| `transform_options` | `None` | Same dict as `GeoAnimatorManager`; applied to both |
| `initial_transform` | `"none"` | Transform applied on startup |
| `buffer_chunk_size` | `200` | Chunk size for buffered readers |
| `map_height` | `500` | Map height in pixels |
| `size` | `3.0` | Line width in pixels |

Both maps share a single set of time controls and a **linked viewport** — pan or
zoom on one map and the other follows instantly (including the diff map).

A single set of contour controls (level count, smoothing, mode, custom levels)
applies to all three figures (map A, map B, diff map) simultaneously.

---

## DSM2 CLI Reference

### `dsm2ui animate hydro`

Animate 1 or 2 DSM2 HYDRO tidefiles.  With 2 files: side-by-side or `--diff`.

```
Usage: dsm2ui animate hydro [OPTIONS] H5FILES...

Options:
  --variable [flow|stage|velocity]         Default: flow
  --location [both|upstream|downstream]    Default: both
  --diff                                   Show diff map (A − B) instead of
                                           side-by-side (only with 2 files)
  --transform [none|daily|rolling-24h|rolling-14d|godin]
                                           Default: none
  --port INTEGER          Web server port (0 = random)
  --desktop               Open in native window (requires pywebview)
  --shapefile FILE        Override bundled channel centreline GeoJSON
                          (repeat for two shapefiles)
  --channel-id-column TEXT  Column in shapefile holding channel numbers
  --vmin FLOAT            Colour scale lower bound
  --vmax FLOAT            Colour scale upper bound
  --colormap NAME         Colormap (default: rainbow)
  --title TEXT            Map title
  --size FLOAT            Line width in pixels (default: 3.0)
  --simplify FLOAT        Geometry simplification tolerance in metres (default: 50)
  --log-level [debug|info|warning|error]  Default: warning
```

### `dsm2ui animate qual`

Animate a DSM2 QUAL or GTM tidefile (single file only; multi-file via Python API).

```
Usage: dsm2ui animate qual [OPTIONS] H5FILE

Options:
  --constituent TEXT      Constituent name, e.g. ec (default: ec)
  --x2-threshold FLOAT    Enable X2 isohaline at this EC threshold (µS/cm)
  --transform [none|daily|rolling-24h|rolling-14d|godin]
                                           Default: none
  --port INTEGER
  --desktop
  --shapefile FILE
  --channel-id-column TEXT  Column in shapefile holding channel numbers
  --vmin FLOAT
  --vmax FLOAT
  --colormap NAME         (default: rainbow)
  --title TEXT
  --size FLOAT            (default: 3.0)
  --simplify FLOAT        (default: 50)
  --log-level [debug|info|warning|error]
```

**Examples:**

```bash
# Tidally filtered EC with X2 overlay
dsm2ui animate qual hist_qual_ec.h5 --constituent ec \
    --transform godin --x2-threshold 2700

# Daily-average flow
dsm2ui animate hydro hist_fc_mss.h5 --variable flow --transform daily

# Custom shapefile with non-standard channel ID column
dsm2ui animate qual hist_qual_ec.h5 \
    --shapefile my_channels.shp --channel-id-column chan_no

# Two HYDRO files side-by-side
dsm2ui animate hydro study_a.h5 study_b.h5

# Two HYDRO files — difference map (A − B), with Godin filter
dsm2ui animate hydro study_a.h5 study_b.h5 --diff --transform godin

# Two files with different shapefiles
dsm2ui animate hydro study_a.h5 study_b.h5 \
    --shapefile net_a.shp --shapefile net_b.shp --diff
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
