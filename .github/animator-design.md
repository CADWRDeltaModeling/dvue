# dvue.animator — Design Document

## Status: Implemented ✅

`dvue/animator/` provides a framework for animating spatial data over time on an
interactive geo map.  Data flows from a **SlicingReader** → **GeoAnimatorManager**
→ direct Bokeh `ColumnDataSource` patch.

---

## 1. Architecture Overview

```
SlicingReader  (ABC)
    │  get_slice(timestamp) → pd.Series(geo_id → float)
    │  get_slice_range(start, end) → pd.DataFrame
    │
    ├── InMemorySlicingReader   wraps pd.DataFrame
    ├── BufferedSlicingReader   chunks HDF5 reads (wraps any reader)
    ├── TransformedSlicingReader  resample / rolling / tidal filter
    └── DiffSlicingReader       A − B on a shared common time index
    │
    ▼
GeoAnimatorManager(pn.viewable.Viewer)          [single-reader]
    │  _base_reader (raw, never transformed)
    │  _reader      (current active reader, may be transformed + buffered)
    │  ColumnDataSource (xs/ys serialised once at init; _value patched per frame)
    │  LinearColorMapper (updated in-place on style changes)
    │  Bokeh figure + WMTSTileSource tiles
    │  Panel DiscretePlayer + DatetimePicker → _on_slider_change → _apply_frame
    ▼

MultiGeoAnimatorManager(pn.viewable.Viewer)     [two-reader, side-by-side or diff]
    │  reader_a, reader_b (+ optional gdf_a, gdf_b)
    │  Shared DiscretePlayer + DatetimePicker (common time index: intersection)
    │  Two Bokeh figures (fig_a, fig_b) — visible in normal mode
    │  One Bokeh figure (fig_diff) — visible in diff mode
    │  DiffSlicingReader constructed lazily on first diff activation
    ▼
Browser (pn.serve / panel serve)
```

---

## 2. SlicingReader ABC (`dvue/animator/reader.py`)

### Contract

```python
class SlicingReader(abc.ABC):
    time_index: pd.DatetimeIndex    # must have non-None freq (regular)
    freq: pd.DateOffset

    @property
    def vmin(self) -> float: ...    # global min across all time / geo_ids
    @property
    def vmax(self) -> float: ...

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series: ...
    def get_slice_nearest(self, dt) -> pd.Series: ...   # default impl
    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame: ...
```

### Rules

- **Regular time index required.** `InMemorySlicingReader` raises `ValueError` on
  irregular indices.  DSM2 HDF5 readers build `pd.date_range()` from the `start_time`
  and `interval` HDF5 attributes — always a regular series.
- **`get_slice_range` must be overridden for HDF5** so the buffer reads a whole
  chunk in one I/O call instead of looping over `get_slice`.
- **`vmin`/`vmax` are sampled** from the first 20 time steps to avoid loading the
  full dataset at startup.  Users can adjust via the UI colour-range input.

---

## 3. BufferedSlicingReader (`dvue/animator/reader.py`)

Wraps any `SlicingReader` and keeps `chunk_size` time steps in RAM.

```
cursor within buffer       → serve from RAM (no I/O)
cursor within margin band  → trigger get_slice_range() for new chunk
cursor outside buffer      → load chunk centred on cursor
```

- `chunk_size = 200` (default): reads 200 steps from HDF5 per refill.
- `refill_margin = 0.15`: refill fires when cursor is within 15% of either edge.
- `get_slice_range` is called for bulk loads; per-step reads use the in-memory buffer.

---

## 3b. TransformedSlicingReader (`dvue/animator/reader.py`)

Wraps any `SlicingReader` and applies a **time-domain transform** once on first
access, then caches the result.  The transform sees the full raw dataset, so
cross-boundary operations (rolling average warmup, tidal filter warmup) work
correctly.

```python
TransformedSlicingReader(
    inner: SlicingReader,
    transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
    warmup_steps: int = 0,
)
```

- Lazy: raw data is fetched from `inner.get_slice_range(0, N)` **once** on first
  `get_slice` / `time_index` / `vmin` / `vmax` call.
- Output `DatetimeIndex` may have a different `freq` and length (resampling) or
  the same length (rolling, filter).
- Leading NaN rows (filter warmup) are discarded according to `warmup_steps`.
- Works as inner reader for `BufferedSlicingReader`.

### Built-in transforms (in `dsm2ui.animate`)

| Factory | Effect |
|---|---|
| `make_resample_transform(freq, agg)` | `df.resample(freq).mean()` — coarser timestep |
| `make_moving_average_transform(window)` | Centred rolling mean, same timestep |
| `make_godin_transform()` | Godin tidal filter via vtools3 (requires `conda install vtools3`) |
| `apply_godin(inner)` | Convenience: wraps with Godin + correct warmup calculation |

### Composition pattern

```python
# Raw HDF5 → daily mean → buffered
BufferedSlicingReader(
    TransformedSlicingReader(raw_reader, make_resample_transform("D")),
    chunk_size=200,
)
```

### In-UI transform switching

`GeoAnimatorManager` stores `_base_reader` (raw) and exposes a `Transform` dropdown
when `transform_options: dict` is passed.  On change, `_setup_reader(name)` re-wraps
`_base_reader` → `TransformedSlicingReader` (if not "none") → `BufferedSlicingReader`.
The current timestamp is preserved: the nearest step in the new (possibly coarser)
time index is restored automatically.

---

## 3c. DiffSlicingReader (`dvue/animator/reader.py`)

Returns the element-wise difference `A − B` for every timestamp on a **common
time index** built from the two inner readers.

### Common index construction (`_build_common_index`)

```
1. Intersect time ranges: start = max(start_a, start_b), end = min(end_a, end_b)
2. Choose coarser frequency: max(freq_a, freq_b) in seconds
3. Build pd.date_range(start, end, freq=coarser)
4. Raise ValueError if intersection is empty
```

### `get_slice(timestamp)`

1. Find the nearest step on the common index.
2. Call `reader_a.get_slice_nearest(ts)` and `reader_b.get_slice_nearest(ts)`.
3. Align on the union of geo_ids (NaN-fill for IDs not in either reader).
4. Return `a_series - b_series`.

### `vmin` / `vmax`

Sampled from the first 20 diff steps, then made symmetric:
```python
abs_max = max(abs(vmin_sample), abs(vmax_sample))
vmin = -abs_max
vmax = +abs_max
```

This ensures diverging colormaps (e.g. `coolwarm`, `RdBu_r`) are centred at zero.

### Usage

```python
from dvue.animator import DiffSlicingReader, BufferedSlicingReader

diff = DiffSlicingReader(reader_a, reader_b)
buffered = BufferedSlicingReader(diff, chunk_size=200)
```

`DiffSlicingReader` **cannot** be further wrapped with `TransformedSlicingReader`
because its inner readers already carry independent transform chains.  Apply
transforms to `reader_a` and `reader_b` individually before constructing the diff.

---

## 4. GeoAnimatorManager (`dvue/animator/ui.py`)

### Geometry handling

| `_geom_type` | Detection | Bokeh glyph | xs/ys format |
|---|---|---|---|
| `"point"` | `Point`, `MultiPoint` | `scatter` | flat x/y lists |
| `"polygon"` | `Polygon`, `MultiPolygon` | `patches` | list-of-coord-lists |
| `"line"` | `LineString`, `MultiLineString` | `multi_line` | list-of-coord-lists |

**GDF must have a CRS.** Projected to EPSG:3857 once at `__init__`.

### Performance design

1. **Geometry serialised once** — `xs`, `ys`, `geo_id` columns in `_bk_source` are
   set at init and never re-sent.
2. **`_bk_source.patch({"_value": [...]})` per frame** — sends only the scalar
   value array over WebSocket.  For 500 DSM2 channels this is ~4 KB vs ~100 KB
   for a full glyph rebuild.
3. **`LinearColorMapper` mutated in-place** — palette/low/high updates do not
   trigger a Bokeh document rebuild.
4. **`Range1d` (not `DataRange1d`)** — viewport never auto-fits on data change.
5. **`match_aspect=True`** — geographic aspect ratio locked.
6. **`x_axis_type="mercator"` / `y_axis_type="mercator"` with `axis.visible=False`** —
   required for WMTS tile renderer projection; axes hidden.

### Widget → Bokeh update path

All Bokeh model mutations **must run under the document lock**.  Panel param watchers
do NOT hold the lock.  The pattern used throughout:

1. Watcher updates Panel widgets (no document interaction) and captures state.
2. All Bokeh mutations are deferred via `doc.add_next_tick_callback(fn)` — the
   IOLoop fires `fn` when the document lock is held.
3. In tests / Jupyter (no document) mutations run directly.

```
DiscretePlayer.value  →  _on_slider_change  (Panel watcher, no lock)
    │  syncs DatetimePicker (Panel)
    │  doc.add_next_tick_callback(λ: _apply_frame(idx, ts_str))
    │
    └→ _apply_frame(idx, ts_str)  (under document lock)
           _time_div.text = ts_str
           _load_frame(idx)
               _bk_source.patch(_value)
               _contour_source.data = ...    (if contours on)
               _contour_label_source.data =  (if labels on)
               _x2_source.data = ...         (if X2 on)
               _bk_figure.title.text = ...

vmin/vmax/colormap/size params  →  _on_style_change  (no lock)
    │  doc.add_next_tick_callback(λ: _apply_bokeh_style())
    └→ _apply_bokeh_style()  (under document lock)
           _bk_mapper.palette / .low / .high
           glyph.size / .line_width  (data renderer only)
           recompute contours + labels if visible

Transform dropdown  →  _on_transform_change  (Panel watcher, no lock)
    snapshot current_ts = time_index[current_step]
    _reader = _setup_reader(new_name)        # wraps _base_reader
    restore nearest step in new time index
    update DiscretePlayer options, DatetimePicker bounds (under _syncing guard)
    _apply_frame(nearest_idx, ...)           # Bokeh mutations direct (already scheduled)
```

---

## 4b. MultiGeoAnimatorManager (`dvue/animator/multi_ui.py`)

Hosts **two** `GeoAnimatorManager`-like Bokeh figures with a **single** shared
time control (DiscretePlayer + DatetimePicker).

### Modes

| Mode | Bokeh layout | Colour scale |
|---|---|---|
| Side-by-side | `pn.Row(fig_a, fig_b)` | Independent per reader (`vmin_a/vmax_a`, `vmin_b/vmax_b`) |
| Diff | `fig_diff` only (replaces both) | Symmetric around 0; diverging colourmap |

The active mode is controlled by a **"Show diff (A − B)"** checkbox param.
Switching is immediate — no page reload.

### Constructor

```python
MultiGeoAnimatorManager(
    reader_a, reader_b,
    gdf_a, gdf_b,              # may be the same GDF
    title_a="", title_b="",
    colormap="rainbow",
    diff_colormap="coolwarm",
    transform_options=None,    # dict applied to both readers identically
    initial_transform="none",
    buffer_chunk_size=200,
    map_height=500,
    size=3.0,
)
```

### Common time index

`MultiGeoAnimatorManager` builds the common time index itself:

```python
start = max(reader_a.time_index[0], reader_b.time_index[0])
end   = min(reader_a.time_index[-1], reader_b.time_index[-1])
freq  = max(reader_a.freq, reader_b.freq)   # coarser
common_index = pd.date_range(start, end, freq=freq)
```

The `DiscretePlayer` and `DatetimePicker` both use this `common_index`.
Per-frame, each figure calls `reader.get_slice_nearest(ts)` independently.

### Diff reader lifecycle

`_diff_reader_cache` holds the `DiffSlicingReader` (or `None`).  It is constructed
**lazily** on first "Show diff" activation:

```python
def _get_diff_reader(self):
    if self._diff_reader_cache is None:
        diff = DiffSlicingReader(self._reader_a, self._reader_b)
        self._diff_reader_cache = BufferedSlicingReader(diff, chunk_size=self.buffer_chunk_size)
    return self._diff_reader_cache
```

When the **Transform** dropdown changes, `_diff_reader_cache` is set to `None`
so the diff is re-built against the newly transformed readers.

### Transform application

```
_on_transform_change(name)
    reader_a = _setup_reader(_base_reader_a, name)   # TransformedSlicingReader → BufferedSlicingReader
    reader_b = _setup_reader(_base_reader_b, name)
    _diff_reader_cache = None                        # invalidate
    rebuild common_index
    restore nearest step
```

---

## 5. Contour overlay

Pipeline per frame:

```
channel values (Series)
    → scipy.interpolate.griddata(method="nearest")  →  200×N Voronoi raster
    → scipy.ndimage.gaussian_filter(sigma)          →  smoothed raster
    → _compute_levels(finite_vals, vmin, vmax)      →  level array
    → matplotlib.contour(levels)                    →  QuadContourSet
    → allsegs[i] / collections  (matplotlib version compat)
    → shapely LineString.intersection(clip_zone)    →  clipped paths
    → contour_source.data = {"xs": ..., "ys": ..., "level": ...}
    → _update_contour_labels(xs, ys, lvls)          →  label_source (if on)
```

### Level placement — `_compute_levels(finite_vals, vmin, vmax)`

| `contour_levels` | Algorithm | Notes |
|---|---|---|
| `"nice"` (default) | `matplotlib.ticker.MaxNLocator` | Round tick-like values (100, 500, 1000) — same as axis ticks |
| `"linear"` | `np.linspace(vmin, vmax, n+2)[1:-1]` | Equally spaced |
| `"eq_hist"` | `np.quantile(finite_vals, quantiles)` | Quantile-spaced; concentrates lines where data is dense |

### Contour labels

- One label per unique level: midpoint of the **longest path** for that level.
- Bokeh `text` glyph with `background_fill_color="white"`, `background_fill_alpha=0.6`.
- Toggled independently from the contour lines via the **Label contours** checkbox.
- `_update_contour_labels` is a no-op when the label renderer is not visible.

Other key decisions (unchanged):
- **`method="nearest"` (Voronoi)** — each grid cell takes the value of its nearest
  channel centroid.
- **Gaussian smoothing** — rounds blocky Voronoi edges.
- **Buffer clip zone** — `unary_union(geometries).buffer(10 × cell_size)` built once
  at init.
- **Dedicated `HoverTool`** restricted to `_contour_renderer` — shows `@level{0.3f}`.
- **Fixed `line_width=2`** — `_on_style_change` skips contour and X2 renderers.

---

## 6. X2 isohaline (`x2_callback`)

The X2 line is computed by an **optional callable** passed as `x2_callback` to
`GeoAnimatorManager`.  The manager itself is domain-agnostic.

```python
x2_callback(step_idx: int, threshold: float) -> (xs: list, ys: list)
```

`dsm2ui.animate.QualH5X2Callback` implements the DSM2-specific version:

1. Read upstream (loc 0) and downstream (loc 1) EC for the constituent at `step_idx`.
2. Find channels where EC crosses `threshold` between the two ends.
3. Linear interpolate: `norm = (threshold - u) / (d - u)` → `geometry.interpolate(norm, normalized=True)`.
4. Sort crossing points by easting (x) → form a single connected `multi_line` path.

X2 uses the **unsimplified** channel centrelines for geometric accuracy.  The
simplified GDF used for rendering may have too few vertices for accurate interpolation.

---

## 7. Subclassing guide

### Adding a new SlicingReader

```python
class MyReader(SlicingReader):
    def __init__(self, source):
        # 1. Open data source
        # 2. Build regular pd.DatetimeIndex
        # 3. Compute vmin/vmax (sample first N steps for large files)
        super().__init__(time_index)

    @property
    def vmin(self): return self._vmin
    @property
    def vmax(self): return self._vmax

    def get_slice(self, timestamp):
        # Return pd.Series(index=geo_ids, values=float)

    def get_slice_range(self, start_idx, end_idx):
        # Read contiguous block — one I/O call
        # Return pd.DataFrame(index=timestamps, columns=geo_ids)
```

### Adding a new X2-like overlay

Pass any callable to `x2_callback`:

```python
def my_isoline_callback(step_idx, threshold):
    # ... compute crossing points ...
    return [[x0, x1, ...]], [[y0, y1, ...]]

mgr = GeoAnimatorManager(reader, gdf, x2_callback=my_isoline_callback)
```

---

## 8. Known limitations / future work

- **Play/loop button** — now built into `DiscretePlayer` (Panel widget).
- **`DatetimePicker`** — bidirectional sync with DiscretePlayer; `_syncing` flag prevents loops.
- **Transform UI** — `Transform` dropdown in sidebar; switches between `none`, `Daily mean`,
  `Rolling 24 h`, `Rolling 14 D`, `Godin filter` without restarting the app.  Current
  timestamp is preserved across transform changes.
- **Shapefile `.shx` auto-restore** — `SHAPE_RESTORE_SHX=YES` GDAL env var set in
  `load_dsm2_channel_gdf` so missing `.shx` files are recreated automatically.
- **`--channel-id-column`** — CLI option to specify non-standard channel ID column names
  in shapefiles; detailed error message shows available columns with dtypes.
- **Multi-file comparison** — ✅ implemented: `MultiGeoAnimatorManager` + `DiffSlicingReader`;
  CLI `dsm2ui animate hydro A.h5 B.h5 [--diff]`.
- **Datashader** — for >10k features, wrap with `datashade()`.  Not in v1.
- **Irregular time index** — explicitly rejected.
- **GTM cell concentration** — not yet supported.
- **Independent colour scales in diff mode** — diff figure always uses symmetric scale
  centred at zero; a separate control for asymmetric diff ranges is not yet implemented.
