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
    chart_pane.loading = True          # immediate spinner over map
    transform_select.disabled = True   # prevent double-trigger
    → daemon thread: _setup_reader(name) + access time_index (slow cache fill)
        → doc.add_next_tick_callback(_apply)
            _reader = new_reader       # swap under document lock
            restore nearest step in new time index
            update DiscretePlayer options, DatetimePicker bounds
            _load_frame(nearest_idx)
            chart_pane.loading = False
            transform_select.disabled = False
```

### Controls layout — `pn.Card` collapsible sections

Widgets are grouped into `pn.Card` objects so the sidebar stays compact:

```
### Controls
[timestamp label]  [DiscretePlayer]  [DatetimePicker]
──────────
▼ Appearance   (open by default)
    clim input, colormap, size/line-width, Show channels, Show basemap
▶ Contours     (collapsed; auto-expands when Show contours ticked)
    Show contours □, n levels, smoothing sigma,
    level mode (nice/linear/eq_hist), custom levels TextInput,
    color contours □, label contours □
▶ Transform    (collapsed; only if transform_options provided)
    transform selector
▶ X2 isohaline (collapsed; only if x2_callback provided)
    Show X2 □, threshold input
```

`_contour_card.collapsed` is toggled programmatically when the Show-contours
checkbox changes — the card auto-expands on enable, auto-collapses on disable.

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
    # Immediately on watcher thread:
    pane_a.loading = True; pane_b.loading = True; pane_diff.loading = True
    transform_select.disabled = True
    → daemon thread:
        reader_a = _setup_reader(_base_reader_a, name)
        reader_b = _setup_reader(_base_reader_b, name)
        # accessing .time_index triggers lazy cache fill (slow step)
        _diff_reader_cache = None                # invalidate
        → doc.add_next_tick_callback(_apply)
            swap _reader_a, _reader_b
            rebuild player options + date-picker bounds
            _apply_frame(nearest_idx, ts_str)
            pane_*.loading = False
            transform_select.disabled = False
```

### Axis synchronisation

All three Bokeh figures (`fig_a`, `fig_b`, `fig_diff`) are constructed with the
**same `Range1d` pair** (`shared_x`, `shared_y`), computed from the union of both
GDFs' bounds.  Bokeh propagates pan / zoom events to every figure that shares the
same `Range1d` object — no extra callbacks required.

### Controls layout — `pn.Card` collapsible sections

```
### Controls
[timestamp label]  [DiscretePlayer]  [DatetimePicker]
──────────
▼ Appearance   (open)
    clim input, colormap, Show channels, Show basemap
▼ Diff (A − B) (open)
    Show diff □, diff colormap
▶ Contours     (collapsed; auto-expands when Show contours ticked)
    Show contours □ + all sub-controls (shared for all three figures)
▶ Transform    (collapsed; conditional)
```

### Contour overlays

`MultiGeoAnimatorManager` reuses the module-level helpers from `dvue.animator.ui`:
`_compute_contour_levels`, `_run_contour_computation`, `_clip_contour_segment`,
`_make_contour_grid`.  Each map has its own `_MapContour` state object (centroid
grid, `ColumnDataSource`, renderers).  The diff map reuses map A's centroid grid.

A single set of shared controls (level count, smoothing, mode, custom levels,
label toggle) drives all three maps simultaneously.

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

Now implemented as the module-level function
`_compute_contour_levels(finite_vals, vmin, vmax, n, mode, custom_levels)`
so both `GeoAnimatorManager` and `MultiGeoAnimatorManager` can call it without
duplication.

| Priority | Source | Algorithm |
|---|---|---|
| 1 | `contour_custom_levels` non-empty | Parse comma-separated floats; sort ascending; ignore count + mode |
| 2 | mode `"nice"` (default) | `matplotlib.ticker.MaxNLocator` — round tick-like values |
| 3 | mode `"linear"` | `np.linspace(vmin, vmax, n+2)[1:-1]` |
| 4 | mode `"eq_hist"` | `np.quantile(finite_vals, quantiles)` — concentrates lines where data is dense |

### Contour labels

- One label per unique level: midpoint of the **longest path** for that level.
- Bokeh `text` glyph with `background_fill_color="white"`, `background_fill_alpha=0.6`.
- Toggled independently from the contour lines via the **Label contours** checkbox.
- `_update_contour_labels` is a no-op when the label renderer is not visible.

### Module-level contour helpers

The contour pipeline was extracted to four module-level functions in `dvue.animator.ui`
so that `multi_ui.py` can reuse them without duplication:

| Function | Purpose |
|---|---|
| `_make_contour_grid(gdf_proj, geom_type)` | Build centroids, 200×N raster grid, and Shapely buffer clip zone |
| `_compute_contour_levels(vals, vmin, vmax, n, mode, custom)` | Return sorted level array (custom > nice > linear > eq_hist) |
| `_run_contour_computation(vals, cx, cy, gx, gy, sigma, levels, clip)` | Full rasterize → smooth → contour → clip pipeline |
| `_clip_contour_segment(seg, lvl, clip_zone, xs, ys, lvl_out)` | Clip one path segment to the buffer zone |

`GeoAnimatorManager`'s instance methods delegate to these functions.
`MultiGeoAnimatorManager` imports them directly.

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

- **Play/loop button** — built into `DiscretePlayer` (Panel widget). ✅
- **`DatetimePicker`** — bidirectional sync with DiscretePlayer; `_syncing` flag prevents loops. ✅
- **Transform UI with loading spinner** — `Transform` card in sidebar; background thread computes cache; spinner overlay on map while loading; timestamp preserved. ✅
- **Custom contour levels** — `contour_custom_levels` TextInput accepts comma-separated values; overrides auto count/mode. ✅
- **Collapsible controls (`pn.Card`)** — Appearance always open; Contours, Transform, X2 collapsed by default; Contours card auto-expands when toggled on. ✅
- **Shapefile `.shx` auto-restore** — `SHAPE_RESTORE_SHX=YES` set in `load_dsm2_channel_gdf`. ✅
- **`--channel-id-column`** — CLI option; detailed error message with available columns + dtypes. ✅
- **Wrong HDF5 file type** — `_DSM2BaseH5Reader` raises `ValueError` with a targeted hint ("try 'dsm2ui animate qual'") when the requested dataset path does not exist. ✅
- **Invalid shapefile geometry** — rows with non-finite coordinates are dropped (with warning) before `.simplify()`. ✅
- **Multi-file comparison** — `MultiGeoAnimatorManager` + `DiffSlicingReader`; shared viewport Range1d; shared contour controls; Show channels/basemap; CLI `dsm2ui animate hydro A.h5 B.h5 [--diff]`. ✅
- **Datashader** — for >10k features, wrap with `datashade()`.  Not in v1.
- **Irregular time index** — explicitly rejected.
- **GTM cell concentration** — not yet supported.
- **Independent colour scales in diff mode** — diff always uses symmetric scale centred at zero.
