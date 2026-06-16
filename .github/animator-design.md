# dvue.animator — Design Document

## Status: Implemented ✅

`dvue/animator/` provides a framework for animating spatial data over time on an
interactive geo map.  Data flows from a **SlicingReader** → **GeoAnimatorManager**
→ direct Bokeh `ColumnDataSource` patch.

---

## 1. Architecture Overview

```
SlicingReader
    │  get_slice(timestamp) → pd.Series(geo_id → float)
    │  get_slice_range(start, end) → pd.DataFrame
    │
    ▼
BufferedSlicingReader   (wraps any SlicingReader; chunks HDF5 reads)
    │
    ▼
GeoAnimatorManager(pn.viewable.Viewer)
    │  ColumnDataSource (xs/ys serialised once at init; _value patched per frame)
    │  LinearColorMapper (updated in-place on style changes)
    │  Bokeh figure + WMTSTileSource tiles
    │  Panel DiscretePlayer → _on_slider_change → _load_frame
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

All callbacks follow the rule: **only Bokeh document mutations inside frame callbacks,
no Panel widget or param mutations** (which trigger layout reflow and reset the
viewport).

```
DiscretePlayer.value  →  _on_slider_change
    │  updates Div.text (Bokeh model, no Panel reflow)
    │  calls _load_frame(idx)
    │      _bk_source.patch(_value)
    │      _contour_source.data = ...              (if contours on)
    │      _update_contour_labels(xs, ys, lvls)   (if labels on)
    │      _x2_source.data = ...                  (if x2 on)
    │      _bk_figure.title.text = ...

vmin/vmax/colormap/size params  →  _on_style_change
    │  _bk_mapper.palette / .low / .high
    │  glyph.size / .line_width  (data renderer only, skip contour/x2)
    │  recompute contours + labels if visible
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

- **Play/loop button** — not yet implemented.  The IntSlider can be driven
  programmatically by setting `time_slider.value` from a `pn.state.add_periodic_callback`.
- **Datashader** — for >10k features, wrap the data glyphs with
  `holoviews.operation.datashader.datashade()`.  Not implemented in v1.
- **Irregular time index** — explicitly rejected.  Callers must resample to a regular
  frequency before constructing a SlicingReader.
- **GTM cell concentration** (`/output/cell concentration`) — a separate, denser
  dataset.  Not yet supported; `QualH5ConcentrationReader` uses channel averages.
