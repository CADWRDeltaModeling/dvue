---
marp: true
theme: default
paginate: true
style: |
  section {
    font-size: 21px;
    background: #ffffff;
    color: #212121;
  }
  section.title {
    background: #1565C0;
    color: #ffffff;
    text-align: center;
    justify-content: center;
  }
  section.title h1 {
    font-size: 2.4em;
    color: #ffffff;
    margin-bottom: 0.2em;
    border: none;
  }
  section.title h2 {
    color: #BBDEFB;
    font-weight: 300;
    font-size: 1.1em;
  }
  section.title h3 {
    color: #90CAF9;
    font-weight: 300;
    font-size: 0.95em;
  }
  section.title strong {
    color: #ffffff;
  }
  section.section-header {
    background: #0097A7;
    color: #ffffff;
    text-align: center;
    justify-content: center;
  }
  section.section-header h1 {
    font-size: 2.2em;
    color: #ffffff;
    border: none;
  }
  h1 { color: #1565C0; border-bottom: 2px solid #0097A7; padding-bottom: 0.15em; margin-bottom: 0.3em; font-size: 1.4em; }
  h2 { color: #1565C0; font-size: 1.15em; }
  h3 { color: #00838F; font-size: 1.0em; margin: 0.3em 0; }
  p { margin: 0.3em 0; }
  ul, ol { margin: 0.2em 0; padding-left: 1.4em; }
  li { margin: 0.15em 0; }
  code { background: #E3F2FD; border-radius: 4px; padding: 1px 5px; color: #0D47A1; font-size: 0.85em; }
  pre { background: #F5F5F5; color: #1A237E; padding: 0.7em 1em; border-radius: 6px; font-size: 0.62em; margin: 0.4em 0; border: 1px solid #BDBDBD; }
  pre code { background: transparent; color: inherit; padding: 0; font-size: 1em; }
  table { font-size: 0.75em; width: 100%; border-collapse: collapse; }
  th { background: #1565C0; color: #ffffff; padding: 4px 8px; }
  td { padding: 3px 8px; border-bottom: 1px solid #E0E0E0; }
  tr:nth-child(even) td { background: #E3F2FD; }
  .columns { display: grid; grid-template-columns: 1fr 1fr; gap: 1em; }
  .highlight { background: #E0F7FA; border-left: 4px solid #0097A7; padding: 0.4em 0.8em; border-radius: 0 6px 6px 0; color: #00363a; margin: 0.3em 0; }
  footer { font-size: 0.7em; color: #888; }
---

<!-- _class: title -->

# dvue

## Domain-Agnostic UI/Data-Catalog Framework  
### for HoloViz Panel + HoloViews Dashboards

---
**May 2026**

---

# Agenda

<div class="columns">
<div>

### Presentation (15 min)
1. The Problem
2. What is dvue?
3. **The Killer App** — Unified Multi-Source Catalog
4. Architecture Overview
5. DataReference & DataCatalog
6. DataUIManager Hierarchy
7. Time-Series Transforms
8. Math References & Expressions
9. Transform → Catalog
10. Actions & Plugin System
11. Named Views & Sessions
12. **Geo-Animation Framework**
13. Downstream Adoption

</div>
<div>

### Demo (15 min)
1. **Unified multi-source catalog** — DSS + HDF5 + datastore + CDEC in one table
2. Geo map + bidirectional selection
3. Transform sidebar
4. Math Ref — model vs observed expression
5. Transform → Catalog
6. YAML round-trip
7. `dvue ui` drag-and-drop + `dvue diagnose`

</div>
</div>

---

# The Problem

Building interactive water-quality dashboards for DSM2, SCHISM, and datastore data used to mean writing the **same boilerplate over and over**:

<div class="columns">
<div>

### Each project wrote its own:
- Tabulator widget + table columns
- Map with bidirectional selection
- Time-range picker
- Plot tabs (non-blocking)
- Download buttons
- Cache management
- Session state

</div>
<div>

### Resulting in:
- 1000-line UI files per project
- Logic duplicated across schismviz, dsm2ui, dms_datastore_ui
- Bugs fixed in one place, not others
- Hard to add features consistently
- No reuse across domains

</div>
</div>

<div class="highlight">

**dvue** extracts this into a single reusable framework — downstream apps implement only domain logic.

</div>

---

# What is dvue?

<div class="highlight">

**dvue** is a **domain-agnostic, reusable UI/data-catalog framework** for Panel + HoloViews dashboards.

</div>

It provides:

- � **Unified multi-source catalog** — DSS + HDF5 + SCHISM + datastore + CDEC in one table/map
- 📦 **DataReference + DataCatalog** — lazy, cached, searchable data index
- 🖥️ **DataUIManager hierarchy** — Panel app skeleton (table, map, actions, tabs)
- 📈 **TimeSeriesDataUIManager** — full time-series transforms, multi-subplot plotting
- ➕ **MathDataReference** — derived series via safe Python expressions (model vs obs, unit conversions)
- 🔌 **ReaderRegistry** — plugin system for new file types
- 💾 **Session persistence** — survive browser refresh & server restarts
- 🗂️ **Named Views** — curated station subsets
- 🖥️ **Desktop mode** — native window via `pywebview`

---

# Architecture Overview

```
  HEC-DSS     HDF5      SCHISM    DMS         CDEC
  (.dss)      (.h5)     netCDF    datastore   REST API
     ↓          ↓          ↓         ↓           ↓
  DssReader  HydroH5   SchismRdr  DSReader  CdecReader   ← domain plugins
     └──────────┴──────────┴─────────┴───────────┘
                          ↓
              DataCatalog (primary_key=["source_num","station","variable"])
              (one unified table — all sources, all formats)
                          ↓
              DataUIManager → TimeSeriesDataUIManager
              (table, geo map, action bar, plot tabs)
                          ↓
              MathDataReference  (model−obs, unit conversions, sums)
```

**One catalog → every source → one UI.** Each reader plugin is ~100 lines of domain logic.

---

<!-- _class: section-header -->

# The Killer App: Unified Multi-Source Catalog

---

# One Catalog — Every Data Source

<div class="highlight">

DWR hydrologists work with data from **5+ incompatible formats** daily.  
dvue's `DataCatalog` unifies them into a **single searchable table and map**.

</div>

<div class="columns">
<div>

### Sources in production today:

| Source | Format | Package |
|--------|--------|---------|
| DSM2 model output | HEC-DSS `.dss` | `dsm2ui` |
| DSM2 tidefile | HDF5 `.h5` | `dsm2ui` |
| SCHISM model output | netCDF `.nc` | `schismviz` |
| DMS datastore | CSV inventory | `dms_datastore_ui` |
| CDEC stations | REST API | `cdec_maps` |
| CalSim | HEC-DSS `.dss` | `dsm2ui` |

</div>
<div>

### What you get in one table:

```
source_num  station    variable  unit    source
0           RSAC075    EC        uS/cm   dsm2_qual.dss
0           RSAC075    FLOW      cfs     dsm2_qual.dss
1           RSAC075    EC        uS/cm   observed.dss
2           RSAC075    EC        uS/cm   CDEC API
3           RSAC075    salinity  ppt     schism_out.nc
```

- Filter by station, variable, source
- Click any row → plot
- Select model + obs → one plot
- Click map point → auto-filter table

</div>
</div>

---

# Multi-Source Catalog: How It Works

`source_num` auto-assigned per unique `source` — no configuration needed:

```python
catalog = DataCatalog(primary_key=["source_num", "station", "variable"])

for ref in DsmDssReader.scan("dsm2_qual.dss"):        # source_num=0
    catalog.add(ref)
for ref in DsmDssReader.scan("observed_ec.dss"):       # source_num=1
    catalog.add(ref)
for ref in SchismReader.scan("schism_output/"):        # source_num=2
    catalog.add(ref)
for ref in DatastoreCatalogBuilder.build("screened/"): # source_num=3
    catalog.add(ref)
```

- Visual encoding is **automatic**: same station = same color, different source = different line style
- Every row is selectable, plottable, downloadable — regardless of origin format

---

# Multi-Source Catalog: Model vs Observed

`search_map` lets math expressions span sources transparently:

```python
MathDataReference(
    name="RSAC075_model_minus_obs",
    expression="model - obs",
    search_map={
        "model": {"station": "RSAC075", "variable": "EC", "source_num": 0},
        "obs":   {"station": "RSAC075", "variable": "EC", "source_num": 1}
    }
)
```

This works whether `source_num=0` is a DSS file and `source_num=1` is a CDEC REST call — the expression doesn't know or care.

### Three variable resolution strategies:
| Priority | Method | Use case |
|---|---|---|
| 1 | `variable_map` | Direct ref binding (TransformToCatalogAction) |
| 2 | `search_map` | Attribute criteria — portable across renames |
| 3 | Catalog name lookup | Token matches a ref `name` directly |

---

# `dvue ui` — Drop Any File, Get the Full UI

```bash
dvue ui dsm2_qual.dss observed_ec.dss schism_output/ screened/
dvue ui dsm2_dss:dsm2_qual.dss obs_dss:observed_ec.dss  # explicit reader
dvue ui --desktop dsm2_qual.dss observed_ec.dss          # native window
```

<div class="columns">
<div>

### What happens:
1. Each path → `ReaderClass.scan()` → `[DataReference]`
2. `normalize_ref()` maps attrs → `station`/`variable`
3. `source_num` auto-assigned per source path
4. `time_range` from `time_extent_start/end`
5. Table + map appear immediately

</div>
<div>

### Visual encoding:
- **Color** by `station`
- **Line style** by `source`
- **Marker** by `variable`

Model vs observed: **same color, different dash** — instantly readable without a legend lookup.

</div>
</div>

---

<!-- _class: section-header -->

# Core: DataReference & DataCatalog

---

# DataReference — Lazy Data Pointer

A **`DataReference`** is a lazy, cached pointer to a single time series.

```python
ref = DataReference(
    name="sac_flow", station="Sacramento",
    variable="flow", unit="cfs", source="./data/flow.csv"
)
ts = ref.getData(time_range=("2020-01-01", "2021-01-01"))
```

### Three built-in reader types:

| Reader | Use case |
|---|---|
| `InMemoryDataReferenceReader` | DataFrame already in memory |
| `CallableDataReferenceReader` | Zero-arg lambda / async fetch |
| `FileDataReferenceReader` | CSV / Parquet / HDF5 / XLSX (local or HTTPS) |

- **Per–time-range caching** — each window cached independently
- **Arithmetic overloading** — `ref_a + ref_b * 2` → `MathDataReference`

---

# DataCatalog — Central Index

```python
from dvue.catalog import DataCatalog

catalog = DataCatalog(primary_key=["station", "variable"])
catalog.add(ref_a)
catalog.add(ref_b)

# Flexible lookup
ref = catalog.get("sac_flow")
ref = catalog.get(station="Sacramento", variable="flow")

# Regex search
subset = catalog.search(variable="EC", station="~RSAC.*")

# Safe atomic rename
catalog.rename("sac_flow", "sacramento_flow")

# Clear all caches (wired to UI button)
catalog.invalidate_all_caches()
```

### Multi-source: `source_num` auto-assigned

```python
catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
# Each unique source path → source_num 0, 1, 2…
# Exposed in to_dataframe(); drives color/dash/marker encoding
# search_map targets {"source_num": 0} for model, 1 for observed
```

---

<!-- _class: section-header -->

# DataUIManager Hierarchy

---

# Three-Layer Class Hierarchy

```
DataProvider                        ← headless / notebook use
  └── DataUIManager                 ← + Panel layout, table, map, actions
        └── TimeSeriesDataUIManager ← + time transforms, HoloViews plots
              └── RegistryUIManager ← + file-type plugin dispatch
```

### To build a domain dashboard, subclass and fill in the gaps:

```python
class MyDashboard(TimeSeriesDataUIManager):

    def _build_catalog(self):
        self._dvue_catalog = DataCatalog(primary_key=["station", "variable"])
        for station in self.stations:
            self._dvue_catalog.add(MyDataReference(station))

    @property
    def data_catalog(self):
        return self._dvue_catalog
```

**That's it.** Table, map, plot tabs, download, cache, transforms — all inherited.

---

# What You Get For Free

<div class="columns">
<div>

### UI Layout
- Sidebar (filters, transforms, views)
- Main area (tabbed plots/tables)
- Action toolbar (buttons)
- Progress bar (threaded loading)
- Geo map (if `GeoDataFrame`)

</div>
<div>

### Data Operations
- Bidirectional map ↔ table selection
- Multi-station time-series plots
- Grouped subplots by unit
- Download data + catalog (CSV)
- Session persistence

</div>
</div>

```python
# Start the app in one line
app = MyDashboard()
app.create_view("My Dashboard").show()
```

---

<!-- _class: section-header -->

# Time-Series Transforms

---

# TimeSeriesDataUIManager — Transform Sidebar

All transforms are `param.Parameterized` — each drives a reactive widget:

| Transform | Widget | Example |
|---|---|---|
| Time range | DatetimeRangeInput | `2020-01-01 → 2022-01-01` |
| Fill gaps | IntInput | forward-fill ≤ N periods |
| **Tidal filter** | Checkbox | cosine-Lanczos 40-hr low-pass |
| Resample | Text + Select | `1D` mean/max/min/sum |
| Rolling window | Text + Select | `24H` mean/std |
| Differencing | Checkbox + Int | period-over-period |
| Cumsum | Checkbox | running total |
| Scale factor | FloatInput | `× 0.0283168` (cfs → m³/s) |
| Color/line encoding | Select | by station, variable, source |
| Y-axis clip | Checkbox | percentile range |

### No extra code needed — transforms apply to every plot action automatically.

---

<!-- _class: section-header -->

# Math References & Expressions

---

# MathDataReference — Derived Series

```python
# Operator overloading — instant derived ref
net_flow = catalog.get("inflow") - catalog.get("outflow")

# Named expression with variable_map (direct binding)
unit_conv = MathDataReference(
    name="sac_cms", expression="sac_cfs * 0.0283168",
    variable_map={"sac_cfs": catalog.get("sac_flow")},
    station="Sacramento", variable="flow", unit="m³/s"
)

# search_map — resolves at load time, portable across renames
comparison = MathDataReference(
    name="model_minus_obs", expression="model - obs",
    search_map={"model": {"variable": "EC", "source_num": 0},
                "obs":   {"variable": "EC", "source_num": 1}}
)
```

---

# Safe Expression Namespace

The expression evaluator uses a **curated namespace** — no `builtins`, no `exec`:

<div class="columns">
<div>

**Available:**
- NumPy ufuncs: `sin`, `cos`, `exp`, `sqrt`, `clip`, `where`, `cumsum`, `diff`, …
- Aggregates: `min`, `max`, `mean`, `std`, `sum`
- Constants: `pi`, `e`, `nan`, `inf`
- Libraries: `np`, `pd`, `math`
- vtools3: `cosine_lanczos`, `godin`, `butterworth`, `lanczos`

</div>
<div>

**Common patterns:**
```python
# Unit conversion
"flow_cfs * 0.0283168"

# Station difference
"upstream - downstream"

# Tidal filtered
"cosine_lanczos(stage, 40, 'H')"

# Clipped to range
"clip(ec, 0, 5000)"

# Running total
"cumsum(inflow - outflow)"
```

</div>
</div>

**YAML persistence** — math refs save/load as portable `.yaml` files.

---

# MathRefEditorAction — Live In-Browser Editor

Click the **Math Ref** button in the toolbar to open the editor:

```
┌──────────────────────────────────────────────────┐
│  Name:        sac_cms                             │
│  Expression:  sac_cfs * 0.0283168                 │
│  Attributes:  station: Sacramento                 │
│               variable: flow                      │
│               unit: m³/s                          │
│  Search Map:  sac_cfs: variable=flow, source_num=0│
│                                                   │
│  [Save]  [Cancel]                                 │
│                                                   │
│  ┌ Upload YAML ┐  ┌ Download YAML ┐              │
└──────────────────────────────────────────────────┘
```

- **Save** → adds/updates ref in live catalog → table refreshes instantly
- **Upload YAML** → merge refs from file
- **Download YAML** → export all math refs for reproducibility

---

<!-- _class: section-header -->

# Transform → Catalog

---

# TransformToCatalogAction

**One click** turns the active transform settings into a permanent catalog entry:

```
Selected: RSAC075 (EC)
Transforms: Resample=1D mean, Tidal filter=ON

   [Transform → Ref]

New row added: RSAC075__tf__1D_mean
```

### Auto-generated name tags:

| Transform | Tag |
|---|---|
| Tidal filter | `tf` |
| Resample 1D mean | `1D_mean` |
| Rolling 24H mean | `r24H_mean` |
| Diff N periods | `diffN` |
| Cumsum | `cumsum` |
| Scale ×2 | `x2.0` |

The new ref **uses `variable_map` binding** — fast, no catalog search at load time.  
In multi-source catalogs: `s0_RSAC075__1D_mean` distinguishes sources.

---

<!-- _class: section-header -->

# Actions & Plugin System

---

# Actions — Modular Toolbar Buttons

Every button is a pluggable action object:

| Action | Button | What it does |
|---|---|---|
| `TimeSeriesPlotAction` | Plot | Multi-subplot HoloViews; threaded; progress bar |
| `TabulateAction` | Table | Wide-format Tabulator, one column per series |
| `DownloadDataAction` | ⬇ Data | CSV of selected series |
| `DownloadDataCatalogAction` | ⬇ Catalog | CSV of full metadata catalog |
| `ClearCacheAction` | Clear Cache | `catalog.invalidate_all_caches()` |
| `MathRefEditorAction` | Math Ref | Inline expression editor |
| `TransformToCatalogAction` | Transform→Ref | Saves active transform as new ref |
| `AddSourceFilesAction` | Add Files | Drag-and-drop file loader |

**All plot actions are non-blocking** — data loads in a daemon thread; Bokeh document updates via `add_next_tick_callback`. Progress bar shows 0 → 85 → 100% per series.

---

# ReaderRegistry — Plugin System

Any file format becomes a first-class citizen in ~100 lines:

```python
class MyNetCDFReader(DataReferenceReader):
    @classmethod
    def scan(cls, path: str) -> list[DataReference]:
        # open file, yield one DataReference per variable
        ...
    @classmethod
    def catalog_crs(cls) -> str:
        return "EPSG:32610"

ReaderRegistry.register("mynetcdf", MyNetCDFReader, extensions=[".nc"])
```

**Auto-discovery via entry points** — installed plugins load at startup:
```toml
[project.entry-points."dvue.plugins"]
mypackage = "mypackage.plugin:register"
```

```bash
dvue ui mydata.nc               # extension auto-dispatched
dvue ui mynetcdf:mydata.nc      # explicit ref_type override
dvue diagnose                   # show all registered readers + health
```

---

# Named Views & Session Persistence

<div class="columns">
<div>

### Named Views
Save curated station subsets:

```python
view = ViewDefinition(
    name="Bay stations",
    criteria={"station": "~(RSAC|RSAN).*"}
)
```

- Switch views → table filters instantly
- Save/load view definitions as CSV
- Always has an implicit "All" view

</div>
<div>

### Session Persistence
Two-layer state survival:

**Layer 1 — in-memory** (browser refresh)  
Manager keyed by `dvue_user_id` cookie

**Layer 2 — diskcache** (server restart)  
`time_range` + `selection` pickled to disk

```python
serve_session_app(build_manager_fn, title="My Dashboard", port=5006)
```

**Desktop mode** — native `pywebview` window:
```bash
dvue ui --desktop mydata.nc
```

</div>
</div>

---

<!-- _class: section-header -->

# Geo-Animation Framework

---

# `dvue.animator` — Spatial Time-Series Animation

<div class="highlight">

`dvue/animator/` provides a domain-agnostic framework for animating spatial data
over time on an interactive tile-backed map — channels, stations, or any
`GeoDataFrame` — without any Bokeh boilerplate.

</div>

<div class="columns">
<div>

### Architecture

```
SlicingReader  (ABC)
  InMemorySlicingReader
  BufferedSlicingReader   (HDF5 chunked)
  TransformedSlicingReader (resample/rolling/Godin)
  DiffSlicingReader       (A − B, shared index)
        ↓
GeoAnimatorManager      (single reader)
MultiGeoAnimatorManager (two readers, linked viewport)
```

### Key design decisions
- Geometry serialised **once** at init; only `_value` patched per frame (~4 KB over WebSocket)
- `Range1d` shared across all figures — pan/zoom is mirrored automatically
- All Bokeh mutations deferred via `doc.add_next_tick_callback()` (document lock)
- Slow transforms (Godin, rolling) run in a **daemon thread**; spinner covers map

</div>
<div>

### Controls layout (`pn.Card` sections)

```
[time label] [DiscretePlayer] [DatetimePicker]
────────
▼ Appearance  (open)
    clim, colormap, size,
    show channels, show basemap
▶ Contours    (collapsed; auto-expands)
    n levels, smoothing, mode,
    custom levels (comma-sep.),
    color, labels
▶ Transform   (conditional)
▶ X2 isohaline (conditional, DSM2)
```

For `MultiGeoAnimatorManager`:
```
▼ Appearance  (open)
▼ Diff (A−B)  (open)
    show diff □, diff colormap
▶ Contours    (shared for both maps)
▶ Transform   (conditional)
```

</div>
</div>

---

# Geo-Animation: Reader Hierarchy

```python
from dvue.animator import (
    InMemorySlicingReader,   # wrap any pd.DataFrame
    BufferedSlicingReader,   # HDF5 chunk cache (200 steps default)
    TransformedSlicingReader,# lazy full-dataset transform + cache
    DiffSlicingReader,       # A−B on intersection time index
    GeoAnimatorManager,
    MultiGeoAnimatorManager,
)
```

<div class="columns">
<div>

### Single-reader animation

```python
from dvue.animator import GeoAnimatorManager
from dsm2ui.animate import HydroH5FlowReader

reader = HydroH5FlowReader("hist_fc_mss.h5")
mgr = GeoAnimatorManager(
    reader, gdf,
    title="Delta channel flow",
    colormap="turbo",
    transform_options={
        "Daily mean": make_resample_transform("D"),
        "Godin filter": make_godin_transform(),
    },
)
mgr.servable()
```

</div>
<div>

### Two-file comparison

```python
from dsm2ui.animate import animate_hydro_multi

mgr = animate_hydro_multi(
    "study_a.h5", "study_b.h5",
    variable="flow",
    title_a="Calibration",
    title_b="Alternative",
    show_diff=False,   # side-by-side
)
mgr.servable()

# Or from the CLI:
# dsm2ui animate hydro a.h5 b.h5
# dsm2ui animate hydro a.h5 b.h5 --diff
# dsm2ui animate hydro a.h5 b.h5 --diff --transform godin
```

</div>
</div>

---

# Geo-Animation: Contour Overlays & Custom Levels

<div class="columns">
<div>

### Contour pipeline (per frame)

```
channel values
  → griddata(nearest) → 200×N Voronoi raster
  → gaussian_filter(sigma)
  → _compute_contour_levels()
      priority 1: custom_levels (comma-sep input)
      priority 2: nice (MaxNLocator)
      priority 3: linear
      priority 4: eq_hist
  → matplotlib.contour()
  → shapely clip to channel buffer zone
  → contour_source.data update
```

Module-level helpers (`_make_contour_grid`,
`_run_contour_computation`, etc.) are
shared between `GeoAnimatorManager` and
`MultiGeoAnimatorManager`.

</div>
<div>

### Transform loading indicator

Slow transforms (Godin filter: ~5 s for a
multi-year 15-min tidefile) are handled
completely non-blocking:

```python
def _on_transform_change(event):
    self._chart_pane.loading = True
    self._transform_select.disabled = True

    def _compute():
        new_reader = self._setup_reader(name)
        ti = new_reader.time_index  # slow step
        doc.add_next_tick_callback(
            lambda: _apply_and_clear_spinner()
        )

    threading.Thread(target=_compute, daemon=True).start()
```

Spinner clears only **after** the first frame
has been rendered at the new transform.

</div>
</div>

---

<!-- _class: section-header -->

# Downstream Adoption

---

# Three Production Dashboards

<div class="columns">
<div>

### schismviz
- SCHISM coastal model outputs
- Multiple study comparison
- `primary_key=["source_num","id","variable"]`
- DMS datastore observations
- UTM Zone 10N map

### dsm2ui
- DSM2 HEC-DSS tidefiles
- HYDRO + QUAL + EC outputs
- Calibration post-processing

</div>
<div>

### dms_datastore_ui
- DMS field data repository
- CSV inventory-driven catalog
- `diskcache`-backed reader
- Flag editor + screener actions
- GeoDataFrame with EPSG:26910 map

### cdec_maps
- CDEC real-time station data
- California weather/hydro network

</div>
</div>

<div class="highlight">

Each downstream package's `DataUIManager` subclass is **~200 lines** of domain logic. The framework handles the rest.

</div>

---

# Summary — What dvue Delivers

<div class="columns">
<div>

### For developers
- One base class → full Panel app
- Plugin system for any file format
- Safe math expressions without `exec` risk
- Session persistence built-in
- Desktop mode built-in
- **Geo-animation framework** (SlicingReader → GeoAnimatorManager)

</div>
<div>

### For analysts
- Interactive time-series exploration
- 9 built-in transforms, one click each
- Live expression editor in the browser
- Save transforms as reusable refs
- YAML export for reproducibility
- Named views for curated subsets
- **Spatial animation with contours, X2, diff maps**

</div>
</div>

### Current downstream users
**schismviz** · **dsm2ui** · **dms_datastore_ui** · **cdec_maps**

```bash
pip install -e ".[dev]"
# Mix four source types in one command:
dvue ui dsm2_qual.dss observed_ec.dss schism_output/ screened/
dvue diagnose   # see all registered readers
```

---

<!-- _class: section-header -->

# Demo Time

---

# Demo Plan (15 min)

| # | What | Time |
|---|------|------|
| 1 | `dvue ui dsm2_qual.dss observed_ec.dss` — unified table, two sources, `source_num` column | 2 min |
| 2 | Add SCHISM netCDF + datastore CSV — table grows, map shows all stations | 2 min |
| 3 | Geo map — click station → table filters; select model+obs row → Plot | 2 min |
| 4 | **Math Ref editor** — `model - obs` with `search_map`; new row appears; plot model, obs, diff together | 3 min |
| 5 | Transform sidebar — tidal filter on all three; **Transform → Catalog** → `RSAC075__tf` | 2 min |
| 6 | YAML round-trip — Download YAML; open in editor; Upload back | 1 min |
| 7 | `dvue diagnose` — show all registered readers (DSS, HDF5, netCDF, datastore, CDEC) | 1 min |

### Key takeaways to reinforce during demo:
- **Any number of sources, one catalog** — the table is the ground truth
- `source_num` drives visual encoding: same station = same color, different source = different dash
- Math refs span sources: `model - obs` works across DSS and netCDF transparently
- Everything is **non-blocking** (progress bar, not frozen UI)

---

<!-- _class: title -->

# Thank You

## dvue — write domain logic, inherit the dashboard

```bash
dvue ui dsm2_qual.dss observed_ec.dss schism_output/ screened/
```

Source: `d:\dev\dvue` &nbsp;·&nbsp; Examples: `dvue/examples/` &nbsp;·&nbsp; Docs: `dvue/docs/Architecture.md`

---
