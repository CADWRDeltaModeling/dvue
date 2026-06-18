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
  pre { background: #F5F5F5; color: #1A237E; padding: 0.7em 1em; border-radius: 6px; font-size: 0.65em; margin: 0.4em 0; border: 1px solid #BDBDBD; }
  pre code { background: transparent; color: inherit; padding: 0; font-size: 1em; }
  table { font-size: 0.78em; width: 100%; border-collapse: collapse; }
  th { background: #1565C0; color: #ffffff; padding: 5px 10px; }
  td { padding: 4px 10px; border-bottom: 1px solid #E0E0E0; }
  tr:nth-child(even) td { background: #E3F2FD; }
  .columns { display: grid; grid-template-columns: 1fr 1fr; gap: 1.2em; }
  .columns3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1em; }
  .highlight { background: #E0F7FA; border-left: 4px solid #0097A7; padding: 0.4em 0.8em; border-radius: 0 6px 6px 0; color: #00363a; margin: 0.3em 0; }
  .box { background: #E8F5E9; border: 1px solid #A5D6A7; border-radius: 8px; padding: 0.5em 0.8em; margin: 0.3em 0; }
  .step { background: #FFF8E1; border-left: 4px solid #FFA000; padding: 0.3em 0.8em; border-radius: 0 6px 6px 0; margin: 0.2em 0; }
  footer { font-size: 0.7em; color: #888; }
---

<!-- _class: title -->

# dvue

## Interactive Data Exploration  
### for Hydrodynamic Model & Field Data

---
June 2026 · California DWR Delta Modeling Section

---

# What is dvue?

<div class="highlight">

**dvue** is an interactive browser-based dashboard for exploring, comparing, and analyzing time-series data from hydrodynamic models and field observations — without writing code.

</div>

<div class="columns">
<div>

### You can:
- Load data from **multiple sources at once**
- Browse all stations on an **interactive map and table**
- **Plot any combination** of model + observed data together
- Apply **transforms** (filters, resampling, differencing) interactively
- Define **derived series** (unit conversions, model−obs differences)
- **Save and share** your work

</div>
<div>

### Works with:
- DSM2 model output (HEC-DSS, HDF5)
- SCHISM model output (netCDF)
- DMS field data repository
- CDEC real-time station data
- CalSim water operations data
- Any CSV / Parquet / Excel file

</div>
</div>

---

# Agenda

<div class="columns">
<div>

### Presentation (15 min)
1. The core idea — one catalog, every source
2. The data table and map
3. Selecting and plotting data
4. The transform sidebar
5. Derived series (Math References)
6. Saving transforms as catalog entries
7. Named views
8. Session persistence

</div>
<div>

### Demo (15 min)
1. Load DSM2 model + observed EC
2. Browse map and table
3. Plot model vs observed at RSAC075
4. Apply tidal filter
5. Create a model−obs difference series
6. Save tidal-filtered series to catalog
7. Create a named view (Delta stations)
8. Share via URL / desktop mode

</div>
</div>

---

<!-- _class: section-header -->

# The Core Idea: One Catalog, Every Source

---

# The Problem Before dvue

<div class="columns">
<div>

### The old workflow:
1. Open DSM2 output in HEC-DSSVue
2. Open observed data in a separate tool
3. Export both to CSV
4. Load CSVs into Python/Excel
5. Write plotting code
6. Repeat for each station

**Every comparison required custom scripts.  
Inconsistent results. Slow iteration.**

</div>
<div>

### The new workflow with dvue:
1. Point dvue at your files
2. Everything appears in one table and map
3. Select stations → Plot
4. Done

**No exports. No scripts. No waiting.**

</div>
</div>

<div class="highlight">

dvue unifies all your data sources into a **single searchable catalog** — one table, one map, one tool.

</div>

---

# All Your Data Sources in One Table

<div class="highlight">

Every row in the table is one time series — regardless of where it came from. The catalog shows **what is available** — data is only read from disk when you select rows and click **Plot**.

</div>

| source | station | variable | unit | period |
|---|---|---|---|---|
| dsm2_qual.dss | RSAC075 | EC | uS/cm | 2020–2022 |
| dsm2_qual.dss | RSAN007 | EC | uS/cm | 2020–2022 |
| observed_ec.dss | RSAC075 | EC | uS/cm | 2019–2023 |
| screened/cdec_... | RSAC075 | EC | uS/cm | 2018–2024 |
| schism_output/ | RSAC075 | salinity | ppt | 2020–2022 |

- **Filter** by station name, variable, data source, date range
- **Search** with text or patterns (`RSAC*`, `EC`)
- **Sort** any column
- **Select one or many rows** → click Plot
- **Show/hide columns** — different sources bring their own metadata columns; toggle any column on or off to manage screen space

---

# The Interactive Map

<div class="columns">
<div>

### The map shows every station with data.

- Stations are **color-coded** by source
- Zoom and pan freely
- Only shown when the data has lat/lon or XY coordinates — no geo info, no map tab

### Map ↔ Table interaction mode:

| **Map filters Table** ✅ (checked) | **Map filters Table** ☐ (unchecked) |
|---|---|
| Clicking a map station **filters** the table to show only that station's rows — useful for long catalogs where highlighted rows would scroll off screen | Map and table selections are **synced**: clicking a map station highlights its rows in the table (without hiding others), and selecting table rows highlights those stations on the map |

</div>
<div>

### Selecting from the map:

```
 Map                    Table
  ┌──────────┐          ┌──────────┐
  │  ● RSAC  │ ──────→  │ RSAC075  │ (filtered
  │          │          │ RSAN007  │  or highlighted)
  │  ○ RSAN  │ ←──────  │ selected │
  └──────────┘          └──────────┘
```

Use the **lasso** or **box** select tool to capture a region.  
Hold **Shift** and click individual stations to add them to the selection.

</div>
</div>

---

<!-- _class: section-header -->

# Selecting and Plotting Data

---

# Select → Plot

<div class="step">1. Select one or more rows in the table (hold Ctrl/Shift for multiple)</div>
<div class="step">2. Click the <strong>Plot</strong> button in the toolbar</div>
<div class="step">3. A new tab opens with your time-series plot — the dashboard stays responsive</div>

<div class="columns">
<div>

### What you see in the plot:
- Each selected series as a line
- **Series from the same station share a color**; different sources use different line styles (solid vs dashed)
- Subplots are **grouped by unit** — EC (uS/cm) in one panel, Flow (cfs) in another (grouping column is configurable)
- Hover for exact values; hover can be toggled off
- Zoom, pan, reset — **wheel-zoom on an axis** controls only that axis
- **Color palettes** are switchable from the plot options panel

</div>
<div>

### Multiple plots at once:
- Each Plot click opens a **new tab**
- Tabs can be closed individually
- Switch between time windows without re-selecting

### Other toolbar actions:
- **Table** — wide-format data table
- **⬇ Download** — CSV of the plotted data
- **⬇ Catalog** — CSV of all metadata

</div>
</div>

---

# Non-Blocking: The Progress Bar

<div class="highlight">

The dashboard never freezes. Data loads in the background while you keep browsing.

</div>

- A **progress bar** appears at the top of the new tab as data loads
- You can select more rows and start another plot while the first one is loading
- Large multi-station, multi-year plots are fully supported
- Each series is cached — plotting the same station a second time is instant

---

<!-- _class: section-header -->

# The Transform Sidebar

---

# 9 Built-in Transforms — No Code Needed

All transforms are applied **before plotting**. Mix and match freely.

| Transform | What it does |
|---|---|
| **Time range** | Zoom to a specific date window |
| **Fill gaps** | Forward-fill short data gaps (specify max gap length) |
| **Tidal filter** | Remove tidal signal — show the daily mean trend (40-hr cosine-Lanczos) |
| **Resample** | Aggregate to a coarser interval: hourly → daily → monthly |
| **Rolling window** | Smoothed moving average or standard deviation |
| **Differencing** | Period-over-period change (e.g. daily change in EC) |
| **Cumulative sum** | Running total |
| **Scale factor** | Multiply all values (e.g. × 0.0283 to convert cfs → m³/s) |
| **Y-axis clip** | Remove extreme outliers from the view |

---

# Transforms in Practice

<div class="columns">
<div>

### Tidal filter example:
Raw 15-min EC data has a strong tidal signal.  
Toggle **Tidal filter ON** → the tidal oscillations are removed, leaving the low-frequency salinity trend.

Useful for: calibration assessment, trend analysis, drought monitoring.

### Resample example:
15-min model output → **Resample: 1D mean** → daily average EC for plotting alongside daily observations.

</div>
<div>

### Scale factor example:
Model outputs flow in **cfs**, but your report uses **m³/s**.  
Set **Scale factor = 0.0283** → all selected flows are converted on the fly.

### Combining transforms:
Transforms **stack** — you can tidal filter AND resample AND scale in one step.  
The transforms apply in a fixed order: fill → filter → resample → rolling → diff → cumsum → scale.

</div>
</div>

---

<!-- _class: section-header -->

# Derived Series: Math References

---

# What is a Math Reference?

<div class="highlight">

A **Math Reference** is a new time series you define by writing a simple formula over existing catalog entries. It becomes a full catalog row — you can plot it, download it, and use it in further formulas.

</div>

<div class="columns">
<div>

### Common uses:

- **Model − Observed** difference (bias check)
- **Unit conversion** (cfs → m³/s, ppt → uS/cm)
- **Net flow** = inflow − outflow − exports
- **Tidal amplitude** = high − low (from tidally filtered series)
- **Normalized error** = (model − obs) / obs × 100

</div>
<div>

### How to create one:
1. Click **Math Ref** in the toolbar
2. Give it a name (e.g. `RSAC075_bias`)
3. Type the formula: `model - obs`
4. Specify which catalog rows are `model` and `obs`
5. Click Save — new row appears in the table
6. Select it and click Plot like any other series

</div>
</div>

---

# The Math Ref Editor

<div class="columns">
<div>

### Fields:
- **Name** — what the new row will be called in the table
- **Formula** — a mathematical expression  
  e.g. `model - obs`, `flow_cfs * 0.0283`, `clip(ec, 0, 5000)`
- **Variables** — map each token in the formula to a catalog row (by station + variable + source)
- **Attributes** — metadata for the new row (station, variable, unit)

### Available math operations:
standard arithmetic (`+`, `−`, `×`, `÷`), `min`, `max`, `mean`, `clip`, `cumsum`, `diff`, tidal filters (`cosine_lanczos`, `godin`), all NumPy ufuncs, vtools3 filter functions, and pandas resampling — if Python can do it, you can write it as a formula

### `match_all: true` — aggregate across many stations:
Set `match_all: true` on a variable to match **all** rows that fit the criteria into a 2-D array. Use NumPy axis operations to reduce them — e.g. `sum(axis=1)` to compute total drainage across every drain channel in one formula.

</div>
<div>

### Example — model vs observed EC at RSAC075:

```
Name:     RSAC075_model_minus_obs
Formula:  model - obs

Variables:
  model → station=RSAC075, variable=EC, source=DSM2
  obs   → station=RSAC075, variable=EC, source=observed

Attributes:
  station:  RSAC075
  variable: EC bias
  unit:     uS/cm
```

Click **Save** → row appears instantly in the table.

</div>
</div>

---

# Math References are Portable

<div class="columns">
<div>

### Save your formulas as a YAML file:
- Click **Download YAML** in the Math Ref sidebar
- The file captures all your formulas and variable mappings
- Share with colleagues or check into version control
- **Loading multiple YAML files merges them** — dvue combines all math refs into a single set and saves them together on the next Download

### Load formulas in a new session:
- Click **Upload YAML**
- All math refs are recreated instantly
- Even if the underlying files have moved — dvue re-resolves the variable bindings from the catalog

### Tip — editing the YAML directly is often easier:
The in-UI editor is good for first-time setup. For bulk definitions or complex formulas, open the saved YAML in a text editor — the structure is simple and self-explanatory, and you can remove redundant attributes to keep it concise.

</div>
<div>

### Formula variables can be defined two ways:

**By name** — directly references a row by its catalog name  
*Fast, but tied to exact naming*

**By attributes** — resolves at plot time by matching station + variable + source  
*Portable — works even if the catalog is rebuilt from different files*

Example: `model` → any row where `variable=EC` and `source_num=0`

</div>
</div>

---

<!-- _class: section-header -->

# Saving Transforms as Catalog Entries

---

# Transform → Catalog

<div class="highlight">

Transforms are temporary by default — they apply to the current plot view.  
**Transform → Catalog** makes a transform permanent: it saves it as a new catalog row.

</div>

<div class="step">1. Select a row in the table (e.g. RSAC075 EC from DSM2)</div>
<div class="step">2. Set transforms in the sidebar (e.g. Tidal filter ON, Resample 1D mean)</div>
<div class="step">3. Click <strong>Transform → Ref</strong> in the toolbar</div>
<div class="step">4. A new row appears: <strong>RSAC075__tf__1D_mean</strong></div>

<div class="columns">
<div>

### Why is this useful?
- You can now **plot the raw series alongside the filtered one** in a single plot
- Use the filtered series as an **input to a Math Reference** (e.g. model_filtered − obs_filtered)
- **Download** the filtered series as CSV
- The name tells you exactly what was applied: `tf` = tidal filter, `1D_mean` = daily mean

</div>
<div>

### Name tags:
| Applied transform | Tag in name |
|---|---|
| Tidal filter | `tf` |
| Resample daily mean | `1D_mean` |
| Rolling 24-hour mean | `r24H_mean` |
| Differencing | `diff` |
| Cumulative sum | `cumsum` |
| Scale × 2 | `x2.0` |

</div>
</div>

---

<!-- _class: section-header -->

# Named Views & Session Persistence

---

# Named Views — Save Your Station Subsets

<div class="highlight">

A **Named View** is a saved filter on the catalog table. Switch between views instantly to focus on different groups of stations.

</div>

<div class="columns">
<div>

### Examples:
- **Delta stations** — only `RSAC` and `RSAN` prefixed stations
- **EC sensors only** — filter to `variable = EC`
- **Calibration set** — a specific list of stations used in a calibration report
- **Problem stations** — stations flagged for data quality review

### How to create:
1. Filter/select rows in the table manually
2. Click **New View from selection**
3. Give it a name
4. It appears in the Views list

</div>
<div>

### Working with views:
- The **All** view always shows everything
- Views are **instantly switchable** — no reloading
- Views can be **saved to a CSV file** and reloaded in a future session
- Views work across all sources — you can have a view that includes DSM2 and observed rows for the same stations

</div>
</div>

---

# Session Persistence — Your Work Survives

<div class="columns">
<div>

### What is remembered:
- **Time range** you selected
- **Table row selection**
- **Math References** you created
- **Named Views** you saved

### When does it persist?

| Event | What happens |
|---|---|
| Refresh browser tab | ✅ Full session restored |
| Close and reopen tab | ✅ Session restored (365-day cookie) |
| Server restart | ✅ Time range + selection restored |
| New browser / device | ✗ Fresh session |
| Session state causes issues | Use the **Reset Session** button to clear and start fresh |

</div>
<div>

### Desktop mode:
dvue can run as a **native desktop window** instead of a browser tab.

- Launch with `dvue ui --desktop`
- Opens a self-contained window — no browser needed
- Useful on laptops without a reliable network connection
- Same full feature set

### Permalink (coming soon):
Share a URL that encodes your current time range, selection, and view — colleagues open the same state.

</div>
</div>

---

<!-- _class: section-header -->

# What dvue Runs On

---

# Supported Data Sources

<div class="columns3">

<div class="box">

### DSM2
**HEC-DSS** (`.dss`)  
Model output: EC, FLOW, STAGE  
Observed EC  
CalSim  

**HDF5** (`.h5`)  
HYDRO tidefile  
Channel flows, stages, areas

</div>

<div class="box">

### SCHISM
**netCDF** (`.nc`)  
Station output  
Flux output  
Multiple study runs  
side-by-side comparison

</div>

<div class="box">

### Field Data
**DMS Datastore**  
Screened/formatted CSV inventory  
CDEC real-time API  
Any CSV, Parquet, or Excel file  
(HTTP/HTTPS URLs supported)

</div>

</div>

<div class="highlight">

All sources appear in the **same table and map**. You can select a model row and an observed row, click Plot, and see them overlaid — regardless of format.

</div>

<div class="highlight">

### Extensible reader architecture
dvue is the base package — it does not know about DSS, HDF5, or SCHISM by default. Support for each format is provided by separate packages (`dsm2ui`, `schismviz`, `dms_datastore_ui`) that **register readers** with dvue at install time. Run `dvue diagnose` in a terminal to see every registered reader in your current environment.

</div>

---

# A Typical Day with dvue

<div class="step">🌅 Morning: Check EC salinity intrusion after weekend storm event</div>

Open dvue with DSM2 output + CDEC observed. Filter table to `variable=EC`. Click map to select Delta stations. Plot → see model vs obs for 5 stations at once.

<div class="step">📊 Calibration review: Compare tidal-filtered model and observed</div>

Toggle Tidal filter ON. Plot again — tidal signal removed, low-frequency bias is visible. Create a `model - obs` Math Reference. Plot difference for all calibration stations at once.

<div class="step">📋 Reporting: Generate daily average time series for the report period</div>

Set Resample = 1D mean. Set time range to report window. Click **Transform → Ref** to freeze the daily-mean series. Click **⬇ Download** → CSV ready for the report.

<div class="step">🔄 Next run: Load new model output, compare with previous run</div>

Add new DSS file. Table gains new rows with `source_num=1`. Select both runs for the same station. Plot → same color, different line style — comparison is immediate.

---

# Summary

<div class="columns">
<div>

### For the analyst:
- One tool for all data sources
- No scripts, no exports
- Interactive transforms in seconds
- Model vs observed in two clicks
- Reusable formulas (Math Refs)
- Saved views for recurring report sets
- Work persists across sessions

</div>
<div>

### For the modeler:
- Load multiple model runs side by side
- Tidal filter toggles on/off instantly
- Daily-average comparison without resampling code
- Bias series in one formula
- Download exactly what you need as CSV
- Desktop mode for offline / laptop use

</div>
</div>

<div class="highlight">

dvue is in production across **schismviz**, **dsm2ui**, **dms_datastore_ui**, and **cdec_maps**. The same features are available in every dashboard.

</div>

---

<!-- _class: section-header -->

# Demo

---

# Demo Step 1 — Launch with Sample Data

Open a **cmd** terminal and run:

```bat
conda activate dsm2ui
cd d:\dev\dvue
python examples\ex_basic_tsdataui.py
```

**What to show:**
- Table with 3 stations (A, B, C) — different variables, units, intervals
- Select **Station A** row → click **Plot** → tab opens with time-series curve
- Select all 3 rows → Plot → subplots grouped by unit (°C / hPa / %)
- Click the **Table** button → wide-format data table tab

---

# Demo Step 2 — Map + Multiple Sources

In a **second cmd** terminal:

```bat
conda activate dsm2ui
cd d:\dev\dvue
python examples\ex_tsdataui.py
```

**What to show:**
- GeoDataFrame — map appears in sidebar alongside the table
- Click a point on the map → corresponding table row highlights
- Select a row in the table → station highlights on the map
- Table has `source_num` column (0 = raw, 1 = math refs once loaded)
- Select wind_speed rows from two different stations → Plot → same subplot (same unit)

---

# Demo Step 3 — Transform Sidebar

With the `ex_tsdataui.py` dashboard still open:

**What to show:**
1. Select **Station A / wind_speed / hourly** row
2. Open the transform sidebar (left panel)
3. Toggle **Tidal filter ON** → Plot → smoothed trend visible
4. Set **Resample = 1D**, aggregation = **mean** → Plot → daily average
5. Set **Scale factor = 2.23694** → Plot → instant unit conversion (m/s → mph)
6. Reset all transforms (uncheck / clear fields)

---

# Demo Step 4 — Math Ref Editor (Live Formula)

With `ex_tsdataui.py` open, click the **Math Ref** toolbar button.

**Type this into the editor:**

```
Name:       wind_diff_A_minus_C
Formula:    ws_a - ws_c
Attributes: station_name=Station A, variable=wind_diff, unit=m/s
Search map:
  ws_a: station_name=Station A, variable=wind_speed, interval=hourly
  ws_c: station_name=Station C, variable=wind_speed, interval=hourly
```

Click **Save** → new row `wind_diff_A_minus_C` appears in the table.  
Select it alongside the two raw wind-speed rows → Plot → all three in one subplot.

---

# Demo Step 5 — Upload YAML Math Refs

Still in `ex_tsdataui.py` — click **Math Ref → Upload YAML** and load:

```
d:\dev\dvue\examples\data\math_refs_search_map.yaml
```

**What to show:**
- Table gains new rows: unit-converted wind speed, cumulative precip, cross-station diff, multi-station mean
- Select `wind_speed_mph__A__hourly` + original `wind_speed` row → Plot → both, different units in separate subplots
- Select `wind_diff_A_minus_B__hourly` → Plot → cross-station difference

**Download YAML** — show the round-trip button, note the file captures all formulas.

---

# Demo Step 6 — Transform → Catalog

Back in the transform sidebar:

1. Select **Station B / precipitation / hourly**
2. Set **Resample = 1D**, aggregation = **sum**
3. Click **Transform → Ref** in the toolbar
4. New row appears: `Station_B__1D_sum`
5. Select original + new row → Plot → raw hourly bars alongside daily totals
6. Now use `Station_B__1D_sum` **as input to a Math Ref**:
   - Open Math Ref editor
   - Formula: `cumsum(daily)`
   - Search map: `daily: station_name=Station B, variable=precipitation, interval=1D_sum`
   - Save → running total series appears in table

---

# Demo Step 7 — Named Views

In the sidebar **Views** tab:

1. Select only Station A rows in the table
2. Click **New View from selection** → name it `Station A only`
3. Click **All** → see everything
4. Click `Station A only` → table instantly filters to Station A rows
5. Click **Save views as CSV** → portable file

**Switching is instant** — no reload, time range and transforms are preserved.

---

# Demo Step 8 — Desktop Mode + dvue diagnose

Open a new cmd terminal:

```bat
conda activate dsm2ui
dvue diagnose
```

Shows all registered readers and their file extensions.

```bat
dvue ui --desktop examples\data\math_refs.yaml
```

Opens a **native desktop window** — no browser needed.  
Drag any CSV file onto the window to add it to the catalog.

```bat
dvue ui --help
```

Shows all CLI options (port, desktop, plugin override, etc.).

---

<!-- _class: title -->

# dsm2ui Demo

## DSM2 Viewer, Calibration & Post-processing

---
June 2026 · California DWR Delta Modeling Section

---

# dsm2ui — What It Provides

<div class="columns">
<div>

### Interactive Viewers
- **HDF5 tidefile viewer** — channel flow, stage, EC from `.h5`
- **DSS browser** — any HEC-DSS time series
- **Echo file viewer** — input boundary conditions + output, on the Delta map
- **Channel map** — color channels by Manning, Dispersion, or Length
- **Cross-section viewer** — channel geometry from tidefile
- **Geo-animation** — animated map of channel flow, stage, velocity, or EC over time

</div>
<div>

### Calibration Tools
- **`calib run`** — apply channel modifications and compute EC slope metrics
- **`calib optimize`** — gradient-based auto-tuning of DISPERSION/MANNING
- **`calib postpro`** — generate per-station comparison plots and heatmaps
- **`calib-ui`** — interactive calibration results dashboard

All driven by a single YAML config file.

</div>
</div>

---

# dsm2ui Demo Step 1 — View HDF5 + DSS Output

Open a **cmd** terminal:

```bat
conda activate dsm2ui

dsm2ui ui d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5
```

**What to show:**
- Delta channel map — channels colored by variable
- Click a channel on the map → table row highlights
- Select `RSAC075 EC` row → Plot → EC time series
- Select multiple stations (EC + FLOW) → Plot → two subplots by unit

Now add the DSS output alongside:

```bat
dsm2ui ui d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5 ^
          d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026_qual.dss
```

- Table now has `source_num=0` (HDF5) and `source_num=1` (DSS) rows
- Select same station from both → Plot → overlay HDF5 vs DSS

---

# dsm2ui Demo Step 2 — Echo File Viewer (Input + Output)

```bat
dsm2ui ui d:\delta\dsm2_studies\studies\mini_2026\output\hydro_echo_mini_2026.inp ^
          d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5
```

**What to show:**
- Channel map with boundary flow stations marked
- Table includes **input boundary conditions** (Sacramento inflow, exports, tides) and **output channels** (EC, flow, stage) in one unified catalog
- Select `Sacramento at Freeport inflow` → Plot → boundary flow time series
- Select `RSAC075 EC output` → Plot → compare input forcing vs output EC

```bat
dsm2ui ui map d:\delta\dsm2_studies\studies\mini_2026\output\hydro_echo_mini_2026.inp
```

- Map colored by **Manning coefficient** — highlights high-friction channels
- Switch coloring to **Dispersion** → reveals dispersion zone assignments

---

# dsm2ui Demo Step 3 — Calibration Run

```bat
conda activate dsm2ui
cd d:\delta\dsm2_studies\studies

dsm2ui calib setup --output calib_config_demo.yml
```

Opens the template YAML. Point it at the mini2026 study, then:

```bat
dsm2ui calib run --config calib_config_mini2026_qual.yml --metrics-only
```

**What to show:**
- Console prints per-station EC slope table (base vs variation)
- `delta_slope` column — positive = improvement toward 1.0
- `results.txt` and `slopes_mini_2026.csv` written to the variation folder

```bat
dsm2ui calib run --config calib_config_mini2026_qual.yml --plot
```

- Generates per-station diagnostic PNGs: observed vs model time series with regression line

---

# dsm2ui Demo Step 4 — Post-processing (Comparison Plots)

```bat
cd d:\delta\dsm2_studies\studies

dsm2ui calib postpro run observed postpro_config_mini2026_qual.yml
dsm2ui calib postpro run model   postpro_config_mini2026_qual.yml
dsm2ui calib postpro run plots   postpro_config_mini2026_qual.yml --workers 4
```

**What to show after `plots`:**
- Per-station HTML plots in the `plots/` folder — open one in the browser
- Each plot: observed vs model time series + tidal-filtered overlay + scatter + metrics table
- `dsm2ui calib postpro run heatmaps ...` → station × study heatmap of slope/RMSE

```bat
dsm2ui calib postpro setup ^
  -s d:\delta\dsm2_studies\studies\mini_2026 ^
  -p d:\delta\postprocessing ^
  -o postpro_config_new.yml
```

- Generates a fresh config from the study folder + postprocessing directory — no manual JSON editing

---

# dsm2ui Demo Step 5 — Calibration UI Dashboard

```bat
dsm2ui calib postpro setup ^
  -s d:\delta\dsm2_studies\studies\historical ^
  -s d:\delta\dsm2_studies\studies\mini_2026 ^
  -p d:\delta\postprocessing ^
  -o postpro_config_compare.yml

dsm2ui calib-ui postpro_config_compare.yml
```

**What to show:**
- Interactive dashboard: all calibration stations on Delta map
- Click a station → station-level time-series comparison plot loads
- Toggle between studies (historical vs mini2026) using the source selector
- Scatter plot: observed vs model with R², slope, RMSE in sidebar
- Station filter: show only EC stations, or only Delta stations
- Download the metrics table as CSV

---

# dsm2ui Demo Step 6 — Channel Map Deep Dive

```bat
dsm2ui ui map d:\delta\dsm2_studies\studies\mini_2026\output\hydro_echo_mini_2026.inp ^
              --colored-by DISPERSION
```

**What to show:**
- Channels color-coded by dispersion value (low = blue, high = red)
- Hover a channel → tooltip shows CHAN_NO, Manning, Dispersion, Length
- Click a channel → select it for comparison

Compare two runs side by side:

```bat
dsm2ui ui map d:\delta\dsm2_studies\studies\mini_2026\output\hydro_echo_mini_2026.inp ^
              --base-file d:\delta\dsm2_studies\studies\historical\output\hydro_echo_hist.inp ^
              --colored-by DISPERSION
```

- Map shows the **difference** in dispersion values between the two runs
- Immediately reveals which channels were modified in the variation

---

# dsm2ui Demo Step 7 — Geo-Animation: Single Tidefile

```bat
conda activate dsm2ui
dsm2ui animate hydro d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5
```

**What to show:**
- Delta channel map animates channel **flow** over time, colour-coded from blue (low) to red (high)
- **DiscretePlayer** at top of sidebar: play, pause, step forward/back, speed control
- **DatetimePicker**: jump directly to any date — snaps to nearest 15-min step
- **Appearance card**: adjust colour range, switch colormap, toggle basemap on/off
- Switch to **EC animation** (QUAL file):
  ```bat
  dsm2ui animate qual d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026_qual.h5 ^^
      --constituent ec --x2-threshold 2700
  ```
- X2 isohaline line appears on the map at 2700 µS/cm

---

# dsm2ui Demo Step 8 — Geo-Animation: Transforms & Contours

With the geo-animation running (from Step 7):

**Transforms:**
1. Open the **Transform** card in the sidebar
2. Select **Godin filter** — spinner appears while the tidal filter is applied (~5 s)
3. Spinner clears — animation now shows tidally filtered EC (slow salinity trend only)
4. Switch to **Daily mean** — time step coarsens to 1 day; position preserved

**Contours:**
1. Open the **Contours** card — it expands automatically when toggled on
2. Tick **Show contours** — iso-EC lines appear on the map
3. Type `500, 1000, 2000, 2700` in the **Custom levels** box — exactly those lines drawn
4. Tick **Label contours** — value labels appear on each line
5. Adjust **Contour smoothing** slider to reduce blocky edges

---

# dsm2ui Demo Step 9 — Geo-Animation: Two-File Comparison

```bat
# Side-by-side: calibration vs alternative run
dsm2ui animate hydro ^^
    d:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5 ^^
    d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5
```

**What to show:**
- **Two maps** side by side; **single shared player** — both advance in lock-step
- **Pan or zoom on either map** — the other follows instantly (linked viewport)
- Contours appear on both maps when enabled

```bat
# Difference map: A − B
dsm2ui animate hydro ^^
    d:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5 ^^
    d:\delta\dsm2_studies\studies\mini_2026\output\mini_2026.h5 ^^
    --diff --transform godin
```

**What to show:**
- Single **A − B difference map** using a diverging colormap (blue = A lower, red = A higher)
- **Show diff** checkbox in the Diff card — toggle between side-by-side and diff at runtime
- Godin spinner while filter loads; diff centred at zero automatically

---

# dsm2ui Command Reference

```bat
dsm2ui --help

dsm2ui ui <file.h5>                         # HDF5 tidefile viewer
dsm2ui ui <file.dss>                        # DSS browser
dsm2ui ui <echo.inp>                        # input + output viewer
dsm2ui ui <echo.inp> <file.h5>              # combined viewer
dsm2ui ui                                   # empty — drag & drop files in
dsm2ui ui map <echo.inp> --colored-by DISPERSION
dsm2ui ui xsect <file.h5>                   # cross-section viewer

dsm2ui calib setup   --output calib_config.yml
dsm2ui calib run     --config calib_config.yml
dsm2ui calib run     --config calib_config.yml --metrics-only
dsm2ui calib run     --config calib_config.yml --plot
dsm2ui calib optimize --config calib_config.yml

dsm2ui calib postpro setup   -s <study_dir> -p <postpro_dir> -o config.yml
dsm2ui calib postpro run observed config.yml
dsm2ui calib postpro run model   config.yml
dsm2ui calib postpro run plots   config.yml --workers 4
dsm2ui calib postpro run heatmaps config.yml

dsm2ui calib-ui postpro_config.yml

# Geo-animation commands
dsm2ui animate hydro <file.h5>                  # animate channel flow
dsm2ui animate hydro <file.h5> --variable stage # animate stage
dsm2ui animate qual  <file.h5> --constituent ec # animate EC with X2
dsm2ui animate qual  <file.h5> --constituent ec ^^
    --x2-threshold 2700 --transform godin       # tidally filtered + X2
dsm2ui animate hydro a.h5 b.h5                  # side-by-side comparison
dsm2ui animate hydro a.h5 b.h5 --diff           # A−B difference map
dsm2ui animate hydro a.h5 b.h5 --diff ^^
    --transform godin                           # filtered difference map
```

---

<!-- _class: title -->

# Thank You

## dvue + dsm2ui — all your data, one dashboard

**dvue:**  `dvue ui your_model.dss your_observed.dss`

**dsm2ui:**  `dsm2ui ui mini_2026.h5 mini_2026_qual.dss`

Questions? Contact the Delta Modeling Section.
