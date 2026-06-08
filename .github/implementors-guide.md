# dvue — Implementors Guide

Design decisions, invariants, and hard-won lessons for anyone extending dvue.
Read this before making changes to `catalog.py`, `tsdataui.py`, `math_reference.py`,
or `math_ref_editor.py`.

---

## 1. Mixed Catalogs (raw + math refs)

When a `DataCatalog` contains both `DataReference` (raw) and `MathDataReference` entries:

- `to_dataframe()` returns NaN in raw-only columns (e.g. `url`, `filename`, `A–F`) for math ref rows.
- **Always guard against NaN** before building a catalog key or passing to path utilities:
  ```python
  def get_data_reference(self, row):
      source = row.get("source", None)
      if pd.isna(source):
          return catalog.get(row["name"])   # math ref → name lookup
      return catalog.get(row["name"])       # raw ref — name is always reliable
  ```
- `get_unique_short_names()` raises `TypeError` on NaN paths. Filter first:
  ```python
  valid = [s for s in df["source"].unique() if not pd.isna(s)]
  short_names = get_unique_short_names(valid)
  ```
- When reading selected rows from the display table, use `dataui._dfcat.iloc[selection]`
  **not** `display_table.value.iloc[selection]` — the display value strips the `name` column,
  which breaks math ref key lookup.

---

## 2. source_num / primary_key — Source Discrimination

`DataCatalog` tracks source identity via a `_source_index` dict: each unique non-empty
`ref.source` value is assigned an integer (0, 1, 2 …) in order of first `add()`.
`source_num` is **never stored on the ref** — it is computed by the catalog on demand.

`to_dataframe()` injects a `source_num` column **only when** `len(_source_index) > 1`.
Single-source catalogs have no `source_num` column and no `s{n}_` name prefix.

Subclasses check multi-source state with:
```python
if "source_num" in df.columns:   # True only for multi-source catalogs
    ...
```

There are **no** `url_column`, `url_num_column`, `display_url_num`, or `_apply_url_num()`
concepts — these have been removed entirely.

### Declaring the primary key

`primary_key` is required at `DataCatalog()` construction:

```python
# Single-source catalog
catalog = DataCatalog(primary_key=["station", "variable"])

# Multi-source catalog — include "source_num" as first pk column
catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
```

`primary_key` controls:
- **Uniqueness**: `ValueError` on duplicate pk-tuple
- **Auto-naming**: refs with no explicit `name` get a name derived from their pk values
  (e.g. `station="RSAC054", variable="FLOW"` → `"RSAC054_FLOW"`)
- **`source_num` prefix**: included in derived names when catalog is multi-source
- **Keyword lookup**: `catalog.get(station="A", variable="discharge")`

### Auto-naming and sanitisation

When a ref is added without an explicit `name`, the catalog derives one:

```
[s{source_num}_]{pk_value_1}_{pk_value_2}_…
```

Values are sanitised: non-alphanumeric runs collapsed to `_`; leading digit prefixed with `_`.
Explicit `name=` always overrides auto-derivation.

### Math ref search_map

Use `source_num:` (integer) in YAML search criteria — not `url_num:`:

```yaml
search_map:
  x:
    variable: flow
    source_num: 0   # only refs from the first loaded source
```

A migration shim in `MathDataCatalogReader` reads old `url_num` keys as `source_num`
and emits a `DeprecationWarning`.

---

## 3. DataReference.matches() — Dynamic Metadata

`matches(**criteria)` checks `_attributes` first, then falls back to `_dynamic_metadata`
for any key not present in `_attributes`. Static attributes always win on conflict.

Math ref `search_map` criteria can include `source_num` to pin a variable to a specific
source file. `source_num` is a special catalog-level key — `catalog.search(source_num=0)`
resolves via `_source_index`, not dynamic metadata:

```yaml
search_map:
  x:
    variable: flow
    source_num: 0    # only refs from the first loaded file
```

### Type coercion in matches()

The math ref editor's search map text input always produces **string values** (e.g.
`source_num=0` becomes the string `'0'`). `matches()` handles this automatically: when
`actual != expected` and their types differ, it attempts `type(actual)(expected)` before
returning `False`.

- `source_num=0` (int from `_source_index`) matched with `'0'` (string from editor) → `int('0') == 0` ✓
- `geoid='437'` (string attribute) matched with `437` (int) → `str(437) == '437'` ✓

**Do not coerce values in the parser** (`_parse_search_map`). Type coercion belongs
exclusively in `matches()` so the logic is in one place and both typed (YAML-loaded)
and untyped (editor text) criteria work without special handling.

Note: `source_num` is handled as a special keyword in `catalog.search()` (not via
`matches()` on individual refs) — the catalog resolves it via `_source_index` before
the per-ref attribute check.

---

## 4. MathDataReference — Variable Resolution Rules

### match_all variables are always DataFrame

When `match_all: true` (or `require_single=False`) is set for a variable, the resolved
value is **always a `pd.DataFrame`** regardless of how many catalog entries matched —
even if only 1 entry matches.

Expressions using `match_all` variables **must** use DataFrame semantics:
```python
x.mean(axis=1)        # ✅ works for 1 or N columns
x.iloc[:, 1] - x.iloc[:, 0]  # ✅ works when ≥2 matches
```

Never write `match_all` expressions that assume a Series — they will break when the
catalog grows.

### match_all concat strategy — axis=1 preferred, axis=0 warns

When multiple refs match a `match_all` variable, results are combined with
`pd.concat(frames, axis=1)` (columns share the time index — the normal case).
If that fails (incompatible indices), it falls back to `pd.concat(axis=0)` **and
emits a `UserWarning`** explaining that `iloc[:,N]` will raise `IndexError`.

If you see this warning, the matched refs likely have non-overlapping or
misaligned time indices. Fix the search criteria so only refs with a shared
time index are matched.

### require_single variables are always Series (when single-column)

Variables resolved via `require_single=True` (the default) are unwrapped to `pd.Series`
when the result is a single-column DataFrame. These are suitable for scalar arithmetic:
```python
obs - model    # ✅ both are Series
```

### Canonical YAML loading — never duplicate build logic

All YAML parsing (including in-memory upload) must go through
`MathDataCatalogReader.build_from_data(data, parent_catalog)`. Never inline a
`for entry in data` loop that pops `match_all` — it will diverge and go stale.

```python
# ✅ correct
refs = MathDataCatalogReader().build_from_data(data, parent_catalog=catalog)

# ❌ wrong — duplicates logic and uses wrong key names
for entry in data:
    req[var] = bool(entry.pop("_require_single", True))  # old key!
```

---

## 5. Catalog Cache — Always Rebuild from Live Catalog

`get_data_catalog()` in `TimeSeriesDataUIManager` rebuilds fresh from `self.data_catalog`
on every call when a live `DataCatalog` is available. This ensures that `catalog.add()`
calls (e.g. from the math ref editor) are immediately visible.

- **Do not override `get_data_catalog()` to return a stale DataFrame** (e.g. `return self.dfcat`).
- `_cached_catalog` is a legacy path for subclasses that override `get_data_catalog()`
  themselves and have no `data_catalog` property. Do not use it in new subclasses.

---

## 6. display_table vs _dfcat

`dataui.display_table.value` is a **display-column subset** of `_dfcat` that strips the
`name` column (and other non-display columns). Any callback that needs to resolve a
`DataReference` must use `dataui._dfcat.iloc[selection]`, not `display_table.value.iloc[selection]`.

The integer selection indices are identical across both DataFrames — only the columns differ.

---

## 7. Adding a New Column to the Manager Table

1. Return the column name from `get_table_column_width_map()` (dvue) or the overridden
   `_get_table_column_width_map()` (subclass).
2. Ensure the column exists in the DataFrame returned by `get_data_catalog()`.
3. If the column should be hidden by default, add it to `hidden_columns` in
   `create_data_table()`. It will still be filterable.
4. Write a test that asserts the column appears in `get_data_catalog()` **and** in
   `get_table_columns()` — the "subset" regression guard catches KeyError at render time.

---

## 8. ref_type — Extending DataReference

`DataReference.ref_type = "raw"` is a class-level attribute. Subclass with one line:
```python
class MyRef(DataReference):
    ref_type = "my_type"
```

`to_dataframe()` always includes a `ref_type` column. The column is hidden by default
when all refs share the same type; it auto-shows when the catalog becomes mixed.
Use `TimeSeriesDataUIManager._has_mixed_ref_types(df)` to check.

---

## 9. Testing Conventions

- **Run with** `pytest tests/ --override-ini="addopts="` — the pyproject.toml injects
  `--cov` flags that require `pytest-cov`; bypass with `--override-ini="addopts="` when
  running in environments without it.
- Keep test fixtures minimal and self-contained. Use `InMemoryDataReferenceReader` for
  catalog tests — no files needed.
- Always add regression tests for any bug that surfaces at the UI level. The test should
  reproduce the exact failure path (e.g. `_resolve_variables()` raising `IndexError`) so
  it cannot silently regress.
- `TestSourceNumSearchable.test_math_ref_search_map_filters_by_source_num` is the canonical
  integration test for the full `source_num → catalog.search → resolve` pipeline.

---

## 10. Downstream Subclass Checklist (e.g. schismviz, dms_datastore_ui)

When updating a `TimeSeriesDataUIManager` subclass for the `primary_key` / `source_num` redesign:

| Item | Check |
|------|-------|
| `DataCatalog(primary_key=[...])` declared in `_build_catalog()` | |
| `super().__init__()` has **no** `url_column` or `url_num_column` kwargs | |
| `display_url_num` replaced with `"source_num" in df.columns` | |
| Row lookups use `dataui._dfcat.iloc[selection]` | |
| Math ref NaN guard uses `pd.isna(row.get("source"))` (not `filename`/`url`) | |
| No `_apply_url_num()` call anywhere in the subclass | |
| `get_data_reference()` uses `row["name"]` (always safe) | |
| Math ref YAML criteria use `source_num:` not `url_num:` | |

---

## 11. Map / Geo Selection with Mixed Catalogs

**Problem:** When `_dfcat` is a `GeoDataFrame` containing both raw refs (with geometry)
and math refs (NaN geometry), two separate bugs interact to silently deselect math ref
rows whenever the user interacts with the map:

### Bug A — `-1` indices passed to Bokeh (`update_map_features`)

`current_view` is filtered to valid-geometry rows via `.is_valid` before `build_map_of_features`.
`pandas.Index.get_indexer` returns `-1` for any label absent from `current_view` (i.e. every
math ref). Without filtering, `-1` is passed directly to `selected=` on the GeoViews Points
opts. Bokeh interprets `-1` as Python-style "last element", visually selecting the wrong geo
point on the map.

**Fix (already applied in `dataui.py → update_map_features`):**
```python
# Strip -1 (math refs absent from the geo-only view)
current_selection = [
    i for i in current_view.index.get_indexer(current_selected.index).tolist()
    if i >= 0
]
```

### Bug B — map callback overwrites table selection (`select_data_catalog`)

`select_data_catalog` rebuilds the table selection from `self._map_features.dframe()`, which
only contains geo rows. Math refs were never added to `_map_features`, so they can never
survive a `merge` with `_dfcat`. `table.param.update(selection=...)` then blindly replaces
the entire table selection — erasing every math ref row on every map click.

**Fix (already applied in `dataui.py → select_data_catalog`):**
Before overwriting, collect any currently-selected rows whose geometry is NaN (they have no
map representation) and include them in the final selection:
```python
geo_selected_indices = self._dfcat.index.get_indexer(merged_indices).tolist()
non_geo_positions = []
if isinstance(self._dfcat, gpd.GeoDataFrame) and table.selection:
    has_no_geo = self._dfcat.geometry.isna()
    non_geo_positions = [
        i for i in table.selection
        if i < len(self._dfcat) and has_no_geo.iloc[i]
    ]
selected_indices = sorted(set(non_geo_positions + geo_selected_indices))
```

### Invariant to maintain

> Any row in `_dfcat` that is absent from `_map_features` (because it has NaN or invalid
> geometry) must be **preserved in the table selection** across all map interaction callbacks.
> Use `ref_type` or `geometry.isna()` to identify such rows.

Both fixes and their regression tests live in
`dvue/tests/test_dataui_geo_selection.py`.

---

## 12. Serving a dvue App — VanillaTemplate Gotchas

### Use VanillaTemplate, not FastListTemplate, as the outer served template

`FastListTemplate.main` wraps each item in a Bootstrap `<li>` with a fixed height, which
collapses the `GridStack` so the data table and display panel disappear.  `VanillaTemplate`
renders `main` items full-width without that wrapping.

`create_view()` returns a `FastListTemplate` (correct for standalone use / notebook).  When
embedding inside an outer served template, transplant sidebar and main content out of the
inner `FastListTemplate` into a `VanillaTemplate`, then clear the inner template so Panel
doesn't try to serve the same Bokeh models from two documents.

### `template.servable()` must be called synchronously in `make_app()`

`make_app()` is the per-session callable passed to `pn.serve()`.  Without a synchronous
`template.servable()` call in `make_app()`, the Bokeh document is never registered and the
browser receives an empty page — even if `_load_app` runs and builds everything correctly.

```python
def make_app():
    ...
    pn.state.onload(_load_app)  # deferred heavy work
    template.servable()          # ← must be here, not only inside onload
```

### VanillaTemplate DOM Placeholder Rule (header and modal)

`VanillaTemplate`'s Jinja HTML embeds a Bokeh root `<div>` for every item in
`template.header` and `template.modal` **at the time `.servable()` is called**.
Items appended to those lists *after* `.servable()` (including inside
`pn.state.onload`) have no DOM placeholder and silently fail to render.

**Pattern:** create `pn.Row()` / `pn.Column()` container objects *before* the
template, pass them at construction or append them before `.servable()`, then
update their `.objects` inside `_load_app`:

```python
header_row = pn.Row(sizing_mode="fixed")       # ← before VanillaTemplate()
modal_pane = pn.Column(sizing_mode="stretch_width")

template = pn.template.VanillaTemplate(..., header=[header_row])
template.modal.append(modal_pane)               # ← before .servable()

def _load_app():
    # ✅ update container contents — root already has a DOM slot
    modal_pane.objects = [ui.get_about_text()]
    header_row.append(about_btn)
    # ❌ never do this — destroys the pre-rendered modal root permanently
    # template.modal.clear()
```

`template.modal.clear()` after `.servable()` destroys the pre-rendered root; all
subsequent `template.open_modal()` calls open an empty dialog.

See `session-management.md` for the complete `make_app()` reference implementation.

---

## 13. `RegistryUIManager` — Building a Registry-Backed Manager

`RegistryUIManager` (in `dvue/registry_ui.py`) is the recommended base class whenever
your manager should accept multiple file types via `ReaderRegistry`.  It handles the
scan-normalise-add loop, catalog wiring, and `DataReference` lookup; you only provide
domain-specific hooks.

### Minimum implementation

```python
from dvue.registry import ReaderRegistry
from dvue.registry_ui import RegistryUIManager, RegistryPlotAction

# 1. Register your reader class at module level
ReaderRegistry.register("myformat", MyReader, extensions=[".xyz"])

# 2. Optionally subclass for domain-specific hooks
class MyUIManager(RegistryUIManager):

    def normalize_ref(self, ref):
        """Map file-sourced attribute names to the station/variable schema.

        Called once per ref returned by ReaderRegistry.scan(), before catalog.add().
        All ref.set_attribute() calls here are safe — the ref is not yet in the catalog.
        """
        if not ref._attributes.get("station"):
            ref.set_attribute("station", ref._attributes.get("id", ""))
        if not ref._attributes.get("variable"):
            ref.set_attribute("variable", ref._attributes.get("quantity", "").lower())

    def on_file_added(self, path, refs):
        """Post-add hook for file-level side effects.

        Called once after all refs from *path* have been added to the catalog.
        Use to expand time_range, load geometry from a companion file, etc.
        """
        pass  # default is a no-op; override only when needed

    def _make_plot_action(self):
        return MyPlotAction()  # subclass of RegistryPlotAction
```

### `RegistryPlotAction` — curve label customisation

Override `format_variable()` to apply domain-specific formatting to the variable
part of a curve label:

```python
class MyPlotAction(RegistryPlotAction):
    def format_variable(self, variable: str) -> str:
        # e.g. title-case long ALL-CAPS names, keep abbreviations unchanged
        return variable.title() if len(variable) > 2 else variable
```

Curve labels are `station/variable` when multiple variables are selected, or just
`station` when only one variable appears in the selection.  Math refs use their
`name` attribute instead.

### What `RegistryUIManager` provides (do not re-implement)

| Provided by base | Notes |
|------------------|-------|
| `add_source_files(*paths)` | scan → normalize → add loop; logs and skips unregistered extensions |
| `data_catalog` property | returns `self._dvue_catalog` |
| `get_data_reference(row)` | catalog lookup by `row["name"]` |
| `get_data_catalog()` | rebuilds `_display_dfcat` only when catalog length changes |
| `primary_key=["source_num", "station", "variable"]` | set in `__init__` |
| Table columns: `station`, `variable`, `ref_type` | from `_get_table_column_width_map()` |
| `build_station_name(r)` | prefix with `source_num:` for multi-source catalogs |
| `is_irregular()`, `get_tooltips()`, `get_map_*()` | sensible defaults; override if needed |

### Invariants

- `normalize_ref()` must finish all `ref.set_attribute()` calls **before** returning.
  `add_source_files()` calls `catalog.add(ref)` immediately after `normalize_ref()`.
- Do **not** call `catalog.add()` inside `normalize_ref()` — that is done by the base.
- Do **not** store a reference to the scanned refs list after `on_file_added()` returns —
  the base does not hold it, and the catalog is the canonical store.
- When `normalize_ref()` cannot determine a `station` value, set it to a non-empty
  fallback (e.g. the `source` basename) so the auto-derived catalog name is stable.

## 14. `ReportAction` — Catalog-Level Report Generation

`ReportAction` is the catalog-level sibling of `PlotAction`.  Use it for any action
that generates output from the **full catalog** rather than from a row selection —
e.g. data-coverage summaries, gap reports, quality metrics, or any output that would
be awkward to frame around a user selection.

### When to use `ReportAction` vs `PlotAction`

| Concern | `PlotAction` | `ReportAction` |
|---|---|---|
| Requires row selection | Yes (warns + aborts if empty) | No — full catalog always passed |
| Data loading pipeline | Row-by-row via `get_refs_and_data()` | Report decides what it needs |
| Override point | `render(df, refs_and_data, manager)` | `generate(catalog_df, manager)` |
| Button state | Requires selection | Always enabled |
| Tab label prefix | `str(n)` (default) | `"R{n}"` (default) |

Both classes use the same threaded worker + `set_progress()` + `pn.Tabs` display
infrastructure.

### Minimal subclass example

```python
from dvue.actions import ReportAction
import panel as pn


class CoverageReportAction(ReportAction):
    """Summarise station coverage per variable."""

    def generate(self, catalog_df, manager):
        # catalog_df is the full _dfcat — all rows, all columns.
        # manager.get_data_reference(row).getData(time_range=...) can be called
        # if the report needs to load actual time-series data.
        summary = (
            catalog_df.groupby("variable")
            .agg(
                stations=("station_id", "nunique"),
                min_year=("min_year", "min"),
                max_year=("max_year", "max"),
            )
            .reset_index()
        )
        return pn.Column(
            pn.pane.Markdown("## Coverage Report"),
            pn.widgets.Tabulator(summary, show_index=False, sizing_mode="stretch_both"),
            sizing_mode="stretch_both",
        )
```

### Registering via `get_data_actions`

```python
class MyManager(TimeSeriesDataUIManager):
    def get_data_actions(self):
        actions = super().get_data_actions()
        report = CoverageReportAction()
        actions.append(dict(
            name="Report",
            button_type="warning",
            icon="report",
            action_type="display",
            callback=report.callback,
        ))
        return actions
```

No changes to `dvue/dataui.py` are needed — `create_data_actions()` is already
duck-typed: any object with `callback(event, dataui)` is valid.

### Threading contract

`generate()` is called inside a daemon worker thread.  **Do not mutate Panel
state directly inside `generate()`.**  Any Panel mutations (widget updates,
`pn.state.curdoc` changes, etc.) must be scheduled via
`pn.state.curdoc.add_next_tick_callback(...)`.  The final tab append and
progress-bar update are handled automatically by `callback()`.

The `doc` reference is not passed to `generate()` because `generate()` should
return a finished Panel object — if it needs the doc for callbacks, capture
`pn.state.curdoc` at the start of `generate()`.

### Static image exports

Return `pn.pane.Matplotlib(fig)`, `pn.pane.PNG(...)`, or `pn.pane.SVG(...)` from
`generate()` to display static images inside the report tab.  No special framework
support is required.


