# dvue — Agent Instructions

## Scope

Use this file when working in the `dvue/` workspace root.

## Purpose

`dvue` is a reusable UI/data-catalog framework for Panel + HoloViews + GeoViews apps. It should stay domain-agnostic.

## Fast Start For Agents

1. Install for development:
   - `pip install -e ".[dev]"`
2. Run tests:
   - `pytest`
3. Run examples:
   - `panel serve examples/ex_basic_tsdataui.py --show`
   - `panel serve examples/ex_tsdataui.py --show`

## Key Design Rules

- Keep `dvue` generic. Do not hard-code datastore-specific naming or repo semantics.
- Extend with subclassing/composition from app code (`DataUIManager`, `TimeSeriesDataUIManager`, action classes, catalog builders).
- Preserve metadata-driven behavior in catalog objects; avoid adding hidden coupling between UI and specific datasets.

## Primary Files To Read First

- `dvue/dataui.py` (base manager/provider patterns)
- `dvue/tsdataui.py` (time-series UI manager behavior)
- `dvue/catalog.py` (`DataReference`, `DataCatalog`, builders, math references)
- `dvue/actions.py` (default actions + extension points)
- `README.md` and `docs/Architecture.md` (usage and architecture)

## Conventions And Pitfalls

- Keep API surface backward-compatible where practical; downstream apps subclass these components.
- Ensure Panel/HoloViews extensions are initialized consistently when changing startup behavior.
- When adding catalog attributes used for filtering/grouping, document them and keep table/map selection flow intact.
- If changing math-reference behavior, update examples and docs linked from `README-mathref.md`.

## TransformToCatalogAction — naming contract

`TransformToCatalogAction` (in `dvue/actions.py`) converts active UI transforms into a
`MathDataReference` and adds it to the live catalog. The name format is:

```
[s{source_num}_]{pk_values}__{tag}
```

- `__` (double underscore) is the separator between pk_values and tag. It is unambiguous
  because pk-value sanitisation collapses all non-alphanumeric runs to a single `_`,
  so `__` can never appear inside the pk_values part.
- `s{n}_` prefix: added only when `len(catalog._source_index) > 1` (multi-source catalog).
  `n` = `catalog._source_index[orig_ref.source]`.
- `{pk_values}`: values of `catalog.primary_key` columns (excluding `source_num`) read
  from the original ref's attributes, joined with `_`, sanitised to a valid Python identifier.
- Short tags: `tf`, `1D_mean`, `r24H_mean`, `diff`, `diffN`, `cumsum`, `x{factor}`.
  See `.github/transform-to-catalog-plan.md` for the full tag table.
- The derived ref's `name` also serves as an expression token in `MathDataReference`
  (variable resolution priority #3 — direct name lookup in attached catalog).

When subclassing `TimeSeriesDataUIManager`, declare `primary_key` when constructing
the `DataCatalog` — not on the manager class:

```python
def _build_catalog(self):
    # Single-source catalog
    self._dvue_catalog = DataCatalog(primary_key=["station", "variable"])
    # Multi-source catalog (source_num auto-computed from ref.source)
    self._dvue_catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
```

`primary_key` is required at `DataCatalog()` construction. It controls:
- Uniqueness enforcement (`ValueError` on duplicate pk-tuple)
- Auto-naming of refs when `name=""` is not provided
- `source_num` prefix in `TransformToCatalogAction` derived names
- `catalog.get(station="A", variable="discharge")` keyword lookup

## Implementation Gotchas For Subclasses

### `name` is the catalog key — never mutate it directly

`DataCatalog._references` is an `OrderedDict` keyed by `ref.name`.  If you mutate
`ref.name` directly the dictionary key becomes stale and every subsequent
`catalog.get(ref.name)` raises `KeyError`.

**Always rename via `catalog.rename(old_name, new_name)`**, which atomically updates
both the key and `ref.name`:

```python
catalog.rename("JER__tf", "JER_tidal_filtered")  # chainable, raises KeyError / ValueError on conflicts
```

`rename()` does **not** affect `_source_index` — the source (file path) is unchanged.

### `name` vs display label in mixed catalogs

`name` serves two roles that can conflict:

| Role | Raw DSS example | Transform ref example |
|------|-----------------|----------------------|
| Catalog lookup key | `"study.dss::/A/JER/EC//15MIN/F/"` | `"JER__tf"` |
| User-visible display | *ugly — should be hidden* | *clean — should be shown* |

**General rule** (handled automatically by `get_table_column_width_map()`):

When math refs are present the table automatically gains a `name` column so users
can see what their transform refs are called.  For managers whose raw-ref names
are human-readable (e.g. `"RSAC075_flow"`) this is sufficient.

**For managers with ugly raw-ref names** (e.g. `DSSDataUIManager` where the raw-ref
key is `"filename::pathname"`), override `get_data_catalog()` to inject a `label`
column:

```python
def get_data_catalog(self):
    df = self._dvue_catalog.to_dataframe().reset_index()
    # blank for raw refs (identified by domain columns); catalog key for derived refs
    df["label"] = df.apply(
        lambda r: r["name"] if r.get("ref_type", "raw") != "raw" else "",
        axis=1,
    )
    return df
```

Add `"label"` to `_get_table_column_width_map()` so it appears first in the table.
When a `label` column is present the framework suppresses the generic `name`
injection, so the two columns never double-up.

### `get_data_reference` — always use `row["name"]`

The safest lookup pattern works for **both** raw and math refs:

```python
def get_data_reference(self, row):
    if "name" in row.index:
        return self._dvue_catalog.get(row["name"])
    return self._dvue_catalog.get(self.build_ref_key(row))  # fallback without reset_index()
```

**Never** branch on `pd.isna(row["filename"])` to detect math refs.
`TransformToCatalogAction` copies *all* original-ref attributes (including
`filename`, `source`, domain columns) into the new `MathDataReference`, so the
NaN guard silently falls through to a wrong key for every transform ref.

The `name` column is always present in the DataFrame returned by
`get_data_catalog()` because the base implementation calls
`catalog.to_dataframe().reset_index()`, which promotes the catalog-key index into
a regular column.

### Mixed catalogs: NaN source for math references

Rows produced by `to_dataframe().reset_index()` have `NaN` in the `source` column
for math references, because derived references carry `source="transform"` or `""`.

**`get_unique_short_names` must not receive NaN paths**

`TimeSeriesPlotAction.render` calls `get_unique_short_names(df["source"].unique())` when
`source_num` is a column in the catalog DataFrame. If math-reference rows are in the
selection, the unique values include `NaN`, which causes `os.path.normpath(NaN)` to raise
`TypeError`. Filter NaN before passing:

```python
valid_sources = [s for s in df["source"].unique() if not pd.isna(s)]
short_names = get_unique_short_names(valid_sources)
```

### Time-range-aware readers — contract and efficiency

`DataReference.getData(time_range=...)` passes `time_range` through `_load_data` into
`reader.load(time_range=..., **attrs)` as a keyword argument alongside all other reference
attributes.  Whether the reader uses it efficiently depends entirely on the backend API:

| Backend API | Time-range behaviour |
|-------------|----------------------|
| `DSSFile.read_rts / read_its(path, start, end)` | **File-level windowing** — only the requested bytes are read |
| HDF5 `get_data_for_catalog_entry(entry, time_window)` | **File-level windowing** |
| `pyhecdss.get_ts(filename, path)` | No time-range parameter — full series loaded, slice in memory |
| `pyhecdss.get_matching_ts(filename, path)` | No time-range parameter — full series loaded, slice in memory |
| In-memory / callable readers | By definition full series — slice in memory |

**Rule**: if the backend API supports native time windowing (e.g. `DSSFile.read_rts`), use
it so that only the requested bytes are read from disk.  If the backend has no such API
(e.g. high-level `pyhecdss.get_ts`), read the full series and slice by `time_range` before
returning — do not skip the slice:

```python
def load(self, **attributes) -> pd.DataFrame:
    time_range = attributes.get("time_range")
    df = _load_full_series(attributes)      # only option with pyhecdss.get_ts
    if time_range is not None:
        start, end = pd.Timestamp(time_range[0]), pd.Timestamp(time_range[1])
        df = df.loc[start:end]
    return df
```

The `DataReference` cache is keyed by `(start, end)` so each unique window is stored
independently and subsequent calls for the same window are served from memory.

### `get_data_for_time_range` is a legacy hook — do not implement in new subclasses

The `TimeSeriesDataUIManager.get_data(df)` method chooses its data-loading path based on
whether `data_catalog` is set:

```python
if data_catalog is not None:
    data = self.get_data_reference(r).getData(time_range=self.time_range)  # preferred
else:
    data, _, _ = self.get_data_for_time_range(r, self.time_range)          # legacy only
```

Any manager that sets `data_catalog` (via the `data_catalog` property) **never calls
`get_data_for_time_range`**.  Do not override it in new subclasses — it is dead code
once a catalog is wired up.  Remove any existing overrides that only duplicated the
`getData(time_range=...)` logic.

## Subclass Migration Guide — primary_key / source_num Redesign

This is a **breaking** redesign. The following concepts have been removed:
- `DataReference.ref_key()`, `set_key_attributes()`, `get_key_attributes()`, `_key_attributes`
- `TimeSeriesDataUIManager.identity_key_columns`, `url_column`, `url_num_column`,
  `display_url_num`, `_apply_url_num()`
- `url_num` dynamic metadata on refs

### DataReference subclasses

Remove any calls to `set_key_attributes()` and any overrides of `ref_key()` — both removed.
No other changes required.

### CatalogBuilder / DataCatalog construction

Old:
```python
catalog = DataCatalog()
ref.set_key_attributes(["station", "variable"])
catalog.add(ref)
```

New:
```python
catalog = DataCatalog(primary_key=["station", "variable"])
catalog.add(ref)  # name auto-derived from pk values; no per-ref key attributes
```

### TimeSeriesDataUIManager subclasses

1. Remove `url_column` and `url_num_column` from `super().__init__()` call.
2. Remove `identity_key_columns` param from the subclass.
3. Remove `display_url_num` usage — replace with `"source_num" in df.columns`.
4. Remove `_apply_url_num()` calls — `source_num` is auto-computed by the catalog.
5. Add `primary_key=[...]` when constructing `DataCatalog` in `_build_catalog()`.
6. NaN guard in `get_data_reference(row)`: use `row.get("source")` not `row.get("filename")`.
7. In plot actions: replace `manager.url_column` → `"source"` and
   `manager.display_url_num` → `"source_num" in dfcat.columns`.

### Math ref YAML files

Replace `url_num:` with `source_num:` in all `search_map` criteria blocks:

```yaml
# Old
search_map:
  x:
    variable: flow
    url_num: 0

# New
search_map:
  x:
    variable: flow
    source_num: 0
```

A migration shim in `MathDataCatalogReader` reads old `url_num` keys as `source_num`
and emits a deprecation warning, so existing YAML files load with a warning rather
than silently failing.

## Downstream Integration Notes

- `dms_datastore_ui` uses `TimeSeriesDataUIManager` subclassing and custom actions.
- For integration-specific behavior, update downstream project code rather than adding domain logic in `dvue`.
- See `../dms_datastore_ui/AGENTS.md` for datastore dashboard conventions.
