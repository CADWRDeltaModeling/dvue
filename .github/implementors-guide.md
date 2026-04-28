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
      url = row.get("url", None)        # or "filename"
      if pd.isna(url):
          return catalog.get(row["name"])   # math ref → name lookup
      return catalog.get(self.build_ref_key(row))
  ```
- `get_unique_short_names()` raises `TypeError` on NaN paths. Filter first:
  ```python
  valid = [f for f in df[manager.url_column].unique() if not pd.isna(f)]
  short_names = get_unique_short_names(valid)
  ```
- When reading selected rows from the display table, use `dataui._dfcat.iloc[selection]`
  **not** `display_table.value.iloc[selection]` — the display value strips the `name` column,
  which breaks math ref key lookup.

---

## 2. url_column / url_num — Source Discrimination

`TimeSeriesDataUIManager` uses `url_column` (default `"url"`) to identify which catalog
column holds the source URL/filepath, and injects a `url_num` integer discriminator
when multiple sources are loaded.

| Attribute | Purpose |
|-----------|---------|
| `url_column` | Name of the catalog column holding the source URL (subclass-controlled; e.g. `"filename"`, `"source"`) |
| `url_num_column` | Name of the injected integer column (default `"url_num"`) |
| `display_url_num` | Set to `True` when >1 unique URL is present |

`_apply_url_num(df, catalog=None)` is called on every `get_data_catalog()` rebuild.
When `catalog` is provided it also calls `ref.set_dynamic_metadata("url", ...)` and
`ref.set_dynamic_metadata("url_num", ...)` on each ref, making them available to
`catalog.search()` and math ref `search_map`.

**Subclass pattern** (pass the correct column name to `super().__init__`):
```python
super().__init__(url_column="filename", **kwargs)   # or "source", etc.
```

**Never reset `display_url_num = False` after `super().__init__()`** — the base class
sets it correctly inside `_apply_url_num`.

---

## 3. DataReference.matches() — Dynamic Metadata

`matches(**criteria)` checks `_attributes` first, then falls back to `_dynamic_metadata`
for any key not present in `_attributes`. Static attributes always win on conflict.

This means math ref `search_map` criteria can include `url_num` to pin a variable to
a specific source file — but only after `_apply_url_num` has been called:

```yaml
search_map:
  x:
    variable: flow
    url_num: 0    # ← only refs from the first loaded file
```

Inject dynamic metadata via `ref.set_dynamic_metadata(key, value)`. Read it back with
`ref.get_dynamic_metadata(key, default=None)`.

### Type coercion in matches()

The math ref editor's search map text input always produces **string values** (e.g.
`url_num=0` becomes the string `'0'`). `matches()` handles this automatically: when
`actual != expected` and their types differ, it attempts `type(actual)(expected)` before
returning `False`.

- `url_num=0` (int in dynamic metadata) matched with `'0'` (string from editor) → `int('0') == 0` ✓
- `geoid='437'` (string attribute) matched with `437` (int) → `str(437) == '437'` ✓

**Do not coerce values in the parser** (`_parse_search_map`). Type coercion belongs
exclusively in `matches()` so the logic is in one place and both typed (YAML-loaded)
and untyped (editor text) criteria work without special handling.

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
- `TestUrlNumSearchable.test_math_ref_search_map_filters_by_url_num` is the canonical
  integration test for the full `url_num → dynamic metadata → search → resolve` pipeline.

---

## 10. Downstream Subclass Checklist (e.g. dsm2ui)

When updating a `TimeSeriesDataUIManager` subclass for a breaking dvue change:

| Item | Check |
|------|-------|
| `super().__init__()` kwargs use current names (`url_column=`, not `filename_column=`) | |
| `build_station_name()` uses `display_url_num` not `display_fileno` | |
| Row lookups use `dataui._dfcat.iloc[selection]` | |
| Math ref rows guarded with `pd.isna(url)` before key construction | |
| No local `display_url_num = False` after `super().__init__()` | |
| `get_data_reference()` handles NaN url (math refs) | |

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
