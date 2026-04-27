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
