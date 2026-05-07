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

### Mixed catalogs: math references alongside raw references

When a `DataCatalog` contains both raw `DataReference` entries and `MathDataReference` entries (mixed catalog), rows produced by `to_dataframe().reset_index()` will have `NaN` in the `source` column for math references, because derived references have no file source.

**`get_data_reference` must guard against NaN source**

Raw entries are keyed by their auto-derived name (from `primary_key` values). Math entries are keyed by their `name` alone. The safe pattern:

```python
def get_data_reference(self, row):
    source = row.get("source", None)
    if pd.isna(source):
        return self._dvue_catalog.get(row["name"])  # math ref
    return self._dvue_catalog.get(row["name"])       # raw ref — name is always reliable
```

Failing to guard against NaN produces a `KeyError: "No DataReference named 'nan::...' in catalog."`.

**`get_unique_short_names` must not receive NaN paths**

`TimeSeriesPlotAction.render` calls `get_unique_short_names(df["source"].unique())` when
`source_num` is a column in the catalog DataFrame. If math-reference rows are in the
selection, the unique values include `NaN`, which causes `os.path.normpath(NaN)` to raise
`TypeError`. Filter NaN before passing:

```python
valid_sources = [s for s in df["source"].unique() if not pd.isna(s)]
short_names = get_unique_short_names(valid_sources)
```

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
