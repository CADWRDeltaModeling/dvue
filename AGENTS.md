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

## Implementation Gotchas For Subclasses

### Mixed catalogs: math references alongside raw references

When a `DataCatalog` contains both raw `DataReference` entries and `MathDataReference` entries (mixed catalog), rows produced by `to_dataframe().reset_index()` will have `NaN` in file/source columns for math references, because those columns do not apply to derived references.

**`get_data_reference` must guard against NaN filename**

Raw entries are keyed by a composite like `{filename}::{path}`. Math entries are keyed by their `name` alone. The safe pattern is to check the `"name"` column first:

```python
def get_data_reference(self, row):
    if "name" in row.index:
        return self._dvue_catalog.get(row["name"])
    return self._dvue_catalog.get(self._ref_name(row))
```

If your manager always builds the key from a file/source column (as `DSSDataUIManager` does with `build_ref_key`), you must explicitly handle NaN before building the key:

```python
def get_data_reference(self, row):
    filename = row.get("filename", None)
    if pd.isna(filename):
        return self._dvue_catalog.get(row["name"])
    return self._dvue_catalog.get(self.build_ref_key(row))
```

Failing to do this produces a `KeyError: "No DataReference named 'nan::...' in catalog."`.

**`get_unique_short_names` must not receive NaN paths**

`TimeSeriesPlotAction.render` calls `get_unique_short_names(df[filename_column].unique())` when `display_fileno` is `True`. If math-reference rows are in the selection, the unique values include `NaN` (a float), which causes `os.path.normpath(NaN)` to raise `TypeError`. Filter NaN before passing to `get_unique_short_names`:

```python
valid_files = [f for f in df[filename_column].unique() if not pd.isna(f)]
short_names = get_unique_short_names(valid_files)
```

`file_index_map.get(NaN_key, "")` returns `""` automatically, so math-ref rows get no file-index label without extra code.

## Downstream Integration Notes

- `dms_datastore_ui` uses `TimeSeriesDataUIManager` subclassing and custom actions.
- For integration-specific behavior, update downstream project code rather than adding domain logic in `dvue`.
- See `../dms_datastore_ui/AGENTS.md` for datastore dashboard conventions.
