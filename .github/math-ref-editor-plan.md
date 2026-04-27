# Plan: dvue Math Reference Editor Improvements

## Status
- ✅ Phase 1 — Bug fixes (YAML download format, table refresh) — **DONE**
- ✅ Phase 2 — Base class integration (`ref_type`, `_has_math_refs`, `_enrich_catalog_with_math_ref_hints`) — **DONE**
- ✅ Phase 3a — Layout reorder (YAML to bottom) — **DONE**
- ✅ Phase 3b-FIX — Test Expression result visibility — **DONE**
- ✅ Phase 3c — Attr picker per search-map row — **DONE**
- ✅ Phase 3d — Per-variable ▶ Test button — **DONE**
- ✅ Phase 3e — Criteria pre-fill from selected catalog table row — **DONE**
- ✅ Phase 3f — Expression alias hint buttons — **DONE**
- ✅ Phase 3g — Save keeps editor open, clears form — **DONE**
- ✅ Phase 3h — Remove catalog names section — **DONE**
- ✅ Phase 4 — Tests (YAML round-trip, ref_type, enrich helper) — **DONE**
- ✅ Phase 5 — Stale catalog cache fix + ClearCacheAction — **DONE**
- ✅ Phase 6 — Plot/Download use `_dfcat` rows; Test Expression respects time_range — **DONE**
- ✅ Phase 7 — Pre-populate fix; edit-mode title; rename logic; Save to Catalog label — **DONE**
- ✅ Phase 8 — vtools filter functions in expression namespace; non-string column normalisation — **DONE**
- ✅ Phase 9 — Column picker + `ref_type` auto-show for mixed catalogs — **DONE**
- ✅ Phase 10 — Rename `_require_single` → `match_all` in YAML; warn on silently dropped matches — **DONE**
- ✅ Phase 11 — `match_all` upload parsing bug; `match_all` variables always DataFrame — **DONE**
  - `MathDataCatalogReader.build_from_data(data, parent_catalog)` — canonical parser for in-memory YAML lists; `_on_upload_yaml` delegates here (never duplicate this logic)
  - `match_all=True` variables always resolve to DataFrame (even for 1 match); expressions must use DataFrame semantics (`.iloc[:,N]`, `.mean(axis=1)`, etc.)
- ✅ Phase 12 — `url_column`/`url_num` rename; `url_num` dynamic metadata; `matches()` dynamic fallback — **DONE**
  - `filename_column` → `url_column`, `FILE_NUM` → `url_num` throughout dvue + dsm2ui
  - `_apply_url_num(df, catalog)` injects `url`/`url_num` as dynamic metadata on each `DataReference`; enables `catalog.search(url_num=0)` and math ref `search_map: {x: {url_num: 0}}`
  - `DataReference.matches()` falls back to `_dynamic_metadata` for keys absent from `_attributes` (`_attributes` wins on conflict)

---

## Phase 1 — Bug Fixes ✅

### 1a. Fix YAML download format mismatch — DONE
- `_yaml_download_callback()` now calls `save_math_refs()` internally, producing the
  canonical flat format. The old nested `{criteria: {...}, match_all: ...}` /
  `attributes: {...}` structure is gone.

### 1b. Fix table refresh on first math ref save — DONE
- `_on_save()` now also updates `display_table.widths` and `display_table.header_filters`
  so the `expression` column gets its width and filter when the first math ref is added.

---

## Phase 2 — Base Class Integration ✅

### 2a. `ref_type` class-level attribute
- `DataReference.ref_type = "raw"` (class-level, overridable with a one-liner)
- `MathDataReference.ref_type = "math"`
- Subclasses: `class MyRef(DataReference): ref_type = "my_type"`

### 2b. `ref_type` column in `DataCatalog.to_dataframe()`
- All catalogs now include a `"ref_type"` column automatically.

### 2c. Base-class helpers on `TimeSeriesDataUIManager`
- `_has_math_refs()` — checks `self.data_catalog` via `ref.ref_type != "raw"`.
- `_enrich_catalog_with_math_ref_hints(df)` — fills blank `expression` cells for raw
  rows with the ref's catalog key; no-op when no `expression` column exists.
- `examples/ex_tsdataui.py` updated to call the base helpers, also exposes `ref_type`
  as a table column and filter.

---

## Phase 3 — Editor UI Improvements

### ✅ 3a. Layout reorder — DONE
New order: title → help → name → expression → attrs → search map → Test button →
attr browser → YAML → status → Save/Cancel

### ✅ 3c. Attr picker per row — DONE
Each search-map row has a `Select` widget populated from catalog column names.
Selecting an attr appends `attr=` to the criteria text input.

---

### 🔲 3b-FIX. Test Expression result is invisible

**Bug**: `status_md` (where results appear) is at the bottom of a long scrollable panel.
Users never see the output after clicking Test Expression.

**Fix**:
- Add `test_result_md = pn.pane.Markdown("")` placed **immediately below** the
  Test Expression button.
- Expression test output goes to `test_result_md`.
- `status_md` at the bottom is reserved for Save / Upload confirmations only.

---

### 🔲 3d. Per-variable ▶ Test button

Each search-map row gets a **▶** button. On click:

1. Runs `catalog.search(**criteria)` for that row's parsed criteria.
2. Shows an inline badge below the row:
   - `"✅ 1 match"` → auto-fills Attributes textarea (if blank) with the match's
     identifying attributes
   - `"⚠️ N matches"` (N > 1) → shows badge + list, attrs **not** touched
   - `"❌ 0 matches"` → shows badge, attrs **not** touched
3. Expands a `pn.pane.Markdown` list of up to 8 matching ref names below the row.
4. The alias becomes immediately available in the expression alias hint (3f) regardless
   of test result.

**Identifying attributes** (used for auto-fill and criteria pre-fill):
- Include: all string-valued non-system columns (`station_id`, `station_name`,
  `variable`, `unit`, `interval`, `subloc`, `param`, etc.)
- Exclude: `source`, `name`, `ref_type`, `expression`, `geometry`, and any numeric /
  year / path columns (`start_year`, `max_year`, `filename`, `file`, etc.)
- Implementation: hardcoded exclusion set + dtype check (`object` dtype only)

---

### 🔲 3e. Criteria pre-fill from selected catalog table row

When `"+ Add variable"` is clicked and a row is selected in the catalog table:
- Pre-fill the new row's criteria input with that row's identifying attributes as
  `attr=val, attr=val` (same filter as 3d).
- Alias input left blank for the user to name.

---

### 🔲 3f. Expression alias hint buttons

- A `pn.Row` container placed **below the Expression textarea** holds one small
  `pn.widgets.Button` per defined alias.
- Aliases appear as soon as the alias input is non-empty (no test required).
- Clicking a button appends the alias name to the end of `expr_input.value`.
- The row updates whenever aliases are added, removed, or renamed (redraw `.objects`).
- Prefix label: `"Aliases:"` as a `pn.pane.Markdown`.

---

### 🔲 3g. Save behavior — keep editor open, clear form

After Save:
- Clear Name, Expression, Attributes fields.
- Search Map rows **stay** for immediate reuse.
- `is_edit` resets to `False`.
- `status_md` shows `"✅ Created <name>"` or `"✅ Updated <name>"`.

---

### 🔲 3h. Remove catalog names section

- Remove `catalog_names` `pn.pane.Markdown` entirely (too long, not useful).
- Per-row test results + attr browser are sufficient reference.

---

## Final Layout Order

```
title
help
name_input
expression_input
[alias hint buttons row]       ← 3f: clickable alias insert buttons
attrs_input
─────────────────────────────
search_map_section
  row: alias | join-all | criteria | attr-picker | ▶ | ✕
  └─ per-row result pane       ← 3d: badge + match list (hidden until ▶ clicked)
─────────────────────────────
[Test Expression button]
test_result_md                 ← 3b-FIX: immediately below button
─────────────────────────────
attr_browser_md
─────────────────────────────
yaml_section
─────────────────────────────
status_md                      ← Save / Upload confirmations only
Row(save, cancel)
```

---

## Phase 5 — Stale Catalog Cache Fix ✅

### Root cause
`TimeSeriesDataUIManager.__init__` stored the catalog DataFrame in `_cached_catalog` once and never invalidated it. Any `catalog.add()` call (e.g. from math ref editor save) was invisible to subsequent `get_data_catalog()` calls in all managers that relied on the base-class path.

### Fix — `tsdataui.py`
- Extracted `_apply_fileno(df)` helper (was inline in `__init__`) to re-apply `FILE_NUM` column on demand.
- Changed `get_data_catalog()` priority:
  1. If `self.data_catalog is not None` → always rebuild fresh from live catalog (`super().get_data_catalog()`) + `_apply_fileno()`.
  2. Else fall back to `_cached_catalog` (legacy path for subclasses without `data_catalog` property).
- `_cached_catalog` is now only populated at init for legacy subclasses (no `data_catalog` property).

### Fix — `dsm2ui.py`
- Removed `DSM2TidefileUIManager.get_data_catalog()` override (`return self.dfcat`).
  The base class now rebuilds from `self._dvue_catalog` automatically.
- All other affected managers (`DSM2DataUIManager`, `DSSDataUIManager`, `DeltaCDNodesUIManager`, `DeltaCDUIManager`) benefit automatically — no per-class changes needed.

### ClearCacheAction wired in
- `ClearCacheAction` existed in `actions.py` but was never registered.
- Added as the last action in `TimeSeriesDataUIManager.get_data_actions()`.

---

## Phase 6 — Plot/Download Row Lookup + Test Expression Time Range ✅

### Bug: `nan::CHAN_1+2_UP/flow` KeyError on plot
`PlotAction.callback` and `DownloadDataAction.callback` used `display_table.value.iloc[selection]` to get selected rows. `display_table.value` is the display-column subset of `_dfcat` — it strips the `name` column. `get_data_reference(row)` then fell through to `_build_ref_key(row)`, which produced `nan::...` for math refs (no `filename` attribute).

**Fix (`actions.py`)**: Both callbacks now use `dataui._dfcat.iloc[selection]`. Integer selection indices are identical across `_dfcat` and `display_table.value`.

### Bug: Test Expression ignored time window
`_on_test` called `tmp_ref.getData()` with no `time_range`, loading full series.

**Fix (`math_ref_editor.py`)**: Pass `getattr(manager, "time_range", None)` to `getData()`.

### Math ref curve labels — `dsm2ui.py`
- `DSM2TidefileUIManager.build_station_name`: returns `row["name"]` for `ref_type == "math"`.
- `_TidefilePlotAction.create_curve`: uses catalog name as `crvlabel` for math refs.

---

## Phase 7 — Editor Pre-populate, Edit Mode, Rename ✅

### Bug: editor always opened blank even when a math ref row was selected
Pre-populate block read `display_table.value.iloc[selected[0]]` — `name` column stripped.

**Fix**: Read `dataui._dfcat.iloc[selected[0]]` so `name_val` resolves correctly and `pre_ref` is found in the catalog.

### Edit mode indicator
Title now shows `### Math Reference Editor — Editing: \`<name>\`` when opened from a selected math ref row.

### Rename logic
`_on_save` previously called `catalog.remove(name)` where `name` was the *new* name — old entry stayed on rename. New logic:
1. If editing and name changed → remove `pre_ref.name` (old) → status `"Renamed"`.
2. Remove new name if already exists (same-name update) → status `"Updated"`.
3. No prior entry → status `"Created"`.

### Save button label
`"Save"` → `"Save to Catalog"`.

---

## Phase 4 — Tests ✅

Added to `tests/test_catalog.py`:
- `TestRefType` (6 tests) — `ref_type` class attr, subclass override, `to_dataframe()` column
- `TestSaveMathRefsRoundTrip` (8 tests) — YAML round-trip via `save_math_refs` + `MathDataCatalogReader`

New file `tests/test_math_ref_editor.py`:
- `TestEnrichCatalogWithMathRefHints` (5 tests)
- `TestYamlDownloadFormat` (4 tests) — verifies canonical flat format and full round-trip

---

## Key Files

| File | Changes |
|------|---------|
| `dvue/math_ref_editor.py` | Ph 3–7: editor UI, pre-populate, rename, time_range test; auto-show `ref_type` on save; Ph 11: upload delegates to `build_from_data()` ✅ |
| `dvue/catalog.py` | `ref_type` class attr, `to_dataframe()` `ref_type` column ✅ |
| `dvue/math_reference.py` | `MathDataReference.ref_type = "math"`; `match_all` YAML key; warn on silently dropped matches; Ph 11: `build_from_data()`, `match_all` always DataFrame ✅ |
| `dvue/tsdataui.py` | `_has_math_refs()`, `_enrich_catalog_with_math_ref_hints()`, cache fix, `ClearCacheAction`, `ref_type` in column map, `_has_mixed_ref_types()`; Ph 12: `url_column`/`url_num_column`/`display_url_num`, `_apply_url_num()` ✅ |
| `dvue/catalog.py` | `ref_type` class attr, `to_dataframe()` `ref_type` column; Ph 12: `matches()` dynamic metadata fallback ✅ |
| `dvue/actions.py` | `PlotAction`/`DownloadAction` use `_dfcat` rows; `ClearCacheAction` registered ✅ |
| `dvue/dataui.py` | `hidden_columns` init in `create_data_table`; `_column_picker` MultiChoice in Table Options ✅ |
| `examples/ex_tsdataui.py` | Uses base helpers, `ref_type` column in table ✅ |
| `dsm2ui/dsm2ui.py` | Removed stale `get_data_catalog()` override; math ref curve labels; Ph 12: `url_column`/`display_url_num` ✅ |
| `dsm2ui/dssui/dssui.py` | Ph 12: `url_column` kwarg, `manager.url_column` ✅ |
| `dsm2ui/deltacdui/deltacdui.py` + `deltacduimgr.py` | Ph 12: `url_column` kwarg ✅ |
| `tests/test_catalog.py` | `TestRefType`, `TestSaveMathRefsRoundTrip`, `test_dataframe_non_string_columns_normalised`; Ph 11: `TestMathDataCatalogReaderBuildFromData`, `TestResolveVariablesMatchAllType`; Ph 12: `TestMatchesDynamicMetadata` ✅ |
| `tests/test_math_ref_editor.py` | YAML + enrich tests; Ph 11: `TestUploadYamlMatchAll` ✅ |
| `tests/test_tsdataui.py` | `url_num`/`display_url_num` tests; `ClearCacheAction`, cache rebuild; Ph 12: `TestUrlNumSearchable` ✅ |

---

## Phase 8 — vtools Filter Functions + Column Normalisation ✅

### vtools functions in expression namespace
- `math_reference.py` now attempts `from vtools.functions.filter import cosine_lanczos, godin, butterworth, lanczos, lowpass_cosine_lanczos_filter_coef` at import time (falls back to `vtools3`, silently skips if neither is installed).
- These are merged into `_MATH_NAMESPACE` so they are available in any `MathDataReference` expression (e.g. `godin(x1 + x2)`).

### Non-string column name normalisation in `_load_data`
- **Root cause**: `vtools.godin()` returns a DataFrame with integer column `0` when its input is a pandas Series. HoloViews rejects non-string column names with `DataError`.
- **Fix**: `_load_data` checks `all(isinstance(c, str) for c in result.columns)` and casts to strings when the condition fails.
- **Test**: `TestMathDataReference.test_dataframe_non_string_columns_normalised` in `tests/test_catalog.py`.

---

## Phase 9 — Column Picker + `ref_type` Auto-Show ✅

### Column visibility picker in Table Options
- `DataUI.create_view()` builds a `pn.widgets.MultiChoice` (`_column_picker`) listing all columns from `get_table_columns()`, placed in the existing "Table Options" sidebar tab.
- Checked = visible; unchecked = hidden (Tabulator `hidden_columns` — column stays in data for filtering).
- Initial state reflects the current hidden state of the Tabulator (so auto-hidden `ref_type` starts unchecked).
- On change, `display_table.hidden_columns` is updated to the unchecked set.

### `ref_type` auto-show/hide
- `TimeSeriesDataUIManager.get_table_column_width_map()` always appends `ref_type: "8%"` so it is present in the Tabulator data slice.
- `TimeSeriesDataUIManager._has_mixed_ref_types(df)` — static helper; returns `True` if `df["ref_type"].nunique() > 1`.
- `DataUI.create_data_table()` sets `hidden_columns=["ref_type"]` initially when catalog is homogeneous.
- When a math ref is saved in the editor, if catalog becomes mixed: `ref_type` is removed from `hidden_columns` and the picker value is updated to include it.
