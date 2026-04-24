# Plan: dvue Math Reference Editor Improvements

## Status
- ✅ Phase 1 — Bug fixes (YAML download format, table refresh) — **DONE**
- ✅ Phase 2 — Base class integration (`ref_type`, `_has_math_refs`, `_enrich_catalog_with_math_ref_hints`) — **DONE**
- ✅ Phase 3a — Layout reorder (YAML to bottom) — **DONE**
- ✅ Phase 3c — Attr picker per search-map row — **DONE**
- 🔲 Phase 3b-FIX — Test Expression result is invisible
- 🔲 Phase 3d — Per-variable ▶ Test button (badge + match list + attr auto-fill)
- 🔲 Phase 3e — Criteria pre-fill from selected catalog table row
- 🔲 Phase 3f — Expression alias hint buttons
- 🔲 Phase 3g — Save keeps editor open, clears form
- 🔲 Phase 3h — Remove catalog names section
- ✅ Phase 4 — Tests (YAML round-trip, ref_type, enrich helper) — **DONE**

---

## Phase 1 — Bug Fixes ✅

### 1a. Fix YAML download format mismatch — DONE
- `_yaml_download_callback()` now calls `save_math_refs()` internally, producing the
  canonical flat format. The old nested `{criteria: {...}, require_single: ...}` /
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
| `dvue/math_ref_editor.py` | Phase 3b-FIX, 3d, 3e, 3f, 3g, 3h |
| `dvue/catalog.py` | `ref_type` class attr, `to_dataframe()` `ref_type` column ✅ |
| `dvue/math_reference.py` | `MathDataReference.ref_type = "math"` ✅ |
| `dvue/tsdataui.py` | `_has_math_refs()`, `_enrich_catalog_with_math_ref_hints()` ✅ |
| `examples/ex_tsdataui.py` | Uses base helpers, `ref_type` column in table ✅ |
| `tests/test_catalog.py` | `TestRefType`, `TestSaveMathRefsRoundTrip` ✅ |
| `tests/test_math_ref_editor.py` | YAML + enrich tests ✅ |
