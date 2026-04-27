# Plan: dvue Math Reference Editor

## Status

- ✅ Phase 1 — YAML download format fix; table refresh on first save
- ✅ Phase 2 — `ref_type` class attr; `_has_math_refs`; `_enrich_catalog_with_math_ref_hints`
- ✅ Phase 3a — Layout reorder (YAML to bottom)
- ✅ Phase 3b-FIX — Test Expression result placed immediately below button
- ✅ Phase 3c — Attr picker per search-map row
- ✅ Phase 3d — Per-variable ▶ Test button with inline match badge
- ✅ Phase 3e — Criteria pre-fill from selected catalog row
- ✅ Phase 3f — Expression alias hint buttons
- ✅ Phase 3g — Save keeps editor open, clears form
- ✅ Phase 3h — Remove catalog names section
- ✅ Phase 4 — Tests: YAML round-trip, `ref_type`, enrich helper
- ✅ Phase 5 — Stale catalog cache fix; `ClearCacheAction` registered
- ✅ Phase 6 — Plot/Download use `_dfcat` rows; Test Expression respects `time_range`
- ✅ Phase 7 — Pre-populate fix; edit-mode title; rename logic; "Save to Catalog" label
- ✅ Phase 8 — vtools filter functions in expression namespace; non-string column normalisation
- ✅ Phase 9 — Column picker in Table Options; `ref_type` auto-show for mixed catalogs
- ✅ Phase 10 — Rename `_require_single` → `match_all` in YAML; warn on silently dropped matches
- ✅ Phase 11 — `match_all` upload bug; `match_all` variables always DataFrame
  - `MathDataCatalogReader.build_from_data(data, parent_catalog)` is the canonical parser — never duplicate this logic
  - `match_all=True` always resolves to DataFrame; use `.iloc[:,N]` / `.mean(axis=1)`, never Series ops
- ✅ Phase 12 — `filename_column`→`url_column`, `FILE_NUM`→`url_num` rename; `url_num` as dynamic metadata
  - `_apply_url_num(df, catalog)` injects `url`/`url_num` dynamic metadata; enables `search_map: {x: {url_num: 0}}`
  - `DataReference.matches()` falls back to `_dynamic_metadata` (`_attributes` wins on conflict)
- ✅ Phase 13 — `matches()` type coercion; `match_all` axis=0 fallback warning
  - `matches()` tries `type(actual)(expected)` on type mismatch — editor string `'0'` matches int `url_num=0`
  - Never coerce in `_parse_search_map`; coercion belongs in `matches()` only
  - `pd.concat(axis=1)` failure warns about `iloc[:,N]` breakage (misaligned time indices)

---

## Editor Layout

```
title
help
name_input
expression_input
[alias hint buttons row]
attrs_input
─────────────────────────────
search_map_section
  row: alias | match-all | criteria | attr-picker | ▶ | ✕
  └─ per-row result pane (badge + match list)
─────────────────────────────
[Test Expression button]
test_result_md
─────────────────────────────
attr_browser_md
─────────────────────────────
yaml_section
─────────────────────────────
status_md                 ← Save / Upload confirmations only
Row(save, cancel)
```

---

## Key Files

| File | Changes |
|------|---------|
| `dvue/math_ref_editor.py` | Editor UI (Ph 3–7); upload → `build_from_data()` (Ph 11); `_parse_search_map` keeps string values (Ph 13) |
| `dvue/math_reference.py` | `ref_type="math"`; `match_all` YAML key; `build_from_data()`; `match_all` always DataFrame; axis=0 fallback warning |
| `dvue/catalog.py` | `ref_type` attr + `to_dataframe()` column; `matches()` dynamic metadata fallback + type coercion |
| `dvue/tsdataui.py` | `_has_math_refs()`; `_enrich_catalog_with_math_ref_hints()`; cache fix; `ClearCacheAction`; `url_column`/`url_num_column`/`display_url_num`; `_apply_url_num()` |
| `dvue/actions.py` | Plot/Download use `_dfcat` rows; `ClearCacheAction` registered |
| `dvue/dataui.py` | `hidden_columns` init; column picker MultiChoice in Table Options |
| `examples/ex_tsdataui.py` | `ref_type` column; base helpers |
| `dsm2ui/dsm2ui.py` | Math ref curve labels; `url_column`/`display_url_num` |
| `dsm2ui/dssui/dssui.py` | `url_column` kwarg |
| `dsm2ui/deltacdui/deltacdui.py` + `deltacduimgr.py` | `url_column` kwarg |
| `tests/test_catalog.py` | `TestRefType`; `TestSaveMathRefsRoundTrip`; `TestMathDataCatalogReaderBuildFromData`; `TestResolveVariablesMatchAllType`; `TestMatchesDynamicMetadata` |
| `tests/test_math_ref_editor.py` | YAML format; enrich; `TestUploadYamlMatchAll` |
| `tests/test_tsdataui.py` | `url_num`/`display_url_num`; cache rebuild; `TestUrlNumSearchable` |

