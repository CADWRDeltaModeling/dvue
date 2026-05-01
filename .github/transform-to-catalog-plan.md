# Plan: Transform-to-Catalog Action & Clean Naming

## Status

- ✅ Phase 1 — Add transform params + pipeline to `TimeSeriesDataUIManager`
  - `resample_period` / `resample_agg` (mean, max, min, sum, std)
  - `rolling_window` / `rolling_agg` (mean, max, min, std)
  - `do_diff` / `diff_periods`
  - `do_cumsum`
  - `scale_factor`
  - `show_transform_to_catalog` (bool, default `True`)
  - Pipeline order: tidal filter → resample → rolling → diff → cumsum → scale
  - **Critical**: tidal filter must run on sub-daily data *before* resampling

- ✅ Phase 2 — Widget layout: "Transform" tab with labelled sections
  - Sections: Data Cleanup, Resampling, Smoothing, Derived, Scaling, Y-Axis
  - Resampling row: `resample_period` + `resample_agg`
  - Rolling row: `rolling_window` + `rolling_agg`
  - Diff row: `do_diff` + `diff_periods`

- ✅ Phase 3 — `TransformToCatalogAction` in `actions.py`
  - Builds chained pandas expression string from active transforms
  - Inherits all non-`source` attributes from the original `DataReference`
  - Tags `F` attribute: `"{original_F}+{tag}"` if `F` exists; else adds `"transform"` attr
  - Creates `MathDataReference` and adds to live catalog
  - Registers "Transform → Ref" in `get_data_actions()` (green/success button)

- ✅ Phase 4 — Attribute inheritance
  - All non-`source` attributes copied from the original ref
  - `F` attribute gets `+{tag}` appended to record what transform was applied
  - When original ref has no `F`, a separate `transform` attribute is set to the tag

- ✅ Phase 5 — Clean naming (`_build_ref_name` + `identity_key_columns`)
  - Problem: raw `DataReference.name` for DSS or file-backed refs is a verbose composite
    key (e.g. `d:\studies\file.dss::AREA/RSAC054/FLOW//1HOUR/V1/`) — unusable as a label
  - Solution: introduce `identity_key_columns` manager param + `set_key_attributes()` on refs
  - Name format: `[f{url_num}_]{identity_key}__{tag}`
  - `__` separator is unambiguous — `ref_key()` sanitiser never produces `__`
  - Short tags: see tag table below

---

## Transform Tag Shorthand

| Transform active | Tag |
|---|---|
| Tidal filter (cosine-Lanczos 40 h) | `tf` |
| `resample_period=1D, resample_agg=mean` | `1D_mean` |
| `resample_period=1H, resample_agg=max` | `1H_max` |
| `rolling_window=24H, rolling_agg=mean` | `r24H_mean` |
| `do_diff=True, diff_periods=1` | `diff` |
| `do_diff=True, diff_periods=3` | `diff3` |
| `do_cumsum=True` | `cumsum` |
| `scale_factor=2.0` | `x2.0` |
| `scale_factor=0.3048` | `x0.3048` |

Multiple active transforms are joined with `_`:
`resample_period=1D, scale_factor=0.3048` → `1D_mean_x0.3048`

---

## Name Format

```
[f{url_num}_]{identity_key}__{tag}
```

| Part | When present | Source |
|---|---|---|
| `f{url_num}_` | Multi-file catalog (`display_url_num=True`) | `orig_ref.get_dynamic_metadata("url_num")` |
| `{identity_key}` | Always | See priority chain below |
| `__{tag}` | At least one active transform | From `_build_expression_and_tag()` |

### Identity key priority chain

1. **`orig_ref._key_attributes` is not None** (set via `orig_ref.set_key_attributes([...])`)
   — the ref itself advertises which attributes form its identity. Use those.
2. **`manager.identity_key_columns` non-empty** (set on the manager instance)
   — the manager knows what columns are the identity for this catalog's ref type.
   Use those columns to read values from the original ref.
3. **Fallback** — neither source available. Use `orig_ref.ref_key()` (verbose but safe).

Examples:
- DSS ref with `B="RSAC054"`, `C="FLOW"`, `identity_key_columns=["B","C"]` → `RSAC054_FLOW`
- Ref with `set_key_attributes(["station","variable"])` → `{station}_{variable}`
- No identity info → full `ref_key()` (all non-source attrs joined with `_`)

### Key attributes on the new MathDataReference

After building the name, the callback also calls
`new_ref.set_key_attributes(identity_cols + ["F"])` (or `["transform"]` when no `F`)
so that the math ref's own `ref_key()` returns the same clean short form, and
`expression` does not leak into its key.

---

## Design Decisions

| Question | Decision | Rationale |
|---|---|---|
| Name for the new math ref | `{identity_key}__{tag}` | Short, human-readable, avoids file path leakage |
| Separator between identity and tag | `__` (double underscore) | `ref_key()` sanitiser collapses all non-alphanumeric runs to single `_`, so `__` can never appear inside the identity part — safe as delimiter |
| How to communicate identity columns | `identity_key_columns` manager param + `set_key_attributes()` on refs | Ref knows its own identity best; manager provides a catalog-level fallback; both avoid hard-coding attribute names in the action |
| Attribute inheritance | Copy all attrs except `source`; tag `F` | Keeps derived refs groupable/searchable by the same station/variable criteria as the source |
| Tag `F` vs new `transform` attr | Tag `F` if present; else add `transform` attr | DSS users have `F` for version; non-DSS catalogs may not. Both approaches are searchable |
| Pipeline order | tidal filter before resample | Filter needs raw sub-daily data; resampling to ≥1D would silently neuter the filter |

---

## Files Changed

| File | Change |
|---|---|
| `dvue/tsdataui.py` | Added 10 new params, extended `_process_curve_data`, updated `get_widgets()` Transform tab, registered "Transform → Ref" action |
| `dvue/actions.py` | Added `TransformToCatalogAction` class with `_build_expression_and_tag`, `_identity_key_columns`, `_base_key`, `_build_ref_name`, `_refresh_table`, `callback` |
| `tests/test_tsdataui.py` | Updated tag assertions for shortened tags; replaced `_safe_ref_name` test; added `TestTransformToCatalogNaming` class (10 tests) |

---

## Gotchas

1. **PeriodIndex** from daily DSS records must be converted to `DatetimeIndex` before any
   transform step. Both `_process_curve_data` and the math expression work on DatetimeIndex.

2. **Tidal filter before resample** — if the pipeline resampled first and then attempted
   the cosine-Lanczos filter on already-daily data, the `>= 1D` guard would silently skip
   the filter. Always apply the filter on the original sub-daily series.

3. **`__` in sanitised keys** — `DataReference.ref_key()` sanitiser uses
   `re.sub(r"[^a-zA-Z0-9]+", "_", ...)`. A run of two or more non-alphanumeric chars
   produces a single `_`, not `__`. So `__` inside the identity part is structurally
   impossible — the separator is unambiguous.

4. **`expression` in key attributes** — `MathDataReference` stores `expression` in
   `_attributes`. Without `set_key_attributes()`, `expression` would be included in
   `ref_key()` and produce an enormous key. Always call `set_key_attributes()` on the
   new math ref, excluding `"expression"`.

5. **Catalog key collision** — two selections with the same identity and tag produce
   the same name. The callback calls `catalog.remove(name)` before `catalog.add(ref)`
   to replace silently. Callers adding the same transform twice always get the freshest ref.

6. **`display_url_num` guard** — `url_num` dynamic metadata is injected only when
   `get_data_catalog()` is called with `display_url_num=True` (multi-file catalogs).
   Single-file catalogs have no `url_num` → no prefix, keeping names clean.
