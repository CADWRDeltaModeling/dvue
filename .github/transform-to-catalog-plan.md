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

> ⚠️ **SUPERSEDED**: The naming logic in Phase 5 (`identity_key_columns`,
> `set_key_attributes()`, `url_num`) has been replaced by the `primary_key` +
> `source_num` redesign. The transform tag table and `__` separator remain unchanged;
> the source of the identity key and the file prefix have changed. See the updated
> Name Format below and `AGENTS.md` for the full migration guide.

## Name Format (current)

```
[s{source_num}_]{pk_values}__{tag}
```

| Part | When present | Source |
|---|---|---|
| `s{source_num}_` | Multi-source catalog (`len(catalog._source_index) > 1`) | `catalog._source_index[orig_ref.source]` |
| `{pk_values}` | Always | Values of `catalog.primary_key` cols (excluding `source_num`), joined `_`, sanitised |
| `__{tag}` | At least one active transform | From `_build_expression_and_tag()` |

Examples:
- Single-source catalog, `primary_key=["station","variable"]`, station=`RSAC054`, variable=`FLOW` → `RSAC054_FLOW__1D_mean`
- Multi-source catalog, `primary_key=["source_num","station","variable"]`, source_num=1 → `s1_RSAC054_FLOW__1D_mean`

## Name Format (superseded — Phase 5 original)

```
[f{url_num}_]{identity_key}__{tag}
```

| Part | When present | Source (old) |
|---|---|---|
| `f{url_num}_` | Multi-file catalog (`display_url_num=True`) | `orig_ref.get_dynamic_metadata("url_num")` |
| `{identity_key}` | Always | `orig_ref._key_attributes` or `manager.identity_key_columns` or `ref_key()` |
| `__{tag}` | At least one active transform | From `_build_expression_and_tag()` |

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

3. **`__` in sanitised keys** — Primary key value sanitisation uses
   `re.sub(r"[^a-zA-Z0-9]+", "_", ...)`. A run of two or more non-alphanumeric chars
   produces a single `_`, not `__`. So `__` inside the identity part is structurally
   impossible — the separator is unambiguous.

4. **`expression` in name** — `MathDataReference` stores `expression` in `_attributes`.
   The auto-derived `name` is based on `primary_key` values only — `expression` is
   never included in the name. No `set_key_attributes()` call needed.

5. **Catalog key collision** — two selections with the same pk values and tag produce
   the same name. The callback calls `catalog.remove(name)` before `catalog.add(ref)`
   to replace silently. Callers adding the same transform twice always get the freshest ref.

6. **`source_num` prefix** — `source_num` is auto-computed by the catalog from
   `_source_index[ref.source]`. Single-source catalogs have no `source_num` column
   and no `s{n}_` prefix, keeping names clean.
