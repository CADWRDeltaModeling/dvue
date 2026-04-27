# Math Reference YAML Examples

Math references (`MathDataReference`) let you define derived time series as
Python expressions evaluated against catalog entries. This folder contains
ready-to-load YAML files covering the most common patterns.

Load any file via the **Math Ref** editor → **Upload YAML**, or in code:

```python
from dvue.math_reference import MathDataCatalogReader
refs = MathDataCatalogReader(parent_catalog=catalog).read("mathref_dss_cz_godin.yaml")
for ref in refs:
    catalog.add(ref)
```

---

## Files

### `mathref_dss_cz_godin.yaml` — vtools filter functions

Applies tidal filters from `vtools` to a single time series. Both filters are
available in every expression without any import.

```yaml
- name: godinx
  expression: godin(x)
  search_map:
    x:
      B: RSAC054
      C: STAGE
      E: 15MIN

- name: czx
  expression: cosine_lanczos(x, cutoff_period='40H')
  search_map:
    x:
      B: RSAC054
      C: STAGE
      E: 15MIN
```

**Built-in filter functions:** `godin`, `cosine_lanczos`, `butterworth`, `lanczos`.
All accept a Series or single-column DataFrame and return a same-length Series.

---

### `mathrefs_total_exports.yaml` — aggregation and chaining

Three entries demonstrating `match_all` aggregation and chaining math refs
together via `search_map`.

**`total_exports`** — sums all matching export time series into a daily total:

```yaml
- name: total_exports
  expression: exports.resample('1D').sum(axis=1)
  search_map:
    exports:
      A: FILL+CHAN
      C: FLOW-EXPORT
      E: 15MIN
      F: DWR-DMS-202312
      match_all: true
```

**`total_boundary_flows`** — same pattern for all boundary flow entries:

```yaml
- name: total_boundary_flows
  expression: boundary_flows.resample('1D').sum(axis=1)
  search_map:
    boundary_flows:
      A: FILL+CHAN
      C: FLOW
      E: 15MIN
      F: DWR-DMS-202312
      match_all: true
```

**`total_net_flow`** — chains the two refs above by searching for them in the
catalog via their `A`/`B` attributes. Both resolve to Series, so the
expression is plain subtraction:

```yaml
- name: total_net_flow
  expression: bflows - exports
  search_map:
    bflows:
      A: CALC
      B: TOTAL_BOUNDARY_FLOWS
    exports:
      A: CALC
      B: TOTAL_EXPORTS
```

Chaining works because all entries are loaded into the catalog before any
`getData()` call — order in the YAML does not matter.

**Key rules for `match_all` variables:**
- The resolved value is always a `pd.DataFrame`, even when only one entry matches.
- Use DataFrame methods: `.sum(axis=1)`, `.mean(axis=1)`, `.iloc[:,N]`.
- Do **not** write `exports - baseline` if `exports` is `match_all` — that is a
  DataFrame minus a Series and will fail unexpectedly.

---

### `mathref_url_num.yaml` — two-file comparison via `url_num`

When two files are loaded (e.g. baseline and scenario), dvue assigns each source
an integer `url_num` (0, 1, …) stored as dynamic metadata. Use it in
`search_map` to pin a variable to a specific file.

```yaml
- name: flow_diff__chipps__scenario_minus_baseline
  expression: flow_1 - flow_0
  search_map:
    flow_0:
      id: CHAN_437_UP
      variable: flow
      url_num: 0        # baseline file
    flow_1:
      id: CHAN_437_UP
      variable: flow
      url_num: 1        # scenario file
```

Both variables resolve to `pd.Series` (one entry each), so the expression is
plain arithmetic. Compare this to the `match_all` approach where you would
get a two-column DataFrame and need `.iloc[:,0]` / `.iloc[:,1]`.

**Three variants in this file:**
| Name | Expression | Use |
|------|-----------|-----|
| `flow_diff` | `flow_1 - flow_0` | instantaneous difference |
| `flow_cumsum_diff` | `np.cumsum((flow_1 - flow_0).resample('1h').mean())` | cumulative drift |
| `flow_pct_change` | `(flow_1 - flow_0) / flow_0.abs() * 100` | relative change (%) |

> **Note:** `url_num` is only available after the data files are loaded and
> `get_data_catalog()` has been called at least once. If you get "0 results",
> reload the catalog (Actions → Clear Cache) and try again.

---

## Expression Cheat Sheet

| Pattern | Expression | Notes |
|---------|-----------|-------|
| Unit conversion | `obs * 2.23694` | `obs` is a Series |
| Cumulative sum | `np.cumsum(x)` or `cumsum(x)` | `cumsum` is a built-in alias |
| Tidal low-pass | `godin(x)` | 30-hr + 24-hr cosine Lanczos cascade |
| Low-pass filter | `cosine_lanczos(x, cutoff_period='40H')` | arbitrary cutoff |
| Difference (two stations) | `a - b` | both are Series |
| Sum all matches | `ws.sum(axis=1)` | `ws` has `match_all: true` |
| Mean all matches | `ws.mean(axis=1)` | `ws` has `match_all: true` |
| Daily resample then sum | `x.resample('1D').sum(axis=1)` | `x` is a `match_all` DataFrame |
| Chain math refs | `a - b` where both resolved via `search_map` | both must be Series |
| Resample then cumsum | `np.cumsum((a - b).resample('1h').mean())` | hourly drift |
| Percent change | `(new - old) / old.abs() * 100` | both are Series |
| Z-score | `(obs - obs.mean()) / obs.std()` | single Series |

**Available names:** `np` (NumPy), `pd` (pandas), `godin`, `cosine_lanczos`,
`butterworth`, `lanczos`, `cumsum` (= `np.cumsum`).

---

## Common Mistakes

**`_require_single: false` does not work** — this is the old key name. Use
`match_all: true` inside the criteria block instead.

**`match_all` variable used as Series** — if `ws` has `match_all: true`, then
`ws` is a DataFrame. `ws - baseline` (Series) will broadcast unexpectedly.
Use `ws.mean(axis=1) - baseline` instead.

**`url_num` not matching** — criteria values from the editor are always strings,
but `url_num` is stored as an integer. `matches()` coerces automatically, so
`url_num: 0` in YAML (int) and `url_num=0` typed in the editor (string `'0'`)
both work.

**`url_num` returns 0 results on first load** — dynamic metadata is injected
when the catalog DataFrame is built. If the math ref is evaluated before any
`get_data_catalog()` call has run, the `url_num` metadata is absent. Trigger
a catalog refresh and retry.
