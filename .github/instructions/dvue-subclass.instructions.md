---
description: "Use when creating a subclass of dvue's TimeSeriesDataUIManager, DataUIManager, or DataUI. Covers required method overrides, initialization order, primary_key catalog patterns, source_num source discrimination, and NaN-safety for mixed catalogs. Relevant for any new data UI manager in schismviz, dms_datastore_ui, or downstream apps."
---
# dvue Subclassing Guide

## Choose Your Base Class

| Base Class | Use When |
|---|---|
| `TimeSeriesDataUIManager` | Time-indexed data (most common) |
| `DataUIManager` | Non-time-series tabular data |
| `DataUI` | Low-level: custom view only, no manager |

## Initialization Order (TimeSeriesDataUIManager)

Always follow this order — violations cause param errors or empty UI:

```python
class MyManager(TimeSeriesDataUIManager):

    def __init__(self, *data_files, **kwargs):
        self._data_files = data_files
        self._build_catalog()          # 1. Build catalog with primary_key declared
        super().__init__(**kwargs)     # 2. Call super — NO url_column/url_num_column args
        self.color_cycle_column = "variable"     # 3. Set param defaults AFTER super()
        self.dashed_line_cycle_column = "source"
        self.marker_cycle_column = "station"

    def _build_catalog(self):
        # Declare primary_key at construction — required
        self._dvue_catalog = DataCatalog(primary_key=["station", "variable"])
        # For multi-source (multiple files/URLs):
        # self._dvue_catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
        for ...
            self._dvue_catalog.add(
                DataReference(source=path, reader=MyReader, station=s, variable=v)
            )  # name auto-derived from pk values; source_num auto-computed
```

## Required Method Overrides

All of these are abstract — the class will not instantiate without them:

```python
# --- Catalog ---
def get_data_catalog(self) -> pd.DataFrame: ...
def get_time_range(self, dfcat: pd.DataFrame) -> tuple: ...  # (start, end) Timestamps

# --- Row-level data access ---
def is_irregular(self, row: pd.Series) -> bool: ...
def build_station_name(self, row: pd.Series) -> str: ...

# --- One of these two patterns (see below) ---
def get_data_for_time_range(self, row, time_range) -> tuple: ...  # (df, unit, ptype)
# OR: implement data_catalog property + get_data_reference()

# --- UI config ---
def get_table_column_width_map(self) -> dict: ...  # {"col": "15%", ...}
def get_table_filters(self) -> dict: ...           # {"col": {"type": "input", "func": "like"}}
def get_tooltips(self) -> list: ...                # [("Label", "@col"), ...]
def get_map_color_columns(self) -> list: ...
def get_name_to_color(self) -> dict: ...
def get_map_marker_columns(self) -> list: ...
def get_name_to_marker(self) -> dict: ...
```

## Two Catalog Patterns

### Pattern A — DataCatalog (preferred for reactive/math refs)

```python
def _build_catalog(self):
    self._dvue_catalog = DataCatalog(primary_key=["station", "variable"])
    for ...
        self._dvue_catalog.add(
            DataReference(source=path, reader=MyReader, station=s, variable=v)
        )  # name auto-derived, e.g. "StationA_discharge"

@property
def data_catalog(self) -> DataCatalog:
    return self._dvue_catalog

def get_data_catalog(self) -> pd.DataFrame:
    return super().get_data_catalog()  # delegates to DataCatalog.to_dataframe()

def get_data_reference(self, row: pd.Series) -> DataReference:
    # name is always set; works for both raw and math refs
    return self._dvue_catalog.get(row["name"])
```

### Pattern B — plain DataFrame (simpler, legacy)

```python
def get_data_catalog(self) -> pd.DataFrame:
    return self._dfcat  # Plain DataFrame built in __init__

def get_data_for_time_range(self, row, time_range):
    # Read data for the row from source
    df = my_read_function(row["filename"], row["path"], time_range)
    return df, row["unit"], None  # (DataFrame, unit_str, ptype or None)
```

## Primary Key (replaces identity_key_columns and set_key_attributes)

Declare `primary_key` at `DataCatalog()` construction — not on the manager class:

```python
def _build_catalog(self):
    # Single-source catalog
    self._dvue_catalog = DataCatalog(primary_key=["station", "variable"])
    # Multi-source catalog (source_num auto-computed from ref.source)
    self._dvue_catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
```

`primary_key` is required at `DataCatalog()` construction. It controls:
- Uniqueness enforcement (`ValueError` on duplicate pk-tuple)
- Auto-naming of refs when `name=""` is not provided (e.g. `"StationA_discharge"` or `"s0_StationA_discharge"`)
- `source_num` prefix in `TransformToCatalogAction` derived names
- `catalog.get(station="A", variable="discharge")` keyword lookup

`source_num` (0, 1, 2…) is auto-computed by the catalog from `ref.source` values;
it is **not** a stored attribute on the ref and does not need to be set manually.

## NaN Safety for Mixed Catalogs

When the catalog contains both raw `DataReference` and `MathDataReference` rows,
the `source` column is `NaN` for math ref rows. One required guard:

**In `get_data_reference`:**
```python
def get_data_reference(self, row):
    # row["name"] is always set (auto-derived or explicit) for both raw and math refs
    return self._dvue_catalog.get(row["name"])
```

If you need source-based logic elsewhere (e.g. plot labels), check `pd.isna(row.get("source"))`
before using the source value.

**In any code using `get_unique_short_names` (file labelling in plots):**
```python
if "source_num" in df.columns:
    valid_sources = [s for s in df["source"].unique() if not pd.isna(s)]
    short_names = get_unique_short_names(valid_sources)
```

Skipping the NaN filter raises `TypeError` from `os.path.normpath(NaN)`.

## Key Visual Styling Params

Set after `super().__init__()`:

| Param | Controls |
|---|---|
| `color_cycle_column` | Line colors |
| `dashed_line_cycle_column` | Dash patterns |
| `marker_cycle_column` | Marker shapes |
| `plot_group_by_column` | Plot grouping (None = group by unit) |

## Custom Plot Action

Override `TimeSeriesPlotAction` and wire it in:

```python
class MyPlotAction(TimeSeriesPlotAction):
    def create_curve(self, data, row, unit, file_index=""):
        label = row["station"]
        if file_index:
            label = f"{file_index}:{label}"
        return hv.Curve(data.iloc[:, [0]], label=label)

class MyManager(TimeSeriesDataUIManager):
    def _make_plot_action(self):
        return MyPlotAction()
```

## Feature Flag Params

`TimeSeriesDataUIManager` (and `DataUIManager`) expose several `param.Boolean` flags
that `DataUI` reads to conditionally add buttons and tabs.  Set them in the class body:

| Param | Default | Effect |
|---|---|---|
| `show_math_ref_editor` | `True` | Math Ref editor tab in the action bar |
| `show_transform_to_catalog` | `True` | "Transform → Ref" button |
| `show_reset_session_button` | `False` | "Reset Session" button at end of action bar |
| `session_cookie_name` | `"dvue_user_id"` | Cookie cleared by Reset Session button |
| `show_permalink` | `False` | "Permalink" button in action bar |
| `disclaimer_text` | `None` | Collapsible Disclaimer card in sidebar |

**`show_reset_session_button`** — set to `True` in any app that uses
`install_session_handler()`.  Also set `session_cookie_name` to match the
`cookie_name` passed to `install_session_handler()` / `SessionManager()`:

```python
class MyManager(TimeSeriesDataUIManager):
    show_reset_session_button = param.Boolean(default=True)
    session_cookie_name       = param.String(default="myapp_user_id")
```

**`show_math_ref_editor`** — default is `True`; the Math Ref editor is on by default.
Do **not** add `show_math_ref_editor = param.Boolean(default=False)` to a subclass
unless you intentionally want to disable it — this is a common mistake that silently
removes a useful feature.

## Disclaimer and About Buttons — Header Modal, Not Action Row

`disclaimer_text` (when non-empty) adds a sidebar card automatically.  For a modal
Disclaimer button in the page header, wire it via the pre-rendered `header_row` /
`modal_pane` pattern described in `session-management.md`.

**Do not** add a Disclaimer action to `get_data_actions()` — that puts it in the
action row next to Plot/Tabulate, which is the wrong affordance for a legal notice:

```python
# ❌ Wrong — disclaimer clutters the data-action row
def get_data_actions(self):
    actions = super().get_data_actions()
    actions.append(dict(name="Disclaimer", icon="alert-circle", ...))
    return actions

# ✅ Correct — only override get_data_actions() when you need extra data actions
def get_data_actions(self):
    return super().get_data_actions()
    # (or omit the override entirely — pure passthrough adds no value)
```

`get_data_actions()` should only be overridden when you need to **add** actions beyond
what `super()` provides.  A pure `return super().get_data_actions()` override is dead
code — delete it.

## Checklist

- [ ] Subclass `TimeSeriesDataUIManager` (not `DataUIManager` for time-series)
- [ ] Build catalog **before** `super().__init__()`
- [ ] `_build_catalog()` constructs `DataCatalog(primary_key=[...])`
- [ ] `super().__init__()` has **no** `url_column` or `url_num_column` arguments
- [ ] Set `color_cycle_column` etc. **after** `super().__init__()`
- [ ] Implement all abstract methods
- [ ] Choose Pattern A (DataCatalog) or Pattern B (plain DataFrame) — not both
- [ ] `get_data_reference()` uses `row["name"]` (always reliable)
- [ ] NaN source guard uses `row.get("source")` not `row.get("filename")`
- [ ] No `display_url_num` or `_apply_url_num()` usage
- [ ] Math ref YAML criteria use `source_num:` not `url_num:`
- [ ] Return `(df, unit_str, ptype_or_None)` from `get_data_for_time_range`
- [ ] Primary key values used as station identifiers start with a letter (e.g. `"STA1"` not `"1"`) — values starting with a digit get a `_` prefix in auto-derived names
- [ ] Do **not** set `show_math_ref_editor = False` unless intentionally disabling
- [ ] If using session persistence: `show_reset_session_button = True` + `session_cookie_name` matches `install_session_handler(cookie_name=...)`
- [ ] Disclaimer/About wired via pre-rendered `header_row` / `modal_pane` — not via `get_data_actions()`
- [ ] Pure `return super().get_data_actions()` override deleted

## Finding Old API Usage (grep patterns)

When migrating an existing subclass, scan for these patterns:

```bash
grep -rn "DataCatalog()" .                    # missing primary_key
grep -rn "url_column\|url_num_column" .        # old super().__init__() args
grep -rn "identity_key_columns" .              # old manager class param
grep -rn "display_url_num\|_apply_url_num" .   # replaced by source_num in df.columns
grep -rn "set_key_attributes\|ref_key()\|get_key_attributes" .  # removed from DataReference
grep -rn "url_num:" .                          # old YAML search_map criteria
```

Every hit must be addressed before the migration is complete.

## References

- [dvue/AGENTS.md](../../AGENTS.md) — core design rules and mixed-catalog pitfalls
- [dvue/tsdataui.py](../../dvue/tsdataui.py) — full `TimeSeriesDataUIManager` implementation
- [dvue/dataui.py](../../dvue/dataui.py) — `DataUIManager` base
- [dvue/catalog.py](../../dvue/catalog.py) — `DataCatalog`, `DataReference`, `MathDataReference`
- [dvue/actions.py](../../dvue/actions.py) — `TimeSeriesPlotAction` and `TransformToCatalogAction`
- Real subclass example (fully migrated, in workspace): `schismviz/schismviz/schismui.py`
