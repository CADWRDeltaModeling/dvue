---
description: "Use when creating a subclass of dvue's TimeSeriesDataUIManager, DataUIManager, or DataUI. Covers required method overrides, initialization order, primary_key catalog patterns, source_num source discrimination, and NaN-safety for mixed catalogs. Relevant for any new data UI manager in pydelmod, schismviz, or downstream apps."
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

## References

- [dvue/AGENTS.md](../../AGENTS.md) — core design rules and mixed-catalog pitfalls
- [dvue/tsdataui.py](../../dvue/tsdataui.py) — full `TimeSeriesDataUIManager` implementation
- [dvue/dataui.py](../../dvue/dataui.py) — `DataUIManager` base
- [dvue/catalog.py](../../dvue/catalog.py) — `DataCatalog`, `DataReference`, `MathDataReference`
- [dvue/actions.py](../../dvue/actions.py) — `TimeSeriesPlotAction` and `TransformToCatalogAction`
- Real subclass examples: `pydelmod/dssui.py`, `schismviz/schismui.py`
