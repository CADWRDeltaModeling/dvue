---
description: "Use when creating a subclass of dvue's TimeSeriesDataUIManager, DataUIManager, or DataUI. Covers required method overrides, initialization order, catalog patterns, identity_key_columns, and NaN-safety for mixed catalogs. Relevant for any new data UI manager in pydelmod, schismviz, or downstream apps."
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
    identity_key_columns = param.List(default=["station", "variable"])

    def __init__(self, *data_files, **kwargs):
        self._data_files = data_files
        self._build_catalog()                     # 1. Build internal state
        super().__init__(url_column="filename",   # 2. Call super (triggers get_data_catalog,
                         url_num_column="url_num",  #    get_time_range, populates params)
                         **kwargs)
        self.color_cycle_column = "variable"      # 3. Set param defaults AFTER super()
        self.dashed_line_cycle_column = "source"
        self.marker_cycle_column = "station"
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
    self._dvue_catalog = DataCatalog()
    for ...:
        self._dvue_catalog.add(DataReference(reader=MyReader(...), name=ref_name, **attrs))

@property
def data_catalog(self) -> DataCatalog:
    return self._dvue_catalog

def get_data_catalog(self) -> pd.DataFrame:
    return super().get_data_catalog()  # Delegates to DataCatalog.to_dataframe()

def get_data_reference(self, row: pd.Series) -> DataReference:
    # Guard against NaN for mixed catalogs — see NaN Safety below
    if "name" in row.index and not pd.isna(row.get("name")):
        return self._dvue_catalog.get(row["name"])
    return self._dvue_catalog.get(f"{row['filename']}::{row['station']}")
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

## Identity Key Columns (for Transform → Catalog names)

Set `identity_key_columns` so `TransformToCatalogAction` generates readable short names:

```python
class MyManager(TimeSeriesDataUIManager):
    identity_key_columns = param.List(default=["station", "variable"])
```

Or per-reference (takes precedence over manager-level param):

```python
ref.set_key_attributes(["station", "variable"])
```

Without this, the full `ref_key()` is used as the name — always valid but verbose.

## NaN Safety for Mixed Catalogs

When the catalog contains both raw `DataReference` and `MathDataReference` rows, file/source columns are `NaN` for math rows. Two required guards:

**In `get_data_reference`:**
```python
def get_data_reference(self, row):
    filename = row.get("filename", None)
    if pd.isna(filename):
        return self._dvue_catalog.get(row["name"])
    return self._dvue_catalog.get(self._build_ref_key(row))
```

**In any code using `get_unique_short_names` (file indexing):**
```python
valid_files = [f for f in df["filename"].unique() if not pd.isna(f)]
short_names = get_unique_short_names(valid_files)
```

Skipping either guard raises `KeyError: "No DataReference named 'nan::...'"` or `TypeError` from `os.path.normpath(NaN)`.

## Key Visual Styling Params

Set after `super().__init__()`:

| Param | Controls |
|---|---|
| `color_cycle_column` | Line colors |
| `dashed_line_cycle_column` | Dash patterns |
| `marker_cycle_column` | Marker shapes |
| `plot_group_by_column` | Plot grouping (None = group by unit) |
| `identity_key_columns` | TransformToCatalogAction naming |

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
- [ ] Set `color_cycle_column` etc. **after** `super().__init__()`
- [ ] Implement all abstract methods
- [ ] Choose Pattern A (DataCatalog) or Pattern B (plain DataFrame) — not both
- [ ] Set `identity_key_columns` for readable TransformToCatalog names
- [ ] Add NaN guards in `get_data_reference` if supporting math references
- [ ] Return `(df, unit_str, ptype_or_None)` from `get_data_for_time_range`

## References

- [dvue/AGENTS.md](../../AGENTS.md) — core design rules and mixed-catalog pitfalls
- [dvue/tsdataui.py](../../dvue/tsdataui.py) — full `TimeSeriesDataUIManager` implementation
- [dvue/dataui.py](../../dvue/dataui.py) — `DataUIManager` base
- [dvue/catalog.py](../../dvue/catalog.py) — `DataCatalog`, `DataReference`, `MathDataReference`
- [dvue/actions.py](../../dvue/actions.py) — `TimeSeriesPlotAction` and `TransformToCatalogAction`
- Real subclass examples: `pydelmod/dssui.py`, `schismviz/schismui.py`
