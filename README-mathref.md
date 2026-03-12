# Math References

A **Math Reference** (`MathDataReference`) is a derived data series whose values are computed from a Python expression evaluated over other series in the catalog. Rather than storing a pre-computed result, it evaluates on demand — every call to `getData()` fetches the underlying source data and applies the expression freshly.

Math References are first-class catalog entries. They appear alongside raw `DataReference` objects in the table, can be plotted, downloaded, and used as inputs to *other* Math References (chaining).

![Main UI showing catalog table with raw and math references](docs/screenshots/extsdataui_main.png)

---

## Concepts

### Variable Resolution

When a Math Reference evaluates its expression it resolves each identifier in priority order:

1. **`variable_map`** — an explicit `{alias: DataReference}` binding provided at construction time.
2. **`search_map`** — each alias is looked up in the catalog at `getData()` time using attribute criteria (e.g. `variable=wind_speed, interval=hourly`). This is the *recommended* approach because it keeps expressions portable.
3. **Catalog name-lookup** — if the identifier matches a catalog key directly, that reference is used. This works for simple cases and for chaining one Math Reference into another.

### Available Functions

Expressions run inside a safe NumPy namespace. The following are available without any import:

| Category | Available names |
|---|---|
| Trig | `sin`, `cos`, `tan`, `arcsin`, `arccos`, `arctan`, `arctan2` |
| Exponential / log | `exp`, `log`, `log2`, `log10`, `sqrt` |
| Rounding | `ceil`, `floor`, `round` |
| Array ops | `abs`, `clip`, `where`, `diff`, `cumsum` |
| Aggregates | `min`, `max`, `sum`, `mean`, `std` |
| Constants | `pi`, `e`, `nan`, `inf` |
| Libraries | `np` (NumPy), `pd` (pandas), `math` |

Pandas Series/DataFrame methods are also available directly on resolved variables (e.g. `obs.mean()`, `ws.mean(axis=1)`).

---

## Creating Math References via YAML

The most portable way to define Math References is with a YAML file. Each entry is a list item with at minimum a `name` and an `expression`. All other keys become metadata attributes (shown as table columns in the UI).

### Style 1 — Direct catalog-name lookup (simple cases)

Expression tokens are the full catalog key names of other references. Good for quick one-offs; breaks if the upstream reference is renamed.

#### How catalog names are built

Every `DataReference` has a `.name` property that is its catalog key — the string used to store and retrieve it. There are two ways a name is set:

1. **Assigned explicitly** — in code you set `ref.name = "my_key"` before calling `catalog.add(ref)`.
2. **Generated from `ref_key()`** — the default `DataReference.ref_key()` joins all string-representable attribute values with `_` separators, sanitising spaces and special characters to underscores:
   ```python
   # ref with attributes: station="A", variable="wind", interval="hourly"
   ref.ref_key()  # → "A_wind_hourly"
   ```
   Subclasses can override `ref_key()` to produce a more domain-specific key from a chosen subset of attributes. For example `ex_tsdataui.py` overrides it to use `station_name__variable__interval`:
   ```python
   def ref_key(self) -> str:
       name = self.get_attribute("station_name", "").replace(" ", "_")
       variable = self.get_attribute("variable", "")
       interval = self.get_attribute("interval", "")
       return f"{name}__{variable}__{interval}"
       # → "Station_A__wind_speed__hourly"
   ```

#### Seeing catalog names in the UI

When the catalog contains any Math References, the table gains an **expression** column. For raw `DataReference` rows this column is intentionally filled with the reference's catalog key name — so users can see at a glance exactly what token to paste into an expression:

```python
# Inside get_data_catalog() in ex_tsdataui.py:
mask = df["expression"].isna() | (df["expression"].str.strip() == "")
df.loc[mask, "expression"] = df.loc[mask, "name"]  # show catalog key for raw refs
```

This means the `expression` column doubles as a "name hint" guide: raw rows show the token you'd use in a direct-name expression; Math Reference rows show their actual expression formula.

You can also list all names in code:
```python
catalog.list_names()
# ['Station_A__wind_speed__hourly', 'Station_B__wind_speed__hourly', ...]
```

Or via the **Catalog names** panel at the bottom of the Math Ref editor (shown automatically, scrollable).

```yaml
# math_refs_tsdataui.yaml

- name: Station_A__wind_speed_mph__hourly
  expression: Station_A__wind_speed__hourly * 2.23694
  station_id: '1'
  station_name: Station A
  variable: wind_speed_mph
  unit: mph
  interval: hourly

- name: Station_B__precip_cumulative__hourly
  expression: cumsum(Station_B__precipitation__hourly)
  station_id: '2'
  station_name: Station B
  variable: precip_cumulative
  unit: mm
  interval: hourly

- name: wind_diff_A_minus_C__hourly
  expression: Station_A__wind_speed__hourly - Station_C__wind_speed__hourly
  station_id: '1'
  station_name: Station A
  variable: wind_diff_A_minus_C
  unit: m/s
  interval: hourly
```

### Style 2 — `search_map` / alias style (recommended)

Each expression variable is a short alias resolved by catalog attribute criteria at `getData()` time. The expression itself stays the same even if catalog key names change — only the search criteria need to match.

```yaml
# math_refs_search_map.yaml

# Unit conversion: obs → first match for station_name=Station A, variable=wind_speed, interval=hourly
- name: wind_speed_mph__A__hourly
  expression: obs * 2.23694
  station_name: Station A
  variable: wind_speed_mph
  unit: mph
  interval: hourly
  search_map:
    obs:
      station_name: Station A
      variable: wind_speed
      interval: hourly

# Cross-station difference: two independent aliases
- name: wind_diff_B_minus_A__hourly
  expression: ws_b - ws_a
  variable: wind_diff_B_minus_A
  unit: m/s
  interval: hourly
  search_map:
    ws_b:
      station_name: Station B
      variable: wind_speed
      interval: hourly
    ws_a:
      station_name: Station A
      variable: wind_speed
      interval: hourly

# Normalised anomaly (z-score)
- name: precip_anomaly__B__hourly
  expression: (obs - obs.mean()) / obs.std()
  variable: precip_anomaly
  unit: ''
  interval: hourly
  search_map:
    obs:
      station_name: Station B
      variable: precipitation
      interval: hourly

# Multi-station mean: _require_single: false concatenates ALL matches into a DataFrame
- name: mean_wind_speed__all_stations__hourly
  expression: ws.mean(axis=1)
  variable: mean_wind_speed
  unit: m/s
  interval: hourly
  search_map:
    ws:
      variable: wind_speed
      interval: hourly
      _require_single: false
```

> **`_require_single: false`** — when set inside a `search_map` criteria block, *all* catalog entries matching those criteria are fetched and concatenated column-wise (axis=1 join by timestamp index) so the alias resolves to a DataFrame. Use this for multi-station aggregates such as `ws.mean(axis=1)`.

### Chaining Math References

A Math Reference can reference another Math Reference — either by catalog key name or via `search_map` attributes. All references in the file are created before any `getData()` call, so ordering within the file does not matter.

```yaml
# Chain by name: cumulative sum of the mph-converted series defined above
- name: wind_speed_mph__A__hourly__cumsum
  expression: cumsum(wind_speed_mph__A__hourly)
  variable: wind_speed_mph_cumsum
  unit: mph
  interval: hourly

# Chain by search_map attributes: more portable than using the catalog key
- name: wind_speed_mph__A__hourly__pct_of_mean
  expression: mph_wind / mph_wind.mean() * 100
  variable: wind_speed_mph_pct_of_mean
  unit: '%'
  interval: hourly
  search_map:
    mph_wind:
      station_name: Station A
      variable: wind_speed_mph
      interval: hourly
```

### Loading YAML in code

```python
from dvue.catalog import DataCatalog
from dvue.math_reference import MathDataCatalogReader

catalog = DataCatalog()
# ... add raw DataReference objects ...

reader = MathDataCatalogReader(parent_catalog=catalog)
catalog.add_reader(reader)
catalog.add_source("math_refs_search_map.yaml")
```

### Saving all math refs to YAML in code

```python
from dvue.math_reference import save_math_refs

save_math_refs(catalog, "math_refs_saved.yaml")
```

---

## Creating Math References via the Editor

The **Math Ref** button in the UI opens an inline editor panel for creating, editing, and managing Math References without restarting the kernel.

### Opening the editor

Click the **Math Ref** button (⚙ icon, orange/warning style) in the action toolbar above the catalog table.

![Math Ref editor open](docs/screenshots/extsdataui_mathrefeditor.png)

The editor has the following sections from top to bottom:

#### YAML file

A path input pre-filled with the default YAML path. Use the two buttons to:

- **Load from YAML** — reads the file and merges all `MathDataReference` objects into the live catalog, then refreshes the table. No restart needed.
- **Save to YAML** — writes all current Math References in the catalog out to the file.

#### Expression

The Python expression string. Use the short aliases you define in the Search Map (or full catalog key names for direct-lookup style).

#### Attributes

One `key: value` pair per line. These become the metadata attributes shown as columns in the catalog table (e.g. `variable`, `unit`, `interval`, `station_name`).

#### Search Map

One row per expression variable:

| Field | Purpose |
|---|---|
| **Alias** | Short identifier used in the Expression (e.g. `obs`) |
| **Join all** | When checked, all matching catalog entries are concatenated into a DataFrame (equivalent to `_require_single: false`) |
| **Catalog criteria** | Comma-separated `attr=val` pairs used to search the catalog (e.g. `variable=wind_speed, interval=hourly`) |

Click **+ Add variable** to add a row, click **✕** to remove one.

#### Catalog attribute browser

A read-only panel showing all attribute columns currently in the catalog and their unique values — handy for writing search criteria without guessing column names.

#### Saving and selecting a Math Ref

Click **Save** to add the new Math Reference to the live catalog and refresh the table. If a reference with the same name already exists it is updated in-place.

![Math Ref editor with all fields visible](docs/screenshots/extsdataui_mathrefeditor_full.png)

### Editing an existing Math Reference

Select a row in the catalog table that corresponds to a Math Reference before clicking **Math Ref**. The editor pre-populates all fields from the selected reference so you can adjust the expression, attributes, or search criteria.

![Math Ref row selected in table](docs/screenshots/extsdataui_mathrefselected.png)

### Plotting a Math Reference

Once a Math Reference is in the catalog it can be selected and plotted just like any raw data series. The expression is evaluated on demand against the live catalog data.

![Math Ref result plotted](docs/screenshots/extsdataui_mathrefretrieved.png)

---

## Creating Math References in Python

You can also construct `MathDataReference` objects directly in code:

```python
from dvue.catalog import DataCatalog, DataReference
from dvue.math_reference import MathDataReference

# Explicit variable_map
a = DataReference(df_a, name="A")
b = DataReference(df_b, name="B")
derived = MathDataReference("A + B * 2", variable_map={"A": a, "B": b}, name="sum_AB")

# Operator overloading shorthand (produces a MathDataReference automatically)
combined = a + b * 2
combined.name = "sum_AB"

# search_map style — resolves against a catalog at getData() time
cat = DataCatalog()
# ... populate cat ...
m = MathDataReference(
    "inflow - outflow",
    search_map={
        "inflow":  {"variable": "discharge", "location": "upstream"},
        "outflow": {"variable": "discharge", "location": "downstream"},
    },
    catalog=cat,
    name="net_discharge",
)
cat.add(m)
```

### Fluent / chainable API

```python
m = (MathDataReference("obs * 2.23694", name="wind_mph")
     .set_search("obs", variable="wind_speed", interval="hourly", station_name="Station A")
     .set_catalog(catalog))
catalog.add(m)
```

---

## Wire the editor into your `TimeSeriesDataUIManager`

```python
from dvue.math_ref_editor import MathRefEditorAction

class MyManager(TimeSeriesDataUIManager):

    @property
    def data_catalog(self):
        return self._catalog          # required: expose the DataCatalog

    def get_data_actions(self):
        actions = super().get_data_actions()
        actions.append(dict(
            name="Math Ref",
            button_type="warning",
            icon="math-function",
            action_type="display",
            callback=MathRefEditorAction(default_yaml_path="math_refs.yaml").callback,
        ))
        return actions
```

The `default_yaml_path` pre-fills the YAML path input in the editor so users don't have to type it manually.

---

## Full working example

See [`examples/ex_tsdataui.py`](examples/ex_tsdataui.py) for a complete end-to-end demonstration:

```bash
conda run -n pydsm panel serve examples/ex_tsdataui.py --show
```

The example includes:
- A `DataCatalog` with 18 raw station/variable/interval references
- Two ready-made YAML files in `examples/data/`:
  - [`math_refs_tsdataui.yaml`](examples/data/math_refs_tsdataui.yaml) — direct catalog-name lookup style
  - [`math_refs_search_map.yaml`](examples/data/math_refs_search_map.yaml) — `search_map` alias style (6 examples including chaining and `[multi]`)
- The **Math Ref** editor action pre-wired and ready to use
