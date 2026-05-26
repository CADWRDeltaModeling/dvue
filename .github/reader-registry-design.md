# Reader Registry — Detailed Design

## Status: Implemented ✅

See [reader-registry-architecture.md](reader-registry-architecture.md) for the high-level overview.

---

## 1. `dvue/registry.py` — New File

### 1.1 Full API

```python
class ReaderRegistry:
    """Central registry mapping ref_type strings to reader classes and cached instances.

    All state is class-level (effectively a singleton).  Downstream packages
    register their reader classes at module import time.
    """

    # ref_type → reader class
    _registry: ClassVar[Dict[str, Type[DataReferenceReader]]] = {}

    # ".ext" → reader class (lower-cased extension)
    _extension_map: ClassVar[Dict[str, Type[DataReferenceReader]]] = {}

    # (ref_type, source) → live reader instance
    _instances: ClassVar[Dict[Tuple[str, str], DataReferenceReader]] = {}

    @classmethod
    def register(
        cls,
        ref_type: str,
        reader_class: Type[DataReferenceReader],
        extensions: Optional[List[str]] = None,
    ) -> None:
        """Register reader_class for ref_type and optionally for file extensions.

        Calling register() with the same ref_type more than once overwrites the
        previous registration (last write wins).  Extensions are additive across
        calls — they are never removed by a subsequent register() call.
        """

    @classmethod
    def get_reader(cls, ref_type: str, source: str = "") -> DataReferenceReader:
        """Return a cached reader instance for (ref_type, source).

        Creates reader_class(source) on the first call for a given (ref_type, source)
        pair.  Subsequent calls return the same instance, keeping file handles open.

        Raises KeyError if ref_type is not registered.
        """

    @classmethod
    def scan(cls, path: str) -> List["DataReference"]:
        """Scan a file by extension and return the DataReferences it contains.

        Looks up the reader class via _extension_map, then calls
        reader_class.scan(path) (a classmethod on the reader).

        Raises KeyError if the file extension is not registered.
        """

    @classmethod
    def can_handle(cls, path: str) -> bool:
        """Return True if a reader is registered for this file's extension."""

    @classmethod
    def clear_instance_cache(
        cls,
        ref_type: Optional[str] = None,
        source: Optional[str] = None,
    ) -> None:
        """Remove cached reader instances matching the given filters.

        Called with no arguments clears all instances (useful in tests).
        Called with ref_type and/or source clears only matching entries.
        """
```

### 1.2 Circular Import Rule

`catalog.py` and `math_reference.py` must **never** import `registry.py` at module level.
The registry is accessed only via a lazy import inside `DataReference._load_data()`:

```python
# inside DataReference._load_data() — lazy, no circular import at module load
from dvue.registry import ReaderRegistry
reader = ReaderRegistry.get_reader(self.ref_type, self.source)
```

### 1.3 Relationship to `DataReferenceTypeRegistry`

The existing plan in `mixed-catalog-registry-plan.md` introduces a separate
`DataReferenceTypeRegistry` (for CSV deserialization) in the same `dvue/registry.py` file.
Both registries are keyed by `ref_type` string.  They may be:

- **Two separate classes** in the same module (`ReaderRegistry` + `DataReferenceTypeRegistry`), or
- **Merged into one** class with combined responsibilities if that proves cleaner.

The invariant is that both are keyed by `ref_type` and live in `dvue/registry.py`.

---

## 2. Changes to `dvue/catalog.py`

### 2.1 `DataReferenceReader.scan()` — New Base Method

Add after the existing `load()` abstract method (currently around line 307):

```python
@classmethod
def scan(cls, path: str) -> List["DataReference"]:
    """Scan a file and return the DataReferences it contains.

    The default implementation raises NotImplementedError.  Reader
    subclasses that support file discovery must override this method.

    Each returned reference must have ``source`` set to ``path``.
    Manager-level enrichment (geoid, geometry merging, time_range) is
    NOT performed here — that responsibility stays in add_source_files().
    """
    raise NotImplementedError(
        f"{cls.__name__}.scan() is not implemented. "
        "Override scan() in the reader subclass to support file discovery."
    )
```

### 2.2 `DataReference._load_data()` — Registry Fallback

**Current code (lines 604–614 of `dvue/catalog.py`):**
```python
def _load_data(self, time_range: Any = None) -> pd.DataFrame:
    return self._get_reader().load(**{**self._attributes, "time_range": time_range})
```

**New code:**
```python
def _load_data(self, time_range: Any = None) -> pd.DataFrame:
    try:
        reader = self._get_reader()
    except ValueError:
        # No embedded reader — resolve via registry using ref_type + source.
        if self.ref_type == "raw":
            raise  # "raw" has no registry entry; re-raise the original error
        from dvue.registry import ReaderRegistry  # lazy to avoid circular import
        reader = ReaderRegistry.get_reader(self.ref_type, self.source)
    return reader.load(**{**self._attributes, "time_range": time_range})
```

**Why this works:**  `_get_reader()` (lines 428–439) raises `ValueError` exactly when both
`_reader_instance` is `None` and `_reader_fqcn` is empty — i.e. `reader=None` was passed
at construction.  For refs with `ref_type != "raw"` this triggers the registry lookup.
Refs with a live reader instance hit the fast path
(`if self._reader_instance is not None: return self._reader_instance`) and never reach
the except branch.

### 2.3 `build_catalog_from_dataframe()` — Make `reader` Optional

Change the signature (currently around line 1681):

```python
def build_catalog_from_dataframe(
    dfcat: pd.DataFrame,
    reader: Optional[DataReferenceReader],   # was: DataReferenceReader (required)
    ref_name_fn,
    primary_key: Optional[List[str]] = None,
    crs: Optional[str] = None,
    ref_class: type = DataReference,
) -> DataCatalog:
```

Pass `reader` (which may be `None`) through to `ref_class(reader=reader, ...)` unchanged.
When `reader=None`, created refs have `_reader_instance=None` and `_reader_fqcn=""`,
so `_load_data()` falls back to the registry.

### 2.4 `dvue/__init__.py` — Export `ReaderRegistry`

Add to `__all__` and the corresponding import:
```python
from dvue.registry import ReaderRegistry
```

---

## 3. Reader Class Migration Contract

Every reader class that participates in the registry must satisfy this contract:

### 3.1 Constructor: `__init__(self, source: str)`

- `source` is the absolute path to the file this reader handles.
- The registry calls `reader_class(source)` exactly once per `(ref_type, source)` pair
  and caches the result.
- Constructor should open the file eagerly or lazily:
  - **Eager**: `self._h5 = HydroH5(source)` — fails fast if file missing.
  - **Lazy**: `self._source = source` — defers failure to first `load()` call.

### 3.2 Classmethod: `@classmethod scan(cls, path: str) -> list[DataReference]`

- Opens the file, reads its metadata catalog, creates one DataReference per data series.
- Each returned ref **must** have `source=path` set.
- **Permissive extraction**: emit every attribute the file itself contains — id, variable,
  units, embedded coordinates, start/end timestamps, any catalog metadata the file stores.
  Do not restrict to the minimum required for `load()`.
- Does **not** consult external lookup tables, shapefiles, or manager configuration.
  Manager-level enrichment (geoid, geometry from shapefile, station names from CSV) is
  applied by `add_source_files()` via `ref.set_attribute()` **after** `scan()` returns.
- Should not retain the file handle opened for scanning; the registry creates a separate
  persistent cached instance when `get_reader()` is first called.

### 3.3 Instance method: `def load(self, **attributes) -> pd.DataFrame`

- Uses `self._source` (or `self._h5`) for the file identity —
  **not** `attributes["filename"]` or `attributes["FILE"]`.
- Reads station/variable/time_range from `attributes` as before.
- `attributes["source"]` equals `self._source` as an invariant (both set at construction).
- Must accept and silently ignore enrichment attributes (geoid, station_name, geometry,
  etc.) that were added by `add_source_files()` and forwarded via `**attributes`.
  Use `**kwargs` to absorb unknown keys.

### 3.4 Enrichment Contract

After `scan()` returns raw refs, `add_source_files()` enriches them via
`ref.set_attribute(name, value)` before calling `catalog.add(ref)`.

**Conflict rule — file wins, warn:**
```python
file_val = ref.get_attribute("station_name")         # from scan()
lookup_val = station_name_lookup.get(ref.get_attribute("id"))
if file_val and lookup_val and file_val != lookup_val:
    logger.warning(
        "station_name from file %r conflicts with lookup %r; keeping file value",
        file_val, lookup_val,
    )
elif not file_val and lookup_val:                    # only set when file didn't provide it
    ref.set_attribute("station_name", lookup_val)
```

**Geometry — store as Shapely, CSV round-trip deferred:**
```python
ref.set_attribute("geometry", shapely_geom)  # enables GeoDataFrame output from catalog
# WKT serialization / deserialization for CSV is deferred to a future task.
```

**Mutation safety rule:**  All `set_attribute()` calls must complete **before**
`catalog.add(ref)`.  Once a ref is in the catalog it may be read for display.  The data
cache key is `time_range` only, so mutating load-critical attributes (`id`, `variable`,
`source`) after the first `getData()` call would silently return stale cached data.

---

## 4. dsm2ui Reader Migration

### 4.1 `TidefileReader` (`dsm2ui/dsm2ui.py` — current line 1259)

**Before (current):**
```python
class TidefileReader(DataReferenceReader):
    def __init__(self, tidefile_map: dict) -> None:
        self._tidefile_map = tidefile_map      # {path: HydroH5 or QualH5}

    def load(self, **attributes) -> pd.DataFrame:
        filename = attributes["filename"]
        h5 = self._tidefile_map[filename]      # shared flyweight lookup
        ...
```

**After:**
```python
class TidefileReader(DataReferenceReader):
    def __init__(self, source: str) -> None:
        self._source = source
        self._h5 = DSM2TidefileUIManager.read_tidefile(source)  # one handle per instance

    @classmethod
    def scan(cls, path: str) -> list:
        h5 = DSM2TidefileUIManager.read_tidefile(path)
        dfcat = h5.create_catalog()
        refs = []
        for _, row in dfcat.iterrows():
            attrs = {k: v for k, v in row.items()}
            refs.append(DSM2TidefileDataReference(
                source=path,
                name=f"{path}::{attrs['id']}/{attrs['variable']}",
                cache=True,
                **attrs,
            ))
        return refs

    def load(self, **attributes) -> pd.DataFrame:
        variable = attributes["variable"]
        id_ = attributes["id"]
        time_range = attributes.get("time_range")
        time_window = self._to_time_window(time_range) if time_range is not None else None
        entry = {"filename": self._source, "variable": variable, "id": id_}
        df = self._h5.get_data_for_catalog_entry(entry, time_window)
        if df is not None and not df.empty:
            df.attrs["unit"] = attributes.get("unit", "")
        return df if df is not None else pd.DataFrame()

# Module-level registration (after class definition):
from dvue.registry import ReaderRegistry
ReaderRegistry.register("dsm2_hdf5", TidefileReader, extensions=[".h5", ".hdf5"])
```

**Key change:** `self._tidefile_map[filename]` → `self._h5`.  The registry's `_instances`
cache replaces the explicit flyweight `_tidefile_map` as the per-source cache.

### 4.2 `DSM2DSSReader` (`dsm2ui/dsm2ui.py` — current line 106)

**Before (current):**
```python
class DSM2DSSReader(DataReferenceReader):
    # No __init__; stateless

    def load(self, **attributes) -> pd.DataFrame:
        dssfile = attributes["FILE"]           # uppercase — from output_channels GeoDataFrame
        name = attributes["NAME"]
        variable = attributes["VARIABLE"]
        ...
```

**After:**
```python
class DSM2DSSReader(DataReferenceReader):
    def __init__(self, source: str) -> None:
        self._source = source                  # absolute DSS file path

    @classmethod
    def scan(cls, path: str) -> list:
        refs = []
        with dss.DSSFile(path) as f:
            df_catalog = f.read_catalog()
        for _, row in df_catalog.iterrows():
            # Filter to output-channel C-parts; see Section 10 (edge cases)
            refs.append(DSM2DSSDataReference(
                source=path,
                name=f"{path}::{row['B']}/{row['C']}",
                cache=True,
                NAME=row["B"],
                VARIABLE=row["C"],
                FILE=path,
            ))
        return refs

    def load(self, **attributes) -> pd.DataFrame:
        name = attributes["NAME"]
        variable = attributes["VARIABLE"]
        time_range = attributes.get("time_range")
        pathname = f"//{name}/{variable}////"
        try:
            df, unit, ptype = next(dss.get_matching_ts(self._source, pathname))
            df.attrs["unit"] = unit
            df.attrs["ptype"] = ptype
            if time_range is not None and len(time_range) == 2:
                start, end = pd.Timestamp(time_range[0]), pd.Timestamp(time_range[1])
                df = df.loc[start:end]
            return df
        except StopIteration:
            logger.warning("No matching DSS time series for %s in %s", pathname, self._source)
            return pd.DataFrame()

# Module-level registration:
ReaderRegistry.register("dsm2_dss", DSM2DSSReader, extensions=[".dss"])
```

---

## 5. dsm2ui Manager Migration

### 5.1 `DSM2TidefileUIManager.__init__` (~line 1427)

Remove the `TidefileReader` instantiation and change the `build_catalog_from_dataframe` call:

```python
# BEFORE (current):
self._reader = TidefileReader(self.tidefile_map)
self._dvue_catalog = build_catalog_from_dataframe(
    self.dfcat, self._reader, self._build_ref_key,
    primary_key=["name"], crs=geo_crs, ref_class=DSM2TidefileDataReference,
)

# AFTER:
# self._reader removed entirely
self._dvue_catalog = build_catalog_from_dataframe(
    self.dfcat, None, self._build_ref_key,          # reader=None
    primary_key=["name"], crs=geo_crs, ref_class=DSM2TidefileDataReference,
)
```

### 5.2 `DSM2TidefileUIManager.add_source_files()` (~line 1551)

Three changes:

1. **Remove** `self._reader._tidefile_map[path] = h5` (reader no longer has a map).
2. Build refs from `ReaderRegistry.scan(path)` rather than constructing inline.
3. Apply enrichment via `ref.set_attribute()` **before** `catalog.add()`, with file-wins
   conflict resolution for any attribute the file already provides.

```python
# AFTER:
refs = ReaderRegistry.scan(path)   # permissive: refs already carry file-sourced attrs
for ref in refs:
    # Enrich with manager-level knowledge only when the file didn't provide the value.
    geoid = ref.get_attribute("id", "").split("_")[1] if "_" in ref.get_attribute("id", "") else None
    if geoid and not ref.get_attribute("geoid"):
        ref.set_attribute("geoid", geoid)
    station_name = ref.get_attribute("geoid") or ref.get_attribute("id")
    if not ref.get_attribute("station_name"):
        ref.set_attribute("station_name", station_name)
    if self.channels is not None:
        geom = self._lookup_geometry(geoid)
        if geom is not None and not ref.get_attribute("geometry"):
            ref.set_attribute("geometry", geom)
    # All mutation complete — safe to add.
    try:
        self._dvue_catalog.add(ref)
    except ValueError:
        pass  # duplicate pk — already present
```

Time-range expansion remains a separate step after the loop (UI state, not stored on refs).

### 5.3 `DSM2DataUIManager.__init__` (~line 220)

```python
# BEFORE:
_reader = DSM2DSSReader()
self._dvue_catalog = build_catalog_from_dataframe(
    _oc, _reader, self._ref_name,
    primary_key=["name"], crs=geo_crs, ref_class=DSM2DSSDataReference,
)

# AFTER:
self._dvue_catalog = build_catalog_from_dataframe(
    _oc, None, self._ref_name,              # reader=None
    primary_key=["name"], crs=geo_crs, ref_class=DSM2DSSDataReference,
)
```

The `source` column is already set to `_oc["FILE"]` in the existing code, so every
`DSM2DSSDataReference` will have `self.source = <dss_file_path>`.  `DSM2DSSReader(source)`
is instantiated by the registry on first `getData()` call.

---

## 5b. `dvue/registry_ui.py` — New File (Implemented)

`registry_ui.py` is the dvue-side generic base for registry-backed UI managers.
It lives in the dvue framework so any downstream app can subclass it without
re-implementing the scan-normalise-add loop.

### Classes

**`RegistryPlotAction(TimeSeriesPlotAction)`**

Generic plot action for registry-backed catalogs.  Labels curves as
`station/variable` (single variable: just `station`).  Extension hook:

```python
def format_variable(self, variable: str) -> str:
    """Override to apply domain-specific label formatting."""
    return variable   # default: unchanged
```

**`RegistryUIManager(TimeSeriesDataUIManager)`**

Starts with an empty catalog; auto-detects file types via `ReaderRegistry.can_handle()`.

```python
class RegistryUIManager(TimeSeriesDataUIManager):
    def __init__(self, files=(), **kwargs): ...

    # Extension hooks (override in subclasses)
    def normalize_ref(self, ref): ...      # map file attrs → station/variable schema
    def on_file_added(self, path, refs): ... # expand time_range, load geometry, etc.

    # Core loop (do not override)
    def add_source_files(self, *paths): ...
```

`primary_key = ["source_num", "station", "variable"]`.  Table columns: `station`, `variable`, `ref_type`.

### Extension pattern

```python
# Register your reader (typically at module import time)
ReaderRegistry.register("myformat", MyReader, extensions=[".xyz"])

# Use directly if default hooks are sufficient
mgr = RegistryUIManager(files=["data/run.xyz"])

# Or subclass for domain-specific behaviour
class MyUIManager(RegistryUIManager):
    def normalize_ref(self, ref):
        # Map file-specific attr names to station/variable
        if not ref._attributes.get("station"):
            ref.set_attribute("station", ref._attributes.get("id", ""))

    def on_file_added(self, path, refs):
        # Expand time range from file metadata
        ...

    def _make_plot_action(self):
        return MyPlotAction()  # subclass of RegistryPlotAction
```

---

## 6. `DSM2CombinedUIManager` (Actual Implementation)

`DSM2CombinedUIManager` is a **thin subclass of `RegistryUIManager`** (not
`TimeSeriesDataUIManager` directly).  The scan-normalise-add loop is entirely in
`RegistryUIManager.add_source_files()`; the dsm2ui subclass overrides only three
methods:

```python
class _CombinedPlotAction(RegistryPlotAction):
    def format_variable(self, variable: str) -> str:
        return _smart_title(variable)   # FLOW → Flow; EC unchanged

class DSM2CombinedUIManager(RegistryUIManager):
    def normalize_ref(self, ref):
        """Prefer geoid over id when mapping to station."""
        if not ref._attributes.get("station"):
            station = (
                ref._attributes.get("geoid")
                or ref._attributes.get("id")
                or ref._attributes.get("NAME")
                or ref._attributes.get("name", "")
            )
            ref.set_attribute("station", str(station))
        if not ref._attributes.get("variable"):
            var = ref._attributes.get("VARIABLE", "")
            ref.set_attribute("variable", str(var).lower())

    def on_file_added(self, path, refs):
        """Expand time_range from HDF5 get_start_end_dates()."""
        import os
        if os.path.splitext(path)[1].lower() in (".h5", ".hdf5"):
            try:
                h5 = ReaderRegistry.get_reader("dsm2_hdf5", path)._h5
                t0, t1 = h5.get_start_end_dates()
                cur = self.time_range or (None, None)
                new_start = min(pd.to_datetime(t0), cur[0]) if cur[0] else pd.to_datetime(t0)
                new_end   = max(pd.to_datetime(t1), cur[1]) if cur[1] else pd.to_datetime(t1)
                self.time_range = (new_start, new_end)
            except Exception:
                pass

    def _make_plot_action(self):
        return _CombinedPlotAction()
```

### `primary_key` and table columns

`RegistryUIManager` uses `primary_key=["source_num", "station", "variable"]`, which
`DSM2CombinedUIManager` inherits.  Table columns: `station`, `variable`, `ref_type`.
For single-file sessions `source_num` is absent (standard catalog behaviour).

---

## 7. CLI Command

In `dsm2ui/cli.py`, add `"combined"` to the `lazy_subcommands` dict for the `ui` group:

```python
"combined": "dsm2ui.dsm2ui:launch_combined_ui",
```

`launch_combined_ui` follows the same pattern as `launch_tide_ui`:

```python
@click.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.option("--port", default=0, help="Port for the Panel server")
def launch_combined_ui(files, port):
    """Start the combined HDF5+DSS data viewer."""
    import panel as pn
    manager = DSM2CombinedUIManager(files=list(files) or None)
    pn.serve(manager.view(), port=port, show=True)
```

---

## 8. Backward Compatibility

| Scenario | Behavior after migration |
|----------|--------------------------|
| `DataReference(reader=instance, ...)` | Unchanged — `_reader_instance` fast path in `_get_reader()` |
| `DataReference(reader="fqcn.string", ...)` | Unchanged — FQCN lazy-instantiation path in `_get_reader()` |
| `DataReference(reader=None, ref_type="raw")` | `_load_data()` re-raises `ValueError` as before |
| `DataReference(reader=None, ref_type="dsm2_hdf5")` | Falls back to `ReaderRegistry.get_reader("dsm2_hdf5", self.source)` |
| `MathDataReference` | Unaffected — overrides `_load_data()` entirely |
| Existing `DSM2TidefileUIManager` users | Same public API; reader wired via registry transparently |
| `DSM2DataUIManager` users | Same public API; same behavior |
| `build_catalog_from_dataframe(dfcat, reader, ...)` | Still works; `reader` is now `Optional` |

---

## 9. Testing Strategy

### dvue tests — new `dvue/tests/test_registry.py`

| Test | What it verifies |
|------|-----------------|
| `test_register_and_get_reader_returns_same_instance` | `get_reader()` returns cached instance on second call |
| `test_register_with_extensions` | `can_handle(".h5")` → True; `scan()` dispatches to `reader_class.scan()` |
| `test_clear_instance_cache_all` | After `clear_instance_cache()`, next `get_reader()` creates fresh instance |
| `test_clear_instance_cache_by_source` | Only clears entries matching the given source path |
| `test_unregistered_ref_type_raises_keyerror` | `get_reader("unknown_type", "")` raises `KeyError` |
| `test_dataref_no_reader_uses_registry` | DataReference with `reader=None` and registered `ref_type` loads via registry |
| `test_dataref_with_reader_instance_bypasses_registry` | Explicit `reader=instance` uses the fast path; registry not called |
| `test_dataref_raw_no_reader_raises_valueerror` | `ref_type="raw"`, no reader → original `ValueError` re-raised |

### dvue regression

All 380 existing dvue tests must pass unchanged.  The `_load_data()` change is purely
additive (a `try/except` wrapper around existing logic).

### dsm2ui tests — new `dsm2ui/tests/test_combined_manager.py`

| Test | What it verifies |
|------|-----------------|
| `test_tidefilereader_scan_returns_refs` | `TidefileReader.scan(h5_path)` → `DSM2TidefileDataReference` list with `source=path` |
| `test_dssreader_scan_returns_refs` | `DSM2DSSReader.scan(dss_path)` → `DSM2DSSDataReference` list with `source=path` |
| `test_combined_manager_starts_empty` | `len(DSM2CombinedUIManager().data_catalog) == 0` |
| `test_combined_add_hdf5` | After `add_source_files(h5_path)`, catalog contains HDF5 refs |
| `test_combined_add_dss` | After `add_source_files(dss_path)`, catalog contains DSS refs |
| `test_combined_mixed_catalog_loads` | Refs from both types call `getData()` without error |

### dsm2ui regression

All 46 existing dsm2ui tests must pass after the reader migration.  Key assertion:
`DSM2TidefileUIManager` and `DSM2DataUIManager` behave identically before and after,
since `_load_data()` falls back to the registry transparently.

---

## 10. Edge Cases and Notes

### `DSM2DSSReader.scan()` path filtering

DSS files can contain thousands of paths including intermediate and summary paths.
`scan()` should filter to output-channel-style paths, for example by restricting `C-part`
to a known set (`{"FLOW", "STAGE", "EC", "VELOCITY"}`).  The exact filter list is a
dsm2ui-level decision and should not be hard-coded in dvue.

### `TidefileReader` instance creation timing

The registry creates `TidefileReader(source)` on the **first `load()` call**, not at
`scan()` time.  The file is therefore opened lazily.  If eager opening is preferred
(to fail fast on bad files), `add_source_files()` can warm the cache explicitly after
adding refs:

```python
ReaderRegistry.get_reader("dsm2_hdf5", path)   # warms cache → opens file now
```

### `TidefileReader.scan()` forward reference

`TidefileReader.scan()` calls `DSM2TidefileUIManager.read_tidefile()`.  Because both
classes live in the same module this is fine, but `TidefileReader` is defined before
`DSM2TidefileUIManager` in the file.  The classmethod is called at runtime (not import
time), so the forward reference is resolved by then.  No change to file ordering needed.

### `primary_key=["source_num", "name"]` when catalog is empty

`DataCatalog` does not inject `source_num` until two or more unique `source` values are
present.  An empty catalog or a single-source catalog will not have `source_num` in its
`to_dataframe()` output.  `DSM2CombinedUIManager._get_table_column_width_map()` must
handle the conditional presence of `source_num` using the same pattern already in
`DSM2TidefileUIManager`.
