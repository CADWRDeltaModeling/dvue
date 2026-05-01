# Plan: Mixed Catalog with DataReference Type Registry

## Status

- ⬜ Phase 1 — `dvue/registry.py`: `DataReferenceTypeRegistry` singleton + `register_ref_type` decorator
- ⬜ Phase 2 — `DataReference.from_dict` base classmethod (catalog.py)
- ⬜ Phase 3 — `MathDataReference.from_dict` (math_reference.py)
- ⬜ Phase 4 — Updated `DataCatalog.from_csv` with registry dispatch + auto-wire
- ⬜ Phase 5 — `DataCatalog.save_mixed` / `load_mixed` (CSV + YAML sidecar)
- ⬜ Phase 6 — `from_dict` on dsm2ui subclasses (DSM2DSSDataReference, DSM2TidefileDataReference, DSM2EchoInputDataReference, CalibDataReference)
- ⬜ Phase 7 — `dsm2ui/registry.py` + `dsm2ui/__init__.py` auto-registration
- ⬜ Phase 8 — Export `ref_type_registry`, `register_ref_type` from `dvue/__init__.py`

---

## Design Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Registry location | Separate `dvue/registry.py`, module-level singleton `ref_type_registry` | Cleaner separation from DataCatalog; avoids growing catalog.py further |
| Instantiation contract | `@classmethod from_dict(cls, d)` on each registered class — explicit, no default | Makes each subclass's construction requirements explicit; no hidden kwargs magic |
| Math ref in mixed CSV | Stub row in CSV (name, ref_type, expression, flat attrs) + full definition in `{stem}_math.yaml` sidecar | CSV stays flat/readable; YAML sidecar retains full nested search_map; UI table shows all refs in one place |
| variable_map in CSV | Serialize as JSON `{varname: ref_name}` column; on load resolve as catalog name lookups | variable_map holds live objects; names are the only portable representation |
| Catalog back-link | Auto-wire in `from_csv` and `load_mixed` — no manual step | Math refs are always useless without a catalog; silent failure otherwise |
| dsm2ui registration | `dsm2ui/registry.py` called from `dsm2ui/__init__.py` on import | All types registered the moment dsm2ui is imported; no action required by callers |
| Unregistered ref_type | Warn + fallback to base `DataReference` | Graceful degradation; existing CSV files with no ref_type column continue to work |
| Save API | New `save_mixed(path)` / `load_mixed(path)` on DataCatalog, leave `to_csv`/`from_csv` unchanged | Avoids surprising behavior change on existing single-type catalogs |

---

## Context

dvue supports self-contained `DataReference` objects that retrieve data without reference to
the catalog containing them. dsm2ui defines several subclasses (`DSM2DSSDataReference`,
`DSM2TidefileDataReference`, etc.), each with a distinct `ref_type` string.

The goal is to allow **mixed catalogs** containing refs of different `ref_type` values to be
saved and loaded from `.csv` files, with the correct subclass instantiated on load. A
lightweight registry in dvue maps `ref_type` strings to subclasses; downstream packages
(dsm2ui) populate the registry at import time.

`MathDataReference` adds a wrinkle: its `search_map` is a nested dict that cannot be stored
flat in CSV. The chosen solution is a **sidecar YAML** (`{stem}_math.yaml`) written alongside
the CSV, containing full math ref definitions. The CSV contains only a stub row for each math
ref (name, ref_type, expression, display attributes) so the UI table remains complete.

---

## Phase 1: dvue/registry.py (new file)

**Contents**
- `DataReferenceTypeRegistry` class:
  - `_registry: Dict[str, type]`
  - `register(ref_type, cls)` → None
  - `get(ref_type) → Optional[type]`
  - `instantiate(ref_type, row_dict) → DataReference` — calls `cls.from_dict(row_dict)`,
    emits `logging.warning` and falls back to `DataReference` if type unknown
  - `__contains__`
- Module-level singleton: `ref_type_registry = DataReferenceTypeRegistry()`
- Decorator: `register_ref_type(ref_type: str)` → class decorator that calls
  `ref_type_registry.register(ref_type, cls)` and returns cls
- At bottom of file: lazy-import `DataReference` and `MathDataReference`, register
  `"raw"` → `DataReference`, `"math"` → `MathDataReference`

**Circular import rule**: `catalog.py` and `math_reference.py` must NEVER import
`registry.py` at module level. The registry is accessed only via lazy imports inside
`from_csv` / `load_mixed` methods.

---

## Phase 2: DataReference.from_dict base (catalog.py)

Add `@classmethod from_dict(cls, d: dict) -> "DataReference"` to base `DataReference`:
- Strip NaN values: `{k: v for k, v in d.items() if not (isinstance(v, float) and math.isnan(v))}`
- Pop standard fields: `name`, `source`, `reader`, `ref_type`
- Return `cls(source=source, reader=reader, name=name, **remaining)`
- Serves as the fallback implementation for unregistered types

---

## Phase 3: MathDataReference.from_dict (math_reference.py)

Add `@classmethod from_dict(cls, d: dict) -> "MathDataReference"`:
- Strip NaN values
- Pop `name`, `expression`, `ref_type`, `source`, `reader` (source/reader unused for math)
- Pop `variable_map_refs` JSON column if present → parse to `{varname: ref_name}` dict
  for later name-lookup wiring via catalog
- Return `cls(expression=expression, name=name, **remaining)`
- **search_map is NOT reconstructed here** — the stub CSV row is display-only; the full
  definition (with search_map) comes from the sidecar YAML via `load_mixed`

---

## Phase 4: DataCatalog.from_csv updated (catalog.py)

Replace the current `from_csv` body:
- For each row, pop `ref_type` (default `"raw"`)
- Lazy import: `from dvue.registry import ref_type_registry`
- Look up `cls_for_type = ref_type_registry.get(ref_type)` → if None, warn + use `DataReference`
- Call `ref = cls_for_type.from_dict(row_dict)`
- After building catalog, auto-wire catalog into math refs:
  ```python
  for ref in catalog._references.values():
      if hasattr(ref, 'set_catalog') and getattr(ref, '_catalog', None) is None:
          ref.set_catalog(catalog)
  ```

---

## Phase 5: DataCatalog.save_mixed / load_mixed (catalog.py)

### save_mixed(self, path)
1. Call `self.to_csv(path)` — all refs written (math refs emit stub rows automatically
   since `expression` is stored in `_attributes` and appears in `to_dict()`)
2. Collect math refs: `[r for r in self._references.values() if r.ref_type == "math"]`
3. If any exist: compute sidecar path `Path(path).with_name(Path(path).stem + '_math.yaml')`
   and call `save_math_refs()` from `math_reference.py` filtered to those refs

### load_mixed(cls, path) — classmethod
1. Detect sidecar: `Path(path).with_name(Path(path).stem + '_math.yaml')`
2. If sidecar exists: load via `MathDataCatalogReader` → list of `MathDataReference` objects
   (full definitions with search_map)
3. Load CSV with registry dispatch (updated `from_csv` logic) but **skip rows where
   ref_type="math"** when sidecar exists — they are replaced by the full YAML definitions
4. Add YAML-loaded math refs to catalog
5. Auto-wire catalog into all math refs (`ref.set_catalog(catalog)`)
6. Return catalog

---

## Phase 6: from_dict on dsm2ui subclasses

All follow the same pattern: strip NaN, pop `name`/`source`/`reader`/`ref_type`,
return `cls(source=..., reader=..., name=..., **remaining)`.

| Class | File |
|-------|------|
| `DSM2DSSDataReference` | `dsm2ui/dsm2ui.py` |
| `DSM2TidefileDataReference` | `dsm2ui/dsm2ui.py` |
| `DSM2EchoInputDataReference` | `dsm2ui/dsm2ui.py` |
| `CalibDataReference` | `dsm2ui/calib/calibplotui.py` |

---

## Phase 7: dsm2ui auto-registration

### dsm2ui/registry.py (new)
- `_register_all()` function with lazy imports of the four subclasses + `ref_type_registry.register(...)` calls
- Registers: `"dsm2_dss"`, `"dsm2_hdf5"`, `"dsm2_echo_input"`, `"calib"`
- Called at module bottom so registration happens on first import

### dsm2ui/__init__.py
- Add at end: `from . import registry as _ref_registry  # registers DataReference types`

---

## Phase 8: dvue/__init__.py exports

Add:
```python
from .registry import ref_type_registry, register_ref_type
```
Add both to `__all__`.

---

## Relevant Files

| File | Change |
|------|--------|
| `dvue/dvue/registry.py` | **new** — global registry singleton + decorator |
| `dvue/dvue/catalog.py` | `DataReference.from_dict`, updated `from_csv`, new `save_mixed`/`load_mixed` |
| `dvue/dvue/math_reference.py` | `MathDataReference.from_dict` |
| `dvue/dvue/__init__.py` | export `ref_type_registry`, `register_ref_type` |
| `dsm2ui/dsm2ui/registry.py` | **new** — registers dsm2ui ref types |
| `dsm2ui/dsm2ui/__init__.py` | `from . import registry as _ref_registry` |
| `dsm2ui/dsm2ui/dsm2ui.py` | `from_dict` on 3 subclasses |
| `dsm2ui/dsm2ui/calib/calibplotui.py` | `from_dict` on `CalibDataReference` |

---

## Verification

1. `DataCatalog.from_csv` with `ref_type="dsm2_dss"` rows after `import dsm2ui` → instances are `DSM2DSSDataReference`
2. Unknown `ref_type` in CSV → warning emitted, falls back to base `DataReference`
3. `catalog.save_mixed(path)` with a mixed catalog → CSV has stub math rows + `_math.yaml` sidecar created
4. `DataCatalog.load_mixed(path)` round-trip → math refs have correct `search_map` from YAML, raw refs are correct subclasses, catalog back-link set
5. `import dsm2ui; from dvue.registry import ref_type_registry; ref_type_registry._registry` → shows all 4 dsm2ui types plus built-ins
6. `pytest dvue/tests/ dsm2ui/tests/` passes with no regressions

---

## Follow-up Consideration

`dssui`'s `DSSDataUIManager` currently uses the base `DataReference` (no subclass, no
`ref_type`). If generic DSS catalogs should also benefit from registry dispatch, a
`DSSDataReference` subclass would need to be added to `dssui/dssui.py`. Deferred until
needed.
