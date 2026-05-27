# DVue Plugin Entry Points — Setup Guide

## Overview

dvue now supports **automatic plugin discovery** via setuptools entry points. Instead of requiring users to pass `--plugin MODULE` on the command line, installed packages can register themselves in the `dvue.plugins` entry point group.

When `dvue ui` launches, all registered plugins are loaded automatically and their readers become available immediately for drag-and-drop file handling.

---

## For Plugin Package Maintainers

### 1. Create a registration function

In your package, define a simple function that registers your readers. Conventionally, this goes in a `readers.py` or similar module:

```python
# my_package/readers.py

def register_readers():
    """Register my package's readers with dvue.
    
    Called automatically by dvue at startup via entry points.
    """
    from dvue.registry import ReaderRegistry
    
    ReaderRegistry.register(
        "my_format",
        MyFormatReader,
        extensions=[".myext", ".myformat"],
    )
    ReaderRegistry.register(
        "my_other_format",
        MyOtherReader,
        extensions=[".other"],
    )
```

The function receives **no arguments** and should only call `ReaderRegistry.register()` to add your readers.

### 2. Add entry point to `pyproject.toml`

In your package's `pyproject.toml`, declare the entry point under the `dvue.plugins` group:

```toml
[project.entry-points."dvue.plugins"]
my_plugin = "my_package.readers:register_readers"
```

The format is:

```
entry_point_name = "module.path:function_name"
```

**Guidelines for naming:**
- Use a short, descriptive name (e.g., `dsm2ui`, `schismviz`, `my_hdf5_format`)
- Use underscores for multi-word names (e.g., `my_custom_reader`)

### 3. Install in development mode

```bash
pip install -e .
# or
pip install -e ".[dev]"
```

When installed, the entry point becomes discoverable by dvue.

---

## Example: dsm2ui Setup

Here's how dsm2ui would be updated:

**dsm2ui/readers.py (new file):**
```python
def register_readers():
    """Register dsm2ui readers with dvue."""
    from dvue.registry import ReaderRegistry
    from dsm2ui.dsm2ui import TidefileReader, DSM2DSSReader
    
    ReaderRegistry.register("dsm2_hdf5", TidefileReader, extensions=[".h5", ".hdf5"])
    ReaderRegistry.register("dsm2_dss", DSM2DSSReader, extensions=[".dss"])
```

**dsm2ui/pyproject.toml:**
```toml
[project.entry-points."dvue.plugins"]
dsm2ui = "dsm2ui.readers:register_readers"
```

Then users can simply run:
```bash
dvue ui run.h5 hist_qual.dss
# Automatically loads dsm2ui's readers without --plugin
```

---

## For dvue Users

### Auto-discovery in action

After plugins are installed with entry points:

```bash
# Before (explicit plugin loading required)
dvue ui --plugin dsm2ui.dsm2ui --plugin schismviz.readers file.h5

# After (auto-discovered)
dvue ui file.h5
```

Startup output shows which plugins were loaded:
```
✓ Loaded dvue plugin: dsm2ui
✓ Loaded dvue plugin: schismviz
```

### List available plugins

```bash
dvue list-plugins
```

Output:
```
Loaded plugins (from entry points):
  • dsm2ui
  • schismviz

Registered readers (3):
  • dsm2_hdf5              → TidefileReader         (.h5, .hdf5)
  • dsm2_dss               → DSM2DSSReader          (.dss)
  • schism_staout          → SchismReader           (.staout)
```

### Per-file reader override

When multiple readers support the same extension, force a reader per file
using `ref_type:path`:

```bash
dvue ui dsm2_dss:my_dsm2_output.dss dss:not_dsm2.dss
```

This bypasses extension dispatch for that file only.

### Diagnose plugin issues

```bash
dvue diagnose
dvue diagnose -v
```

This command reports entry-point discovery, plugin load failures, registered
reader keys, extension mappings, and environment details.

### Override or add plugins manually

The `--plugin` flag still works for:
- **Development** — loading local modules not yet installed
- **Testing** — selectively loading plugins
- **Custom extensions** — loading domain-specific readers

```bash
# Load entry-point plugins plus a custom local module
dvue ui --plugin my_local_readers file.h5
```

---

## Backward Compatibility

- **Existing `--plugin` usage still works** — explicit module imports are not removed
- **Last-write-wins** — if both entry points and `--plugin` register the same extension, the last one wins (typically the `--plugin` arg)
- **Graceful degradation** — if an entry point fails to load, a warning is logged but other plugins continue loading

---

## Troubleshooting

### Plugin not discovered

**Check installation:**
```bash
pip show my_package | grep Location
# Then verify entry_points in the installed metadata
pip show my_package -f | grep entry_points
```

**Or manually inspect:**
```bash
python -c "from importlib.metadata import entry_points; print(entry_points(group='dvue.plugins'))"
```

### Plugin fails to load

Check the startup logs for errors:
```bash
dvue ui 2>&1 | grep -i plugin
```

Or run the new diagnostics command:
```bash
dvue diagnose
# Use -v for detailed tracebacks
```

### Disable a plugin temporarily

Uninstall it, or use the `--plugin` flag to override entry points with a subset of readers.

---

## API Reference

### `ReaderRegistry.load_plugins_from_entry_points()`

```python
@classmethod
def load_plugins_from_entry_points() -> List[str]:
    """Auto-discover and load all plugins from setuptools entry points.
    
    Returns
    -------
    List[str]
        Names of successfully loaded plugins, in order.
    """
```

Called automatically by `dvue ui` on startup. Can also be called manually:

```python
from dvue.registry import ReaderRegistry

loaded = ReaderRegistry.load_plugins_from_entry_points()
print(f"Loaded {len(loaded)} plugins")
```

### `ReaderRegistry.get_registered_readers()`

```python
@classmethod
def get_registered_readers() -> Dict[str, Type[DataReferenceReader]]:
    """Return a shallow copy of the registered ref_type → reader class mapping."""
```

### `ReaderRegistry.get_registered_extensions()`

```python
@classmethod
def get_registered_extensions() -> Dict[str, Type[DataReferenceReader]]:
    """Return a shallow copy of the registered extension → reader class mapping."""
```

---

## See Also

- [setuptools entry points documentation](https://setuptools.readthedocs.io/en/latest/userguide/entry_points.html)
- [importlib.metadata documentation](https://docs.python.org/3/library/importlib.metadata.html)
- [DVue README — Plugin System](../README.md#how-the-plugin-system-works)
