# dvue Package

## 📦 Overview

The `dvue` package provides interactive data visualization components, primarily focused on geospatial and time series data, using libraries such as Panel, HoloViews, and GeoViews.

The core functionality is encapsulated in the `DataUIManager` class, which offers a user-friendly interface for exploring datasets with features like plotting, downloading, and permalink generation.
The Time Series Data UI component (`TSDataUI`) extends this functionality to handle time series data.

### Core Package Structure
```
dvue/
├── __init__.py              - Package initialization with exports
├── actions.py               - Action handlers (Plot, Download, Permalink)
├── catalog.py               - DataReference, DataCatalog, MathDataReference
├── cli.py                   - Console entry point (`dvue` command)
├── dataui.py                - Main DataUIManager class
├── registry.py              - ReaderRegistry (extension → reader dispatch)
├── registry_ui.py           - RegistryUIManager, RegistryPlotAction
├── session_persistence.py   - serve_session_app, serve_desktop_app
├── tsdataui.py              - Time series data UI component
├── fullscreen.py            - Fullscreen component
├── utils.py                 - Utility functions
└── dataui.noselection.html  - HTML template
```

---

## 🚀 Quickstart — command line

The simplest way to explore data files is the `dvue ui` command.  It starts
a `RegistryUIManager` window where you can drag-and-drop files or pre-load
them from the command line.

```bash
# Install dvue (development mode)
pip install -e .

# Launch an empty window — drag-and-drop files onto it
dvue ui --desktop

# Pre-load files (installed plugins auto-load via entry points)
dvue ui run.h5 hist_qual.dss

# Multiple reader packages in one session (optional explicit module)
dvue ui --plugin schismviz.readers output.staout run.h5

# Per-file reader override when multiple readers support same extension
dvue ui dsm2_dss:my_dsm2.dss dss:not_dsm2.dss
```

Installed plugins are discovered automatically from the `dvue.plugins`
entry-point group at startup. The `--plugin` flag is still useful for local
development modules that are not installed as entry points.

The optional `ref_type:path` syntax lets you force a specific reader for a
single file when extensions overlap. For example,
`dsm2_dss:my_dsm2.dss` uses the `dsm2_dss` reader key even if `.dss`
would otherwise map to another reader.

The `--desktop` flag opens the app in a native OS window (requires
`pip install pywebview`) instead of a browser tab.

---

## 🧩 ReaderRegistry — adding new file types

To make a new file extension available in `dvue ui`, register a reader class
with `ReaderRegistry` anywhere in your package that runs at import time:

```python
# my_package/readers.py
from dvue.registry import ReaderRegistry

class MyFormatReader:
    def __init__(self, path):
        self.path = path

    def scan(self):
        """Return a list of DataReference objects describing the file contents."""
        ...

    def load(self, **attributes):
        """Load and return a pandas DataFrame for the given attributes."""
        ...

ReaderRegistry.register("my_format", MyFormatReader, extensions=[".myext"])
```

Then expose that registration function through a package entry point:

```toml
[project.entry-points."dvue.plugins"]
my_package = "my_package.readers:register_readers"
```

After installation, `.myext` files are available in `dvue ui` automatically
without requiring `--plugin`.

Use `dvue diagnose` to verify plugin discovery and extension mappings.

---

## 🏗️ Subclassing RegistryUIManager

For domain-specific applications, subclass `RegistryUIManager` to normalise
attribute names and expand metadata after a file is loaded:

```python
from dvue.registry_ui import RegistryUIManager, RegistryPlotAction

class MyAppUIManager(RegistryUIManager):

    def normalize_ref(self, ref):
        """Map source-specific attribute names to the common station/variable schema."""
        if not ref._attributes.get("station"):
            ref.set_attribute("station", ref._attributes.get("id", ""))
        if not ref._attributes.get("variable"):
            ref.set_attribute("variable", ref._attributes.get("param", "").lower())

    def on_file_added(self, path, refs):
        """Called after a file's refs are added to the catalog."""
        # e.g. expand time_range from file metadata
        pass

    def _make_plot_action(self):
        return MyPlotAction()
```

Override only the hooks you need; everything else (catalog building,
drag-and-drop wiring, session persistence, plot actions) is handled by the
base classes.

---

## 📚 Further reading

- **[README.md](README.md)** — Feature overview, examples, dependencies
- **[README-mathref.md](README-mathref.md)** — Math Reference expression system
- **[docs/Architecture.md](docs/Architecture.md)** — High-level architecture
- **[AGENTS.md](AGENTS.md)** — Agent/developer conventions and pitfalls
