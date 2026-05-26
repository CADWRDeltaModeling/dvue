# dvue

**Data Visualization and UI components for interactive data exploration**

`dvue` is a Python package that provides reusable UI components for creating interactive data visualization dashboards using Panel, HoloViews, and GeoViews. It's designed to make it easy to build rich, interactive web applications for exploring time series catalogs represented as tabular data where each row corresponds to a data series (e.g., monitoring stations). If the catalog contains geospatial information (latitude/longitude), `dvue` can integrate maps for spatial selection.

## Features

- 🗺️ **Interactive Map Integration**: Seamlessly integrate maps with data tables that contain gis information using GeoViews and Cartopy
- 📊 **Time Series Visualization**: Built-in support for time series data multi-line plotting
- 📋 **Rich Table Interface**: Interactive tables with sorting, filtering, and pagination using Panel's Tabulator
- 🔗 **Bidirectional Selection**: Click on map features to select table rows and vice versa
- 🎨 **Customizable Styling**: Configure colors, markers, and plot options
- 📥 **Data Export**: Download data and catalogs as CSV files
- 🔗 **Permalink Support**: Generate shareable links to specific views
- 🎯 **Fullscreen Mode**: Expand visualizations to fullscreen for detailed analysis

## Installation

### From PyPI (when published)

```bash
pip install dvue
```

### From Source

```bash
git clone https://github.com/CADWRDeltaModeling/dvue.git
cd dvue
pip install -e .
```

For development:

```bash
pip install dvue[dev]
```

## Quick Start

The easiest way to get started is to explore the working examples in the `examples/` directory:

### Basic Time Series Data UI (No Map)

A simple example showing time series visualization without geographic components:

```bash
panel serve examples/ex_basic_tsdataui.py --show
```

See [`examples/ex_basic_tsdataui.py`](examples/ex_basic_tsdataui.py) for the complete implementation. This example demonstrates:
- Creating a data catalog with station information
- Generating synthetic time series data
- Implementing a custom `TimeSeriesDataUIManager`
- Interactive table selection and plotting

### Time Series Data UI with Map Integration

A complete example with geographic features and interactive map:

```bash
panel serve examples/ex_tsdataui.py --show
```

See [`examples/ex_tsdataui.py`](examples/ex_tsdataui.py) for the complete implementation. This example shows:
- GeoDataFrame integration with coordinates
- Interactive map with station locations
- Advanced time series plotting with multiple variables
- Bidirectional selection between map and table

## Command-Line Interface

After installing dvue, the `dvue` command is available on your PATH.

### `dvue ui` — generic file viewer

```
dvue ui [--plugin MODULE]... [--port PORT] [--desktop] [FILES...]
```

Launches a `RegistryUIManager` window pre-loaded with FILES.  Omit FILES to
start empty and add files via drag-and-drop.

| Option | Description |
|---|---|
| `--plugin MODULE` | Import MODULE before launching (may be repeated). Modules that call `ReaderRegistry.register()` at import time register their file-type readers. |
| `--port PORT` | TCP port for the Panel server. `0` (default) picks a free port automatically. |
| `--desktop` | Open in a native OS window via pywebview instead of a browser tab. Requires `pip install pywebview`. |

**Examples**

```bash
# Drag-and-drop DSM2 HDF5 tidefiles and DSS output files
dvue ui --plugin dsm2ui.dsm2ui --desktop

# Pre-load specific files; mix .h5 and .dss freely
dvue ui --plugin dsm2ui.dsm2ui run.h5 hist_qual.dss hist_hydro.dss

# Multiple plugin packages — each registers its own file extensions
dvue ui --plugin dsm2ui.dsm2ui --plugin schismviz.readers output.staout run.h5

# Browser mode on a fixed port
dvue ui --plugin dsm2ui.dsm2ui --port 5007 run.h5
```

### How the plugin system works

`ReaderRegistry` (in `dvue.registry`) is a class-level dict mapping a
`ref_type` key and file extension list to a reader class.  Any installed
package can register its readers at module-import time:

```python
# inside dsm2ui/dsm2ui.py (runs when --plugin dsm2ui.dsm2ui is passed)
from dvue.registry import ReaderRegistry
ReaderRegistry.register("dsm2_hdf5", TidefileReader, extensions=[".h5", ".hdf5"])
ReaderRegistry.register("dsm2_dss",  DSM2DSSReader,  extensions=[".dss"])
```

The `dvue ui` command imports each `--plugin` module *before* constructing
the manager, so all registered readers are available when the catalog is
built.  Dropping additional files onto the running window after launch also
works — the registry resolves the reader from the dropped file's extension.

### `dvue show-version`

```bash
dvue show-version
```

## Core Components

### DataUIManager

Base class for creating custom data visualization UIs. Provides:
- Interactive table with selection support
- Action buttons (Plot, Download, Permalink)
- Map integration for geospatial data
- Progress indicators
- Customizable widgets

### TimeSeriesDataUIManager

Specialized manager for time series data with features like:
- Multi-station plotting with customizable colors and line styles
- Date range selection
- Data filtering (e.g., Lanczos filter)
- Aggregation options (daily, monthly, yearly averages)
- Overlay capabilities for comparing multiple datasets
- Interactive catalog table with row selection
- Dynamic plot generation based on selections

### Actions

Pre-built action handlers:
- `PlotAction`: Generate plots from selected data
- `DownloadDataAction`: Export selected data to CSV
- `DownloadDataCatalogAction`: Export full catalog to CSV
- `PermalinkAction`: Create shareable URLs

### Math References

`MathDataReference` lets you define derived series as Python expressions evaluated over other catalog entries — with full NumPy support and a built-in interactive editor. See **[README-mathref.md](README-mathref.md)** for the full guide including YAML format, the in-UI editor, load/save workflow, and code examples.

### FullScreen

Component for adding fullscreen capability to any Panel object.

## Dependencies

- **pandas**: Data manipulation
- **geopandas**: Geospatial data support
- **panel**: Interactive dashboards
- **holoviews**: Declarative visualization
- **hvplot**: High-level plotting API
- **geoviews**: Geographic plotting
- **bokeh**: Interactive visualization backend
- **param**: Parameterized objects
- **cartopy**: Cartographic projections
- **colorcet**: Perceptually uniform colormaps

## Development

### Setup Development Environment

```bash
git clone https://github.com/CADWRDeltaModeling/dvue.git
cd dvue
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest
```

### Code Formatting

```bash
black .
isort .
flake8 .
```

## Examples

The `examples/` directory contains complete working examples:

- **[`ex_basic_tsdataui.py`](examples/ex_basic_tsdataui.py)** - Basic time series visualization without geographic features
  - Simple data catalog with stations
  - Synthetic time series generation
  - Interactive table and plotting
  - Good starting point for learning the framework

- **[`ex_tsdataui.py`](examples/ex_tsdataui.py)** - Advanced example with full features
  - GeoDataFrame integration
  - Interactive map with station locations
  - Multiple variables and intervals
  - Bidirectional map/table selection
  - Math Reference editor with YAML load/save
  - Complete implementation reference

See **[README-mathref.md](README-mathref.md)** for the full Math Reference guide.

Run any example with:
```bash
panel serve examples/<example_name>.py --show
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Authors

California Department of Water Resources - Delta Modeling Section

## Acknowledgments

This package was originally part of the `pydelmod` package and has been extracted into a standalone library for broader use.
