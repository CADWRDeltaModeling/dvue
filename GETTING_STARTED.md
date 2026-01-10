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
├── dataui.py                - Main DataUIManager class
├── tsdataui.py              - Time series data UI component
├── fullscreen.py            - Fullscreen component
├── utils.py                 - Utility functions
└── dataui.noselection.html  - HTML template
```

