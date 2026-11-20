# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **UI migrated to Panel Material UI.** `DataUI.create_view()`,
  `create_mobile_view()` and `create_responsive_view()` now return a
  `panel_material_ui.Page` instead of a `FastListTemplate`. The About and
  Disclaimer modals are a shared `pmui.Dialog` placed in the page's `main`
  area; `DataUI.add_header_buttons()` accepts either a `Page` or a classic
  Panel template.
- Requires `panel>=1.10`, `panel-material-ui>=0.16.6` and `bokeh>=3.10`.
- dvue's own buttons are now `dvue.mui.Button`, a `pmui.Button` that still
  accepts the classic arguments (`name`, `button_type`, `button_style`, tabler
  icon names such as `"chart-line"`) and translates them to Material labels,
  colours, variants and icons. Action dictionaries returned by
  `get_data_actions()` therefore need **no changes**. Code that finds buttons
  with `isinstance(x, pn.widgets.Button)` must also accept `pmui.Button`.
  `MenuButton`, `FileDownload` and the input widgets remain classic Panel
  widgets (themed Material via `design="material"`).
- Apps that read `.sidebar`/`.main`/`.modal` of the returned template must now
  treat them as plain lists on the Page (assign a new list rather than
  mutating in place) and use `page.main`/`page.header` for the dialog.

### Added
- Initial standalone release of dvue package
- Extracted from pydelmod parent package
- Core components:
  - `DataUIManager`: Base class for interactive data UIs
  - `TimeSeriesDataUI`: Specialized time series visualization
  - `FullScreen`: Fullscreen component for Panel objects
  - Action handlers: `PlotAction`, `DownloadDataAction`, `DownloadDataCatalogAction`, `PermalinkAction`
  - Utility functions for data handling
- Interactive features:
  - Map and table integration with bidirectional selection
  - Customizable plotting options
  - Data export capabilities
  - Permalink generation
- Documentation and examples
- Modern Python packaging with pyproject.toml
- Development tools configuration (black, isort, pytest, mypy)

### Changed
- Package structure reorganized for standalone distribution
- Updated dependencies to modern versions
- Improved type hints and documentation

## [0.1.0] - 2026-01-10

### Added
- Initial release as standalone package
