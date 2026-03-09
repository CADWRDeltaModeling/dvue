# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
