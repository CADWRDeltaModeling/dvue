"""
dvue - Data Visualization and UI components
"""

__version__ = "0.1.0"

from .dataui import DataProvider, DataUIManager
from .actions import (
    PlotAction,
    PermalinkAction,
    DownloadDataAction,
    DownloadDataCatalogAction,
)
from .fullscreen import FullScreen
from .catalog import (
    DataReference,
    CatalogView,
    MathDataReference,
    DataCatalogReader,
    DataCatalog,
)
from .readers import (
    CSVDirectoryReader,
    PatternCSVDirectoryReader,
)

__all__ = [
    # UI layer
    "DataProvider",
    "DataUIManager",
    "PlotAction",
    "PermalinkAction",
    "DownloadDataAction",
    "DownloadDataCatalogAction",
    "FullScreen",
    # Catalog core
    "DataReference",
    "CatalogView",
    "MathDataReference",
    "DataCatalogReader",
    "DataCatalog",
    # Sample reader implementations (dvue.readers)
    "CSVDirectoryReader",
    "PatternCSVDirectoryReader",
]
