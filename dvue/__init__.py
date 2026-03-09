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
from .math_ref_editor import MathRefEditorAction
from .fullscreen import FullScreen
from .catalog import (
    DataReference,
    CatalogView,
    DataCatalogReader,
    DataCatalog,
)
from .math_reference import (
    MathDataReference,
    MathDataCatalogReader,
    save_math_refs,
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
    "MathRefEditorAction",
    "FullScreen",
    # Catalog core
    "DataReference",
    "CatalogView",
    "MathDataReference",
    "DataCatalogReader",
    "DataCatalog",
    # Math ref persistence
    "MathDataCatalogReader",
    "save_math_refs",
    # Sample reader implementations (dvue.readers)
    "CSVDirectoryReader",
    "PatternCSVDirectoryReader",
]
