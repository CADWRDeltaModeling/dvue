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
    TransformToCatalogAction,
    SourceCompareAction,
)
from .math_ref_editor import MathRefEditorAction
from .fullscreen import FullScreen
from .catalog import (
    DataReferenceReader,
    InMemoryDataReferenceReader,
    CallableDataReferenceReader,
    FileDataReferenceReader,
    DataReference,
    CatalogView,
    CatalogBuilder,
    DataCatalogReader,  # backward-compat alias for CatalogBuilder
    DataCatalog,
)
from .math_reference import (
    MathDataReference,
    MathDataCatalogReader,
    save_math_refs,
)
from .readers import (
    CSVDirectoryBuilder,
    PatternCSVDirectoryBuilder,
    CSVDirectoryReader,          # backward-compat alias
    PatternCSVDirectoryReader,   # backward-compat alias
)

__all__ = [
    # UI layer
    "DataProvider",
    "DataUIManager",
    "PlotAction",
    "PermalinkAction",
    "DownloadDataAction",
    "DownloadDataCatalogAction",
    "TransformToCatalogAction",
    "SourceCompareAction",
    "MathRefEditorAction",
    "FullScreen",
    # DataReferenceReader hierarchy
    "DataReferenceReader",
    "InMemoryDataReferenceReader",
    "CallableDataReferenceReader",
    "FileDataReferenceReader",
    # Catalog core
    "DataReference",
    "CatalogView",
    "MathDataReference",
    "CatalogBuilder",
    "DataCatalogReader",   # backward-compat alias for CatalogBuilder
    "DataCatalog",
    # Math ref persistence
    "MathDataCatalogReader",
    "save_math_refs",
    # Builder implementations (dvue.readers)
    "CSVDirectoryBuilder",
    "PatternCSVDirectoryBuilder",
    "CSVDirectoryReader",          # backward-compat alias
    "PatternCSVDirectoryReader",   # backward-compat alias
]
