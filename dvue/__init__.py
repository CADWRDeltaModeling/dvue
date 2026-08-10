"""
dvue - Data Visualization and UI components
"""

import os

__version__ = "0.1.0"

# Disable PROJ's on-demand network grid downloads (cdn.proj.org). Recent
# PROJ/pyproj versions may fetch high-accuracy datum-shift grids (e.g.
# us_noaa_cnhpgn.tif) over the network when reprojecting CRSs for map
# rendering. Behind corporate proxies/firewalls this can fail with a
# certificate revocation check error (pyproj.exceptions.ProjError) and crash
# the UI. Map visualizations here do not need that level of geodetic
# accuracy, so force offline/local grids only. Must run before cartopy/pyproj
# are imported below.
os.environ.setdefault("PROJ_NETWORK", "OFF")
try:
    import pyproj
    pyproj.network.set_network_enabled(False)
except Exception:
    pass

from .dataui import DataProvider, DataUIManager
from .actions import (
    PlotAction,
    ReportAction,
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
from .session_persistence import make_reset_session_button, SessionManager
from .registry import ReaderRegistry
from .registry_ui import RegistryUIManager, RegistryPlotAction
from .views import ViewDefinition, ViewsManager

__all__ = [
    # UI layer
    "DataProvider",
    "DataUIManager",
    "PlotAction",
    "ReportAction",
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
    # Session utilities
    "make_reset_session_button",
    "SessionManager",
    # Reader registry
    "ReaderRegistry",
    "RegistryUIManager",
    "RegistryPlotAction",
    # Catalog views
    "ViewDefinition",
    "ViewsManager",
]
