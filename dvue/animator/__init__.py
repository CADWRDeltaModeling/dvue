"""dvue.animator — geo-animation sub-package.

Provides a framework for animating spatial data over time:
- :class:`SlicingReader` — abstract base; subclass to connect any data source.
- :class:`InMemorySlicingReader` — wraps a ``pd.DataFrame(index=DatetimeIndex,
  columns=geo_ids)`` for immediate use.
- :class:`BufferedSlicingReader` — keeps a rolling in-memory buffer for HDF5.
- :class:`TransformedSlicingReader` — applies a time-domain transform (full load, legacy).
- :class:`TransformSpec` — describes a streaming transform (kind, overlap, output_freq).
- :class:`StreamingTransformedSlicingReader` — applies a transform per-chunk, no full load.
- :class:`DiffSlicingReader` — computes element-wise A − B between two readers.
- :class:`GeoAnimatorManager` — single-reader Panel Viewer animated by a DateSlider.
- :class:`MultiGeoAnimatorManager` — two-reader side-by-side or diff Panel Viewer.
- :data:`CURATED_COLORMAPS` — curated list of valid colormap names.

UI classes are lazily imported so that importing reader classes does not pull
in Panel, HoloViews, GeoViews, Cartopy, or geopandas.
"""

from .reader import (
    SlicingReader,
    InMemorySlicingReader,
    BufferedSlicingReader,
    TransformedSlicingReader,
    TransformSpec,
    StreamingTransformedSlicingReader,
    DiffSlicingReader,
)

__all__ = [
    "SlicingReader",
    "InMemorySlicingReader",
    "BufferedSlicingReader",
    "TransformedSlicingReader",
    "TransformSpec",
    "StreamingTransformedSlicingReader",
    "DiffSlicingReader",
    "GeoAnimatorManager",
    "MultiGeoAnimatorManager",
    "CURATED_COLORMAPS",
]


def __getattr__(name: str):
    if name in ("GeoAnimatorManager", "CURATED_COLORMAPS"):
        from .ui import GeoAnimatorManager, CURATED_COLORMAPS  # noqa: F401
        g = {"GeoAnimatorManager": GeoAnimatorManager, "CURATED_COLORMAPS": CURATED_COLORMAPS}
        return g[name]
    if name == "MultiGeoAnimatorManager":
        from .multi_ui import MultiGeoAnimatorManager  # noqa: F401
        return MultiGeoAnimatorManager
    raise AttributeError(f"module 'dvue.animator' has no attribute {name!r}")


def __getattr__(name: str):
    if name in ("GeoAnimatorManager", "CURATED_COLORMAPS"):
        from .ui import GeoAnimatorManager, CURATED_COLORMAPS  # noqa: F401
        g = {"GeoAnimatorManager": GeoAnimatorManager, "CURATED_COLORMAPS": CURATED_COLORMAPS}
        return g[name]
    if name == "MultiGeoAnimatorManager":
        from .multi_ui import MultiGeoAnimatorManager  # noqa: F401
        return MultiGeoAnimatorManager
    raise AttributeError(f"module 'dvue.animator' has no attribute {name!r}")
