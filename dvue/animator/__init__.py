"""dvue.animator — geo-animation sub-package.

Provides a framework for animating spatial data over time:
- :class:`SlicingReader` — abstract base; subclass to connect any data source.
- :class:`InMemorySlicingReader` — wraps a ``pd.DataFrame(index=DatetimeIndex,
  columns=geo_ids)`` for immediate use.
- :class:`GeoAnimatorManager` — Panel Viewer that combines a SlicingReader with
  a GeoDataFrame to produce a map animated by a DateSlider.
- :data:`CURATED_COLORMAPS` — curated list of valid colormap names.

``GeoAnimatorManager`` and ``CURATED_COLORMAPS`` are lazily imported so that
code which only uses :class:`SlicingReader` or :class:`InMemorySlicingReader`
does not pull in Panel, HoloViews, GeoViews, Cartopy, or geopandas.
"""

from .reader import SlicingReader, InMemorySlicingReader, BufferedSlicingReader

__all__ = [
    "SlicingReader",
    "InMemorySlicingReader",
    "BufferedSlicingReader",
    "GeoAnimatorManager",
    "CURATED_COLORMAPS",
]

# GeoAnimatorManager and CURATED_COLORMAPS are imported lazily so that
# heavy optional dependencies (Panel, HoloViews, GeoViews, Cartopy,
# geopandas) are NOT pulled in just because code imports SlicingReader or
# InMemorySlicingReader.


def __getattr__(name: str):
    if name in ("GeoAnimatorManager", "CURATED_COLORMAPS"):
        from .ui import GeoAnimatorManager, CURATED_COLORMAPS  # noqa: F401
        g = {"GeoAnimatorManager": GeoAnimatorManager, "CURATED_COLORMAPS": CURATED_COLORMAPS}
        return g[name]
    raise AttributeError(f"module 'dvue.animator' has no attribute {name!r}")
