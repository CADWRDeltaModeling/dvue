"""GeoAnimatorManager — Panel Viewer for time-animated geo maps.

Uses direct Bokeh ``ColumnDataSource`` patching for fast animation:
only the scalar ``_value`` column is sent over WebSocket per frame.
Geometry (xs/ys) is pre-extracted at init and never re-serialized.
"""

from __future__ import annotations

import datetime
from typing import Optional

import geopandas as gpd
import matplotlib.cm
import matplotlib.colors
import numpy as np
import pandas as pd
import panel as pn
import param
from bokeh.models import (
    BasicTicker,
    ColorBar,
    ColumnDataSource,
    Div,
    HoverTool,
    LinearColorMapper,
    Range1d,
    WMTSTileSource,
)
from bokeh.plotting import figure as bk_figure

from .reader import (SlicingReader, BufferedSlicingReader,
                     StreamingTransformedSlicingReader, TransformSpec,
                     RawSequentialBuffer)

# ---------------------------------------------------------------------------
# Curated colormaps (subset that works well with numeric data on maps)
# ---------------------------------------------------------------------------

# Flat list used by param.Selector for validation.
CURATED_COLORMAPS: list[str] = [
    # Sequential — perceptually uniform, colorblind-safe (recommended)
    "viridis",          # perceptually uniform; safe for all dichromacy types
    "plasma",           # warm alternative; high discrimination
    "cividis",          # optimized specifically for deuteranopia (Nuñez et al. 2018)
    "inferno",          # high contrast on dark backgrounds
    "magma",            # high contrast on dark backgrounds
    # Sequential — domain-intuitive
    "cet_fire",         # black→blue→orange→white; dark=fresh, bright=saline (EC)
    "cet_CET_L2",       # blue→green→yellow; strong water-level intuition
    "Blues",            # water-blue association; not perceptually uniform
    "YlOrRd",           # warm; intuitive for heat / conductivity
    # Sequential — high-contrast (not colorblind-safe)
    "turbo",            # good contrast; not perceptually uniform; not colorblind-safe
    # Diverging — two-hue, centred on a neutral midpoint
    "coolwarm",         # best general-purpose diverging; perceptually symmetric
    "RdBu_r",           # blue=negative/upstream, red=positive/downstream convention
    "RdYlBu_r",         # yellow midpoint aids near-zero discrimination
    "cet_CET_D9",       # blue→yellow→red; colorblind-accessible diverging
    "PiYG",             # pink/green; useful for anomaly-style differences
]

# Flat list for pn.widgets.Select that includes a visual separator.
# The separator string is not a valid colormap — callbacks guard against it.
_COLORMAP_SEPARATOR = "── Diverging ──"
CURATED_COLORMAPS_WITH_SEP: list = [
    "viridis", "plasma", "cividis", "inferno", "magma",
    "cet_fire", "cet_CET_L2", "Blues", "YlOrRd", "turbo",
    _COLORMAP_SEPARATOR,
    "coolwarm", "RdBu_r", "RdYlBu_r", "cet_CET_D9", "PiYG",
]

# ---------------------------------------------------------------------------
# Tile URL constant (WMTSTileSource is instantiated inside __init__ so it
# is always created within an active Bokeh document context)
# ---------------------------------------------------------------------------

_CARTO_LIGHT_URL = "https://basemaps.cartocdn.com/light_all/{Z}/{X}/{Y}.png"
_CARTO_LIGHT_ATTR = "© CARTO / © OpenStreetMap contributors"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cmap_to_palette(name: str, n: int = 256) -> list[str]:
    """Convert a matplotlib colormap name to a Bokeh-compatible hex palette."""
    try:
        cmap = matplotlib.colormaps[name]       # matplotlib ≥ 3.7
    except AttributeError:
        cmap = matplotlib.cm.get_cmap(name)     # older matplotlib
    return [matplotlib.colors.to_hex(cmap(i / max(n - 1, 1))) for i in range(n)]


def _nice_decimal_places(levels: list) -> int:
    """Return the number of decimal places needed for plain-numeric labels.

    Rules:
    - If all levels are whole numbers (or very close): 0 decimals.
    - Otherwise find the smallest gap between adjacent levels and use
      enough decimals to show at least one significant digit of that gap.
    - Cap at 4 decimals to avoid excessive precision.
    """
    if not levels:
        return 0
    # Treat as integer if all within floating-point noise of a whole number
    if all(abs(v - round(v)) < 1e-9 for v in levels):
        return 0
    sorted_lvls = sorted(levels)
    gaps = [sorted_lvls[i + 1] - sorted_lvls[i]
            for i in range(len(sorted_lvls) - 1) if sorted_lvls[i + 1] > sorted_lvls[i]]
    if not gaps:
        return 1
    min_gap = min(gaps)
    if min_gap >= 1.0:
        return 0
    import math
    # digits needed to represent min_gap with 1 significant figure
    n_dec = max(0, -int(math.floor(math.log10(min_gap))))
    return min(n_dec, 4)


def _format_level(value: float, n_dec: int) -> str:
    """Format a contour level value as a plain number (no scientific notation)."""
    if n_dec == 0:
        return str(int(round(value)))
    return f"{value:.{n_dec}f}"


def _level_colors(
    lvls: list, vmin: float, vmax: float, colormap: str
) -> list[str]:
    """Map a list of isovalues to hex colours using *colormap*.

    Each level is normalised to [0, 1] within [vmin, vmax] and sampled from
    the colormap palette so contour lines carry the same colour as the
    corresponding data region.
    """
    palette = _cmap_to_palette(colormap, n=256)
    span = vmax - vmin if vmax != vmin else 1.0
    colors = []
    for lvl in lvls:
        t = max(0.0, min(1.0, (float(lvl) - vmin) / span))
        colors.append(palette[int(t * 255)])
    return colors


# ---------------------------------------------------------------------------
# Shared contour computation — module-level so multi_ui.py can import them
# ---------------------------------------------------------------------------

def _clip_contour_segment(
    seg: np.ndarray,
    lvl: float,
    clip_zone,
    xs_out: list,
    ys_out: list,
    lvl_out: list,
) -> None:
    """Clip one contour segment to *clip_zone* and append valid sub-paths."""
    if clip_zone is None:
        xs_out.append(seg[:, 0].tolist())
        ys_out.append(seg[:, 1].tolist())
        lvl_out.append(lvl)
        return
    from shapely.geometry import LineString, MultiLineString
    line = LineString(seg)
    clipped = line.intersection(clip_zone)
    if clipped.is_empty:
        return
    if isinstance(clipped, LineString):
        parts = [clipped]
    elif isinstance(clipped, MultiLineString):
        parts = list(clipped.geoms)
    else:
        parts = [
            g for g in (clipped.geoms if hasattr(clipped, "geoms") else [clipped])
            if isinstance(g, LineString) and len(g.coords) > 1
        ]
    for part in parts:
        coords = np.array(part.coords)
        if len(coords) > 1:
            xs_out.append(coords[:, 0].tolist())
            ys_out.append(coords[:, 1].tolist())
            lvl_out.append(lvl)


def _compute_contour_levels(
    finite_vals: np.ndarray,
    vmin: float,
    vmax: float,
    n: int,
    mode: str,
    custom_levels: str,
) -> np.ndarray:
    """Return a sorted level array for contouring.

    When *custom_levels* is a non-empty comma-separated string, those values
    are returned directly (sorted) and all other parameters are ignored.
    """
    custom_str = (custom_levels or "").strip()
    if custom_str:
        try:
            explicit = np.array(
                [float(v) for v in custom_str.split(",") if v.strip()]
            )
            explicit = np.sort(explicit)
            if len(explicit) > 0:
                return explicit
        except ValueError:
            pass

    if mode == "eq_hist" and len(finite_vals) >= n:
        quantiles = np.linspace(0.0, 1.0, n + 2)[1:-1]
        levels = np.quantile(finite_vals, quantiles)
        levels = levels[(levels > vmin) & (levels < vmax)]
        if len(levels) == 0:
            levels = np.linspace(vmin, vmax, n + 2)[1:-1]
        return levels

    if mode == "nice":
        from matplotlib.ticker import MaxNLocator
        locator = MaxNLocator(nbins=n, steps=[1, 2, 2.5, 5, 10])
        levels = np.asarray(locator.tick_values(vmin, vmax))
        levels = levels[(levels > vmin) & (levels < vmax)]
        if len(levels) == 0:
            levels = np.linspace(vmin, vmax, n + 2)[1:-1]
        return levels

    return np.linspace(vmin, vmax, n + 2)[1:-1]


def _run_contour_computation(
    vals: np.ndarray,
    centroids_x: np.ndarray,
    centroids_y: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    sigma: float,
    levels_arr: np.ndarray,
    clip_zone,
) -> tuple:
    """Rasterize *vals* → smooth → contour at *levels_arr* → clip.

    Returns ``(xs_out, ys_out, lvl_out)`` suitable for a Bokeh
    ``multi_line`` + ``level`` column-data-source.
    """
    from scipy.interpolate import griddata
    from scipy.ndimage import gaussian_filter
    import matplotlib.pyplot as plt

    pts = np.column_stack([centroids_x, centroids_y])
    vals = np.asarray(vals, dtype=float)
    mask = np.isfinite(vals)
    if mask.sum() < 4:
        return [], [], []

    grid_z = griddata(pts[mask], vals[mask], (grid_x, grid_y), method="nearest")
    if sigma > 0:
        grid_z = gaussian_filter(grid_z.astype(float), sigma=sigma)

    xs_out, ys_out, lvl_out = [], [], []
    fig, ax = plt.subplots(1, 1)
    try:
        # Guard: matplotlib requires strictly increasing levels.
        levels_arr = np.unique(np.asarray(levels_arr, dtype=float))
        if len(levels_arr) < 2:
            return [], [], []
        cs = ax.contour(grid_x, grid_y, grid_z, levels=levels_arr)
        if hasattr(cs, "allsegs"):
            for i, lvl in enumerate(cs.levels):
                for seg in cs.allsegs[i]:
                    if len(seg) > 1:
                        _clip_contour_segment(seg, float(lvl), clip_zone, xs_out, ys_out, lvl_out)
        else:
            for collection, lvl in zip(cs.collections, cs.levels):
                for path in collection.get_paths():
                    v = path.vertices
                    if len(v) > 1:
                        _clip_contour_segment(v, float(lvl), clip_zone, xs_out, ys_out, lvl_out)
    finally:
        plt.close(fig)
    return xs_out, ys_out, lvl_out


def _make_contour_grid(
    gdf_proj: "gpd.GeoDataFrame",
    geom_type: str,
    buffer_m: float = None,
) -> tuple:
    """Build the contour interpolation grid + clip zone for a projected GDF.

    Returns ``(centroids_x, centroids_y, grid_x, grid_y, clip_zone, cell_size)``.
    All coordinates are in the GDF's CRS (expected EPSG:3857).

    Parameters
    ----------
    buffer_m : float or None
        Clip zone buffer radius in metres.  Defaults to ``cell_size * 3``.
    """
    if geom_type == "point":
        centroids_x = gdf_proj.geometry.x.values.copy()
        centroids_y = gdf_proj.geometry.y.values.copy()
    else:
        cx_list, cy_list = [], []
        for geom in gdf_proj.geometry:
            c = geom.centroid
            if c.is_empty:
                # Degenerate geometry (NaN/inf coords from a bad CRS conversion).
                # Use NaN so the centroid array stays the same length as geo_ids.
                cx_list.append(float("nan"))
                cy_list.append(float("nan"))
            else:
                cx_list.append(c.x)
                cy_list.append(c.y)
        centroids_x = np.array(cx_list)
        centroids_y = np.array(cy_list)

    bounds = gdf_proj.total_bounds
    span_x = float(bounds[2] - bounds[0])
    span_y = float(bounds[3] - bounds[1])
    if not np.isfinite(span_x) or span_x <= 0:
        span_x = 1.0
    if not np.isfinite(span_y) or span_y <= 0:
        span_y = 1.0
    nx = 200
    ny = max(int(round(200 * span_y / span_x)), 10)
    grid_x, grid_y = np.meshgrid(
        np.linspace(bounds[0], bounds[2], nx),
        np.linspace(bounds[1], bounds[3], ny),
    )
    _cell_size = max(span_x / nx, span_y / ny)
    _buf_radius = buffer_m if buffer_m is not None else _cell_size * 3.0
    try:
        from shapely.ops import unary_union
        clip_zone = unary_union(gdf_proj.geometry).buffer(_buf_radius)
    except Exception:
        clip_zone = None
    return centroids_x, centroids_y, grid_x, grid_y, clip_zone, _cell_size


# ---------------------------------------------------------------------------
# Geometry type constants
# ---------------------------------------------------------------------------

_POINT_TYPES = {"Point", "MultiPoint"}
_POLYGON_TYPES = {"Polygon", "MultiPolygon"}
_LINE_TYPES = {"LineString", "MultiLineString", "LinearRing"}


def _detect_geom_type(gdf: gpd.GeoDataFrame) -> str:
    """Return ``'point'``, ``'polygon'``, or ``'line'`` from modal geometry type.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame

    Returns
    -------
    str
        One of ``'point'``, ``'polygon'``, ``'line'``.

    Raises
    ------
    ValueError
        If the geometry type is not one of the three recognised families.
    """
    types = set(gdf.geometry.geom_type.dropna().unique())
    if not types:
        raise ValueError(
            "GeoDataFrame has no valid (non-null) geometries. "
            "Ensure the shapefile or GeoJSON contains geometries that match the "
            "channel IDs in the HDF5 file."
        )
    if types & _POLYGON_TYPES:
        return "polygon"
    if types & _POINT_TYPES:
        return "point"
    if types & _LINE_TYPES:
        return "line"
    raise ValueError(
        f"Unsupported geometry type(s): {types}. "
        "Supported families: Point, Polygon, LineString (and Multi- variants)."
    )


# ---------------------------------------------------------------------------
# GeoAnimatorManager
# ---------------------------------------------------------------------------

class GeoAnimatorManager(pn.viewable.Viewer):
    """Panel Viewer that animates geo shapes coloured by time-varying values.

    Uses direct Bokeh ``ColumnDataSource`` patching for fast animation:
    geometry (xs/ys) is pre-extracted at init and **never re-serialized**.
    Only the scalar ``_value`` column is patched per frame, so the WebSocket
    payload is proportional to the number of features, not to the vertex count.

    Parameters
    ----------
    reader : SlicingReader
        Data source.  Must have a regular ``time_index``.
    geodataframe : geopandas.GeoDataFrame
        Shapes to colour.  Each row corresponds to one geo feature.
    geo_id_column : str, optional
        Column in *geodataframe* holding the integer feature id used to
        align values returned by ``reader.get_slice_nearest()``.
        Default ``"geo_id"``.
    title : str, optional
        Map title prefix.  The current timestamp is appended automatically.
    vmin, vmax : float or None, optional
        Colour-scale bounds.  ``None`` falls back to ``reader.vmin/vmax``.
    colormap : str, optional
        Initial colormap.  Must be one of :data:`CURATED_COLORMAPS`.
    size : float, optional
        Point radius (points) or line width in pixels (lines).
        Not used for polygons.  Default ``8``.
    map_height : int, optional
        Minimum map height in pixels.  Default ``500``.
    """

    vmin: Optional[float] = param.Number(
        default=None, allow_None=True,
        doc="Lower colour-scale bound. None → reader.vmin.",
    )
    vmax: Optional[float] = param.Number(
        default=None, allow_None=True,
        doc="Upper colour-scale bound. None → reader.vmax.",
    )
    colormap: str = param.Selector(
        default="viridis", objects=CURATED_COLORMAPS,
        doc="Colormap name for the value dimension.",
    )
    size: float = param.Number(
        default=6.0, bounds=(1.0, 50.0),
        doc="Point radius (points) or line width (px). Not used for polygons.",
    )
    show_contours: bool = param.Boolean(
        default=False, doc="Overlay contour lines on the map."
    )
    n_contours: int = param.Integer(
        default=8, bounds=(2, 30), doc="Number of contour levels.",
    )
    contour_smooth: float = param.Number(
        default=3.0, bounds=(0.0, 20.0),
        doc="Gaussian smoothing sigma applied to the raster before contouring "
            "(grid cells). 0 = no smoothing.",
    )
    contour_levels: str = param.Selector(
        default="nice",
        objects=["linear", "nice", "eq_hist"],
        doc="How contour levels are placed.\n"
            "linear  — equally spaced between vmin and vmax.\n"
            "nice    — rounded tick-like values (matplotlib MaxNLocator).\n"
            "eq_hist — quantile-spaced so each band covers equal data density.",
    )
    contour_custom_levels: str = param.String(
        default="",
        doc="Comma-separated explicit contour levels (e.g. '500, 1000, 2000').  "
            "When non-empty this overrides the level count and mode selectors.",
    )
    current_dt: Optional[datetime.datetime] = param.Parameter(
        default=None, doc="Current animation datetime.",
    )

    def __init__(
        self,
        reader: SlicingReader,
        geodataframe: gpd.GeoDataFrame,
        geo_id_column: str = "geo_id",
        title: str = "",
        vmin: Optional[float] = None,
        vmax: Optional[float] = None,
        colormap: str = "viridis",
        size: float = 6.0,
        map_width: int = 750,
        map_height: int = 500,
        x2_callback: Optional[object] = None,
        transform_options: Optional[dict] = None,
        initial_transform: str = "none",
        buffer_chunk_size: int = 200,
        **params,
    ) -> None:
        # ----------------------------------------------------------------
        # 1. Config
        # ----------------------------------------------------------------
        self._base_reader = reader          # raw reader, never transformed
        self._transform_options = transform_options or {}
        self._buffer_chunk_size = buffer_chunk_size
        self._reader = self._setup_reader(initial_transform)
        self._geo_id_column = geo_id_column
        self._title = title
        self._map_height = map_height
        self._x2_callback = x2_callback
        self._extra_frame_callbacks: list = []
        self._transform_callbacks: list = []
        self._extra_save_callbacks: list = []
        self._frame_seq = 0

        # ----------------------------------------------------------------
        # 2. Project GDF to EPSG:3857 once.
        # ----------------------------------------------------------------
        if geodataframe.crs is None:
            raise ValueError("geodataframe must have a CRS set (e.g. EPSG:4326).")
        self._gdf_proj = geodataframe.to_crs("EPSG:3857").copy()

        # ----------------------------------------------------------------
        # 3. Detect geometry type and pre-extract Bokeh-compatible xs/ys.
        #    Geometry is serialised ONCE here and never again — only the
        #    scalar _value array changes per frame.
        # ----------------------------------------------------------------
        self._geom_type = _detect_geom_type(self._gdf_proj)
        self._geo_ids: list[int] = [
            int(v) for v in self._gdf_proj[geo_id_column].values
        ]

        if self._geom_type == "point":
            bk_xs: list = self._gdf_proj.geometry.x.values.tolist()
            bk_ys: list = self._gdf_proj.geometry.y.values.tolist()
        else:
            bk_xs, bk_ys = [], []
            for geom in self._gdf_proj.geometry:
                if self._geom_type == "polygon":
                    coords = np.array(geom.exterior.coords)
                else:
                    coords = np.array(geom.coords)
                bk_xs.append(coords[:, 0].tolist())
                bk_ys.append(coords[:, 1].tolist())

        # Contour grid (centroids, regular raster, clip zone) — built once.
        (
            self._centroids_x, self._centroids_y,
            self._grid_x, self._grid_y,
            self._contour_clip_zone,
            _cell_size,
        ) = _make_contour_grid(self._gdf_proj, self._geom_type, buffer_m=4000.0)
        _default_clip_km = 4.0

        # ----------------------------------------------------------------
        # 4. Effective vmin/vmax and initial frame values.
        # ----------------------------------------------------------------
        init_vmin = float(vmin if vmin is not None else reader.vmin)
        init_vmax = float(vmax if vmax is not None else reader.vmax)
        if init_vmin == init_vmax:
            init_vmax = init_vmin + 1.0

        init_series = self._reader.get_slice(self._reader.time_index[0])
        _init_step = 0
        # Filter-based transforms (e.g. Godin) produce NaN for the first
        # ~33.5 h of output because the file has no before-data for warmup.
        # Skip forward to the first frame with at least one finite value so
        # the map initialises with meaningful colours rather than all-grey.
        if len(init_series) > 0 and init_series.isna().all():
            _ti0 = self._reader.time_index
            for _s in range(1, min(len(_ti0), 50)):
                _probe = self._reader.get_slice(_ti0[_s])
                if _probe.notna().any():
                    init_series = _probe
                    _init_step = _s
                    break
        init_values = [
            float(init_series.get(gid, np.nan)) for gid in self._geo_ids
        ]

        # ----------------------------------------------------------------
        # 5. Bokeh ColumnDataSource — geometry set once; _value patched.
        # ----------------------------------------------------------------
        self._bk_source = ColumnDataSource({
            "xs": bk_xs, "ys": bk_ys,
            "_value": init_values, "geo_id": self._geo_ids,
        })
        # Contour source — empty until show_contours is enabled.
        # Carries xs, ys, level (isovalue), and color (hex from colormap).
        self._contour_source = ColumnDataSource(
            {"xs": [], "ys": [], "level": [], "color": []}
        )

        # ----------------------------------------------------------------
        # 6. LinearColorMapper — updated in-place on style changes.
        # ----------------------------------------------------------------
        self._bk_mapper = LinearColorMapper(
            palette=_cmap_to_palette(colormap),
            low=init_vmin,
            high=init_vmax,
            nan_color="lightgrey",
        )

        # ----------------------------------------------------------------
        # 7. Bokeh figure with tile background.
        #
        # Key decisions:
        # - Do NOT use x_axis_type="mercator" / y_axis_type="mercator".
        #   Those add a Bokeh-side Mercator transformation on top of the
        #   already-projected EPSG:3857 data.  When any Bokeh model
        #   property changes (palette, low, high, glyph size), Bokeh
        #   re-validates the document and that transformation state can
        #   be reset, causing the viewport to jump.  Axes are hidden
        #   anyway so the tick label format doesn't matter.
        # - Use explicit Range1d (not DataRange1d) so the viewport is
        #   never auto-fitted on data patch or model update.
        # - match_aspect=True locks the geographic aspect ratio so
        #   WMTS tiles always render undistorted regardless of figure
        #   dimensions.
        # ----------------------------------------------------------------
        bounds = self._gdf_proj.total_bounds   # [xmin, ymin, xmax, ymax] in EPSG:3857
        pad_x = max((bounds[2] - bounds[0]) * 0.05, 1000.0)
        pad_y = max((bounds[3] - bounds[1]) * 0.05, 1000.0)
        x_range = Range1d(bounds[0] - pad_x, bounds[2] + pad_x, bounds=None)
        y_range = Range1d(bounds[1] - pad_y, bounds[3] + pad_y, bounds=None)

        p = bk_figure(
            x_range=x_range,
            y_range=y_range,
            x_axis_type="mercator",
            y_axis_type="mercator",
            match_aspect=True,
            sizing_mode="stretch_both",
            min_height=map_height,
            title=f"{title + ' \u2014 ' if title else ''}"
                  f"{self._reader.time_index[_init_step].strftime('%Y-%m-%d %H:%M')}",
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
        )
        p.axis.visible = False
        # Tiles must be added as the first renderer so they sit behind data.
        # WMTSTileSource is instantiated here (inside __init__) so it is
        # always created in the context of an active Bokeh document.
        tile_source = WMTSTileSource(
            url=_CARTO_LIGHT_URL,
            attribution=_CARTO_LIGHT_ATTR,
        )
        self._tile_renderer = p.add_tile(tile_source)

        # Two separate HoverTools so each renderer gets the right tooltip:
        # - data_hover: restricted to the data (channel) renderer
        # - contour_hover: restricted to the contour renderer (shows level)
        color_field = {"field": "_value", "transform": self._bk_mapper}
        if self._geom_type == "point":
            self._data_renderer = p.scatter(
                x="xs", y="ys", source=self._bk_source,
                color=color_field, size=size, line_color=None,
            )
        elif self._geom_type == "line":
            self._data_renderer = p.multi_line(
                xs="xs", ys="ys", source=self._bk_source,
                line_color=color_field, line_width=size,
            )
        else:  # polygon
            self._data_renderer = p.patches(
                xs="xs", ys="ys", source=self._bk_source,
                fill_color=color_field,
                line_color="white", line_width=0.5, line_alpha=0.2,
            )

        # Restrict to the channel data renderer only — prevents the tooltip
        # firing with "???" on overlay renderers (e.g. flow-arrow patches)
        # that don't have @geo_id / @_value columns.
        data_hover = HoverTool(
            renderers=[self._data_renderer],
            tooltips=[
                ("Channel", "@geo_id"),
                ("Value",   "@_value{0.3f}"),
            ],
            point_policy="follow_mouse",
        )
        p.add_tools(data_hover)

        colorbar = ColorBar(
            color_mapper=self._bk_mapper,
            ticker=BasicTicker(desired_num_ticks=6),
            label_standoff=8,
            border_line_color=None,
            location=(0, 0),
        )
        p.add_layout(colorbar, "right")

        # Contour renderer — coloured by isovalue via the same colormap,
        # fixed width=3, sits on top, initially invisible.
        self._contour_renderer = p.multi_line(
            xs="xs", ys="ys", source=self._contour_source,
            line_color="color", line_width=3.0, line_alpha=0.9,
            visible=False,
        )
        # Dedicated hover for contour renderer showing the isovalue level.
        contour_hover = HoverTool(
            renderers=[self._contour_renderer],
            tooltips=[("Level", "@level{0.3f}")],
            point_policy="follow_mouse",
        )
        p.add_tools(contour_hover)

        # Contour label renderer — one text label per level (longest path).
        self._contour_label_source = ColumnDataSource(
            {"x": [], "y": [], "text": []}
        )
        self._contour_label_renderer = p.text(
            x="x", y="y", text="text",
            source=self._contour_label_source,
            text_font_size="13px",
            text_color="black",
            text_align="center",
            text_baseline="middle",
            background_fill_color="white",
            background_fill_alpha=0.6,
            visible=False,
        )

        # X2 isohaline renderer — a single bold line, initially invisible.
        self._x2_source = ColumnDataSource({"xs": [], "ys": []})
        self._x2_renderer = p.multi_line(
            xs="xs", ys="ys", source=self._x2_source,
            line_color="black", line_width=3.0, line_alpha=0.9,
            line_dash="solid", visible=False,
        )

        self._bk_figure = p
        self._chart_pane = pn.pane.Bokeh(p, sizing_mode="stretch_both", min_height=map_height)

        # ----------------------------------------------------------------
        # 8. Control widgets.
        #
        # Use self._reader.time_index (the potentially-transformed reader)
        # rather than the raw 'reader' parameter so the slider options and
        # DatetimePicker range reflect the ACTUAL output time resolution.
        # When initial_transform is "none" these are identical; when it is
        # e.g. "Rolling 14 D → Daily mean" self._reader has a daily
        # time_index while 'reader' has the raw hourly one.  Using the
        # wrong one here is the root cause of the "slider index N out of
        # range [0, M)" warnings when loading from a saved config.
        # ----------------------------------------------------------------
        ti = self._reader.time_index
        # DiscretePlayer with integer indices 0..N-1 — compact options list,
        # no per-step string serialisation overhead.  Built-in play/pause/
        # loop controls drive the animation; the Bokeh Div shows the resolved
        # timestamp so the user sees readable dates throughout.
        self._time_div = Div(
            text=f"<b>{ti[_init_step].strftime('%Y-%m-%d %H:%M')}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(self._time_div, sizing_mode="stretch_width")
        self._time_slider = pn.widgets.DiscretePlayer(
            name="",
            options=list(range(len(ti))),
            value=_init_step,
            interval=500,            # ms between steps when playing
            loop_policy="once",
            show_value=False,        # we show timestamp in the Div above
            sizing_mode="stretch_width",
        )
        # DatetimePicker — lets the user jump directly to any date/time.
        # Synced bidirectionally with the DiscretePlayer (snaps to nearest step).
        self._datetime_picker = pn.widgets.DatetimePicker(
            name="Go to date/time",
            value=ti[_init_step].to_pydatetime(),
            start=ti[0].to_pydatetime(),
            end=ti[-1].to_pydatetime(),
            sizing_mode="stretch_width",
        )
        self._clim_input = pn.widgets.TextInput(
            name="Color range  (min, max)",
            value=f"{init_vmin:.4g}, {init_vmax:.4g}",
            sizing_mode="stretch_width",
        )
        self._colormap_select = pn.widgets.Select(
            name="Colormap", options=CURATED_COLORMAPS_WITH_SEP, value=colormap,
            sizing_mode="stretch_width",
        )
        self._size_slider = pn.widgets.FloatSlider(
            name="Size" if self._geom_type == "point" else "Line width",
            start=1.0, end=50.0, step=0.5, value=size,
            sizing_mode="stretch_width",
        )
        self._contours_check = pn.widgets.Checkbox(
            name="Show contours", value=False, sizing_mode="stretch_width",
        )
        self._contour_color_check = pn.widgets.Checkbox(
            name="Color contours (colormap)", value=True,
            sizing_mode="stretch_width", visible=False,
        )
        self._n_contours_slider = pn.widgets.IntSlider(
            name="Contour levels", start=2, end=30, step=1, value=8,
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_smooth_slider = pn.widgets.FloatSlider(
            name="Contour smoothing", start=0.0, end=20.0, step=0.5, value=3.0,
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_levels_select = pn.widgets.Select(
            name="Contour level mode",
            options=["linear", "nice", "eq_hist"],
            value="nice",
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_custom_input = pn.widgets.TextInput(
            name="Custom levels (comma-separated)",
            placeholder="e.g. 500, 1000, 2000",
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_labels_check = pn.widgets.Checkbox(
            name="Label contours", value=False,
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_label_spacing_slider = pn.widgets.IntSlider(
            name="Label spacing", start=5, end=200, value=150, step=5,
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_clip_slider = pn.widgets.FloatSlider(
            name="Contour clip radius (km)", start=0.5, end=15.0,
            value=_default_clip_km, step=0.5,
            sizing_mode="stretch_width", visible=False,
        )
        # Channel and basemap opacity sliders (0 = invisible, 100 = fully opaque)
        self._channels_alpha_slider = pn.widgets.IntSlider(
            name="Channel lines opacity", start=0, end=100, value=100, step=5,
            sizing_mode="stretch_width",
        )
        self._basemap_alpha_slider = pn.widgets.IntSlider(
            name="Background map opacity", start=0, end=100, value=100, step=5,
            sizing_mode="stretch_width",
        )
        # Transform selector — only shown when transform_options is provided
        _transform_names = ["none"] + list(self._transform_options.keys())
        self._transform_select = pn.widgets.Select(
            name="Transform",
            options=_transform_names,
            value=initial_transform if initial_transform in _transform_names else "none",
            sizing_mode="stretch_width",
            visible=bool(self._transform_options),
        )
        # X2 controls — only shown when an x2_callback is provided.
        _has_x2 = x2_callback is not None
        self._x2_check = pn.widgets.Checkbox(
            name="Show X2 line", value=False,
            sizing_mode="stretch_width", visible=_has_x2,
        )
        self._x2_threshold_input = pn.widgets.FloatInput(
            name="X2 threshold", value=2700.0,
            sizing_mode="stretch_width", visible=False,
        )

        # ----------------------------------------------------------------
        # Build accordion sections.  Time is always visible above the
        # accordion so it is never collapsed away.
        # ----------------------------------------------------------------
        _size_widgets: list = (
            [] if self._geom_type == "polygon" else [self._size_slider]
        )
        _appearance_card = pn.Card(
            self._clim_input,
            self._colormap_select,
            *_size_widgets,
            self._channels_alpha_slider,
            self._basemap_alpha_slider,
            title="Appearance", collapsed=False,
            sizing_mode="stretch_width",
        )
        _contour_card = pn.Card(
            self._contours_check,
            self._n_contours_slider,
            self._contour_smooth_slider,
            self._contour_levels_select,
            self._contour_custom_input,
            self._contour_color_check,
            self._contour_labels_check,
            self._contour_label_spacing_slider,
            self._contour_clip_slider,
            title="Contours", collapsed=True,
            sizing_mode="stretch_width",
        )
        self._contour_card = _contour_card
        _optional_cards: list = []
        if self._transform_options:
            _optional_cards.append(pn.Card(
                self._transform_select,
                title="Transform", collapsed=True,
                sizing_mode="stretch_width",
            ))
        if _has_x2:
            _optional_cards.append(pn.Card(
                self._x2_check,
                self._x2_threshold_input,
                title="X2 isohaline", collapsed=True,
                sizing_mode="stretch_width",
            ))

        # Save config card — at the bottom; filled in by dsm2ui after construction.
        self._animate_meta: dict = {}
        self._config_path_input = pn.widgets.TextInput(
            name="Save path (.yml)",
            placeholder="/path/to/config.yml",
            sizing_mode="stretch_width",
        )
        self._save_config_btn = pn.widgets.Button(
            name="Save config to YAML",
            button_type="primary",
            sizing_mode="stretch_width",
        )
        self._save_config_status = pn.pane.Markdown("", sizing_mode="stretch_width")
        _save_card = pn.Card(
            self._config_path_input,
            self._save_config_btn,
            self._save_config_status,
            title="Save config", collapsed=True,
            sizing_mode="stretch_width",
        )

        self._sidebar_toggle = pn.widgets.Toggle(
            name="\u25c4", value=True, button_type="light",
            width=32, height=32, margin=(4, 0, 4, 4),
        )
        self._controls = pn.Column(
            pn.pane.Markdown("### Controls", margin=(4, 0, 2, 0)),
            self._time_label_pane,
            self._time_slider,
            self._datetime_picker,
            pn.layout.Divider(margin=(4, 0, 4, 0)),
            _appearance_card,
            _contour_card,
            *_optional_cards,
            _save_card,
            sizing_mode="stretch_width",
            max_width=280,
            margin=(4, 8, 4, 0),
        )

        # ----------------------------------------------------------------
        # 9. super().__init__ with initial param values.
        # ----------------------------------------------------------------
        super().__init__(vmin=vmin, vmax=vmax, colormap=colormap, size=size, **params)

        # ----------------------------------------------------------------
        # 10. Wire watchers (after super so param system is initialised).
        # ----------------------------------------------------------------
        self._time_slider.param.watch(self._on_slider_change, "value")
        self._datetime_picker.param.watch(self._on_datetime_picker_change, "value")
        self._syncing = False  # guard against slider ↔ picker feedback loops
        self.param.watch(self._on_style_change, ["vmin", "vmax", "colormap", "size"])
        self._clim_input.param.watch(self._on_clim_text_change, "value")
        self._colormap_select.param.watch(self._on_colormap_widget_change, "value")
        if self._geom_type != "polygon":
            self._size_slider.param.watch(self._on_size_widget_change, "value")
        self._contours_check.param.watch(self._on_contours_toggle, "value")
        self._contour_color_check.param.watch(self._on_contour_color_toggle, "value")
        self._n_contours_slider.param.watch(self._on_n_contours_change, "value")
        self._contour_smooth_slider.param.watch(self._on_contour_smooth_change, "value")
        self._contour_levels_select.param.watch(self._on_contour_levels_change, "value")
        self._contour_custom_input.param.watch(self._on_contour_custom_levels_change, "value")
        self._contour_labels_check.param.watch(self._on_contour_labels_toggle, "value")
        self._contour_label_spacing_slider.param.watch(self._on_label_spacing_change, "value")
        self._contour_clip_slider.param.watch(self._on_contour_clip_change, "value")
        self._channels_alpha_slider.param.watch(self._on_channels_alpha_change, "value")
        self._basemap_alpha_slider.param.watch(self._on_basemap_alpha_change, "value")
        if self._transform_options:
            self._transform_select.param.watch(self._on_transform_change, "value")
        if x2_callback is not None:
            self._x2_check.param.watch(self._on_x2_toggle, "value")
            self._x2_threshold_input.param.watch(self._on_x2_threshold_change, "value")
        self._save_config_btn.on_click(self._on_save_config)
        self._sidebar_toggle.param.watch(self._on_sidebar_toggle, "value")

        self.current_dt = ti[0].to_pydatetime()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _on_sidebar_toggle(self, event: param.parameterized.Event) -> None:
        """Collapse or expand the controls sidebar, letting the map fill freed space."""
        self._controls.visible = bool(event.new)
        self._sidebar_toggle.name = "\u25c4" if event.new else "\u25ba"

    def _current_clim(self) -> tuple[float, float]:
        vmin = self.vmin if self.vmin is not None else self._reader.vmin
        vmax = self.vmax if self.vmax is not None else self._reader.vmax
        if vmin == vmax:
            vmax = vmin + 1.0
        return float(vmin), float(vmax)

    def _compute_label_positions(
        self,
        xs_list: list,
        ys_list: list,
        lvls: list,
    ) -> dict:
        """Pick one label position per unique level (midpoint of longest path).

        Returns a dict suitable for ``ColumnDataSource.data``:
        ``{"x": [...], "y": [...], "text": [...]}``.
        """
        # Group paths by level, keep the longest for labelling
        best: dict[float, tuple[float, float, int]] = {}  # level -> (mx, my, length)
        for xs, ys, lvl in zip(xs_list, ys_list, lvls):
            n = len(xs)
            if n < 2:
                continue
            if lvl not in best or n > best[lvl][2]:
                mid = n // 2
                best[lvl] = (xs[mid], ys[mid], n)

        lx, ly, lt = [], [], []
        all_levels = sorted(best.keys())
        # Determine decimal places needed to distinguish adjacent levels.
        # If levels are all whole numbers, format as integers.  Otherwise
        # use enough decimal places so no two labels are identical.
        n_dec = _nice_decimal_places(all_levels)
        for lvl, (mx, my, _) in sorted(best.items()):
            lx.append(mx)
            ly.append(my)
            lt.append(_format_level(lvl, n_dec))
        return {"x": lx, "y": ly, "text": lt}

    def _update_contour_labels(
        self, xs_list: list, ys_list: list, lvls: list
    ) -> None:
        """Update the label source if the label renderer is visible.

        Must be called while holding the Bokeh document lock (i.e. inside
        ``doc.add_next_tick_callback`` or during normal render/update).
        """
        if self._contour_label_renderer.visible:
            self._contour_label_source.data = (
                self._compute_label_positions(xs_list, ys_list, lvls)
            )

    def _rebuild_contours(self) -> None:
        """Recompute contour polygons for the current frame and apply them.

        Safe to call from any Panel/param watcher callback.  All Bokeh
        ``ColumnDataSource.data`` mutations are routed through
        ``doc.add_next_tick_callback`` when a live Bokeh document is
        present, avoiding the ``RuntimeError: _pending_writes should be
        non-None`` that occurs when Bokeh sources are mutated from inside
        Panel's async event-processing coroutine (outside the doc lock).
        """
        if not self._contour_renderer.visible:
            return
        current_values = list(self._bk_source.data["_value"])
        xs, ys, lvls = self._compute_contours(current_values)
        colors = self._contour_colors(lvls)
        new_data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}

        def _apply():
            self._contour_source.data = new_data
            self._update_contour_labels(xs, ys, lvls)

        doc = self._contour_source.document
        if doc is not None:
            doc.add_next_tick_callback(_apply)
        else:
            _apply()

    def _compute_levels(
        self, finite_vals: np.ndarray, vmin: float, vmax: float
    ) -> np.ndarray:
        """Delegate to module-level ``_compute_contour_levels``."""
        return _compute_contour_levels(
            finite_vals, vmin, vmax,
            self.n_contours, self.contour_levels, self.contour_custom_levels,
        )

    def _compute_contours(self, values: list[float]) -> tuple[list, list, list]:
        """Interpolate values to a regular grid, optionally smooth, then contour.

        Gaussian smoothing (``contour_smooth`` sigma) is applied to the
        Voronoi-nearest raster before contouring.  This rounds the blocky
        edges produced by nearest-neighbour interpolation into smooth curves
        without introducing artefacts between disconnected channel branches.

        Returns
        -------
        (xs, ys, levels) : each a flat list — one entry per contour path.
            ``levels[i]`` is the isovalue of path ``i``.
        """
        eff_vmin, eff_vmax = self._current_clim()
        vals = np.asarray(values, dtype=float)
        mask = np.isfinite(vals)
        if mask.sum() < 4:
            return [], [], []
        levels = _compute_contour_levels(
            vals[mask], eff_vmin, eff_vmax,
            self.n_contours, self.contour_levels, self.contour_custom_levels,
        )
        return _run_contour_computation(
            vals,
            self._centroids_x, self._centroids_y,
            self._grid_x, self._grid_y,
            float(self.contour_smooth), levels, self._contour_clip_zone,
        )

        return xs_out, ys_out, lvl_out

    def _clip_and_append(
        self,
        seg: np.ndarray,
        lvl: float,
        xs_out: list,
        ys_out: list,
        lvl_out: list,
    ) -> None:
        """Delegate to module-level ``_clip_contour_segment``."""
        _clip_contour_segment(seg, lvl, self._contour_clip_zone, xs_out, ys_out, lvl_out)

    def _load_frame(self, step_idx: int) -> None:
        """Synchronous fetch + render — must run under document lock."""
        ts = self._reader.time_index[step_idx]
        series = self._reader.get_slice_nearest(ts)
        new_values = series.reindex(self._geo_ids).fillna(np.nan).tolist()
        self._render_frame_data(step_idx, ts, ts.strftime("%Y-%m-%d %H:%M"), new_values)

    def _render_frame_data(
        self, step_idx: int, ts: pd.Timestamp, ts_str: str, new_values: list
    ) -> None:
        """Apply pre-fetched frame data to Bokeh models — must run under document lock."""
        self._time_div.text = f"<b>{ts_str}</b>"
        self._bk_source.patch({"_value": [(slice(None), new_values)]})

        if self._contour_renderer.visible:
            xs, ys, lvls = self._compute_contours(new_values)
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

        if self._x2_renderer.visible and self._x2_callback is not None:
            threshold = float(self._x2_threshold_input.value)
            xs, ys = self._x2_callback(step_idx, threshold)
            self._x2_source.data = {"xs": xs, "ys": ys}

        for _cb in self._extra_frame_callbacks:
            _cb(ts)

        self._bk_figure.title.text = (
            f"{self._title + ' \u2014 ' if self._title else ''}{ts_str}"
        )

    def add_save_callback(self, fn) -> None:
        """Register a callback invoked after each successful config save.

        The callback receives the saved file path as its sole argument.
        Multiple callbacks are supported and are called in registration order.

        Parameters
        ----------
        fn : callable(path: str) -> None
        """
        self._extra_save_callbacks.append(fn)

    def add_frame_callback(self, fn) -> None:
        """Register a callback invoked on every animation frame.

        The callback receives a single :class:`pandas.Timestamp` argument:
        the current animation time resolved from the active reader's time
        index.  It is called inside the Bokeh document lock, so Bokeh model
        mutations are safe.  Using a timestamp (rather than a step index)
        means the callback is robust to time-index changes caused by
        transform switches.

        Parameters
        ----------
        fn : callable(ts: pd.Timestamp) -> None
            Called after the main ``_bk_source`` patch and any X2 update.
        """
        self._extra_frame_callbacks.append(fn)

    def add_transform_callback(self, fn) -> None:
        """Register a callback invoked when the animation transform changes.

        Called from the background thread inside :meth:`_on_transform_change`
        (before the Bokeh document lock is re-entered), so the callback may
        perform slow I/O (e.g. rebuilding a filtered reader) without blocking
        the browser.

        Parameters
        ----------
        fn : callable(spec_or_none) -> None
            Receives the new :class:`~dvue.animator.TransformSpec` (or
            ``None`` when the transform is set to ``"none"``).  The
            ``TransformSpec`` is the same object stored in
            ``_transform_options[name]``.
        """
        self._transform_callbacks.append(fn)

    # ------------------------------------------------------------------
    # Widget callbacks
    # ------------------------------------------------------------------

    def _on_slider_change(self, event: param.parameterized.Event) -> None:
        if self._syncing:
            return
        idx = int(event.new)
        ti = self._reader.time_index
        if len(ti) == 0:
            return
        if not (0 <= idx < len(ti)):
            import logging as _log
            _log.getLogger(__name__).warning(
                "GeoAnimatorManager: slider index %d is out of range "
                "[0, %d) — stale browser session value; resetting to 0. "
                "This typically happens when a browser tab reconnects to a "
                "new server session that has a shorter dataset.",
                idx, len(ti),
            )
            # Snap to the beginning and push that value back to the browser
            # so the slider thumb and the displayed timestamp are in sync.
            idx = 0
            self._syncing = True
            try:
                self._time_slider.value = idx
            finally:
                self._syncing = False
        ts = ti[idx]
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        # Sync DatetimePicker without causing a feedback loop.
        self._syncing = True
        try:
            self._datetime_picker.value = ts.to_pydatetime()
        finally:
            self._syncing = False

        doc = self._bk_figure.document
        self._frame_seq += 1
        seq = self._frame_seq

        if doc is not None:
            import threading
            # Snapshot reader/geo_ids so the background thread uses the state
            # that was current when the slider event fired.
            _reader_snap = self._reader
            _geo_ids = self._geo_ids

            def _bg() -> None:
                """Fetch frame data off the IOLoop; apply under document lock."""
                try:
                    series = _reader_snap.get_slice_nearest(ts)
                    new_values = series.reindex(_geo_ids).fillna(np.nan).tolist()
                except Exception:
                    return

                def _ui() -> None:
                    if self._frame_seq != seq:
                        return  # a newer slider event arrived; drop stale result
                    self._render_frame_data(idx, ts, ts_str, new_values)

                doc.add_next_tick_callback(_ui)

            threading.Thread(target=_bg, daemon=True).start()
        else:
            # No document (tests / Jupyter) — run synchronously.
            self._apply_frame(idx, ts_str)

    def _on_datetime_picker_change(self, event: param.parameterized.Event) -> None:
        """Jump the animation to the nearest time step for the picked datetime."""
        if self._syncing or event.new is None:
            return
        ts = pd.Timestamp(event.new)
        idx = int(self._reader.time_index.get_indexer([ts], method="nearest")[0])
        self._syncing = True
        try:
            self._time_slider.value = idx
        finally:
            self._syncing = False
        actual_ts = self._reader.time_index[idx]
        ts_str = actual_ts.strftime("%Y-%m-%d %H:%M")

        doc = self._bk_figure.document
        self._frame_seq += 1
        seq = self._frame_seq

        if doc is not None:
            import threading
            _reader_snap = self._reader
            _geo_ids = self._geo_ids

            def _bg() -> None:
                try:
                    series = _reader_snap.get_slice_nearest(actual_ts)
                    new_values = series.reindex(_geo_ids).fillna(np.nan).tolist()
                except Exception:
                    return

                def _ui() -> None:
                    if self._frame_seq != seq:
                        return
                    self._render_frame_data(idx, actual_ts, ts_str, new_values)

                doc.add_next_tick_callback(_ui)

            threading.Thread(target=_bg, daemon=True).start()
        else:
            self._apply_frame(idx, ts_str)

    def _apply_frame(self, idx: int, ts_str: str) -> None:
        """Run all Bokeh mutations for a single frame step under document lock."""
        self._load_frame(idx)

    def _on_style_change(self, *events) -> None:
        """Update LinearColorMapper in-place — no frame rebuild needed.

        Bokeh model property mutations (mapper.low, glyph.size, etc.) require
        the Bokeh server session's document lock.  When this method is called
        from inside param's synchronous watcher chain (e.g. TextInput →
        _on_clim_text_change → self.vmin = ... → _on_style_change), the
        document lock is NOT held, which triggers Bokeh's
        ``_pending_writes should be non-None`` error.

        Fix: defer all direct Bokeh property mutations to the next IOLoop tick
        via ``add_next_tick_callback`` when a document is attached (Panel serve
        context).  In tests / notebooks there is no document, so run directly.
        """
        doc = self._bk_figure.document
        if doc is not None:
            doc.add_next_tick_callback(self._apply_bokeh_style)
        else:
            self._apply_bokeh_style()

    def _apply_bokeh_style(self) -> None:
        """Apply direct Bokeh property mutations — must run under document lock."""
        eff_vmin, eff_vmax = self._current_clim()
        self._bk_mapper.palette = _cmap_to_palette(self.colormap)
        self._bk_mapper.low = eff_vmin
        self._bk_mapper.high = eff_vmax
        new_size = float(self.size)
        # Only update size on the data renderer — contour and X2 renderers
        # have their own fixed line widths and must not be resized.
        _skip = {id(self._contour_renderer), id(self._x2_renderer)}
        for r in self._bk_figure.renderers:
            if id(r) in _skip or not hasattr(r, "glyph"):
                continue
            g = r.glyph
            if hasattr(g, "size"):
                g.size = new_size
            elif hasattr(g, "line_width") and not hasattr(g, "fill_color"):
                g.line_width = new_size
        # Recompute contour levels when clim changes
        if self._contour_renderer.visible:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

    def _on_colormap_widget_change(self, event: param.parameterized.Event) -> None:
        if event.new in CURATED_COLORMAPS:   # ignore separator clicks
            self.colormap = event.new

    def _on_size_widget_change(self, event: param.parameterized.Event) -> None:
        self.size = float(event.new)

    def _contour_colors(self, lvls: list) -> list:
        """Return per-path hex colors — colormap or black depending on toggle."""
        if self._contour_color_check.value:
            eff_vmin, eff_vmax = self._current_clim()
            return _level_colors(lvls, eff_vmin, eff_vmax, self.colormap)
        return ["black"] * len(lvls)

    def _setup_reader(self, transform_name: str) -> "SlicingReader":
        """Wrap ``_base_reader`` with an optional transform then buffer it.

        The transform option value must be a :class:`TransformSpec`.
        A :class:`RawSequentialBuffer` is inserted between the raw reader and
        the transform when the spec has a non-zero overlap (e.g. Godin,
        rolling average), so HDF5 I/O and transform computation overlap in
        time.
        """
        reader = self._base_reader
        if transform_name and transform_name != "none" and transform_name in self._transform_options:
            spec_or_fn = self._transform_options[transform_name]
            if not isinstance(spec_or_fn, TransformSpec):
                raise TypeError(
                    f"transform_options[{transform_name!r}] must be a TransformSpec, "
                    f"got {type(spec_or_fn).__name__}. "
                    "Use make_resample_transform(), make_moving_average_transform(), "
                    "or make_godin_transform() from dsm2ui.animate."
                )
            try:
                freq_nanos = int(
                    pd.tseries.frequencies.to_offset(
                        reader.time_index.freq
                    ).nanos
                )
                raw_overlap = spec_or_fn.get_overlap(freq_nanos)
            except (AttributeError, TypeError):
                raw_overlap = 0
            if raw_overlap > 0:
                reader = RawSequentialBuffer(reader)
            reader = StreamingTransformedSlicingReader(reader, spec_or_fn)
        return BufferedSlicingReader(
            reader, chunk_size=self._buffer_chunk_size, prefetch=True,
            adaptive=True, min_chunk_size=50, max_chunk_size=2000,
        )

    # ------------------------------------------------------------------
    # Config save / load state
    # ------------------------------------------------------------------

    def collect_state(self) -> dict:
        """Return a complete dict representing the current UI state + metadata.

        The returned dict can be serialised to YAML and later passed back to
        the CLI via ``dsm2ui animate hydro --config config.yml`` to recreate
        the session.  The ``_animate_meta`` attribute must have been set by the
        DSM2 layer (``dsm2ui.animate``) after construction; without it the data
        fields will be empty.
        """
        meta = self._animate_meta

        state: dict = {
            "version": 1,
            "mode": meta.get("mode", "single"),
            "files": meta.get("files", []),
            "file_type": meta.get("file_type", "hydro"),
            "variable": meta.get("variable", "flow"),
            "location": meta.get("location", "both"),
            "shapefile": meta.get("shapefile"),
            "channel_id_column": meta.get("channel_id_column"),
            "colormap": self.colormap,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "size": self.size,
            "show_channels": self._channels_alpha_slider.value,
            "show_basemap":  self._basemap_alpha_slider.value,
            "contours": {
                "enabled": self._contours_check.value,
                "n_levels": self._n_contours_slider.value,
                "smoothing": float(self._contour_smooth_slider.value),
                "level_mode": self._contour_levels_select.value,
                "custom_levels": self._contour_custom_input.value,
                "color": self._contour_color_check.value,
                "labels": self._contour_labels_check.value,
                "label_spacing": self._contour_label_spacing_slider.value,
                "clip_radius_km": self._contour_clip_slider.value,
            },
            "diff": {"show": False, "colormap": "coolwarm"},
            "sidebar_collapsed": not self._sidebar_toggle.value,
            "map_extents": {
                "x_start": float(self._bk_figure.x_range.start),
                "x_end":   float(self._bk_figure.x_range.end),
                "y_start": float(self._bk_figure.y_range.start),
                "y_end":   float(self._bk_figure.y_range.end),
            },
        }
        if self._x2_callback is not None:
            state["x2"] = {
                "enabled": self._x2_check.value,
                "threshold": float(self._x2_threshold_input.value),
            }
        else:
            state["x2"] = {"enabled": False, "threshold": 2700.0}
        return state

    def _on_save_config(self, event) -> None:
        """Write current state to a YAML file at the path in the text input."""
        path = self._config_path_input.value.strip()
        if not path:
            self._save_config_status.object = "\u26a0 Enter a file path first."
            return
        try:
            import yaml
            state = self.collect_state()
            from pathlib import Path as _Path
            _Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(state, f, default_flow_style=False,
                          sort_keys=False, allow_unicode=True)
            self._save_config_status.object = f"\u2713 Saved to `{path}`"
            for _cb in self._extra_save_callbacks:
                _cb(path)
        except Exception as exc:
            self._save_config_status.object = f"\u2717 {exc}"

    def _on_channels_alpha_change(self, event: param.parameterized.Event) -> None:
        """Apply channel line opacity (0–100) to the map renderer."""
        alpha = event.new / 100.0
        try:
            self._data_renderer.glyph.line_alpha = alpha
        except AttributeError:
            self._data_renderer.visible = (alpha > 0)

    def _on_basemap_alpha_change(self, event: param.parameterized.Event) -> None:
        """Apply background map opacity (0–100) to the tile renderer."""
        self._tile_renderer.alpha = event.new / 100.0

    def _on_transform_change(self, event: param.parameterized.Event) -> None:
        """Apply a new time-domain transform; preserve current playback position.

        The transform may require loading the full HDF5 dataset and running a
        tidal filter (several seconds).  The heavy work is done in a daemon
        thread so the IOLoop stays responsive; a loading spinner covers the map
        while the computation is in progress.
        """
        import threading
        import logging as _log

        new_name = event.new
        self._frame_seq += 1  # invalidate any in-flight slider fetch threads

        current_ts = pd.Timestamp(
            self._reader.time_index[self._time_slider.value]
        )

        # Show loading indicator immediately (Panel param — safe from watcher).
        self._chart_pane.loading = True
        self._transform_select.disabled = True
        # Pause playback while the new reader is being built so the slider
        # does not advance past frames that have not yet been loaded.
        _was_playing = self._time_slider.direction == 1
        self._time_slider.pause()
        self._time_slider.loading = True

        doc = self._bk_figure.document

        def _compute() -> None:
            """Run in a background thread: build reader, warm up buffer."""
            new_reader = self._setup_reader(new_name)
            ti = new_reader.time_index
            nearest_idx = max(0, min(
                int(ti.get_indexer([current_ts], method="nearest")[0]),
                len(ti) - 1,
            ))

            # Fire transform callbacks in the background thread so heavy
            # reader rebuilds (e.g. Godin) happen outside the document lock.
            _new_spec = (
                self._transform_options.get(new_name)
                if (new_name and new_name != "none")
                else None
            )
            for _tcb in self._transform_callbacks:
                _tcb(_new_spec)

            def _apply() -> None:
                """Update all Bokeh/Panel state under the document lock."""
                self._reader = new_reader
                self._syncing = True
                try:
                    self._time_slider.options = list(range(len(ti)))
                    self._time_slider.value = nearest_idx
                    self._datetime_picker.start = ti[0].to_pydatetime()
                    self._datetime_picker.end = ti[-1].to_pydatetime()
                    self._datetime_picker.value = ti[nearest_idx].to_pydatetime()
                finally:
                    self._syncing = False
                self._time_div.text = (
                    f"<b>{ti[nearest_idx].strftime('%Y-%m-%d %H:%M')}</b>"
                )
                self._load_frame(nearest_idx)
                # Clear loading indicator last, after the frame is rendered.
                self._chart_pane.loading = False
                self._time_slider.loading = False
                self._transform_select.disabled = False
                # Resume playback only if it was running before the transform change.
                if _was_playing:
                    self._time_slider.play()

            if doc is not None:
                doc.add_next_tick_callback(_apply)
            else:
                # No live document (Jupyter / tests) — run synchronously.
                _apply()

        threading.Thread(target=_compute, daemon=True).start()

    def _on_contour_color_toggle(self, event: param.parameterized.Event) -> None:
        """Switch contour line colour between colormap-derived and black."""
        lvls = self._contour_source.data.get("level", [])
        if lvls:
            colors = self._contour_colors(lvls)
            new_data = dict(self._contour_source.data, color=colors)
            doc = self._contour_source.document
            if doc is not None:
                doc.add_next_tick_callback(
                    lambda: setattr(self._contour_source, "data", new_data)
                )
            else:
                self._contour_source.data = new_data

    def _on_contours_toggle(self, event: param.parameterized.Event) -> None:
        """Show or hide contours; recompute for current frame when enabling."""
        on = bool(event.new)
        self._contour_card.collapsed = not on
        self._contour_renderer.visible = on
        self._n_contours_slider.visible = on
        self._contour_smooth_slider.visible = on
        self._contour_levels_select.visible = on
        self._contour_custom_input.visible = on
        self._contour_color_check.visible = on
        self._contour_labels_check.visible = on
        self._contour_label_spacing_slider.visible = on
        self._contour_clip_slider.visible = on
        if on:
            self._rebuild_contours()
        else:
            empty_contours = {"xs": [], "ys": [], "level": [], "color": []}
            empty_labels = {"x": [], "y": [], "text": []}
            doc = self._contour_source.document
            if doc is not None:
                doc.add_next_tick_callback(
                    lambda: (
                        setattr(self._contour_source, "data", empty_contours)
                        or setattr(self._contour_label_source, "data", empty_labels)
                    )
                )
            else:
                self._contour_source.data = empty_contours
                self._contour_label_source.data = empty_labels

    def _on_n_contours_change(self, event: param.parameterized.Event) -> None:
        self.n_contours = int(event.new)
        self._rebuild_contours()

    def _on_contour_smooth_change(self, event: param.parameterized.Event) -> None:
        self.contour_smooth = float(event.new)
        self._rebuild_contours()

    def _on_contour_levels_change(self, event: param.parameterized.Event) -> None:
        self.contour_levels = event.new
        self._rebuild_contours()

    def _on_contour_custom_levels_change(self, event: param.parameterized.Event) -> None:
        """Recompute contours when the user edits the custom levels text box."""
        self.contour_custom_levels = event.new
        # When the user fills in explicit levels, the count / mode selectors
        # become redundant; dim them visually to hint they are being overridden.
        auto_controls_active = not bool(event.new.strip())
        self._n_contours_slider.disabled = not auto_controls_active
        self._contour_levels_select.disabled = not auto_controls_active
        self._rebuild_contours()

    def _on_contour_labels_toggle(self, event: param.parameterized.Event) -> None:
        """Show or hide contour labels."""
        show = bool(event.new)
        self._contour_label_renderer.visible = show
        if show:
            xs = self._contour_source.data["xs"]
            ys = self._contour_source.data["ys"]
            lvls = self._contour_source.data["level"]
            if xs:
                new_label_data = self._compute_label_positions(xs, ys, lvls)
                doc = self._contour_label_source.document
                if doc is not None:
                    doc.add_next_tick_callback(
                        lambda: setattr(
                            self._contour_label_source, "data", new_label_data
                        )
                    )
                else:
                    self._contour_label_source.data = new_label_data
        else:
            empty = {"x": [], "y": [], "text": []}
            doc = self._contour_label_source.document
            if doc is not None:
                doc.add_next_tick_callback(
                    lambda: setattr(self._contour_label_source, "data", empty)
                )
            else:
                self._contour_label_source.data = empty

    def _on_label_spacing_change(self, event: param.parameterized.Event) -> None:
        """Re-place contour labels with the updated spacing."""
        if not self._contour_label_renderer.visible:
            return
        xs   = self._contour_source.data.get("xs", [])
        ys   = self._contour_source.data.get("ys", [])
        lvls = self._contour_source.data.get("level", [])
        if xs:
            new_label_data = self._compute_label_positions(xs, ys, lvls)
            doc = self._contour_label_source.document
            if doc is not None:
                doc.add_next_tick_callback(
                    lambda: setattr(
                        self._contour_label_source, "data", new_label_data
                    )
                )
            else:
                self._contour_label_source.data = new_label_data

    def _on_contour_clip_change(self, event: param.parameterized.Event) -> None:
        """Rebuild the contour clip zone with the new radius and recompute."""
        buffer_m = event.new * 1000.0
        try:
            from shapely.ops import unary_union
            self._contour_clip_zone = unary_union(
                self._gdf_proj.geometry
            ).buffer(buffer_m)
        except Exception:
            self._contour_clip_zone = None
        self._rebuild_contours()

    def _on_x2_toggle(self, event: param.parameterized.Event) -> None:
        """Show or hide the X2 isohaline line."""
        self._x2_renderer.visible = bool(event.new)
        self._x2_threshold_input.visible = bool(event.new)
        if event.new and self._x2_callback is not None:
            threshold = float(self._x2_threshold_input.value)
            xs, ys = self._x2_callback(self._time_slider.value, threshold)
            new_data = {"xs": xs, "ys": ys}
        else:
            new_data = {"xs": [], "ys": []}
        doc = self._x2_source.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda: setattr(self._x2_source, "data", new_data)
            )
        else:
            self._x2_source.data = new_data

    def _on_x2_threshold_change(self, event: param.parameterized.Event) -> None:
        """Recompute X2 line when the threshold value changes."""
        if self._x2_renderer.visible and self._x2_callback is not None:
            try:
                threshold = float(event.new)
            except (TypeError, ValueError):
                return
            xs, ys = self._x2_callback(self._time_slider.value, threshold)
            new_data = {"xs": xs, "ys": ys}
            doc = self._x2_source.document
            if doc is not None:
                doc.add_next_tick_callback(
                    lambda: setattr(self._x2_source, "data", new_data)
                )
            else:
                self._x2_source.data = new_data

    def _on_clim_text_change(self, event: param.parameterized.Event) -> None:
        try:
            parts = [p.strip() for p in event.new.split(",")]
            if len(parts) == 2:
                self.vmin, self.vmax = float(parts[0]), float(parts[1])
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # pn.viewable.Viewer protocol
    # ------------------------------------------------------------------

    def __panel__(self) -> pn.viewable.Viewable:
        # The outer Column with sizing_mode="stretch_both" anchors the height
        # to the browser viewport.  This means internal changes to the controls
        # column (label text, clim values, etc.) don't propagate a new height
        # to the chart pane, which would resize the Bokeh figure and reset the
        # viewport / aspect ratio.
        return pn.Column(
            pn.Row(self._sidebar_toggle, self._controls, self._chart_pane, sizing_mode="stretch_both"),
            sizing_mode="stretch_both",
            min_height=self._map_height,
        )

    def servable(self, title: Optional[str] = None, **kwargs) -> "GeoAnimatorManager":
        """Mark this component as the app entry point."""
        super().servable(title=title or self._title or "GeoAnimatorManager", **kwargs)
        return self

