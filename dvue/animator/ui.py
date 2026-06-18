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

from .reader import (SlicingReader, BufferedSlicingReader, TransformedSlicingReader,
                     StreamingTransformedSlicingReader, TransformSpec)

# ---------------------------------------------------------------------------
# Curated colormaps (subset that works well with numeric data on maps)
# ---------------------------------------------------------------------------

# Flat list used by param.Selector for validation.
CURATED_COLORMAPS: list[str] = [
    # Sequential — single-hue / perceptually uniform
    "viridis",
    "plasma",
    "inferno",
    "magma",
    "Blues",
    "YlOrRd",
    "turbo",
    "rainbow",
    # Diverging — two-hue, centred on a neutral midpoint
    "coolwarm",
    "RdBu_r",
    "RdYlBu_r",
    "PiYG",
    "bwr",
    "seismic",
]

# Grouped dict used by pn.widgets.Select for a labelled optgroup dropdown.
# The group labels appear as non-selectable section headers in the browser.
CURATED_COLORMAPS_GROUPS: dict[str, list[str]] = {
    "Sequential": ["viridis", "plasma", "inferno", "magma",
                   "Blues", "YlOrRd", "turbo", "rainbow"],
    "─── Diverging ───": ["coolwarm", "RdBu_r", "RdYlBu_r", "PiYG", "bwr", "seismic"],
}

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
) -> tuple:
    """Build the contour interpolation grid + clip zone for a projected GDF.

    Returns ``(centroids_x, centroids_y, grid_x, grid_y, clip_zone)``.
    All coordinates are in the GDF's CRS (expected EPSG:3857).
    """
    if geom_type == "point":
        centroids_x = gdf_proj.geometry.x.values.copy()
        centroids_y = gdf_proj.geometry.y.values.copy()
    else:
        cx_list, cy_list = [], []
        for geom in gdf_proj.geometry:
            c = geom.centroid
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
    _buf_radius = _cell_size * 10.0
    try:
        from shapely.ops import unary_union
        clip_zone = unary_union(gdf_proj.geometry).buffer(_buf_radius)
    except Exception:
        clip_zone = None
    return centroids_x, centroids_y, grid_x, grid_y, clip_zone


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
        ) = _make_contour_grid(self._gdf_proj, self._geom_type)

        # ----------------------------------------------------------------
        # 4. Effective vmin/vmax and initial frame values.
        # ----------------------------------------------------------------
        init_vmin = float(vmin if vmin is not None else reader.vmin)
        init_vmax = float(vmax if vmax is not None else reader.vmax)
        if init_vmin == init_vmax:
            init_vmax = init_vmin + 1.0

        init_series = reader.get_slice(reader.time_index[0])
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
                  f"{reader.time_index[0].strftime('%Y-%m-%d %H:%M')}",
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
        data_hover = HoverTool(
            tooltips=[
                ("Channel", "@geo_id"),
                ("Value",   "@_value{0.3f}"),
            ],
            point_policy="follow_mouse",
        )
        p.add_tools(data_hover)

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
        # Time slider: DiscreteSlider whose options are the actual datetime
        # strings.  The user sees readable timestamps instead of integers.
        # The slider value is the timestamp string; the index is derived
        # from the options list position inside _on_slider_change.
        # ----------------------------------------------------------------
        ti = reader.time_index
        # DiscretePlayer with integer indices 0..N-1 — compact options list,
        # no per-step string serialisation overhead.  Built-in play/pause/
        # loop controls drive the animation; the Bokeh Div shows the resolved
        # timestamp so the user sees readable dates throughout.
        self._time_div = Div(
            text=f"<b>{ti[0].strftime('%Y-%m-%d %H:%M')}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(self._time_div, sizing_mode="stretch_width")
        self._time_slider = pn.widgets.DiscretePlayer(
            name="",
            options=list(range(len(ti))),
            value=0,
            interval=500,            # ms between steps when playing
            loop_policy="once",
            show_value=False,        # we show timestamp in the Div above
            sizing_mode="stretch_width",
        )
        # DatetimePicker — lets the user jump directly to any date/time.
        # Synced bidirectionally with the DiscretePlayer (snaps to nearest step).
        self._datetime_picker = pn.widgets.DatetimePicker(
            name="Go to date/time",
            value=ti[0].to_pydatetime(),
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
            name="Colormap", options=CURATED_COLORMAPS_GROUPS, value=colormap,
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
        # Channel visibility toggle — always shown.
        self._show_channels_check = pn.widgets.Checkbox(
            name="Show channels", value=True, sizing_mode="stretch_width",
        )
        self._show_basemap_check = pn.widgets.Checkbox(
            name="Show background map", value=True, sizing_mode="stretch_width",
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
            self._show_channels_check,
            self._show_basemap_check,
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
            margin=(4, 8, 4, 4),
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
        self._show_channels_check.param.watch(self._on_show_channels_toggle, "value")
        self._show_basemap_check.param.watch(self._on_show_basemap_toggle, "value")
        if self._transform_options:
            self._transform_select.param.watch(self._on_transform_change, "value")
        if x2_callback is not None:
            self._x2_check.param.watch(self._on_x2_toggle, "value")
            self._x2_threshold_input.param.watch(self._on_x2_threshold_change, "value")
        self._save_config_btn.on_click(self._on_save_config)

        self.current_dt = ti[0].to_pydatetime()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
        """Update the label source if the label renderer is visible."""
        if self._contour_label_renderer.visible:
            self._contour_label_source.data = (
                self._compute_label_positions(xs_list, ys_list, lvls)
            )

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
        """Fast path: patch _value, optionally update contours and X2 line."""
        ts = self._reader.time_index[step_idx]
        series = self._reader.get_slice_nearest(ts)
        new_values = series.reindex(self._geo_ids).fillna(np.nan).tolist()

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

        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        self._bk_figure.title.text = (
            f"{self._title + ' \u2014 ' if self._title else ''}{ts_str}"
        )

    # ------------------------------------------------------------------
    # Widget callbacks
    # ------------------------------------------------------------------

    def _on_slider_change(self, event: param.parameterized.Event) -> None:
        if self._syncing:
            return
        idx = int(event.new)
        ts = self._reader.time_index[idx]
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        # Sync DatetimePicker without causing a feedback loop.
        self._syncing = True
        try:
            self._datetime_picker.value = ts.to_pydatetime()
        finally:
            self._syncing = False

        # Defer Bokeh model mutations to the next IOLoop tick so they run
        # inside the document lock (avoids _pending_writes RuntimeError).
        doc = self._bk_figure.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _idx=idx, _s=ts_str: self._apply_frame(_idx, _s)
            )
        else:
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
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _idx=idx, _s=ts_str: self._apply_frame(_idx, _s)
            )
        else:
            self._apply_frame(idx, ts_str)

    def _apply_frame(self, idx: int, ts_str: str) -> None:
        """Run all Bokeh mutations for a single frame step under document lock."""
        self._time_div.text = f"<b>{ts_str}</b>"
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

        When the transform option value is a :class:`TransformSpec` the
        streaming implementation is used (no full-file load at startup).
        A bare callable falls back to the legacy :class:`TransformedSlicingReader`.
        """
        reader = self._base_reader
        if transform_name and transform_name != "none" and transform_name in self._transform_options:
            spec_or_fn = self._transform_options[transform_name]
            if isinstance(spec_or_fn, TransformSpec):
                reader = StreamingTransformedSlicingReader(reader, spec_or_fn)
            else:
                reader = TransformedSlicingReader(reader, spec_or_fn)
        return BufferedSlicingReader(reader, chunk_size=self._buffer_chunk_size)

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
        cli_keys = meta.get("_transform_cli_keys", {})

        transform_display = (
            self._transform_select.value
            if self._transform_options
            else "none"
        )

        state: dict = {
            "version": 1,
            "mode": meta.get("mode", "single"),
            "files": meta.get("files", []),
            "file_type": meta.get("file_type", "hydro"),
            "variable": meta.get("variable", "flow"),
            "location": meta.get("location", "both"),
            "shapefile": meta.get("shapefile"),
            "channel_id_column": meta.get("channel_id_column"),
            "transform": cli_keys.get(transform_display, "none"),
            "colormap": self.colormap,
            "vmin": self.vmin,
            "vmax": self.vmax,
            "size": self.size,
            "show_channels": self._show_channels_check.value,
            "show_basemap": self._show_basemap_check.value,
            "contours": {
                "enabled": self._contours_check.value,
                "n_levels": self._n_contours_slider.value,
                "smoothing": float(self._contour_smooth_slider.value),
                "level_mode": self._contour_levels_select.value,
                "custom_levels": self._contour_custom_input.value,
                "color": self._contour_color_check.value,
                "labels": self._contour_labels_check.value,
            },
            "diff": {"show": False, "colormap": "coolwarm"},
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
        except Exception as exc:
            self._save_config_status.object = f"\u2717 {exc}"

    def _on_show_channels_toggle(self, event: param.parameterized.Event) -> None:
        self._data_renderer.visible = bool(event.new)

    def _on_show_basemap_toggle(self, event: param.parameterized.Event) -> None:
        self._tile_renderer.visible = bool(event.new)

    def _on_transform_change(self, event: param.parameterized.Event) -> None:
        """Apply a new time-domain transform; preserve current playback position.

        The transform may require loading the full HDF5 dataset and running a
        tidal filter (several seconds).  The heavy work is done in a daemon
        thread so the IOLoop stays responsive; a loading spinner covers the map
        while the computation is in progress.
        """
        import threading

        current_ts = pd.Timestamp(
            self._reader.time_index[self._time_slider.value]
        )
        new_name = event.new

        # Show loading indicator immediately (Panel param — safe from watcher).
        self._chart_pane.loading = True
        self._transform_select.disabled = True

        doc = self._bk_figure.document

        def _compute() -> None:
            """Run in a background thread: build reader (triggers lazy cache)."""
            new_reader = self._setup_reader(new_name)
            # Accessing time_index triggers TransformedSlicingReader._ensure_cache()
            # which is the slow step (loads HDF5 + applies filter).
            ti = new_reader.time_index
            nearest_idx = max(0, min(
                int(ti.get_indexer([current_ts], method="nearest")[0]),
                len(ti) - 1,
            ))

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
                self._transform_select.disabled = False

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
            self._contour_source.data = dict(self._contour_source.data, color=colors)

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
        if on:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)
        else:
            self._contour_source.data = {"xs": [], "ys": [], "level": [], "color": []}
            self._contour_label_source.data = {"x": [], "y": [], "text": []}

    def _on_n_contours_change(self, event: param.parameterized.Event) -> None:
        self.n_contours = int(event.new)
        if self._contour_renderer.visible:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

    def _on_contour_smooth_change(self, event: param.parameterized.Event) -> None:
        self.contour_smooth = float(event.new)
        if self._contour_renderer.visible:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

    def _on_contour_levels_change(self, event: param.parameterized.Event) -> None:
        self.contour_levels = event.new
        if self._contour_renderer.visible:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

    def _on_contour_custom_levels_change(self, event: param.parameterized.Event) -> None:
        """Recompute contours when the user edits the custom levels text box."""
        self.contour_custom_levels = event.new
        # When the user fills in explicit levels, the count / mode selectors
        # become redundant; dim them visually to hint they are being overridden.
        auto_controls_active = not bool(event.new.strip())
        self._n_contours_slider.disabled = not auto_controls_active
        self._contour_levels_select.disabled = not auto_controls_active
        if self._contour_renderer.visible:
            current_values = self._bk_source.data["_value"]
            xs, ys, lvls = self._compute_contours(list(current_values))
            colors = self._contour_colors(lvls)
            self._contour_source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
            self._update_contour_labels(xs, ys, lvls)

    def _on_contour_labels_toggle(self, event: param.parameterized.Event) -> None:
        """Show or hide contour labels."""
        self._contour_label_renderer.visible = bool(event.new)
        if event.new:
            xs = self._contour_source.data["xs"]
            ys = self._contour_source.data["ys"]
            lvls = self._contour_source.data["level"]
            if xs:
                self._contour_label_source.data = (
                    self._compute_label_positions(xs, ys, lvls)
                )
        else:
            self._contour_label_source.data = {"x": [], "y": [], "text": []}

    def _on_x2_toggle(self, event: param.parameterized.Event) -> None:
        """Show or hide the X2 isohaline line."""
        self._x2_renderer.visible = bool(event.new)
        self._x2_threshold_input.visible = bool(event.new)
        if event.new and self._x2_callback is not None:
            threshold = float(self._x2_threshold_input.value)
            xs, ys = self._x2_callback(self._time_slider.value, threshold)
            self._x2_source.data = {"xs": xs, "ys": ys}
        else:
            self._x2_source.data = {"xs": [], "ys": []}

    def _on_x2_threshold_change(self, event: param.parameterized.Event) -> None:
        """Recompute X2 line when the threshold value changes."""
        if self._x2_renderer.visible and self._x2_callback is not None:
            try:
                threshold = float(event.new)
            except (TypeError, ValueError):
                return
            xs, ys = self._x2_callback(self._time_slider.value, threshold)
            self._x2_source.data = {"xs": xs, "ys": ys}

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
            pn.Row(self._controls, self._chart_pane, sizing_mode="stretch_both"),
            sizing_mode="stretch_both",
            min_height=self._map_height,
        )

    def servable(self, title: Optional[str] = None, **kwargs) -> "GeoAnimatorManager":
        """Mark this component as the app entry point."""
        super().servable(title=title or self._title or "GeoAnimatorManager", **kwargs)
        return self

