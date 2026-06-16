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

from .reader import SlicingReader

# ---------------------------------------------------------------------------
# Curated colormaps (subset that works well with numeric data on maps)
# ---------------------------------------------------------------------------

CURATED_COLORMAPS: list[str] = [
    "viridis",
    "plasma",
    "inferno",
    "magma",
    "rainbow",
    "coolwarm",
    "RdBu_r",
    "Blues",
    "YlOrRd",
    "turbo",
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
        default=8.0, bounds=(1.0, 50.0),
        doc="Point radius (points) or line width (px). Not used for polygons.",
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
        size: float = 8.0,
        map_width: int = 750,
        map_height: int = 500,
        **params,
    ) -> None:
        # ----------------------------------------------------------------
        # 1. Config — stored before super().__init__() because on_init
        #    watchers fire during super().
        # ----------------------------------------------------------------
        self._reader = reader
        self._geo_id_column = geo_id_column
        self._title = title
        self._map_height = map_height

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
        # 5. Bokeh ColumnDataSource — geometry is set here once.
        # ----------------------------------------------------------------
        self._bk_source = ColumnDataSource({
            "xs": bk_xs,
            "ys": bk_ys,
            "_value": init_values,
            "geo_id": self._geo_ids,
        })

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
        p.add_tile(tile_source)

        # Configure hover to show only channel id and the data value.
        # The value format uses {0.3f}; the label is set later via
        # _update_hover_tool() so it reflects the variable name.
        hover = HoverTool(
            tooltips=[
                ("Channel", "@geo_id"),
                ("Value",   "@_value{0.3f}"),
            ],
            point_policy="follow_mouse",
        )
        p.add_tools(hover)

        color_field = {"field": "_value", "transform": self._bk_mapper}
        if self._geom_type == "point":
            p.scatter(
                x="xs", y="ys", source=self._bk_source,
                color=color_field, size=size, line_color=None,
            )
        elif self._geom_type == "line":
            p.multi_line(
                xs="xs", ys="ys", source=self._bk_source,
                line_color=color_field, line_width=size,
            )
        else:  # polygon
            p.patches(
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
        # IntSlider (0..N-1): fast even for multi-year 15-min series.
        # A Bokeh Div shows the current timestamp below the slider;
        # it is updated via _on_slider_change without causing a Panel
        # layout reflow (Div is a leaf Bokeh model, not a Panel pane).
        self._time_div = Div(
            text=f"<b>{ti[0].strftime('%Y-%m-%d %H:%M')}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(self._time_div, sizing_mode="stretch_width")
        self._time_slider = pn.widgets.IntSlider(
            name="", start=0, end=len(ti) - 1, step=1, value=0,
            sizing_mode="stretch_width",
        )
        self._clim_input = pn.widgets.TextInput(
            name="Color range  (min, max)",
            value=f"{init_vmin:.4g}, {init_vmax:.4g}",
            sizing_mode="stretch_width",
        )
        self._colormap_select = pn.widgets.Select(
            name="Colormap", options=CURATED_COLORMAPS, value=colormap,
            sizing_mode="stretch_width",
        )
        self._size_slider = pn.widgets.FloatSlider(
            name="Size" if self._geom_type == "point" else "Line width",
            start=1.0, end=50.0, step=0.5, value=size,
            sizing_mode="stretch_width",
        )
        size_row: list = (
            [] if self._geom_type == "polygon"
            else [pn.pane.Markdown("**Size**"), self._size_slider]
        )
        self._controls = pn.Column(
            pn.pane.Markdown("### Controls", margin=(4, 0, 0, 0)),
            pn.pane.Markdown("**Time**"),
            self._time_label_pane,
            self._time_slider,
            pn.pane.Markdown("**Colour scale**"),
            self._clim_input,
            pn.pane.Markdown("**Colormap**"),
            self._colormap_select,
            *size_row,
            sizing_mode="stretch_width",
            max_width=260,
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
        self.param.watch(self._on_style_change, ["vmin", "vmax", "colormap", "size"])
        self._clim_input.param.watch(self._on_clim_text_change, "value")
        self._colormap_select.param.watch(self._on_colormap_widget_change, "value")
        if self._geom_type != "polygon":
            self._size_slider.param.watch(self._on_size_widget_change, "value")

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

    def _load_frame(self, step_idx: int) -> None:
        """Fast path: patch only _value in the ColumnDataSource.

        Only Bokeh-document mutations happen here.  No Panel widget or
        param changes — those trigger a Panel layout reflow which resizes
        the ``stretch_both`` figure and resets the viewport/aspect ratio.
        """
        ts = self._reader.time_index[step_idx]
        series = self._reader.get_slice_nearest(ts)
        new_values = series.reindex(self._geo_ids).fillna(np.nan).tolist()

        # patch() sends only _value over WebSocket — geometry never re-sent
        self._bk_source.patch({"_value": [(slice(None), new_values)]})

        # Update Bokeh title (pure Bokeh document change, no Panel reflow)
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        plot_title = f"{self._title + ' — ' if self._title else ''}{ts_str}"
        self._bk_figure.title.text = plot_title

    # ------------------------------------------------------------------
    # Widget callbacks
    # ------------------------------------------------------------------

    def _on_slider_change(self, event: param.parameterized.Event) -> None:
        idx = int(event.new)
        ts = self._reader.time_index[idx]
        # Update the Div text (Bokeh model — no Panel layout reflow)
        self._time_div.text = f"<b>{ts.strftime('%Y-%m-%d %H:%M')}</b>"
        self._load_frame(idx)

    def _on_style_change(self, *events) -> None:
        """Update LinearColorMapper in-place — no frame rebuild needed."""
        eff_vmin, eff_vmax = self._current_clim()
        self._bk_mapper.palette = _cmap_to_palette(self.colormap)
        self._bk_mapper.low = eff_vmin
        self._bk_mapper.high = eff_vmax
        # Update glyph size/line_width if size param changed
        new_size = float(self.size)
        for r in self._bk_figure.renderers:
            if not hasattr(r, "glyph"):
                continue
            g = r.glyph
            if hasattr(g, "size"):          # Scatter
                g.size = new_size
            elif hasattr(g, "line_width") and not hasattr(g, "fill_color"):
                g.line_width = new_size     # MultiLine

    def _on_colormap_widget_change(self, event: param.parameterized.Event) -> None:
        self.colormap = event.new

    def _on_size_widget_change(self, event: param.parameterized.Event) -> None:
        self.size = float(event.new)

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

