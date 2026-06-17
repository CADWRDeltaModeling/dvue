"""MultiGeoAnimatorManager — side-by-side geo-animation of two readers.

Provides a shared-controls panel with two Bokeh maps (one per reader) that
animate in lock-step.  A **Diff mode** checkbox replaces both maps with a
single diff map (reader A − reader B) coloured with a diverging palette.

Architecture
------------
Both Bokeh figures share the **same** ``Range1d`` x/y objects so any pan or
zoom on one map is mirrored instantly on the other (and on the diff map).

Contour overlays use the same rasterise → smooth → contour pipeline as
:class:`~dvue.animator.GeoAnimatorManager`, delegating to the shared module-
level helpers ``_run_contour_computation`` and ``_compute_contour_levels``
extracted from ``dvue.animator.ui``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import numpy as np
import pandas as pd
import panel as pn
import param
import geopandas as gpd
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

from .reader import SlicingReader, DiffSlicingReader, BufferedSlicingReader
from .ui import (
    CURATED_COLORMAPS,
    _CARTO_LIGHT_URL,
    _CARTO_LIGHT_ATTR,
    _cmap_to_palette,
    _detect_geom_type,
    _clip_contour_segment,          # noqa: F401 (re-exported for tests)
    _compute_contour_levels,
    _run_contour_computation,
    _make_contour_grid,
    _level_colors,
    _nice_decimal_places,
    _format_level,
)


# ---------------------------------------------------------------------------
# Per-map contour state container
# ---------------------------------------------------------------------------

class _MapContour:
    """Holds contour rendering state (sources, renderers, grid) for one map."""

    __slots__ = (
        "centroids_x", "centroids_y",
        "grid_x", "grid_y", "clip_zone",
        "source", "renderer",
        "label_source", "label_renderer",
    )

    def __init__(
        self,
        centroids_x, centroids_y, grid_x, grid_y, clip_zone,
        source, renderer, label_source, label_renderer,
    ):
        self.centroids_x = centroids_x
        self.centroids_y = centroids_y
        self.grid_x = grid_x
        self.grid_y = grid_y
        self.clip_zone = clip_zone
        self.source = source
        self.renderer = renderer
        self.label_source = label_source
        self.label_renderer = label_renderer


# ---------------------------------------------------------------------------
# Module-level Bokeh-map builder
# ---------------------------------------------------------------------------

def _extract_geo_arrays(gdf_proj, geo_id_column: str, geom_type: str):
    """Extract geo_ids and Bokeh xs/ys from a projected GDF."""
    geo_ids = [int(v) for v in gdf_proj[geo_id_column].values]
    if geom_type == "point":
        bk_xs = gdf_proj.geometry.x.values.tolist()
        bk_ys = gdf_proj.geometry.y.values.tolist()
    else:
        bk_xs, bk_ys = [], []
        for geom in gdf_proj.geometry:
            if geom_type == "polygon":
                coords = np.array(geom.exterior.coords)
            else:
                coords = np.array(geom.coords)
            bk_xs.append(coords[:, 0].tolist())
            bk_ys.append(coords[:, 1].tolist())
    return geo_ids, bk_xs, bk_ys


def _build_bokeh_map(
    gdf_proj,
    geo_ids: list,
    bk_xs: list,
    bk_ys: list,
    geom_type: str,
    init_values: list,
    colormap: str,
    vmin: float,
    vmax: float,
    size: float,
    map_height: int,
    title: str,
    geo_id_column: str,
    shared_x_range=None,
    shared_y_range=None,
) -> SimpleNamespace:
    """Build one Bokeh figure and all associated rendering objects.

    When *shared_x_range* / *shared_y_range* are supplied the figure uses
    those ``Range1d`` instances, linking its viewport to other figures that
    share the same objects (pan/zoom synchronisation).

    Returns a ``SimpleNamespace`` with attributes:
        ``fig``, ``source``, ``mapper``, ``data_renderer``, ``tile_renderer``,
        ``contour_source``, ``contour_renderer``,
        ``contour_label_source``, ``contour_label_renderer``.
    """
    source = ColumnDataSource({
        "xs": bk_xs, "ys": bk_ys,
        "_value": init_values, "geo_id": geo_ids,
    })
    mapper = LinearColorMapper(
        palette=_cmap_to_palette(colormap),
        low=vmin, high=vmax,
        nan_color="lightgrey",
    )

    if shared_x_range is not None:
        x_range = shared_x_range
        y_range = shared_y_range
    else:
        bounds = gdf_proj.total_bounds
        pad_x = max((bounds[2] - bounds[0]) * 0.05, 1000.0)
        pad_y = max((bounds[3] - bounds[1]) * 0.05, 1000.0)
        x_range = Range1d(bounds[0] - pad_x, bounds[2] + pad_x, bounds=None)
        y_range = Range1d(bounds[1] - pad_y, bounds[3] + pad_y, bounds=None)

    p = bk_figure(
        x_range=x_range, y_range=y_range,
        x_axis_type="mercator", y_axis_type="mercator",
        match_aspect=True,
        sizing_mode="stretch_both", min_height=map_height,
        title=title,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    p.axis.visible = False
    tile_source = WMTSTileSource(url=_CARTO_LIGHT_URL, attribution=_CARTO_LIGHT_ATTR)
    tile_renderer = p.add_tile(tile_source)

    data_hover = HoverTool(
        tooltips=[("Channel", "@geo_id"), ("Value", "@_value{0.3f}")],
        point_policy="follow_mouse",
    )
    p.add_tools(data_hover)

    color_field = {"field": "_value", "transform": mapper}
    if geom_type == "point":
        data_renderer = p.scatter(
            x="xs", y="ys", source=source,
            color=color_field, size=size, line_color=None,
        )
    elif geom_type == "line":
        data_renderer = p.multi_line(
            xs="xs", ys="ys", source=source,
            line_color=color_field, line_width=size,
        )
    else:
        data_renderer = p.patches(
            xs="xs", ys="ys", source=source,
            fill_color=color_field,
            line_color="white", line_width=0.5, line_alpha=0.2,
        )

    colorbar = ColorBar(
        color_mapper=mapper,
        ticker=BasicTicker(desired_num_ticks=6),
        label_standoff=8, border_line_color=None, location=(0, 0),
    )
    p.add_layout(colorbar, "right")

    # Contour overlay — initially invisible.
    contour_source = ColumnDataSource({"xs": [], "ys": [], "level": [], "color": []})
    contour_renderer = p.multi_line(
        xs="xs", ys="ys", source=contour_source,
        line_color="color", line_width=2.5, line_alpha=0.9,
        visible=False,
    )
    contour_hover = HoverTool(
        renderers=[contour_renderer],
        tooltips=[("Level", "@level{0.3f}")],
        point_policy="follow_mouse",
    )
    p.add_tools(contour_hover)

    contour_label_source = ColumnDataSource({"x": [], "y": [], "text": []})
    contour_label_renderer = p.text(
        x="x", y="y", text="text",
        source=contour_label_source,
        text_font_size="13px",
        text_color="black",
        text_align="center",
        text_baseline="middle",
        background_fill_color="white",
        background_fill_alpha=0.6,
        visible=False,
    )

    return SimpleNamespace(
        fig=p,
        source=source,
        mapper=mapper,
        data_renderer=data_renderer,
        tile_renderer=tile_renderer,
        contour_source=contour_source,
        contour_renderer=contour_renderer,
        contour_label_source=contour_label_source,
        contour_label_renderer=contour_label_renderer,
    )


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class MultiGeoAnimatorManager(pn.viewable.Viewer):
    """Side-by-side geo-animation of two :class:`~dvue.animator.SlicingReader` s.

    Both maps share a single set of time controls **and** a linked viewport —
    pan / zoom on one map mirrors the other instantly.

    A **"Show diff (A − B)"** checkbox collapses both maps into a single diff
    map using a diverging colourmap centred on zero.

    **Contour overlays** are driven by shared controls; the same levels and
    smoothing are applied to both maps (and to the diff map).

    Parameters
    ----------
    reader_a, reader_b : SlicingReader
    gdf_a, gdf_b : gpd.GeoDataFrame
        Channel centreline GDFs.  May be the same object.
    title_a, title_b : str, optional
    geo_id_column : str, optional
    colormap : str, optional
    diff_colormap : str, optional
    vmin, vmax : float or None, optional
    size : float, optional
    map_height : int, optional
    show_diff : bool, optional
    transform_options : dict or None, optional
    initial_transform : str, optional
    buffer_chunk_size : int, optional
    """

    # ------------------------------------------------------------------
    # Params
    # ------------------------------------------------------------------
    vmin: Optional[float] = param.Number(default=None, allow_None=True)
    vmax: Optional[float] = param.Number(default=None, allow_None=True)
    colormap: str = param.Selector(default="rainbow", objects=CURATED_COLORMAPS)
    diff_colormap: str = param.Selector(default="coolwarm", objects=CURATED_COLORMAPS)
    size: float = param.Number(default=6.0, bounds=(1.0, 50.0))
    show_diff: bool = param.Boolean(
        default=False, doc="Show A−B diff map instead of side-by-side."
    )
    # Contour params (shared for both maps)
    n_contours: int = param.Integer(default=8, bounds=(2, 30))
    contour_smooth: float = param.Number(default=3.0, bounds=(0.0, 20.0))
    contour_levels_mode: str = param.Selector(
        default="nice", objects=["linear", "nice", "eq_hist"]
    )
    contour_custom_levels: str = param.String(default="")

    def __init__(
        self,
        reader_a: SlicingReader,
        reader_b: SlicingReader,
        gdf_a,
        gdf_b=None,
        title_a: str = "Study A",
        title_b: str = "Study B",
        geo_id_column: str = "geo_id",
        colormap: str = "rainbow",
        diff_colormap: str = "coolwarm",
        vmin=None,
        vmax=None,
        size: float = 6.0,
        map_height: int = 500,
        show_diff: bool = False,
        transform_options=None,
        initial_transform: str = "none",
        buffer_chunk_size: int = 200,
        **params,
    ) -> None:
        # ----------------------------------------------------------------
        # 1. Store configuration
        # ----------------------------------------------------------------
        self._base_reader_a = reader_a
        self._base_reader_b = reader_b
        if gdf_b is None:
            gdf_b = gdf_a
        self._title_a = title_a
        self._title_b = title_b
        self._geo_id_column = geo_id_column
        self._map_height = map_height
        self._transform_options = transform_options or {}
        self._buffer_chunk_size = buffer_chunk_size
        self._diff_reader_cache = None
        self._syncing = False
        self._contour_color = True

        # ----------------------------------------------------------------
        # 2. Readers (transform + buffer)
        # ----------------------------------------------------------------
        self._reader_a = self._setup_reader(self._base_reader_a, initial_transform)
        self._reader_b = self._setup_reader(self._base_reader_b, initial_transform)
        ti = self._reader_a.time_index

        # ----------------------------------------------------------------
        # 3. Project GDFs and extract geometry arrays
        # ----------------------------------------------------------------
        if gdf_a.crs is None:
            raise ValueError("gdf_a must have a CRS set.")
        if gdf_b.crs is None:
            raise ValueError("gdf_b must have a CRS set.")
        self._gdf_a_proj = gdf_a.to_crs("EPSG:3857").copy()
        self._gdf_b_proj = gdf_b.to_crs("EPSG:3857").copy()

        self._geom_type_a = _detect_geom_type(self._gdf_a_proj)
        self._geom_type_b = _detect_geom_type(self._gdf_b_proj)

        self._geo_ids_a, bk_xs_a, bk_ys_a = _extract_geo_arrays(
            self._gdf_a_proj, geo_id_column, self._geom_type_a)
        self._geo_ids_b, bk_xs_b, bk_ys_b = _extract_geo_arrays(
            self._gdf_b_proj, geo_id_column, self._geom_type_b)

        # ----------------------------------------------------------------
        # 4. Contour grids (centroids + raster + clip zone) — built once
        # ----------------------------------------------------------------
        cx_a, cy_a, gx_a, gy_a, cz_a = _make_contour_grid(
            self._gdf_a_proj, self._geom_type_a)
        cx_b, cy_b, gx_b, gy_b, cz_b = _make_contour_grid(
            self._gdf_b_proj, self._geom_type_b)

        # ----------------------------------------------------------------
        # 5. Effective colour-scale bounds
        # ----------------------------------------------------------------
        eff_vmin = float(vmin if vmin is not None else reader_a.vmin)
        eff_vmax = float(vmax if vmax is not None else reader_a.vmax)
        if eff_vmin == eff_vmax:
            eff_vmax = eff_vmin + 1.0

        init_a = reader_a.get_slice(ti[0])
        init_b = reader_b.get_slice_nearest(ti[0])
        init_vals_a = [float(init_a.get(gid, np.nan)) for gid in self._geo_ids_a]
        init_vals_b = [float(init_b.get(gid, np.nan)) for gid in self._geo_ids_b]

        # ----------------------------------------------------------------
        # 6. Shared viewport Range1d — one object shared by all figures
        # ----------------------------------------------------------------
        bounds_a = self._gdf_a_proj.total_bounds
        bounds_b = self._gdf_b_proj.total_bounds
        combined = np.array([
            min(bounds_a[0], bounds_b[0]),
            min(bounds_a[1], bounds_b[1]),
            max(bounds_a[2], bounds_b[2]),
            max(bounds_a[3], bounds_b[3]),
        ])
        pad_x = max((combined[2] - combined[0]) * 0.05, 1000.0)
        pad_y = max((combined[3] - combined[1]) * 0.05, 1000.0)
        shared_x = Range1d(combined[0] - pad_x, combined[2] + pad_x, bounds=None)
        shared_y = Range1d(combined[1] - pad_y, combined[3] + pad_y, bounds=None)

        # ----------------------------------------------------------------
        # 7. Build Bokeh figures — all three share the same Range1d pair
        # ----------------------------------------------------------------
        ts0 = ti[0].strftime("%Y-%m-%d %H:%M")

        ma = _build_bokeh_map(
            self._gdf_a_proj, self._geo_ids_a, bk_xs_a, bk_ys_a,
            self._geom_type_a, init_vals_a,
            colormap, eff_vmin, eff_vmax, size, map_height,
            f"{title_a} \u2014 {ts0}", geo_id_column,
            shared_x_range=shared_x, shared_y_range=shared_y,
        )
        mb = _build_bokeh_map(
            self._gdf_b_proj, self._geo_ids_b, bk_xs_b, bk_ys_b,
            self._geom_type_b, init_vals_b,
            colormap, eff_vmin, eff_vmax, size, map_height,
            f"{title_b} \u2014 {ts0}", geo_id_column,
            shared_x_range=shared_x, shared_y_range=shared_y,
        )
        # Diff figure reuses map A's geometry and shares the viewport.
        md = _build_bokeh_map(
            self._gdf_a_proj, self._geo_ids_a, bk_xs_a, bk_ys_a,
            self._geom_type_a, [np.nan] * len(self._geo_ids_a),
            diff_colormap, -1.0, 1.0, size, map_height,
            f"{title_a} \u2212 {title_b} \u2014 {ts0}", geo_id_column,
            shared_x_range=shared_x, shared_y_range=shared_y,
        )

        self._fig_a = ma.fig;  self._src_a = ma.source;  self._mapper_a = ma.mapper
        self._fig_b = mb.fig;  self._src_b = mb.source;  self._mapper_b = mb.mapper
        self._fig_diff = md.fig;  self._src_diff = md.source
        self._mapper_diff = md.mapper
        # Keep renderer references so show/hide checkboxes can toggle them.
        self._all_data_renderers = (
            ma.data_renderer, mb.data_renderer, md.data_renderer)
        self._all_tile_renderers = (
            ma.tile_renderer, mb.tile_renderer, md.tile_renderer)

        # ----------------------------------------------------------------
        # 8. Contour state objects per map
        # ----------------------------------------------------------------
        self._ctour_a = _MapContour(
            cx_a, cy_a, gx_a, gy_a, cz_a,
            ma.contour_source, ma.contour_renderer,
            ma.contour_label_source, ma.contour_label_renderer,
        )
        self._ctour_b = _MapContour(
            cx_b, cy_b, gx_b, gy_b, cz_b,
            mb.contour_source, mb.contour_renderer,
            mb.contour_label_source, mb.contour_label_renderer,
        )
        # Diff map uses map A's centroid grid.
        self._ctour_diff = _MapContour(
            cx_a, cy_a, gx_a, gy_a, cz_a,
            md.contour_source, md.contour_renderer,
            md.contour_label_source, md.contour_label_renderer,
        )

        # ----------------------------------------------------------------
        # 9. Panel wrappers
        # ----------------------------------------------------------------
        self._pane_a = pn.pane.Bokeh(
            self._fig_a, sizing_mode="stretch_both", min_height=map_height)
        self._pane_b = pn.pane.Bokeh(
            self._fig_b, sizing_mode="stretch_both", min_height=map_height)
        self._pane_diff = pn.pane.Bokeh(
            self._fig_diff, sizing_mode="stretch_both", min_height=map_height)

        # ----------------------------------------------------------------
        # 10. Time controls
        # ----------------------------------------------------------------
        self._time_div = Div(
            text=f"<b>{ts0}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(
            self._time_div, sizing_mode="stretch_width")
        self._time_slider = pn.widgets.DiscretePlayer(
            name="", options=list(range(len(ti))), value=0,
            interval=500, loop_policy="once", show_value=False,
            sizing_mode="stretch_width",
        )
        self._datetime_picker = pn.widgets.DatetimePicker(
            name="Go to date/time",
            value=ti[0].to_pydatetime(),
            start=ti[0].to_pydatetime(), end=ti[-1].to_pydatetime(),
            sizing_mode="stretch_width",
        )

        # ----------------------------------------------------------------
        # 11. Style + diff controls
        # ----------------------------------------------------------------
        self._clim_input = pn.widgets.TextInput(
            name="Color range  (min, max)",
            value=f"{eff_vmin:.4g}, {eff_vmax:.4g}",
            sizing_mode="stretch_width",
        )
        self._colormap_select = pn.widgets.Select(
            name="Colormap", options=CURATED_COLORMAPS, value=colormap,
            sizing_mode="stretch_width",
        )
        self._diff_colormap_select = pn.widgets.Select(
            name="Diff colormap", options=CURATED_COLORMAPS, value=diff_colormap,
            sizing_mode="stretch_width", visible=show_diff,
        )
        self._show_diff_check = pn.widgets.Checkbox(
            name="Show diff (A \u2212 B)", value=show_diff,
            sizing_mode="stretch_width",
        )
        _transform_names = ["none"] + list(self._transform_options.keys())
        self._transform_select = pn.widgets.Select(
            name="Transform", options=_transform_names,
            value=initial_transform if initial_transform in _transform_names else "none",
            sizing_mode="stretch_width",
            visible=bool(self._transform_options),
        )

        # ----------------------------------------------------------------
        # 12. Contour controls (shared for both maps)
        # ----------------------------------------------------------------
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
            options=["linear", "nice", "eq_hist"], value="nice",
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
        # Show / hide toggles for channels and basemap (all three figures).
        self._show_channels_check = pn.widgets.Checkbox(
            name="Show channels", value=True, sizing_mode="stretch_width",
        )
        self._show_basemap_check = pn.widgets.Checkbox(
            name="Show background map", value=True, sizing_mode="stretch_width",
        )

        # ----------------------------------------------------------------
        # 13. Controls column — grouped into collapsible pn.Card sections
        # ----------------------------------------------------------------
        _appearance_card = pn.Card(
            self._clim_input,
            self._colormap_select,
            self._show_channels_check,
            self._show_basemap_check,
            title="Appearance", collapsed=False,
            sizing_mode="stretch_width",
        )
        _diff_card = pn.Card(
            self._show_diff_check,
            self._diff_colormap_select,
            title="Diff (A − B)", collapsed=False,
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

        self._controls = pn.Column(
            pn.pane.Markdown("### Controls", margin=(4, 0, 2, 0)),
            self._time_label_pane,
            self._time_slider,
            self._datetime_picker,
            pn.layout.Divider(margin=(4, 0, 4, 0)),
            _appearance_card,
            _diff_card,
            _contour_card,
            *_optional_cards,
            sizing_mode="stretch_width",
            max_width=280,
            margin=(4, 8, 4, 4),
        )

        # ----------------------------------------------------------------
        # 14. Maps layout placeholder
        # ----------------------------------------------------------------
        self._maps_pane = pn.Row(
            self._pane_a, self._pane_b,
            sizing_mode="stretch_both",
        )

        # ----------------------------------------------------------------
        # 15. super().__init__
        # ----------------------------------------------------------------
        super().__init__(
            vmin=vmin, vmax=vmax,
            colormap=colormap, diff_colormap=diff_colormap,
            size=size, show_diff=show_diff,
            **params,
        )

        # ----------------------------------------------------------------
        # 16. Wire watchers
        # ----------------------------------------------------------------
        self._time_slider.param.watch(self._on_slider_change, "value")
        self._datetime_picker.param.watch(self._on_datetime_picker_change, "value")
        self.param.watch(self._on_style_change, ["vmin", "vmax", "colormap", "size"])
        self.param.watch(self._on_diff_colormap_change, ["diff_colormap"])
        self._clim_input.param.watch(self._on_clim_text_change, "value")
        self._colormap_select.param.watch(self._on_colormap_change, "value")
        self._diff_colormap_select.param.watch(self._on_diff_colormap_widget_change, "value")
        self._show_diff_check.param.watch(self._on_diff_toggle, "value")
        self._contours_check.param.watch(self._on_contours_toggle, "value")
        self._contour_color_check.param.watch(self._on_contour_color_toggle, "value")
        self._n_contours_slider.param.watch(self._on_n_contours_change, "value")
        self._contour_smooth_slider.param.watch(self._on_contour_smooth_change, "value")
        self._contour_levels_select.param.watch(self._on_contour_levels_change, "value")
        self._contour_custom_input.param.watch(self._on_contour_custom_change, "value")
        self._contour_labels_check.param.watch(self._on_contour_labels_toggle, "value")
        self._show_channels_check.param.watch(self._on_show_channels_toggle, "value")
        self._show_basemap_check.param.watch(self._on_show_basemap_toggle, "value")
        if self._transform_options:
            self._transform_select.param.watch(self._on_transform_change, "value")

    # ------------------------------------------------------------------
    # Reader setup
    # ------------------------------------------------------------------

    def _setup_reader(self, base: SlicingReader, transform_name: str) -> SlicingReader:
        reader = base
        if (transform_name and transform_name != "none"
                and transform_name in self._transform_options):
            from .reader import TransformedSlicingReader
            reader = TransformedSlicingReader(reader, self._transform_options[transform_name])
        return BufferedSlicingReader(reader, chunk_size=self._buffer_chunk_size)

    def _get_diff_reader(self) -> SlicingReader:
        """Return a buffered DiffSlicingReader (constructed lazily, cached)."""
        if self._diff_reader_cache is None:
            self._diff_reader_cache = DiffSlicingReader(
                self._base_reader_a, self._base_reader_b,
            )
        return BufferedSlicingReader(
            self._diff_reader_cache, chunk_size=self._buffer_chunk_size)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _current_clim(self) -> tuple:
        vmin = self.vmin if self.vmin is not None else self._reader_a.vmin
        vmax = self.vmax if self.vmax is not None else self._reader_a.vmax
        if vmin == vmax:
            vmax = vmin + 1.0
        return float(vmin), float(vmax)

    def _recompute_contours(
        self,
        ctour: _MapContour,
        vals: list,
        vmin: float,
        vmax: float,
        colormap: str,
    ) -> None:
        """Recompute contour paths for one map and update its ColumnDataSources."""
        vals_arr = np.asarray(vals, dtype=float)
        mask = np.isfinite(vals_arr)
        if mask.sum() < 4:
            ctour.source.data = {"xs": [], "ys": [], "level": [], "color": []}
            ctour.label_source.data = {"x": [], "y": [], "text": []}
            return

        levels = _compute_contour_levels(
            vals_arr[mask], vmin, vmax,
            self.n_contours, self.contour_levels_mode, self.contour_custom_levels,
        )
        xs, ys, lvls = _run_contour_computation(
            vals_arr,
            ctour.centroids_x, ctour.centroids_y,
            ctour.grid_x, ctour.grid_y,
            float(self.contour_smooth), levels, ctour.clip_zone,
        )
        colors = (
            _level_colors(lvls, vmin, vmax, colormap)
            if self._contour_color
            else ["black"] * len(lvls)
        )
        ctour.source.data = {"xs": xs, "ys": ys, "level": lvls, "color": colors}
        if ctour.label_renderer.visible:
            ctour.label_source.data = self._label_positions(xs, ys, lvls)

    def _label_positions(self, xs_list, ys_list, lvls) -> dict:
        """Compute one label anchor per unique level (midpoint of longest path)."""
        best: dict = {}
        for xs, ys, lvl in zip(xs_list, ys_list, lvls):
            n = len(xs)
            if n < 2:
                continue
            if lvl not in best or n > best[lvl][2]:
                mid = n // 2
                best[lvl] = (xs[mid], ys[mid], n)
        lx, ly, lt = [], [], []
        n_dec = _nice_decimal_places(sorted(best.keys()))
        for lvl, (mx, my, _) in sorted(best.items()):
            lx.append(mx)
            ly.append(my)
            lt.append(_format_level(lvl, n_dec))
        return {"x": lx, "y": ly, "text": lt}

    # ------------------------------------------------------------------
    # Frame update
    # ------------------------------------------------------------------

    def _update_map_a(self, idx: int, ts: pd.Timestamp) -> None:
        series = self._reader_a.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_a).fillna(np.nan).tolist()
        self._src_a.patch({"_value": [(slice(None), vals)]})
        self._fig_a.title.text = (
            f"{self._title_a} \u2014 {ts.strftime('%Y-%m-%d %H:%M')}"
        )
        if self._ctour_a.renderer.visible:
            eff_vmin, eff_vmax = self._current_clim()
            self._recompute_contours(
                self._ctour_a, vals, eff_vmin, eff_vmax, self.colormap)

    def _update_map_b(self, idx: int, ts: pd.Timestamp) -> None:
        series = self._reader_b.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_b).fillna(np.nan).tolist()
        self._src_b.patch({"_value": [(slice(None), vals)]})
        self._fig_b.title.text = (
            f"{self._title_b} \u2014 {ts.strftime('%Y-%m-%d %H:%M')}"
        )
        if self._ctour_b.renderer.visible:
            eff_vmin, eff_vmax = self._current_clim()
            self._recompute_contours(
                self._ctour_b, vals, eff_vmin, eff_vmax, self.colormap)

    def _update_diff_map(self, ts: pd.Timestamp) -> None:
        diff_r = self._get_diff_reader()
        series = diff_r.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_a).fillna(np.nan).tolist()
        self._src_diff.data = {
            "xs": self._src_a.data["xs"],
            "ys": self._src_a.data["ys"],
            "_value": vals,
            "geo_id": self._geo_ids_a,
        }
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        self._fig_diff.title.text = (
            f"{self._title_a} \u2212 {self._title_b} \u2014 {ts_str}"
        )
        d = self._diff_reader_cache
        if d is not None:
            absmax = max(abs(d.vmin), abs(d.vmax), 1e-9)
            self._mapper_diff.low = -absmax
            self._mapper_diff.high = absmax
        if self._ctour_diff.renderer.visible:
            absmax = max(
                abs(self._mapper_diff.low), abs(self._mapper_diff.high), 1e-9)
            self._recompute_contours(
                self._ctour_diff, vals, -absmax, absmax, self.diff_colormap)

    def _apply_frame(self, idx: int, ts_str: str) -> None:
        """All Bokeh mutations for one frame step — must run under document lock."""
        self._time_div.text = f"<b>{ts_str}</b>"
        ts = self._reader_a.time_index[idx]
        if self.show_diff:
            self._update_diff_map(ts)
        else:
            self._update_map_a(idx, ts)
            self._update_map_b(idx, ts)

    # ------------------------------------------------------------------
    # Callbacks — time
    # ------------------------------------------------------------------

    def _on_slider_change(self, event: param.parameterized.Event) -> None:
        if self._syncing:
            return
        idx = int(event.new)
        ts = self._reader_a.time_index[idx]
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        self._syncing = True
        try:
            self._datetime_picker.value = ts.to_pydatetime()
        finally:
            self._syncing = False
        doc = self._fig_a.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _s=ts_str: self._apply_frame(_i, _s))
        else:
            self._apply_frame(idx, ts_str)

    def _on_datetime_picker_change(self, event: param.parameterized.Event) -> None:
        if self._syncing or event.new is None:
            return
        ts = pd.Timestamp(event.new)
        idx = int(self._reader_a.time_index.get_indexer([ts], method="nearest")[0])
        self._syncing = True
        try:
            self._time_slider.value = idx
        finally:
            self._syncing = False
        actual_ts = self._reader_a.time_index[idx]
        ts_str = actual_ts.strftime("%Y-%m-%d %H:%M")
        doc = self._fig_a.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _s=ts_str: self._apply_frame(_i, _s))
        else:
            self._apply_frame(idx, ts_str)

    # ------------------------------------------------------------------
    # Callbacks — style
    # ------------------------------------------------------------------

    def _on_style_change(self, *events) -> None:
        doc = self._fig_a.document
        if doc is not None:
            doc.add_next_tick_callback(self._apply_bokeh_style)
        else:
            self._apply_bokeh_style()

    def _apply_bokeh_style(self) -> None:
        eff_vmin, eff_vmax = self._current_clim()
        pal = _cmap_to_palette(self.colormap)
        for mapper in (self._mapper_a, self._mapper_b):
            mapper.palette = pal
            mapper.low = eff_vmin
            mapper.high = eff_vmax
        # If contours are visible, refresh them (colours may have changed).
        if self._ctour_a.renderer.visible:
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            self._update_map_a(idx, ts)
            self._update_map_b(idx, ts)

    def _on_diff_colormap_change(self, *events) -> None:
        pal = _cmap_to_palette(self.diff_colormap)
        doc = self._fig_diff.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda: setattr(self._mapper_diff, "palette", pal))
        else:
            self._mapper_diff.palette = pal

    def _on_colormap_change(self, event: param.parameterized.Event) -> None:
        self.colormap = event.new

    def _on_diff_colormap_widget_change(self, event: param.parameterized.Event) -> None:
        self.diff_colormap = event.new

    def _on_clim_text_change(self, event: param.parameterized.Event) -> None:
        try:
            parts = [p.strip() for p in event.new.split(",")]
            if len(parts) == 2:
                self.vmin, self.vmax = float(parts[0]), float(parts[1])
        except ValueError:
            pass

    def _on_show_channels_toggle(self, event: param.parameterized.Event) -> None:
        """Show/hide channel data renderers on all three figures."""
        for r in self._all_data_renderers:
            r.visible = bool(event.new)

    def _on_show_basemap_toggle(self, event: param.parameterized.Event) -> None:
        """Show/hide tile (basemap) renderers on all three figures."""
        for r in self._all_tile_renderers:
            r.visible = bool(event.new)

    # ------------------------------------------------------------------
    # Callbacks — diff toggle
    # ------------------------------------------------------------------

    def _on_diff_toggle(self, event: param.parameterized.Event) -> None:
        self.show_diff = bool(event.new)
        self._diff_colormap_select.visible = self.show_diff
        if self.show_diff:
            self._maps_pane.objects = [self._pane_diff]
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            doc = self._fig_diff.document
            if doc is not None:
                doc.add_next_tick_callback(lambda _ts=ts: self._update_diff_map(_ts))
            else:
                self._update_diff_map(ts)
        else:
            self._maps_pane.objects = [self._pane_a, self._pane_b]

    # ------------------------------------------------------------------
    # Callbacks — contours
    # ------------------------------------------------------------------

    def _on_contours_toggle(self, event: param.parameterized.Event) -> None:
        on = bool(event.new)
        self._contour_card.collapsed = not on
        # Show/hide all contour renderers
        for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
            ctour.renderer.visible = on
        # Show/hide contour control widgets
        for w in (self._n_contours_slider, self._contour_smooth_slider,
                  self._contour_levels_select, self._contour_custom_input,
                  self._contour_color_check, self._contour_labels_check):
            w.visible = on
        if on:
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            if self.show_diff:
                self._update_diff_map(ts)
            else:
                self._update_map_a(idx, ts)
                self._update_map_b(idx, ts)
        else:
            empty = {"xs": [], "ys": [], "level": [], "color": []}
            empty_lbl = {"x": [], "y": [], "text": []}
            for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
                ctour.source.data = dict(empty)
                ctour.label_source.data = dict(empty_lbl)

    def _on_contour_color_toggle(self, event: param.parameterized.Event) -> None:
        self._contour_color = bool(event.new)
        self._refresh_contours()

    def _on_n_contours_change(self, event: param.parameterized.Event) -> None:
        self.n_contours = int(event.new)
        self._refresh_contours()

    def _on_contour_smooth_change(self, event: param.parameterized.Event) -> None:
        self.contour_smooth = float(event.new)
        self._refresh_contours()

    def _on_contour_levels_change(self, event: param.parameterized.Event) -> None:
        self.contour_levels_mode = event.new
        self._refresh_contours()

    def _on_contour_custom_change(self, event: param.parameterized.Event) -> None:
        self.contour_custom_levels = event.new
        auto_active = not bool(event.new.strip())
        self._n_contours_slider.disabled = not auto_active
        self._contour_levels_select.disabled = not auto_active
        self._refresh_contours()

    def _on_contour_labels_toggle(self, event: param.parameterized.Event) -> None:
        on = bool(event.new)
        for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
            ctour.label_renderer.visible = on
        if on:
            for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
                xs = ctour.source.data["xs"]
                ys = ctour.source.data["ys"]
                lvls = ctour.source.data["level"]
                if xs:
                    ctour.label_source.data = self._label_positions(xs, ys, lvls)

    def _refresh_contours(self) -> None:
        """Recompute contours for the currently-visible maps."""
        if not self._ctour_a.renderer.visible:
            return
        idx = self._time_slider.value
        ts = self._reader_a.time_index[idx]
        doc = self._fig_a.document
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _ts=ts: (
                    self._update_diff_map(_ts) if self.show_diff
                    else (self._update_map_a(_i, _ts)
                          or self._update_map_b(_i, _ts))
                )
            )
        else:
            if self.show_diff:
                self._update_diff_map(ts)
            else:
                self._update_map_a(idx, ts)
                self._update_map_b(idx, ts)

    # ------------------------------------------------------------------
    # Callbacks — transform
    # ------------------------------------------------------------------

    def _on_transform_change(self, event: param.parameterized.Event) -> None:
        """Apply a new transform to both readers, showing a loading indicator."""
        import threading

        current_ts = pd.Timestamp(
            self._reader_a.time_index[self._time_slider.value]
        )
        new_name = event.new

        # Show loading spinner on all three map panes immediately.
        for pane in (self._pane_a, self._pane_b, self._pane_diff):
            pane.loading = True
        self._transform_select.disabled = True

        doc = self._fig_a.document

        def _compute() -> None:
            """Background thread: build both readers (triggers lazy cache)."""
            new_reader_a = self._setup_reader(self._base_reader_a, new_name)
            new_reader_b = self._setup_reader(self._base_reader_b, new_name)
            # Accessing time_index forces TransformedSlicingReader._ensure_cache()
            ti = new_reader_a.time_index
            nearest_idx = max(0, min(
                int(ti.get_indexer([current_ts], method="nearest")[0]),
                len(ti) - 1,
            ))

            def _apply() -> None:
                self._reader_a = new_reader_a
                self._reader_b = new_reader_b
                self._diff_reader_cache = None

                self._syncing = True
                try:
                    self._time_slider.options = list(range(len(ti)))
                    self._time_slider.value = nearest_idx
                    self._datetime_picker.start = ti[0].to_pydatetime()
                    self._datetime_picker.end = ti[-1].to_pydatetime()
                    self._datetime_picker.value = ti[nearest_idx].to_pydatetime()
                finally:
                    self._syncing = False

                ts_str = ti[nearest_idx].strftime("%Y-%m-%d %H:%M")
                self._time_div.text = f"<b>{ts_str}</b>"
                self._apply_frame(nearest_idx, ts_str)

                for pane in (self._pane_a, self._pane_b, self._pane_diff):
                    pane.loading = False
                self._transform_select.disabled = False

            if doc is not None:
                doc.add_next_tick_callback(_apply)
            else:
                _apply()

        threading.Thread(target=_compute, daemon=True).start()

    # ------------------------------------------------------------------
    # pn.viewable.Viewer protocol
    # ------------------------------------------------------------------

    def __panel__(self) -> pn.viewable.Viewable:
        return pn.Column(
            pn.Row(
                self._controls,
                self._maps_pane,
                sizing_mode="stretch_both",
            ),
            sizing_mode="stretch_both",
            min_height=self._map_height,
        )

    def servable(self, title=None, **kwargs) -> "MultiGeoAnimatorManager":
        super().servable(title=title or "MultiGeoAnimatorManager", **kwargs)
        return self
