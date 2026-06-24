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
    CURATED_COLORMAPS_WITH_SEP,
    _COLORMAP_SEPARATOR,
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
    TransformSpec,
    StreamingTransformedSlicingReader,
    TransformedSlicingReader,
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
        "_value": init_values, "_color_value": list(init_values), "geo_id": geo_ids,
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

    color_field = {"field": "_color_value", "transform": mapper}
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
        colorbar=colorbar,
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
        cx_a, cy_a, gx_a, gy_a, cz_a, _cell_size_a = _make_contour_grid(
            self._gdf_a_proj, self._geom_type_a, buffer_m=4000.0)
        cx_b, cy_b, gx_b, gy_b, cz_b, _cell_size_b = _make_contour_grid(
            self._gdf_b_proj, self._geom_type_b, buffer_m=4000.0)
        _default_clip_km = 4.0

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
            colormap, eff_vmin, eff_vmax, size, map_height,
            f"{title_a} \u2212 {title_b} \u2014 {ts0}", geo_id_column,
            shared_x_range=shared_x, shared_y_range=shared_y,
        )

        self._fig_a = ma.fig;  self._src_a = ma.source;  self._mapper_a = ma.mapper
        self._fig_b = mb.fig;  self._src_b = mb.source;  self._mapper_b = mb.mapper
        self._colorbar_a = ma.colorbar
        self._colorbar_b = mb.colorbar
        self._colorbar_a = ma.colorbar
        self._colorbar_b = mb.colorbar
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
            name="Colormap", options=CURATED_COLORMAPS_WITH_SEP, value=colormap,
            sizing_mode="stretch_width",
        )
        self._diff_colormap_select = pn.widgets.Select(
            name="Diff colormap", options=CURATED_COLORMAPS_WITH_SEP, value="coolwarm",
            sizing_mode="stretch_width", visible=False,
        )
        self._size_slider = pn.widgets.FloatSlider(
            name="Line width",
            start=1.0, end=50.0, step=0.5, value=size,
            sizing_mode="stretch_width",
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
        self._contour_label_spacing_slider = pn.widgets.IntSlider(
            name="Label spacing", start=5, end=200, value=150, step=5,
            sizing_mode="stretch_width", visible=False,
        )
        self._contour_clip_slider = pn.widgets.FloatSlider(
            name="Contour clip radius (km)", start=0.5, end=15.0,
            value=_default_clip_km, step=0.5,
            sizing_mode="stretch_width", visible=False,
        )
        # Show / hide toggles for channels and basemap (all three figures).
        # Channel and basemap opacity sliders (0 = invisible, 100 = fully opaque)
        self._channels_alpha_slider = pn.widgets.IntSlider(
            name="Channel lines opacity", start=0, end=100, value=100, step=5,
            sizing_mode="stretch_width",
        )
        self._basemap_alpha_slider = pn.widgets.IntSlider(
            name="Background map opacity", start=0, end=100, value=100, step=5,
            sizing_mode="stretch_width",
        )

        # ----------------------------------------------------------------
        # 13. Controls column — grouped into collapsible pn.Card sections
        # ----------------------------------------------------------------
        _appearance_card = pn.Card(
            self._clim_input,
            self._colormap_select,
            self._size_slider,
            self._channels_alpha_slider,
            self._basemap_alpha_slider,
            title="Appearance", collapsed=False,
            sizing_mode="stretch_width",
        )
        _diff_card = pn.Card(
            self._show_diff_check,
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
            _diff_card,
            _contour_card,
            *_optional_cards,
            _save_card,
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
            colormap=colormap,
            size=size, show_diff=show_diff,
            **params,
        )

        # ----------------------------------------------------------------
        # 16. Wire watchers
        # ----------------------------------------------------------------
        self._time_slider.param.watch(self._on_slider_change, "value")
        self._datetime_picker.param.watch(self._on_datetime_picker_change, "value")
        self.param.watch(self._on_style_change, ["vmin", "vmax", "colormap", "size"])
        self._clim_input.param.watch(self._on_clim_text_change, "value")
        self._colormap_select.param.watch(self._on_colormap_change, "value")
        self._size_slider.param.watch(self._on_size_widget_change, "value")
        self._show_diff_check.param.watch(self._on_diff_toggle, "value")
        self._contours_check.param.watch(self._on_contours_toggle, "value")
        self._contour_color_check.param.watch(self._on_contour_color_toggle, "value")
        self._n_contours_slider.param.watch(self._on_n_contours_change, "value")
        self._contour_smooth_slider.param.watch(self._on_contour_smooth_change, "value")
        self._contour_levels_select.param.watch(self._on_contour_levels_change, "value")
        self._contour_custom_input.param.watch(self._on_contour_custom_change, "value")
        self._contour_labels_check.param.watch(self._on_contour_labels_toggle, "value")
        self._contour_label_spacing_slider.param.watch(self._on_label_spacing_change, "value")
        self._contour_clip_slider.param.watch(self._on_contour_clip_change, "value")
        self._channels_alpha_slider.param.watch(self._on_channels_alpha_change, "value")
        self._basemap_alpha_slider.param.watch(self._on_basemap_alpha_change, "value")
        if self._transform_options:
            self._transform_select.param.watch(self._on_transform_change, "value")
        self._save_config_btn.on_click(self._on_save_config)

        self._color_norm_boundaries: "list[float] | None" = None  # unused placeholder

    # ------------------------------------------------------------------
    # Reader setup
    # ------------------------------------------------------------------

    def collect_state(self) -> dict:
        """Return a complete dict representing the current UI state + metadata."""
        meta = self._animate_meta
        cli_keys = meta.get("_transform_cli_keys", {})
        transform_display = (
            self._transform_select.value
            if self._transform_options
            else "none"
        )
        state: dict = {
            "version": 1,
            "mode": meta.get("mode", "multi"),
            "files": meta.get("files", []),
            "file_type": meta.get("file_type", "hydro"),
            "variable": meta.get("variable", "flow"),
            "location": meta.get("location", "both"),
            "shapefile": meta.get("shapefile"),
            "shapefile_b": meta.get("shapefile_b"),
            "channel_id_column": meta.get("channel_id_column"),
            "transform": cli_keys.get(transform_display, "none"),
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
            "diff": {
                "show": self.show_diff,
            },
            "x2": {"enabled": False, "threshold": 2700.0},
        }
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

    def _active_doc(self):
        """Return the live Bokeh document, whichever figure is currently displayed.

        When diff mode is on, ``pane_a`` and ``pane_b`` are detached from the
        layout, so ``fig_a.document`` / ``fig_b.document`` return ``None``.
        When side-by-side mode is on, ``pane_diff`` is detached.
        Always try the figure that should be visible first, then fall back to
        the others so we never return ``None`` while a document exists.
        """
        candidates = (
            (self._fig_diff, self._fig_a, self._fig_b)
            if self.show_diff
            else (self._fig_a, self._fig_b, self._fig_diff)
        )
        for fig in candidates:
            doc = fig.document
            if doc is not None:
                return doc
        return None

    def _setup_reader(self, base: SlicingReader, transform_name: str) -> SlicingReader:
        reader = base
        if (transform_name and transform_name != "none"
                and transform_name in self._transform_options):
            spec_or_fn = self._transform_options[transform_name]
            if isinstance(spec_or_fn, TransformSpec):
                reader = StreamingTransformedSlicingReader(reader, spec_or_fn)
            else:
                from .reader import TransformedSlicingReader as _TFR
                reader = _TFR(reader, spec_or_fn)
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
        """Label every contour segment, repeating along long segments.

        Every segment of \u2265 2 points gets a label at its midpoint.  Segments
        longer than *label_spacing* points also get additional evenly-spaced
        labels so that a label stays visible across a wide range of zoom levels.
        The Label spacing slider controls how far apart the repeated labels are.
        """
        spacing = self._contour_label_spacing_slider.value
        n_dec = _nice_decimal_places(sorted(set(lvls)))
        lx, ly, lt = [], [], []
        for xs, ys, lvl in zip(xs_list, ys_list, lvls):
            n = len(xs)
            if n < 2:
                continue
            text = _format_level(lvl, n_dec)
            if n <= spacing:
                # Short segment: one label at the midpoint
                lx.append(xs[n // 2])
                ly.append(ys[n // 2])
                lt.append(text)
            else:
                # Long segment: distribute labels every `spacing` points
                n_labels = max(1, n // spacing)
                step = n // (n_labels + 1)
                for k in range(1, n_labels + 1):
                    idx = min(k * step, n - 1)
                    lx.append(xs[idx])
                    ly.append(ys[idx])
                    lt.append(text)
        return {"x": lx, "y": ly, "text": lt}

    # ------------------------------------------------------------------
    # Frame update
    # ------------------------------------------------------------------

    def _update_map_a(self, idx: int, ts: pd.Timestamp) -> None:
        series = self._reader_a.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_a).fillna(np.nan).tolist()
        cvals = vals
        self._src_a.patch({
            "_value": [(slice(None), vals)],
            "_color_value": [(slice(None), cvals)],
        })
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
        cvals = vals
        self._src_b.patch({
            "_value": [(slice(None), vals)],
            "_color_value": [(slice(None), cvals)],
        })
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
            "_color_value": vals,
            "geo_id": self._geo_ids_a,
        }
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        self._fig_diff.title.text = (
            f"{self._title_a} \u2212 {self._title_b} \u2014 {ts_str}"
        )
        eff_vmin, eff_vmax = self._current_clim()
        self._mapper_diff.low  = eff_vmin
        self._mapper_diff.high = eff_vmax
        if self._ctour_diff.renderer.visible:
            self._recompute_contours(
                self._ctour_diff, vals, eff_vmin, eff_vmax, self.colormap)

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
        doc = self._active_doc()
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
        doc = self._active_doc()
        if doc is not None:
            doc.add_next_tick_callback(
                lambda _i=idx, _s=ts_str: self._apply_frame(_i, _s))
        else:
            self._apply_frame(idx, ts_str)

    # ------------------------------------------------------------------
    # Callbacks — style
    # ------------------------------------------------------------------

    def _on_style_change(self, *events) -> None:
        doc = self._active_doc()
        if doc is not None:
            doc.add_next_tick_callback(self._apply_bokeh_style)
        else:
            self._apply_bokeh_style()

    def _on_size_widget_change(self, event: param.parameterized.Event) -> None:
        self.size = float(event.new)

    def _apply_bokeh_style(self) -> None:
        eff_vmin, eff_vmax = self._current_clim()
        from bokeh.models import BasicTicker, BasicTickFormatter
        pal = _cmap_to_palette(self.colormap)
        for mapper in (self._mapper_a, self._mapper_b, self._mapper_diff):
            mapper.palette = pal
            mapper.low  = eff_vmin
            mapper.high = eff_vmax
        for cb in (self._colorbar_a, self._colorbar_b):
            cb.ticker    = BasicTicker(desired_num_ticks=6)
            cb.formatter = BasicTickFormatter()
        # Apply line width to all three figures' data renderers.
        new_size = float(self.size)
        _skip = {
            id(self._ctour_a.renderer), id(self._ctour_b.renderer),
            id(self._ctour_diff.renderer),
        }
        for fig in (self._fig_a, self._fig_b, self._fig_diff):
            for r in fig.renderers:
                if id(r) in _skip or not hasattr(r, "glyph"):
                    continue
                g = r.glyph
                if hasattr(g, "size"):
                    g.size = new_size
                elif hasattr(g, "line_width") and not hasattr(g, "fill_color"):
                    g.line_width = new_size
        # If contours are visible, refresh them (colours may have changed).
        if self._ctour_a.renderer.visible:
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            self._update_map_a(idx, ts)
            self._update_map_b(idx, ts)

    def _on_diff_colormap_change(self, *events) -> None:
        pass  # diff mapper driven by Appearance colormap via _apply_bokeh_style

    def _on_colormap_change(self, event: param.parameterized.Event) -> None:
        if event.new in CURATED_COLORMAPS:   # ignore separator clicks
            self.colormap = event.new

    def _on_clim_text_change(self, event: param.parameterized.Event) -> None:
        try:
            parts = [float(p.strip()) for p in event.new.split(",") if p.strip()]
        except ValueError:
            return
        if len(parts) == 2:
            self._color_norm_boundaries = None
            self.vmin, self.vmax = parts[0], parts[1]
        elif len(parts) > 2:
            # More than 2 values: treat as vmin / vmax using first and last
            self._color_norm_boundaries = None
            self.vmin = min(parts)
            self.vmax = max(parts)
            self._on_style_change()

    def _on_channels_alpha_change(self, event: param.parameterized.Event) -> None:
        """Apply channel line opacity (0–100) to all three map renderers."""
        alpha = event.new / 100.0

        def _apply():
            for r in self._all_data_renderers:
                try:
                    r.glyph.line_alpha = alpha
                except AttributeError:
                    r.visible = (alpha > 0)

        doc = self._active_doc()
        if doc is not None:
            doc.add_next_tick_callback(_apply)
        else:
            _apply()

    def _on_basemap_alpha_change(self, event: param.parameterized.Event) -> None:
        """Apply background map opacity (0–100) to all three tile renderers."""
        alpha = event.new / 100.0

        def _apply():
            for r in self._all_tile_renderers:
                r.alpha = alpha

        doc = self._active_doc()
        if doc is not None:
            doc.add_next_tick_callback(_apply)
        else:
            _apply()

    # ------------------------------------------------------------------
    # Callbacks — diff toggle
    # ------------------------------------------------------------------

    def _on_diff_toggle(self, event: param.parameterized.Event) -> None:
        self.show_diff = bool(event.new)
        if self.show_diff:
            self._maps_pane.objects = [self._pane_diff]
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            doc = self._active_doc()
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
        self._contour_label_spacing_slider.visible = on
        self._contour_clip_slider.visible = on
        for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
            ctour.label_renderer.visible = on
        if on:
            for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
                xs = ctour.source.data["xs"]
                ys = ctour.source.data["ys"]
                lvls = ctour.source.data["level"]
                if xs:
                    ctour.label_source.data = self._label_positions(xs, ys, lvls)

    def _on_label_spacing_change(self, event: param.parameterized.Event) -> None:
        """Re-place contour labels with the new spacing value."""
        if not self._contour_labels_check.value:
            return
        for ctour in (self._ctour_a, self._ctour_b, self._ctour_diff):
            xs   = ctour.source.data.get("xs", [])
            ys   = ctour.source.data.get("ys", [])
            lvls = ctour.source.data.get("level", [])
            if xs:
                ctour.label_source.data = self._label_positions(xs, ys, lvls)

    def _on_contour_clip_change(self, event: param.parameterized.Event) -> None:
        """Rebuild contour clip zones with the new radius and recompute."""
        buffer_m = event.new * 1000.0
        try:
            from shapely.ops import unary_union
            cz_a = unary_union(self._gdf_a_proj.geometry).buffer(buffer_m)
            cz_b = unary_union(self._gdf_b_proj.geometry).buffer(buffer_m)
            self._ctour_a.clip_zone    = cz_a
            self._ctour_b.clip_zone    = cz_b
            self._ctour_diff.clip_zone = cz_a   # diff uses map-A geometry
        except Exception:
            pass
        self._refresh_contours()

    def _refresh_contours(self) -> None:
        """Recompute contours for the currently-visible maps."""
        if not self._ctour_a.renderer.visible:
            return
        idx = self._time_slider.value
        ts = self._reader_a.time_index[idx]
        doc = self._active_doc()
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

        doc = self._active_doc()

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
