"""MultiGeoAnimatorManager — side-by-side geo-animation of two readers.

Provides a shared-controls panel with two Bokeh maps (one per reader) that
animate in lock-step.  A **Diff mode** checkbox replaces both maps with a
single diff map (reader A − reader B) coloured with a diverging palette.

Architecture
------------
``MultiGeoAnimatorManager`` reuses the core Bokeh plumbing from
:class:`~dvue.animator.GeoAnimatorManager` by constructing two *panels*
(independent Bokeh figures sharing a ``ColumnDataSource`` per reader) and
wiring them to a single set of time-control widgets.

The diff reader (:class:`~dvue.animator.DiffSlicingReader`) is constructed
lazily on first activation so startup is always fast.
"""

from __future__ import annotations

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
)

# Diverging colormaps suitable for A−B diff display
_DIFF_COLORMAPS = ["coolwarm", "RdBu_r", "bwr", "seismic"]


def _build_bokeh_map(
    gdf_proj: gpd.GeoDataFrame,
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
) -> tuple:
    """Build one Bokeh figure + supporting objects for a single map panel.

    Returns
    -------
    (fig, source, mapper, data_renderer, tile_renderer)
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
    return p, source, mapper, data_renderer, tile_renderer


def _extract_geo_arrays(gdf_proj: gpd.GeoDataFrame, geo_id_column: str, geom_type: str):
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


class MultiGeoAnimatorManager(pn.viewable.Viewer):
    """Side-by-side geo-animation of two :class:`~dvue.animator.SlicingReader` s.

    Displays two maps (one per reader) driven by shared time controls.
    A **Diff mode** toggle collapses both maps into a single diff map
    showing ``reader_a - reader_b`` on a diverging colormap.

    Parameters
    ----------
    reader_a, reader_b : SlicingReader
        The two readers to animate.  Time indices need not be identical;
        :class:`~dvue.animator.DiffSlicingReader` handles alignment.
    gdf_a, gdf_b : gpd.GeoDataFrame
        Channel centreline GDFs for each reader.  May be the same object.
    title_a, title_b : str, optional
        Panel titles for each map.
    geo_id_column : str, optional
        Column in the GDFs holding integer channel IDs.  Default ``"geo_id"``.
    colormap : str, optional
        Initial colormap.  Default ``"rainbow"``.
    diff_colormap : str, optional
        Colormap used in diff mode.  Default ``"coolwarm"``.
    vmin, vmax : float or None, optional
        Initial colour-scale bounds.
    size : float, optional
        Line/point size.  Default ``6.0``.
    map_height : int, optional
        Minimum map height.  Default ``500``.
    show_diff : bool, optional
        Start in diff mode.  Default ``False``.
    transform_options : dict or None, optional
        Transform labels → callables for the Transform dropdown.
    initial_transform : str, optional
        Initial transform selection.  Default ``"none"``.
    buffer_chunk_size : int, optional
        HDF5 read chunk size.  Default ``200``.
    """

    # ------------------------------------------------------------------
    # Params
    # ------------------------------------------------------------------
    vmin: Optional[float] = param.Number(default=None, allow_None=True)
    vmax: Optional[float] = param.Number(default=None, allow_None=True)
    colormap: str = param.Selector(default="rainbow", objects=CURATED_COLORMAPS)
    diff_colormap: str = param.Selector(default="coolwarm",
                                        objects=CURATED_COLORMAPS)
    size: float = param.Number(default=6.0, bounds=(1.0, 50.0))
    show_diff: bool = param.Boolean(default=False,
                                    doc="Show A-B diff map instead of side-by-side.")

    def __init__(
        self,
        reader_a: SlicingReader,
        reader_b: SlicingReader,
        gdf_a: gpd.GeoDataFrame,
        gdf_b: Optional[gpd.GeoDataFrame] = None,
        title_a: str = "Study A",
        title_b: str = "Study B",
        geo_id_column: str = "geo_id",
        colormap: str = "rainbow",
        diff_colormap: str = "coolwarm",
        vmin: Optional[float] = None,
        vmax: Optional[float] = None,
        size: float = 6.0,
        map_height: int = 500,
        show_diff: bool = False,
        transform_options: Optional[dict] = None,
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
        self._diff_reader_cache: Optional[DiffSlicingReader] = None
        self._syncing = False

        # ----------------------------------------------------------------
        # 2. Set up readers (with transform + buffer)
        # ----------------------------------------------------------------
        self._reader_a = self._setup_reader(self._base_reader_a, initial_transform)
        self._reader_b = self._setup_reader(self._base_reader_b, initial_transform)

        # Use reader_a's time index as the master
        ti = self._reader_a.time_index

        # ----------------------------------------------------------------
        # 3. Project GDFs and extract Bokeh geometry arrays
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
        # 4. Effective vmin/vmax
        # ----------------------------------------------------------------
        eff_vmin_a = float(vmin if vmin is not None else reader_a.vmin)
        eff_vmax_a = float(vmax if vmax is not None else reader_a.vmax)
        if eff_vmin_a == eff_vmax_a:
            eff_vmax_a = eff_vmin_a + 1.0

        init_a = reader_a.get_slice(ti[0])
        init_b = reader_b.get_slice_nearest(ti[0])
        init_vals_a = [float(init_a.get(gid, np.nan)) for gid in self._geo_ids_a]
        init_vals_b = [float(init_b.get(gid, np.nan)) for gid in self._geo_ids_b]

        # ----------------------------------------------------------------
        # 5. Build Bokeh figures for both maps
        # ----------------------------------------------------------------
        ts0 = ti[0].strftime("%Y-%m-%d %H:%M")
        (self._fig_a, self._src_a, self._mapper_a,
         self._data_rend_a, self._tile_rend_a) = _build_bokeh_map(
            self._gdf_a_proj, self._geo_ids_a, bk_xs_a, bk_ys_a,
            self._geom_type_a, init_vals_a,
            colormap, eff_vmin_a, eff_vmax_a, size, map_height,
            f"{title_a} — {ts0}", geo_id_column,
        )
        (self._fig_b, self._src_b, self._mapper_b,
         self._data_rend_b, self._tile_rend_b) = _build_bokeh_map(
            self._gdf_b_proj, self._geo_ids_b, bk_xs_b, bk_ys_b,
            self._geom_type_b, init_vals_b,
            colormap, eff_vmin_a, eff_vmax_a, size, map_height,
            f"{title_b} — {ts0}", geo_id_column,
        )

        # Diff map (constructed lazily but Bokeh figure built now)
        self._diff_src = ColumnDataSource({"xs": [], "ys": [], "_value": [], "geo_id": []})
        self._diff_mapper = LinearColorMapper(
            palette=_cmap_to_palette(diff_colormap),
            low=-1.0, high=1.0, nan_color="lightgrey",
        )
        (self._fig_diff, _, self._diff_mapper,
         self._diff_data_rend, self._diff_tile_rend) = _build_bokeh_map(
            self._gdf_a_proj, self._geo_ids_a, bk_xs_a, bk_ys_a,
            self._geom_type_a, [np.nan] * len(self._geo_ids_a),
            diff_colormap, -1.0, 1.0, size, map_height,
            f"{title_a} − {title_b} — {ts0}", geo_id_column,
        )
        # Override the diff source in the diff figure
        for r in self._fig_diff.renderers:
            if hasattr(r, "data_source") and r.data_source is not r.data_source:
                pass
        # Re-use _diff_src for the diff figure data renderer
        self._diff_data_rend.data_source = self._diff_src

        self._pane_a = pn.pane.Bokeh(self._fig_a, sizing_mode="stretch_both", min_height=map_height)
        self._pane_b = pn.pane.Bokeh(self._fig_b, sizing_mode="stretch_both", min_height=map_height)
        self._pane_diff = pn.pane.Bokeh(self._fig_diff, sizing_mode="stretch_both", min_height=map_height)

        # ----------------------------------------------------------------
        # 6. Shared time controls
        # ----------------------------------------------------------------
        self._time_div = Div(
            text=f"<b>{ts0}</b>",
            styles={"font-size": "13px", "margin": "2px 0 6px 0"},
        )
        self._time_label_pane = pn.pane.Bokeh(self._time_div, sizing_mode="stretch_width")
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
        # 7. Style controls
        # ----------------------------------------------------------------
        clim_val = f"{eff_vmin_a:.4g}, {eff_vmax_a:.4g}"
        self._clim_input = pn.widgets.TextInput(
            name="Color range  (min, max)", value=clim_val,
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
            name="Show diff (A − B)", value=show_diff,
            sizing_mode="stretch_width",
        )
        _transform_names = ["none"] + list(self._transform_options.keys())
        self._transform_select = pn.widgets.Select(
            name="Transform", options=_transform_names,
            value=initial_transform if initial_transform in _transform_names else "none",
            sizing_mode="stretch_width",
            visible=bool(self._transform_options),
        )
        transform_row: list = (
            [pn.pane.Markdown("**Transform**"), self._transform_select]
            if self._transform_options else []
        )

        self._controls = pn.Column(
            pn.pane.Markdown("### Controls", margin=(4, 0, 0, 0)),
            pn.pane.Markdown("**Time**"),
            self._time_label_pane,
            self._time_slider,
            self._datetime_picker,
            pn.pane.Markdown("**Colour scale**"),
            self._clim_input,
            pn.pane.Markdown("**Colormap**"),
            self._colormap_select,
            pn.pane.Markdown("**Diff**"),
            self._show_diff_check,
            self._diff_colormap_select,
            *transform_row,
            sizing_mode="stretch_width",
            max_width=260,
            margin=(4, 8, 4, 4),
        )

        # ----------------------------------------------------------------
        # 8. Layout placeholder — switched on diff toggle
        # ----------------------------------------------------------------
        self._maps_pane = pn.Row(
            self._pane_a, self._pane_b,
            sizing_mode="stretch_both",
        )

        # ----------------------------------------------------------------
        # 9. super().__init__
        # ----------------------------------------------------------------
        super().__init__(
            vmin=vmin, vmax=vmax,
            colormap=colormap, diff_colormap=diff_colormap,
            size=size, show_diff=show_diff,
            **params,
        )

        # ----------------------------------------------------------------
        # 10. Wire watchers
        # ----------------------------------------------------------------
        self._time_slider.param.watch(self._on_slider_change, "value")
        self._datetime_picker.param.watch(self._on_datetime_picker_change, "value")
        self.param.watch(self._on_style_change, ["vmin", "vmax", "colormap", "size"])
        self.param.watch(self._on_diff_colormap_change, ["diff_colormap"])
        self._clim_input.param.watch(self._on_clim_text_change, "value")
        self._colormap_select.param.watch(self._on_colormap_change, "value")
        self._diff_colormap_select.param.watch(self._on_diff_colormap_widget_change, "value")
        self._show_diff_check.param.watch(self._on_diff_toggle, "value")
        if self._transform_options:
            self._transform_select.param.watch(self._on_transform_change, "value")

    # ------------------------------------------------------------------
    # Reader setup
    # ------------------------------------------------------------------

    def _setup_reader(
        self,
        base: SlicingReader,
        transform_name: str,
    ) -> SlicingReader:
        from .reader import TransformedSlicingReader, BufferedSlicingReader
        reader = base
        if transform_name and transform_name != "none" and transform_name in self._transform_options:
            reader = TransformedSlicingReader(reader, self._transform_options[transform_name])
        return BufferedSlicingReader(reader, chunk_size=self._buffer_chunk_size)

    def _get_diff_reader(self) -> SlicingReader:
        """Return a buffered DiffSlicingReader (built once, cached)."""
        if self._diff_reader_cache is None:
            self._diff_reader_cache = DiffSlicingReader(
                self._base_reader_a, self._base_reader_b
            )
        return BufferedSlicingReader(self._diff_reader_cache, chunk_size=self._buffer_chunk_size)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _current_clim(self) -> tuple[float, float]:
        vmin = self.vmin if self.vmin is not None else self._reader_a.vmin
        vmax = self.vmax if self.vmax is not None else self._reader_a.vmax
        if vmin == vmax:
            vmax = vmin + 1.0
        return float(vmin), float(vmax)

    def _update_map_a(self, idx: int, ts: pd.Timestamp) -> None:
        series = self._reader_a.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_a).fillna(np.nan).tolist()
        self._src_a.patch({"_value": [(slice(None), vals)]})
        self._fig_a.title.text = f"{self._title_a} \u2014 {ts.strftime('%Y-%m-%d %H:%M')}"

    def _update_map_b(self, idx: int, ts: pd.Timestamp) -> None:
        series = self._reader_b.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_b).fillna(np.nan).tolist()
        self._src_b.patch({"_value": [(slice(None), vals)]})
        self._fig_b.title.text = f"{self._title_b} \u2014 {ts.strftime('%Y-%m-%d %H:%M')}"

    def _update_diff_map(self, ts: pd.Timestamp) -> None:
        diff_r = self._get_diff_reader()
        series = diff_r.get_slice_nearest(ts)
        vals = series.reindex(self._geo_ids_a).fillna(np.nan).tolist()
        self._diff_src.data = {
            "xs": self._src_a.data["xs"],
            "ys": self._src_a.data["ys"],
            "_value": vals,
            "geo_id": self._geo_ids_a,
        }
        ts_str = ts.strftime("%Y-%m-%d %H:%M")
        self._fig_diff.title.text = f"{self._title_a} \u2212 {self._title_b} \u2014 {ts_str}"
        # Auto-scale diff mapper symmetrically
        d = self._diff_reader_cache
        if d is not None:
            absmax = max(abs(d.vmin), abs(d.vmax), 1e-9)
            self._diff_mapper.low = -absmax
            self._diff_mapper.high = absmax

    def _apply_frame(self, idx: int, ts_str: str) -> None:
        """All Bokeh mutations for a single frame step — must run under document lock."""
        self._time_div.text = f"<b>{ts_str}</b>"
        ts = self._reader_a.time_index[idx]
        if self.show_diff:
            self._update_diff_map(ts)
        else:
            self._update_map_a(idx, ts)
            self._update_map_b(idx, ts)

    # ------------------------------------------------------------------
    # Widget callbacks
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
            doc.add_next_tick_callback(lambda _i=idx, _s=ts_str: self._apply_frame(_i, _s))
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
            doc.add_next_tick_callback(lambda _i=idx, _s=ts_str: self._apply_frame(_i, _s))
        else:
            self._apply_frame(idx, ts_str)

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

    def _on_diff_colormap_change(self, *events) -> None:
        doc = self._fig_diff.document
        if doc is not None:
            doc.add_next_tick_callback(lambda: setattr(
                self._diff_mapper, "palette", _cmap_to_palette(self.diff_colormap)
            ))
        else:
            self._diff_mapper.palette = _cmap_to_palette(self.diff_colormap)

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

    def _on_diff_toggle(self, event: param.parameterized.Event) -> None:
        """Switch between side-by-side and diff mode."""
        self.show_diff = bool(event.new)
        self._diff_colormap_select.visible = self.show_diff
        if self.show_diff:
            self._maps_pane.objects = [self._pane_diff]
            # Populate diff map at current frame
            idx = self._time_slider.value
            ts = self._reader_a.time_index[idx]
            doc = self._fig_diff.document
            if doc is not None:
                doc.add_next_tick_callback(lambda _ts=ts: self._update_diff_map(_ts))
            else:
                self._update_diff_map(ts)
        else:
            self._maps_pane.objects = [self._pane_a, self._pane_b]

    def _on_transform_change(self, event: param.parameterized.Event) -> None:
        """Apply a new transform to both readers, preserving position."""
        current_ts = pd.Timestamp(
            self._reader_a.time_index[self._time_slider.value]
        )
        self._reader_a = self._setup_reader(self._base_reader_a, event.new)
        self._reader_b = self._setup_reader(self._base_reader_b, event.new)
        # Invalidate diff cache so it is rebuilt with new readers
        self._diff_reader_cache = None

        ti = self._reader_a.time_index
        nearest_idx = max(0, min(
            int(ti.get_indexer([current_ts], method="nearest")[0]),
            len(ti) - 1,
        ))
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

    def servable(self, title: Optional[str] = None, **kwargs) -> "MultiGeoAnimatorManager":
        super().servable(title=title or "MultiGeoAnimatorManager", **kwargs)
        return self
