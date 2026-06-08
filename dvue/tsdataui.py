from .utils import get_unique_short_names
from .dataui import DataUIManager, full_stack
from .actions import PlotAction
from datetime import datetime, timedelta
import logging
import warnings
from functools import lru_cache
import os

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

import pandas as pd

# viz and ui
import holoviews as hv
from holoviews import opts

hv.extension("bokeh")
import param
import panel as pn
import colorcet as cc
from holoviews.plotting.util import process_cmap

pn.extension("tabulator", notifications=True, design="native")
#
LINE_DASH_MAP = ["solid", "dashed", "dotted", "dotdash", "dashdot"]
#
try:
    from vtools.functions.filter import cosine_lanczos #IMPROVEMENT NEEDED
    _VTOOLS_AVAILABLE = True
except ImportError:
    _VTOOLS_AVAILABLE = False


def unique_preserve_order(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]


import re as _re


def _sanitize_vdim(label: str) -> str:
    """Return a Bokeh-safe dimension name from a curve label.

    Replaces every run of non-alphanumeric characters with a single
    underscore so the name is valid as a Bokeh ColumnDataSource column.
    """
    s = _re.sub(r'[^A-Za-z0-9]+', '_', str(label)).strip('_')
    if not s:
        return "value"
    if s[0].isdigit():
        s = f"v_{s}"
    return s

def get_color_dataframe(stations, color_cycle=hv.Cycle()):
    """
    Create a dataframe with station names and colors
    """
    cc = color_cycle.values
    # extend cc to the size of stations
    while len(cc) < len(stations):
        cc = cc + cc
    dfc = pd.DataFrame({"stations": stations, "color": cc[: len(stations)]})
    dfc.set_index("stations", inplace=True)
    return dfc


def get_colors(stations, dfc):
    """
    Create a dictionary with station names and colors
    """
    return hv.Cycle(list(dfc.loc[stations].values.flatten()))


@lru_cache
def get_categorical_color_maps():
    cmaps = hv.plotting.util.list_cmaps(records=True, category="Categorical", reverse=False)
    cmaps = {c.name + "." + c.provider: c for c in cmaps}
    return cmaps


class TimeSeriesDataUIManager(DataUIManager):
    time_range = param.CalendarDateRange(
        default=None,
        doc="Time window for data. If None, all data is displayed. Format: (start, end)",
    )
    show_legend = param.Boolean(default=True, doc="Show legend")
    show_gridlines = param.Boolean(default=True, doc="Show gridlines on plot")
    legend_position = param.Selector(
        objects=["top_right", "top_left", "bottom_right", "bottom_left"],
        default="top_right",
        doc="Legend position",
    )
    fill_gap = param.Integer(
        default=0, doc="Fill gaps in data upto this limit, only when a positive integer"
    )
    # --- Time shift ---
    time_shift = param.String(
        default="",
        doc="Shift the time index by a pandas offset string (e.g. '-1H', '15min', '1D'). "
            "Positive values shift forward; negative values shift backward. Empty = disabled.",
    )
    # --- Clip / screen ---
    clip_lower = param.Number(
        default=None, allow_None=True,
        doc="Clip values below this threshold (screen outliers). None = no lower bound.",
    )
    clip_upper = param.Number(
        default=None, allow_None=True,
        doc="Clip values above this threshold (screen outliers). None = no upper bound.",
    )
    do_tidal_filter = param.Boolean(default=False, doc="Apply tidal filter", constant=not _VTOOLS_AVAILABLE)
    tidal_filter_period = param.String(
        default="40H",
        doc="Cutoff period for the cosine-Lanczos tidal filter (e.g. '40H', '36H'). "
            "Requires sub-daily input data.",
    )
    # --- Resampling ---
    resample_period = param.String(
        default="",
        doc="Resample period string (e.g. '1D', '1H', '15min'). Empty = disabled.",
    )
    resample_agg = param.Selector(
        default="mean",
        objects=["mean", "max", "min", "sum", "std"],
        doc="Aggregation method when resampling.",
    )
    resample_fill = param.Selector(
        default="",
        objects=["", "ffill", "bfill", "interpolate"],
        doc="Fill method applied after resampling (useful for upsampling). "
            "Empty = no fill (NaN left in place).",
    )
    # --- Rolling window ---
    rolling_window = param.String(
        default="",
        doc="Rolling window size (e.g. '24H', '7D'). Empty = disabled.",
    )
    rolling_agg = param.Selector(
        default="mean",
        objects=["mean", "max", "min", "std", "median"],
        doc="Aggregation method for rolling window.",
    )
    rolling_min_periods = param.Integer(
        default=1, bounds=(1, None),
        doc="Minimum number of non-NaN observations required in the rolling window to "
            "produce a non-NaN result. Default 1 keeps edge values.",
    )
    # --- Differencing ---
    do_diff = param.Boolean(default=False, doc="Apply first-difference (period-over-period change).")
    diff_periods = param.Integer(
        default=1, bounds=(1, None), doc="Number of lag periods for differencing."
    )
    # --- Cumulative sum ---
    do_cumsum = param.Boolean(default=False, doc="Apply cumulative sum.")
    # --- Scale factor ---
    scale_factor = param.Number(
        default=1.0, doc="Multiply all values by this factor. 1.0 = no scaling."
    )
    # --- Offset (add constant) ---
    offset_value = param.Number(
        default=0.0,
        doc="Add this constant to all values after scaling (e.g. datum shift, bias correction). "
            "0.0 = disabled.",
    )
    irregular_curve_connection = param.Selector(
        objects=["steps-post", "steps-pre", "steps-mid", "linear"],
        default="steps-post",
        doc="Curve connection method for irregular data",
    )
    regular_curve_connection = param.Selector(
        objects=["linear", "steps-pre", "steps-post", "steps-mid"],
        default="linear",
        doc="Curve connection method for regular period type data",
    )
    period_curve_connection = param.Selector(
        objects=["steps-post", "steps-pre", "steps-mid", "linear"],
        default="steps-post",
        doc="Curve connection method for PeriodIndex (period-typed) data",
    )
    sensible_range_yaxis = param.Boolean(
        default=False,
        doc="Sensible range (in percentile) or auto range for y axis",
    )
    sensible_percentile_range = param.Range(
        default=(0.01, 0.99), bounds=(0, 1), step=0.01, doc="Percentile range"
    )
    color_cycle_name = param.Selector(
        objects=list(get_categorical_color_maps().keys()),
        default="glasbey_dark.colorcet",
        doc="Color cycle name",
    )
    plot_group_by_column = param.Selector(
        default=None,
        objects=[],
        doc="Column to group plots by. When None, curves are grouped by unit.",
    )
    shared_axes = param.Boolean(default=True, doc="Share axes across plots")
    marker_cycle_column = param.Selector(
        default=None, objects=[], doc="Column to use for marker cycle"
    )
    dashed_line_cycle_column = param.Selector(
        default=None, objects=[], doc="Column to use for dashed line cycle"
    )
    color_cycle_column = param.Selector(
        default=None, objects=[], doc="Column to use for color cycle"
    )
    show_math_ref_editor = param.Boolean(
        default=True,
        doc="Show the Math Ref editor button in the action bar. Set to False to hide it.",
    )
    show_transform_to_catalog = param.Boolean(
        default=True,
        doc="Show the 'Transform → Ref' button in the action bar. Set to False to hide it.",
    )
    show_source_compare = param.Boolean(
        default=False,
        doc=(
            "Show the 'Source Compare' action.  When True, the 'Transform → Ref' and "
            "'Source Compare' actions are merged into a single 'Add to Catalog' MenuButton."
        ),
    )
    show_clear_cache = param.Boolean(
        default=True,
        doc="Show the 'Clear Cache' button in the action bar and transform panel. Set to False to hide it.",
    )

    def __init__(self, **params):
        self._cached_catalog = None
        # Populate _cached_catalog for subclasses that don't expose data_catalog.
        # Managers with a data_catalog property always rebuild fresh in get_data_catalog().
        if self.data_catalog is None:
            self._cached_catalog = self.get_data_catalog()
        super().__init__(**params)

        self.change_color_cycle()

        # Only derive a default time range when the caller did not provide one.
        if self.time_range is None:
            self.time_range = self.get_time_range(self.get_data_catalog())

        table_columns = list(self.get_table_columns())
        # Add blank (None) option at the start
        columns_with_blank = [None] + table_columns
        self.param.marker_cycle_column.objects = columns_with_blank
        self.param.dashed_line_cycle_column.objects = columns_with_blank
        self.param.color_cycle_column.objects = columns_with_blank
        self.param.plot_group_by_column.objects = columns_with_blank

    def get_data_catalog(self):
        # When a live DataCatalog is available, always rebuild from it so that
        # mutations (e.g. catalog.add() in the math-ref editor) are immediately
        # visible without requiring a cache invalidation step.
        if self.data_catalog is not None:
            return super().get_data_catalog()
        # Legacy path: subclasses that override get_data_catalog() themselves
        # (no data_catalog property) store the result in _cached_catalog.
        if hasattr(self, '_cached_catalog') and self._cached_catalog is not None:
            return self._cached_catalog
        raise NotImplementedError("Method get_data_catalog not implemented")

    def _make_plot_action(self):
        """Factory that returns the :class:`TimeSeriesPlotAction` for this manager.

        Override to inject a customised :class:`TimeSeriesPlotAction` subclass
        without touching :meth:`get_data_actions` or :meth:`create_panel`.
        """
        return TimeSeriesPlotAction()

    # ------------------------------------------------------------------
    # Math reference helpers — available to all subclasses
    # ------------------------------------------------------------------

    def _has_math_refs(self) -> bool:
        """Return True if the backing catalog contains at least one math reference.

        Uses ``self.data_catalog`` when available (O(1) via ``_math_ref_count``
        maintained by :class:`DataCatalog`).  Falls back to inspecting the
        ``ref_type`` column of the cached catalog DataFrame.
        """
        cat = getattr(self, "data_catalog", None)
        if cat is not None:
            return getattr(cat, "_math_ref_count", 0) > 0
        # Fallback: check cached DataFrame
        cached = getattr(self, "_cached_catalog", None)
        if cached is not None and "ref_type" in cached.columns:
            return (cached["ref_type"] == "math").any()
        return False

    def _enrich_catalog_with_math_ref_hints(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Fill blank ``expression`` cells for raw refs with their catalog key name.

        When the catalog contains any math references, ``to_dataframe()``
        includes an ``expression`` column.  For raw :class:`DataReference` rows
        the column is empty/NaN.  This helper fills those blanks with the ref's
        catalog key so users can see exactly which token to use in new
        expressions.

        Call this inside ``get_data_catalog()`` after calling
        ``self._cat.to_dataframe()``:

        .. code-block:: python

            def get_data_catalog(self):
                df = self._cat.to_dataframe().reset_index()
                return self._enrich_catalog_with_math_ref_hints(df)

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame produced by :meth:`~dvue.catalog.DataCatalog.to_dataframe`
            after ``reset_index()`` (so ``"name"`` is a regular column).

        Returns
        -------
        pd.DataFrame
            The same DataFrame with blank ``expression`` cells populated.
        """
        if "expression" not in df.columns:
            return df
        mask = df["expression"].isna() | (df["expression"].astype(str).str.strip() == "")
        if "name" in df.columns:
            df.loc[mask, "expression"] = df.loc[mask, "name"]
        return df

    def get_data_actions(self):
        """Return default actions, replacing PlotAction with TimeSeriesPlotAction and
        optionally appending MathRefEditorAction when show_math_ref_editor is True."""
        actions = super().get_data_actions()
        # Upgrade the generic PlotAction to TimeSeriesPlotAction
        for action in actions:
            if action.get("name") == "Plot":
                action["callback"] = self._make_plot_action().callback
                break
        if self.show_math_ref_editor:
            from .math_ref_editor import MathRefEditorAction
            math_action = MathRefEditorAction()
            actions.append(dict(
                name="Math Ref",
                button_type="warning",
                icon="math-function",
                action_type="display",
                callback=math_action.callback,
            ))
        from .actions import AddSourceFilesAction
        actions.append(dict(
            name="Add Files",
            button_type="light",
            icon="folder-plus",
            action_type="display",
            callback=AddSourceFilesAction().callback,
        ))
        from .actions import DescriptiveStatsAction
        actions.append(dict(
            name="Stats",
            button_type="light",
            icon="chart-bar",
            action_type="display",
            callback=DescriptiveStatsAction().callback,
        ))
        if self.show_clear_cache:
            from .actions import ClearCacheAction
            actions.append(dict(
                name="Clear Cache",
                button_type="light",
                icon="trash",
                action_type="inline",
                callback=ClearCacheAction().callback,
            ))
        return actions

    def add_source_files(self, *paths: str) -> list:
        """Incrementally add source files to the live catalog.

        Override in subclasses to support specific file types.  The base
        implementation is a no-op that returns an empty list.

        Parameters
        ----------
        *paths : str
            Absolute OS paths of files to add.

        Returns
        -------
        list[str]
            Paths that were successfully added (for status feedback).
        """
        return []

    def get_time_range(self, dfcat):
        raise NotImplementedError("Method get_time_range not implemented")

    def get_table_filters(self):
        """Return filter specs for table columns.

        The default proxies to ``get_table_schema()["filters"]``.
        Legacy subclasses that override this method directly still work;
        subclasses that override ``get_table_schema()`` get filters from there.
        """
        return dict(self.get_table_schema().get("filters", {}))

    def _get_table_column_width_map(self) -> dict:
        """Hook for subclasses to declare column widths.

        The default ``get_table_schema()`` implementation calls this method
        to seed the ``column_widths`` in the schema.  Subclasses that override
        ``get_table_schema()`` directly do not need to implement this hook.
        Subclasses that only want to declare widths without overriding the full
        schema may override this method instead.

        Returns an empty dict by default (no widths declared).
        """
        return {}

    def is_irregular(self, r):
        raise NotImplementedError("Method is_irregular not implemented")

    def get_data_for_time_range(self, r, time_range):
        raise NotImplementedError("Method get_data_for_time_range not implemented")

    def get_tooltips(self):
        raise NotImplementedError("Method get_tooltips not implemented")

    # methods below if geolocation data is available

    def get_map_color_columns(self):
        """return the columns that can be used to color the map"""
        pass

    def get_name_to_color(self):
        """return a dictionary mapping column names to color names"""
        return hv.Cycle("Category10").values

    def get_map_marker_columns(self):
        """return the columns that can be used to color the map"""
        pass

    def get_name_to_marker(self):
        """return a dictionary mapping column names to marker names"""
        # from bokeh.core.enums import MarkerType
        # list(MarkerType) -> ['asterisk', 'circle', 'circle_cross', 'circle_dot', 'circle_x', 'circle_y', 'cross', 'dash', 'diamond', 'diamond_cross', 'diamond_dot', 'dot', 'hex', 'hex_dot', 'inverted_triangle', 'plus', 'square', 'square_cross', 'square_dot', 'square_pin', 'square_x', 'star', 'star_dot', 'triangle', 'triangle_dot', 'triangle_pin', 'x', 'y']
        return [
            "circle",
            "triangle",
            "square",
            "diamond",
            "cross",
            "x",
            "star",
            "plus",
            "dot",
            "hex",
            "inverted_triangle",
            "asterisk",
            "circle_cross",
            "square_cross",
            "diamond_cross",
            "circle_dot",
            "square_dot",
            "diamond_dot",
            "star_dot",
            "hex_dot",
            "triangle_dot",
            "circle_x",
            "square_x",
            "circle_y",
            "y",
            "dash",
            "square_pin",
            "triangle_pin",
        ]

    @param.depends("color_cycle_name", watch=True)
    def change_color_cycle(self):
        cmapinfo = get_categorical_color_maps()[self.color_cycle_name]
        color_list = unique_preserve_order(process_cmap(cmapinfo.name, provider=cmapinfo.provider))
        self.color_cycle = hv.Cycle(color_list)

    def get_widgets(self):
        _M = (1, 3, 1, 0)
        # DatetimeRangeInput.from_param requires a concrete (start, end) tuple;
        # it fails when time_range is None (e.g. empty catalog at startup).
        # Set a sensible default so the widget can initialize; the user or
        # add_source_files will update it once real data is loaded.
        if self.time_range is None:
            _today = pd.Timestamp.now().normalize()
            self.time_range = (_today - pd.DateOffset(years=2), _today)
        time_range_w = pn.widgets.DatetimeRangeInput.from_param(
            self.param.time_range,
            name="Time range",
            format="%Y-%m-%d %H:%M",
            sizing_mode="stretch_width",
            margin=_M,
        )
        control_widgets = pn.Column(
            pn.pane.HTML(
                "<div style='font-size:11px;color:#666;margin:4px 0 2px 4px'>"
                "Select the time range of data to display:</div>",
                margin=(0, 0, 0, 0),
            ),
            time_range_w,
            sizing_mode="stretch_width",
            margin=(4, 8, 4, 4),
        )
        plot_widgets = pn.Column(
            pn.WidgetBox(
                self.param.show_legend,
                self.param.show_gridlines,
                self.param.legend_position,
            ),
            pn.WidgetBox(
                self.param.irregular_curve_connection,
                self.param.regular_curve_connection,
                self.param.period_curve_connection,
            ),
            pn.WidgetBox(
                pn.pane.Markdown("**Group and Style Options:**"),
                self.param.plot_group_by_column,  # Option for grouping plots
                self.param.color_cycle_column,  # Group related options together
                self.param.dashed_line_cycle_column,
                self.param.marker_cycle_column,
            ),
            self.param.color_cycle_name,
            self.param.shared_axes,  # Add checkbox for shared_axes
        )
        def _clear_cache_cb(event):
            catalog = self.data_catalog
            if catalog is not None:
                catalog.invalidate_all_caches()
                if pn.state.notifications is not None:
                    pn.state.notifications.success(
                        "Data cache cleared — next plot will reload from source.",
                        duration=4000,
                    )
            else:
                if pn.state.notifications is not None:
                    pn.state.notifications.warning(
                        "No catalog attached — nothing to clear.", duration=3000
                    )

        clear_cache_btn = pn.widgets.Button(
            name="Clear Cache", button_type="light", icon="trash", sizing_mode="stretch_width",
        )
        clear_cache_btn.on_click(_clear_cache_cb)

        # ── Direct widget creation (no pn.Param wrapper, eliminates extra padding) ──
        _M = (1, 3, 1, 0)  # tight uniform margin for all widgets
        fill_gap_w = pn.widgets.IntInput.from_param(
            self.param.fill_gap, name="", width=60, margin=_M)
        time_shift_w = pn.widgets.TextInput.from_param(
            self.param.time_shift, name="", placeholder="e.g. -1H, 15min", width=100, margin=_M)
        clip_lower_w = pn.widgets.FloatInput.from_param(
            self.param.clip_lower, name="", placeholder="min", width=70, margin=_M)
        clip_upper_w = pn.widgets.FloatInput.from_param(
            self.param.clip_upper, name="", placeholder="max", width=70, margin=_M)
        resample_period_w = pn.widgets.TextInput.from_param(
            self.param.resample_period, name="", placeholder="e.g. 1D", width=80, margin=_M)
        resample_agg_w = pn.widgets.Select.from_param(
            self.param.resample_agg, name="", width=80, margin=_M)
        resample_fill_w = pn.widgets.Select.from_param(
            self.param.resample_fill, name="", width=80, margin=_M)
        tidal_w = pn.widgets.Checkbox.from_param(
            self.param.do_tidal_filter, name="Tidal filter",
            disabled=not _VTOOLS_AVAILABLE, margin=_M)
        tidal_period_w = pn.widgets.TextInput.from_param(
            self.param.tidal_filter_period, name="", placeholder="e.g. 40H", width=60, margin=_M)
        rolling_window_w = pn.widgets.TextInput.from_param(
            self.param.rolling_window, name="", placeholder="e.g. 24H", width=80, margin=_M)
        rolling_agg_w = pn.widgets.Select.from_param(
            self.param.rolling_agg, name="", width=80, margin=_M)
        rolling_min_periods_w = pn.widgets.IntInput.from_param(
            self.param.rolling_min_periods, name="", width=50, margin=_M)
        do_diff_w = pn.widgets.Checkbox.from_param(
            self.param.do_diff, name="Diff", margin=_M)
        diff_n_w = pn.widgets.IntInput.from_param(
            self.param.diff_periods, name="", width=50, margin=_M)
        cumsum_w = pn.widgets.Checkbox.from_param(
            self.param.do_cumsum, name="Cumsum", margin=_M)
        scale_w = pn.widgets.FloatInput.from_param(
            self.param.scale_factor, name="", width=80, margin=_M)
        offset_w = pn.widgets.FloatInput.from_param(
            self.param.offset_value, name="", width=80, margin=_M)
        sensible_w = pn.widgets.Checkbox.from_param(
            self.param.sensible_range_yaxis, name="Sensible", margin=_M)
        pct_range_w = pn.widgets.RangeSlider.from_param(
            self.param.sensible_percentile_range, name="",
            sizing_mode="stretch_width", margin=_M)

        # ── Section header: left accent bar + bold label ──────────────────
        def _section(title):
            return pn.pane.HTML(
                f"<div style='border-left:3px solid #4a90d9;padding:1px 7px;"
                f"font-size:11px;font-weight:700;color:#333;letter-spacing:.3px;"
                f"margin:8px 0 2px 0'>{title}</div>",
                sizing_mode="stretch_width", margin=(0, 0, 0, 0),
            )

        # ── Yellow warning: both smooth ops active ───────────────────────────
        smooth_warning = pn.pane.HTML(
            "<div style='background:#fff3cd;border:1px solid #ffc107;border-radius:4px;"
            "padding:5px 8px;font-size:11px;color:#664d03;margin:2px 0 4px 0'>"
            "&#9888; Both Tidal Filter and Rolling are active — results may be unexpected."
            "</div>",
            sizing_mode="stretch_width",
            margin=(0, 0, 0, 0),
            visible=False,
        )

        def _update_smooth_warning(*args):
            smooth_warning.visible = (
                getattr(self, "do_tidal_filter", False)
                and bool(getattr(self, "rolling_window", "").strip())
            )

        self.param.watch(_update_smooth_warning, ["do_tidal_filter", "rolling_window"])

        # ── Expression preview ───────────────────────────────────────────────
        expr_preview = pn.pane.HTML("", sizing_mode="stretch_width", margin=(2, 0, 4, 0))
        expr_section = pn.Column(
            pn.layout.Divider(margin=(8, 0, 4, 0)),
            _section("Expression"),
            expr_preview,
            sizing_mode="stretch_width",
            visible=False,
        )

        _transform_watch_params = [
            "time_shift", "clip_lower", "clip_upper",
            "fill_gap", "do_tidal_filter", "tidal_filter_period", "resample_period", "resample_agg",
            "resample_fill", "rolling_window", "rolling_agg", "rolling_min_periods",
            "do_diff", "diff_periods",
            "do_cumsum", "scale_factor", "offset_value",
        ]

        def _is_any_transform_active():
            return (
                bool(getattr(self, "time_shift", "").strip())
                or (getattr(self, "fill_gap", 0) or 0) > 0
                or getattr(self, "clip_lower", None) is not None
                or getattr(self, "clip_upper", None) is not None
                or getattr(self, "do_tidal_filter", False)
                or bool(getattr(self, "resample_period", "").strip())
                or bool(getattr(self, "rolling_window", "").strip())
                or getattr(self, "do_diff", False)
                or getattr(self, "do_cumsum", False)
                or getattr(self, "scale_factor", 1.0) != 1.0
                or getattr(self, "offset_value", 0.0) != 0.0
            )

        def _update_expr_and_btn(*args):
            from .actions import TransformToCatalogAction
            expr, _ = TransformToCatalogAction._build_expression_and_tag(self)
            any_active = _is_any_transform_active()
            # Expression preview: only when exactly 1 row selected + transforms active
            dataui = getattr(self, "_dataui", None)
            sel = []
            if dataui is not None and hasattr(dataui, "display_table"):
                sel = dataui.display_table.selection or []
            show_preview = any_active and len(sel) == 1
            if show_preview:
                display_expr = expr[1:] if expr.startswith("x.") else expr
                expr_preview.object = (
                    f"<div style='font-family:monospace;font-size:11px;"
                    f"background:#f4f6f9;padding:5px 8px;border-radius:3px;"
                    f"border:1px solid #ddd;color:#333;word-break:break-all'>"
                    f"{display_expr}</div>"
                )
            expr_section.visible = show_preview
            # Disable the save button when no transforms are active
            btn = getattr(self, "_xform_btn", None)
            if btn is not None:
                btn.disabled = not any_active

        self.param.watch(_update_expr_and_btn, _transform_watch_params)

        # ── Build Transform tab ──────────────────────────────────────────────
        transform_widgets = pn.Column(
            # ── ① Data Prep ——————————————————————————————————————
            _section("① Data Prep"),
            pn.Row(
                pn.pane.HTML(
                    "<span style='font-size:11px;color:#666'>Gap fill limit</span>",
                    align=("start", "center"), margin=(0, 6, 0, 6),
                ),
                fill_gap_w,
                align="center",
            ),
            pn.Row(
                pn.pane.HTML(
                    "<span style='font-size:11px;color:#666'>Time shift</span>",
                    align=("start", "center"), margin=(0, 6, 0, 6),
                ),
                time_shift_w,
                align="center",
            ),
            # ── ② Screen / Clip ——————————————————————————————————
            _section("② Screen / Clip"),
            pn.pane.HTML(
                "<div style='font-size:10px;color:#888;font-style:italic;"
                "margin:0 0 4px 6px'>Set bounds to screen out outliers. Leave blank for no limit.</div>",
                sizing_mode="stretch_width", margin=(0, 0, 0, 0),
            ),
            pn.Row(
                clip_lower_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>lower</span>",
                    align=("start", "center"),
                ),
                clip_upper_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>upper</span>",
                    align=("start", "center"),
                ),
                align="center",
            ),
            # ── ③ Smooth ————————————————————————————————————————
            _section("③ Smooth"),
            pn.pane.HTML(
                "<div style='font-size:10px;color:#888;font-style:italic;"
                "margin:0 0 4px 6px'>Tidal filter runs <b>before</b> Resample"
                " (requires sub-daily data); Rolling runs <b>after</b>.</div>",
                sizing_mode="stretch_width", margin=(0, 0, 0, 0),
            ),
            pn.Row(
                tidal_w,
                tidal_period_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>"
                    "cutoff</span>",
                    align=("start", "center"),
                ),
                align="center",
            ),
            pn.layout.Divider(margin=(4, 0, 4, 0)),
            pn.Row(
                rolling_window_w, rolling_agg_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>"
                    "window · agg</span>",
                    align=("start", "center"),
                ),
                align="center",
            ),
            pn.Row(
                pn.pane.HTML(
                    "<span style='font-size:11px;color:#666'>Min periods</span>",
                    align=("start", "center"), margin=(0, 6, 0, 6),
                ),
                rolling_min_periods_w,
                align="center",
            ),
            smooth_warning,
            # ── ④ Resample —————————————————————————————————————
            _section("④ Resample"),
            pn.Row(
                resample_period_w, resample_agg_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>"
                    "period · agg</span>",
                    align=("start", "center"),
                ),
                align="center",
            ),
            pn.Row(
                resample_fill_w,
                pn.pane.HTML(
                    "<span style='font-size:10px;color:#999;align-self:center'>"
                    "fill (upsampling)</span>",
                    align=("start", "center"),
                ),
                align="center",
            ),
            # ── ⑤ Diff / Cumsum ————————————————————————————————
            _section("⑤ Diff / Cumsum"),
            pn.Row(do_diff_w, diff_n_w, cumsum_w, align="center"),
            # ── ⑥ Arithmetic ————————————————————————————————————
            _section("⑥ Arithmetic"),
            pn.Row(
                pn.pane.HTML(
                    "<span style='font-size:11px;color:#666'>Scale ×</span>",
                    align=("start", "center"), margin=(0, 6, 0, 6),
                ),
                scale_w,
                pn.pane.HTML(
                    "<span style='font-size:11px;color:#666'>Offset +</span>",
                    align=("start", "center"), margin=(0, 6, 0, 6),
                ),
                offset_w,
                align="center",
            ),
            # ── Expression preview + Save ──────────────────────────────────
            expr_section,
            *self._make_transform_ref_widgets(),
            sizing_mode="stretch_width",
            margin=(4, 8, 4, 4),
        )

        # Initialise button disabled state now that _xform_btn has been created.
        _update_expr_and_btn()

        # ── Build Time tab (base time-range + Display + Utilities) ───────────
        time_tab_content = pn.Column(
            control_widgets,
            pn.layout.Divider(margin=(8, 0, 4, 0)),
            _section("Display"),
            pn.Row(sensible_w, pct_range_w,
                   align="center", sizing_mode="stretch_width"),
            *(
                [
                    pn.layout.Divider(margin=(8, 0, 4, 0)),
                    _section("Utilities"),
                    clear_cache_btn,
                ]
                if self.show_clear_cache else []
            ),
            sizing_mode="stretch_width",
        )

        widget_tabs = {
            "Time": time_tab_content,
            "Transform": transform_widgets,
            "Plot": plot_widgets,
        }
        return widget_tabs

    def _make_transform_ref_widgets(self):
        """Return a list of Panel objects to append at the bottom of the Transform tab.

        When ``show_transform_to_catalog`` or ``show_source_compare`` is True a
        "Transform → Ref" button (and optionally "Source Compare") is rendered
        here so the action lives next to the transform settings it applies to.
        """
        items = []
        if not (self.show_transform_to_catalog or self.show_source_compare):
            return items
        items.append(pn.layout.Divider(margin=(8, 0, 4, 0)))
        items.append(pn.pane.HTML(
            "<div style='border-left:3px solid #4a90d9;padding:1px 7px;"
            "font-size:11px;font-weight:700;color:#333;letter-spacing:.3px;"
            "margin:8px 0 2px 0'>Save Transform</div>",
            sizing_mode="stretch_width", margin=(0, 0, 0, 0),
        ))
        if self.show_source_compare:
            from .actions import TransformToCatalogAction, SourceCompareAction
            xform_action = TransformToCatalogAction()
            compare_action = SourceCompareAction()
            menu_btn = pn.widgets.MenuButton(
                name="Add to Catalog",
                items=["Transform → MathRef", "Source Compare"],
                button_type="success",
                icon="arrows-collapse",
                sizing_mode="stretch_width",
            )
            def _make_menu_handler(_xa, _ca):
                def _on_click(event):
                    _dataui = getattr(self, "_dataui", None)
                    if _dataui is None:
                        return
                    if event.new == "Transform → MathRef":
                        _xa.callback(event, _dataui)
                    elif event.new == "Source Compare":
                        _ca.callback(event, _dataui)
                return _on_click
            menu_btn.on_click(_make_menu_handler(xform_action, compare_action))
            self._xform_btn = menu_btn
            items.append(menu_btn)
        else:
            from .actions import TransformToCatalogAction
            xform_action = TransformToCatalogAction()
            xform_btn = pn.widgets.Button(
                name="Transform → MathRef",
                button_type="success",
                icon="transform",
                sizing_mode="stretch_width",
            )
            def _make_xform_handler(_xa):
                def _on_click(event):
                    _dataui = getattr(self, "_dataui", None)
                    if _dataui is not None:
                        _xa.callback(event, _dataui)
                return _on_click
            xform_btn.on_click(_make_xform_handler(xform_action))
            self._xform_btn = xform_btn
            items.append(xform_btn)
        return items

    def setup_url_sync(self):
        """Bi-directionally sync transform/display params with URL query string.

        On page load, URL query params override defaults.  On param change,
        the URL is updated so that F5 / bookmark / share preserves the state.

        Must be called inside a live server session (``pn.state.location``
        is not ``None``).  Safe to call multiple times — ``location.sync``
        is idempotent per parameterized instance.
        """
        if not pn.state.location:
            return
        # Map param names → short URL query keys to keep URLs compact.
        # Also exposed as _URL_PARAM_MAP for DataUI session-cache integration.
        # time_range is handled separately: pn.state.location.sync() deserialises
        # CalendarDateRange as date objects, but the DatetimeRangeInput widget
        # requires datetime objects, causing a TypeError on comparison.
        import datetime as _dt
        import json as _json

        loc = pn.state.location
        qp = loc.query_params or {}

        # Build (url_key, default) pairs for all non-time_range params.
        # We never write defaults to the URL; on restore, absent/empty keys
        # mean "keep the default".
        _non_tr = {p: k for p, k in self._URL_PARAM_MAP.items()
                   if p != "time_range" and p in self.param}

        # --- Restore from URL (load) ---
        for p_name, url_key in _non_tr.items():
            raw = qp.get(url_key)
            if not raw:
                continue
            p_obj = self.param[p_name]
            default = p_obj.default
            try:
                if isinstance(p_obj, param.Boolean):
                    val = raw.lower() in ("true", "1", "yes")
                elif isinstance(p_obj, param.Integer):
                    val = int(raw)
                elif isinstance(p_obj, param.Number):
                    val = float(raw)
                elif isinstance(p_obj, param.Selector):
                    val = raw if raw in p_obj.objects else default
                else:
                    val = raw
                if val != default:
                    setattr(self, p_name, val)
            except Exception:
                pass

        # --- Write back on change (only when value differs from default) ---
        def _make_watcher(url_key, default):
            def _on_change(event):
                if event.new == default:
                    loc.update_query(**{url_key: ""})
                else:
                    loc.update_query(**{url_key: str(event.new)})
            return _on_change

        for p_name, url_key in _non_tr.items():
            default = self.param[p_name].default
            self.param.watch(_make_watcher(url_key, default), p_name)

        # --- time_range: manual sync ---
        if "tr" in qp:
            try:
                raw = qp["tr"]
                parts = _json.loads(raw) if isinstance(raw, str) and raw.startswith("[") else raw.split(",")
                def _to_dt(v):
                    v = str(v).strip()
                    # JSON may give 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'
                    d = _dt.datetime.fromisoformat(v)
                    return d
                self.time_range = (_to_dt(parts[0]), _to_dt(parts[1]))
            except Exception:
                logger.warning("Could not restore time_range from URL: %s", qp["tr"])

        def _on_time_range(event):
            if not pn.state.location:
                return
            tr = event.new
            if tr is None:
                # None is the default — omit from URL rather than writing tr=
                return
            # Serialise both bounds as ISO date strings (date only, no time part,
            # to keep the URL compact and avoid TZ issues).
            try:
                s = tr[0].isoformat()[:10] if hasattr(tr[0], "isoformat") else str(tr[0])[:10]
                e = tr[1].isoformat()[:10] if hasattr(tr[1], "isoformat") else str(tr[1])[:10]
                pn.state.location.update_query(tr=f"{s},{e}")
            except Exception:
                pass

        self.param.watch(_on_time_range, "time_range")

    # Exposed as a class attribute so DataUI can introspect without importing
    # tsdataui (avoids circular imports when session_state.py snapshots state).
    _URL_PARAM_MAP: dict = {
        "time_range": "tr",
        "fill_gap": "fg",
        "time_shift": "ts",
        "clip_lower": "cll",
        "clip_upper": "clu",
        "do_tidal_filter": "tf",
        "resample_period": "rp",
        "resample_agg": "ra",
        "resample_fill": "rf",
        "rolling_window": "rw",
        "rolling_agg": "rwa",
        "rolling_min_periods": "rmp",
        "do_diff": "dd",
        "diff_periods": "dp",
        "do_cumsum": "cs",
        "scale_factor": "sf",
        "offset_value": "ov",
        "show_legend": "sl",
        "legend_position": "lp",
        "regular_curve_connection": "rcc",
        "irregular_curve_connection": "icc",
        "period_curve_connection": "pcc",
        "sensible_range_yaxis": "sry",
        "color_cycle_name": "ccn",
        "shared_axes": "sa",
    }

    def get_mobile_widgets(self):
        """Return a compact widget set for mobile: time range + key plot options."""
        time_range_w = pn.widgets.DatetimeRangeInput.from_param(
            self.param.time_range,
            name="Time range",
            format="%Y-%m-%d %H:%M",
            sizing_mode="stretch_width",
        )
        time_widget = pn.Column(
            pn.pane.HTML("Time range:"),
            time_range_w,
        )
        plot_opts = pn.Column(
            self.param.show_legend,
            self.param.color_cycle_name,
            self.param.shared_axes,
        )
        return pn.Column(time_widget, plot_opts, sizing_mode="stretch_width")

    def get_data(self, df, time_range=None):
        # Start with 0 progress
        # Get the DataUI instance from the caller
        dataui = self._dataui if hasattr(self, "_dataui") else None
        if dataui:
            dataui.set_progress(0)

        # Calculate progress increment per row
        total_rows = len(df)
        if total_rows == 0:  # Avoid division by zero
            return

        progress_per_row = 50 / total_rows  # We'll use 0-50% range for the iteration

        # When a DataCatalog is available, delegate data loading to each DataReference
        # so that mixed catalogs with different reader types are handled automatically.
        # Otherwise fall back to the legacy get_data_for_time_range() hook.
        use_catalog = self.data_catalog is not None
        effective_time_range = time_range if time_range is not None else self.time_range

        # Process each row, updating progress as we go
        for i, (_, r) in enumerate(df.iterrows()):
            if use_catalog:
                data = self.get_data_reference(r).getData(time_range=effective_time_range)
            else:
                data, _, _ = self.get_data_for_time_range(r, effective_time_range)

            # Update progress - scale from 0 to 50%
            if dataui:
                current_progress = int(progress_per_row * (i + 1))
                dataui.set_progress(current_progress)

            yield data

        # After completing all rows, ensure progress is at 50%
        if dataui:
            dataui.set_progress(50)

    # display related support for tables
    def get_table_columns(self):
        df = self.get_data_catalog()
        schema = self.get_table_schema(df)
        return self._resolve_table_columns_from_schema(df, schema)

    def get_table_schema(self, df: pd.DataFrame | None = None) -> dict:
        """Return the formal table schema for time-series managers.

        This default implementation preserves legacy behavior built from
        ``_get_table_column_width_map()`` and ``get_table_filters()`` while
        exposing the schema contract for subclasses.
        """
        if df is None:
            df = self.get_data_catalog()

        column_width_map = dict(self._get_table_column_width_map())
        # Keep longstanding defaults unless a subclass sets these explicitly.
        column_width_map.setdefault("source", "10%")
        if "source_num" in (df.columns if hasattr(df, "columns") else []):
            column_width_map.setdefault("source_num", "5%")
        column_width_map.setdefault("ref_type", "8%")
        self.adjust_column_width(column_width_map)

        optional_columns = []
        cat = getattr(self, "data_catalog", None)
        if cat is not None and self._has_math_refs():
            if "name" not in column_width_map and "name" in df.columns:
                optional_columns.append("name")
            for col in df.columns:
                if col not in column_width_map and col not in ("geometry", "source"):
                    optional_columns.append(col)

        hidden_by_default = []
        if "ref_type" in df.columns and not self._has_mixed_ref_types(df):
            hidden_by_default.append("ref_type")

        return {
            "required_columns": list(column_width_map.keys()),
            "optional_columns": optional_columns,
            "hidden_by_default": hidden_by_default,
            "drop_if_all_null": False,
            "column_widths": column_width_map,
            "filters": {},  # not populated in default; subclasses provide via get_table_schema()
        }

    @staticmethod
    def _is_effectively_empty_column(series: "pd.Series") -> bool:
        if series.isna().all():
            return True
        non_na = series.dropna()
        if non_na.empty:
            return True
        as_text = non_na.astype(str).str.strip().str.lower()
        return as_text.isin({"", "nan", "none", "null"}).all()

    def _resolve_table_columns_from_schema(self, df: pd.DataFrame, schema: dict) -> list[str]:
        required = list(schema.get("required_columns", []))
        optional = list(schema.get("optional_columns", []))
        drop_if_all_null = bool(schema.get("drop_if_all_null", False))

        columns = []
        seen = set()
        for col in required + optional:
            if col in seen or col not in df.columns:
                continue
            if drop_if_all_null and col in optional and self._is_effectively_empty_column(df[col]):
                continue
            columns.append(col)
            seen.add(col)
        return columns

    def get_table_width_sum(self, column_width_map):
        width = 0
        for k, v in column_width_map.items():
            width += float(v[:-1])  # drop % sign
        return width

    def adjust_column_width(self, column_width_map, max_width=100):
        width_sum = self.get_table_width_sum(column_width_map)
        if width_sum > max_width:
            for k, v in column_width_map.items():
                column_width_map[k] = f"{(float(v[:-1]) / width_sum) * max_width}%"
        return column_width_map

    def get_table_column_width_map(self):
        df = self.get_data_catalog()
        schema = self.get_table_schema(df)
        column_width_map = dict(schema.get("column_widths", {}))
        for col in self._resolve_table_columns_from_schema(df, schema):
            if col not in column_width_map:
                column_width_map[col] = "10%"
        return column_width_map

    @staticmethod
    def _has_mixed_ref_types(df: "pd.DataFrame") -> bool:
        """Return True if *df* contains more than one distinct ``ref_type`` value."""
        if df is None or "ref_type" not in df.columns:
            return False
        return df["ref_type"].nunique() > 1

    def get_color_style_mapping(self, unique_values):
        """
        Map unique values to colors.
        """
        color_df = get_color_dataframe(unique_values, self.color_cycle)
        return {value: color_df.at[value, "color"] for value in unique_values}

    def get_line_style_mapping(self, unique_values):
        """
        Map unique values to line dash styles.
        """
        return {
            value: LINE_DASH_MAP[i % len(LINE_DASH_MAP)] for i, value in enumerate(unique_values)
        }

    def get_marker_style_mapping(self, unique_values):
        """
        Map unique values to marker styles.
        """
        from bokeh.core.enums import MarkerType

        marker_types = [None] + list(MarkerType)
        return {value: marker_types[i % len(marker_types)] for i, value in enumerate(unique_values)}

    def _process_curve_data(self, data, r, time_range):
        """Process time series data based on index type and apply transformations."""
        # Normalise PeriodIndex → DatetimeIndex so Timestamp comparisons work
        # uniformly.  pyhecdss sometimes returns an object-dtype index whose
        # elements are pd.Period values rather than a proper pd.PeriodIndex, so
        # check both forms.
        if isinstance(data.index, pd.PeriodIndex):
            data.index = data.index.to_timestamp()
        elif len(data.index) > 0 and isinstance(data.index[0], pd.Period):
            data.index = pd.PeriodIndex(data.index).to_timestamp()
        # Coerce time_range bounds to Timestamp so that comparison with a
        # datetime64 index works regardless of whether the values arrived as
        # datetime, date, or string (e.g. after URL query-param deserialization).
        # When time_range is None (no filter set), return all data unchanged.
        if time_range is not None:
            t0 = pd.Timestamp(time_range[0])
            t1 = pd.Timestamp(time_range[1])
            data = data[(data.index >= t0) & (data.index <= t1)]

        # Apply optional data transformations

        # ① Time shift — move the index forward/backward before any processing
        if self.time_shift.strip():
            try:
                data = data.shift(freq=self.time_shift)
            except Exception as e:
                logger.warning(
                    f"Time shift (shift={self.time_shift!r}) failed and was skipped: {e}"
                )

        if self.fill_gap > 0:
            data = data.interpolate(limit=self.fill_gap)

        # ③ Clip / screen — remove outliers before filtering or resampling
        if self.clip_lower is not None or self.clip_upper is not None:
            data = data.clip(lower=self.clip_lower, upper=self.clip_upper)

        # Tidal filter is applied first — it requires raw sub-daily data.
        # Resampling afterwards makes sense (e.g. daily-average the filtered signal);
        # resampling before would destroy the high-frequency information the filter needs.
        if self.do_tidal_filter and _VTOOLS_AVAILABLE and not self.is_irregular(r):
            # Interpolate internal NaN gaps before filtering so the cosine-Lanczos
            # kernel does not propagate NaN across sparse or gappy data (e.g. event
            # sensors resampled to a regular grid).  Edge NaN from fill_edge_nan=True
            # remain in the output to indicate the filter warm-up period.
            data_for_filter = data.astype("float64").interpolate(method="time")
            # Infer and set frequency if missing (slicing/filtering can drop it)
            if hasattr(data_for_filter.index, "freq") and data_for_filter.index.freq is None:
                inferred_freq = pd.infer_freq(data_for_filter.index)
                if inferred_freq is not None:
                    data_for_filter.index.freq = pd.tseries.frequencies.to_offset(inferred_freq)
            # Skip tidal filter for daily or coarser data — the 40h cosine-Lanczos
            # filter requires sub-daily data, and vtools cannot convert daily DateOffsets
            # (e.g. pandas Day) to pd.Timedelta.
            if len(data_for_filter) >= 2:
                median_dt = pd.Series(data_for_filter.index.to_numpy()).diff().dropna().median()
                _skip_filter = median_dt >= pd.Timedelta("1D")
            else:
                _skip_filter = True
            if _skip_filter:
                import logging
                logging.getLogger(__name__).debug(
                    "Skipping tidal filter: data interval is >= 1 day (filter requires sub-daily data)."
                )
            else:
                try:
                    data = cosine_lanczos(data_for_filter, self.tidal_filter_period)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"Tidal filter (cosine_lanczos) failed and was skipped: {e}"
                    )

        # Resampling (applied after tidal filter — downsamples the filtered signal)
        if self.resample_period.strip():
            try:
                agg_fn = getattr(data.resample(self.resample_period), self.resample_agg)
                data = agg_fn()
                # Drop all-NaN rows that arise when resampling sparse data
                data = data.dropna(how="all")
                # Optional fill for upsampling (e.g. ffill when going to finer resolution)
                if self.resample_fill == "ffill":
                    data = data.ffill()
                elif self.resample_fill == "bfill":
                    data = data.bfill()
                elif self.resample_fill == "interpolate":
                    data = data.interpolate()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"Resampling (period={self.resample_period!r}, agg={self.resample_agg!r}) "
                    f"failed and was skipped: {e}"
                )

        # Rolling window (applied after resample)
        if self.rolling_window.strip():
            try:
                roller = data.astype("float64").rolling(
                    self.rolling_window, min_periods=self.rolling_min_periods
                )
                agg_fn = getattr(roller, self.rolling_agg)
                data = agg_fn()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"Rolling window (window={self.rolling_window!r}, agg={self.rolling_agg!r}) "
                    f"failed and was skipped: {e}"
                )

        # Differencing
        if self.do_diff:
            data = data.diff(self.diff_periods)

        # Cumulative sum
        if self.do_cumsum:
            data = data.cumsum()

        # Scale factor
        if self.scale_factor != 1.0:
            data = data * self.scale_factor

        # Offset (add constant) — applied last, after scaling
        if self.offset_value != 0.0:
            data = data + self.offset_value

        return data

    def _calculate_range(self, current_range, df, factor=0.0):
        if df.empty:
            return current_range
        else:
            new_range = df.iloc[:, 0].quantile(list(self.sensible_percentile_range)).values
            scaleval = new_range[1] - new_range[0]
            new_range = [
                new_range[0] - scaleval * factor,
                new_range[1] + scaleval * factor,
            ]
        if current_range is not None:
            new_range = [
                min(current_range[0], new_range[0]),
                max(current_range[1], new_range[1]),
            ]
        return new_range

    def _prepare_style_maps(self, df):
        """Prepare color, line style, and marker style mappings."""
        style_maps = {"color": None, "line": None, "marker": None}

        # Color map
        if self.color_cycle_column and self.color_cycle_column in df.columns:
            color_values = df[self.color_cycle_column].unique()
            style_maps["color"] = self.get_color_style_mapping(color_values)

        # Line style map
        if self.dashed_line_cycle_column and self.dashed_line_cycle_column in df.columns:
            line_style_values = df[self.dashed_line_cycle_column].unique()
            style_maps["line"] = self.get_line_style_mapping(line_style_values)

        # Marker map
        if self.marker_cycle_column and self.marker_cycle_column in df.columns:
            marker_values = df[self.marker_cycle_column].unique()
            style_maps["marker"] = self.get_marker_style_mapping(marker_values)

        return style_maps

    def _calculate_has_duplicates(self, curves_data):
        """Check if there are duplicate station names in the curves data."""
        # If no color cycle column is specified, return False
        if not self.color_cycle_column:
            return False

        try:
            station_names = []
            for i, (_, row) in enumerate(curves_data):
                if self.color_cycle_column in row:
                    station_names.append(row[self.color_cycle_column])
                else:
                    # If missing, use index to avoid duplicates
                    station_names.append(f"curve_{i}")
            return len(station_names) != len(set(station_names))
        except Exception as e:
            # Fallback to avoid breaking the app
            print(f"Error in _calculate_has_duplicates: {e}")
            return False

    def _get_style_combinations(self, stations, curves_data, style_maps):
        """
        Determine which color and line style combinations exist within a unit.

        Args:
            stations: List of station names
            curves_data: List of (curve, row) tuples
            style_maps: Dictionary of style mappings

        Returns:
            tuple: (combinations_dict, has_duplicates, has_style_duplicates)
        """
        has_duplicates = self._calculate_has_duplicates(curves_data)
        color_map, line_map = style_maps["color"], style_maps["line"]

        # First pass to collect color + line style combinations
        combinations = {}
        for i, (_, row) in enumerate(curves_data):
            color_val = (
                row[self.color_cycle_column] if color_map and self.color_cycle_column else None
            )
            line_style_val = (
                row[self.dashed_line_cycle_column]
                if line_map and self.dashed_line_cycle_column and has_duplicates
                else None
            )

            combo_key = (color_val, line_style_val)
            if combo_key not in combinations:
                combinations[combo_key] = []
            combinations[combo_key].append(i)

        # Check for duplicate combinations
        has_style_duplicates = any(len(indices) > 1 for indices in combinations.values())

        return combinations, has_duplicates, has_style_duplicates

    def _apply_curve_styling(
        self,
        curve,
        row,
        has_duplicates,
        has_style_duplicates,
        style_maps,
        style_combinations,
    ):
        """
        Apply styling options to a curve based on context and available styles.

        The logic ensures markers are only used when there are multiple curves
        with the same color and line style combination in the layout.
        """
        color_map, line_map, marker_map = (
            style_maps["color"],
            style_maps["line"],
            style_maps["marker"],
        )

        # Base styling options
        curve_opts = {}

        # Apply color
        if color_map and self.color_cycle_column:
            curve_opts["color"] = color_map.get(row[self.color_cycle_column], "black")

        # Apply line style if needed
        if has_duplicates and line_map and self.dashed_line_cycle_column:
            curve_opts["line_dash"] = line_map.get(row[self.dashed_line_cycle_column], "solid")

        # Apply basic styling
        styled_curve = curve.opts(opts.Curve(**curve_opts))

        # Add markers only when there are multiple curves with the same color and line style
        if marker_map and self.marker_cycle_column:
            # Get the combo key for this curve
            current_color = row[self.color_cycle_column] if self.color_cycle_column else None
            current_line_style = (
                row[self.dashed_line_cycle_column]
                if has_duplicates and self.dashed_line_cycle_column
                else None
            )
            combo_key = (current_color, current_line_style)

            # Only add markers if this specific style combination appears multiple times
            if combo_key in style_combinations and len(style_combinations[combo_key]) > 1:
                marker_style = marker_map.get(row[self.marker_cycle_column], None)
                if marker_style is not None:
                    scatter = hv.Scatter(curve.data, label=curve.label).opts(
                        opts.Scatter(
                            marker=marker_style,
                            size=5,
                            color=curve_opts.get("color", "black"),
                        )
                    )
                    styled_curve = styled_curve * scatter

        return styled_curve

    def create_panel(self, df):
        """Delegate to :class:`TimeSeriesPlotAction` for programmatic access."""
        action = self._make_plot_action()
        refs_and_data = list(action.get_refs_and_data(df, self))
        return action.render(df, refs_and_data, self)

class TimeSeriesPlotAction(PlotAction):
    """PlotAction for time-series data backed by a :class:`~dvue.catalog.DataCatalog`.

    Owns the full visualisation pipeline: curve creation, title accumulation,
    layout assembly, and styling.  Subclass to customise any part of the pipeline,
    then wire your subclass in via :meth:`TimeSeriesDataUIManager._make_plot_action`.

    The *curve_creator* constructor argument provides a lightweight alternative to
    subclassing when only :meth:`create_curve` needs customising.

    Customisation hooks
    -------------------
    * :meth:`create_curve` — build a single HoloViews element for one time series.
    * :meth:`append_to_title_map` — accumulate per-group title info from a row.
    * :meth:`create_title` — convert accumulated title info to a display string.
    """

    def __init__(self, curve_creator=None):
        """
        Parameters
        ----------
        curve_creator : callable, optional
            ``f(data, row, unit, file_index) -> hv.Element``.  When supplied,
            :meth:`create_curve` delegates to this callable instead of using
            the built-in default.  Useful for quick customisation without
            subclassing.
        """
        self._curve_creator = curve_creator

    # ------------------------------------------------------------------
    # Customisation hooks
    # ------------------------------------------------------------------

    def create_curve(self, data, row, unit, file_index=""):
        """Build a HoloViews element for a single time series.

        Override in a subclass or supply *curve_creator* at construction time
        for domain-specific axis labels, titles, and curve options.

        Parameters
        ----------
        data : pd.DataFrame
            Time-indexed DataFrame (single column) for this series.
        row : pd.Series
            Catalog row containing metadata (station_name, variable, unit, …).
        unit : str
            Physical unit string (lower-cased).
        file_index : str, optional
            Short file identifier appended to the label when *display_fileno*
            is set on the manager.

        Returns
        -------
        hv.Element
        """
        if self._curve_creator is not None:
            return self._curve_creator(data, row, unit, file_index)
        # Generic default: use first available identifying column as label
        label = None
        for col in ("name", "station_name", "station_id"):
            val = row.get(col) if hasattr(row, "get") else None
            if val:
                label = str(val)
                break
        label = label or "value"
        if file_index:
            label = f"{label} [{file_index}]"
        safe_vdim = _sanitize_vdim(label)
        ts = data.iloc[:, [0]].copy()
        ts.index.name = "Time"
        return hv.Curve(ts, label=label).redim(value=safe_vdim).opts(
            responsive=True,
            active_tools=["wheel_zoom"],
        )

    def append_to_title_map(self, title_map, group_key, row):
        """Accumulate per-group title information from *row*.

        The default stores the *group_key* string itself as the title.
        Override (together with :meth:`create_title`) to build richer titles,
        e.g. ``"station_ids(variables)"``.
        """
        title_map.setdefault(group_key, str(group_key))

    def create_title(self, title_info) -> str:
        """Convert accumulated title info for one group to a display string.

        Override together with :meth:`append_to_title_map` when *title_info*
        is a structured object rather than a plain string.
        """
        return str(title_info)

    @staticmethod
    def _get_connection(data: "pd.DataFrame", orig_is_period: bool, manager) -> str:
        """Determine the curve interpolation style from the data's index.

        Parameters
        ----------
        data :
            Processed time series (PeriodIndex already converted to DatetimeIndex
            by ``_process_curve_data``).
        orig_is_period :
            ``True`` when the *original* index (before processing) was a
            ``pd.PeriodIndex``.
        manager :
            Provides ``regular_curve_connection``, ``irregular_curve_connection``,
            and optionally ``period_curve_connection``.
        """
        if orig_is_period:
            return getattr(manager, "period_curve_connection", manager.irregular_curve_connection)
        idx = data.index
        if isinstance(idx, pd.DatetimeIndex):
            freq = idx.freq
            if freq is None and len(idx) >= 3:
                freq = pd.infer_freq(idx)
            if freq is not None:
                return manager.regular_curve_connection
            return manager.irregular_curve_connection
        return manager.regular_curve_connection

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    def _add_curve_to_layout(
        self,
        layout_map,
        station_map,
        title_map,
        range_map,
        curve,
        row,
        unit,
        station_name,
        group_key=None,
    ):
        """Add a curve to the layout maps using the specified group key."""
        group_key = group_key if group_key is not None else unit

        if group_key not in layout_map:
            layout_map[group_key] = []
            range_map[group_key] = None
            station_map[group_key] = []

        layout_map[group_key].append((curve, row))
        station_map[group_key].append(station_name)
        self.append_to_title_map(title_map, group_key, row)

    def _update_title_for_custom_grouping(self, title_map, manager):
        """Build final title strings, adding group-column context when applicable."""
        processed_titles = {}
        for group_key, title_info in title_map.items():
            base_title = self.create_title(title_info)
            if manager.plot_group_by_column:
                column_name = manager.plot_group_by_column
                if str(group_key) != base_title:
                    title = f"{column_name}: {group_key} - {base_title}"
                else:
                    title = f"{column_name}: {group_key}"
            else:
                title = base_title
            processed_titles[group_key] = title
        return processed_titles

    # ------------------------------------------------------------------
    # render
    # ------------------------------------------------------------------

    def render(self, df, refs_and_data, manager):
        """Build a HoloViews Layout from *refs_and_data*.

        Parameters
        ----------
        df : pd.DataFrame
            The selected rows from the catalog table (used for style
            preparation and file-index mapping).
        refs_and_data : list of (row, ref, data)
            Pre-loaded triples produced by
            :meth:`~dvue.actions.PlotAction.get_refs_and_data`.
            Entries where *ref* or *data* is ``None`` are skipped.
        manager : TimeSeriesDataUIManager
            Provides styling helpers, widget params, and data transformation.
        """
        time_range = manager.time_range

        # source_num label map (short unique names per source when multiple sources exist)
        file_index_map = {}
        if "source_num" in df.columns:
            local_unique_sources = df["source"].unique() if "source" in df.columns else []
            # Math references have NaN for source; exclude them from path shortening.
            valid_files = [f for f in local_unique_sources if not pd.isna(f)]
            short_unique_files = get_unique_short_names(valid_files)
            file_index_map = dict(zip(valid_files, short_unique_files))

        style_maps = manager._prepare_style_maps(df)

        layout_map = {}
        title_map = {}
        range_map = {}
        station_map = {}

        for row, ref, data in refs_and_data:
            try:
                if data is None:
                    continue

                # Resolve unit: prefer data.attrs["unit"] (set by reader after any
                # conversion), then DataReference attribute, then catalog row.
                unit = str(
                    data.attrs.get("unit") or
                    (ref.get_attribute("unit", row.get("unit", "")) if ref is not None
                     else row.get("unit", ""))
                ).lower()

                # Detect PeriodIndex before _process_curve_data converts it to DatetimeIndex
                _orig_is_period = isinstance(data.index, pd.PeriodIndex) or (
                    len(data.index) > 0 and isinstance(data.index[0], pd.Period)
                )

                data = manager._process_curve_data(data, row, time_range)

                if data is None or len(data) == 0:
                    logger.warning("Skipping empty data for row: %s", row.get("station_id", row.get("name", "")))
                    continue

                file_index = (
                    file_index_map.get(row.get("source", ""), "")
                    if "source_num" in df.columns
                    else ""
                )
                curve = self.create_curve(data, row, unit, file_index=file_index)
                # Determine curve connection from data index characteristics
                connection = self._get_connection(data, _orig_is_period, manager)
                curve = curve.opts(opts.Curve(interpolation=connection))
                station_name = manager.build_station_name(row)

                # Determine group key: custom column > unit
                group_key = None
                if manager.plot_group_by_column and manager.plot_group_by_column in row:
                    group_value = row[manager.plot_group_by_column]
                    group_str = str(group_value).strip() if group_value is not None else ""
                    if group_str and group_str.lower() != "nan":
                        group_key = group_str

                self._add_curve_to_layout(
                    layout_map, station_map, title_map, range_map,
                    curve, row, unit, station_name, group_key=group_key,
                )
            except Exception as e:
                print(full_stack())
                if pn.state.notifications:
                    pn.state.notifications.error(f"Error processing row: {row}: {e}")

        if not layout_map:
            return hv.Div(manager.get_no_selection_message()).opts(sizing_mode="stretch_both")

        title_map = self._update_title_for_custom_grouping(title_map, manager)

        if manager.sensible_range_yaxis:
            for group_key, curves in layout_map.items():
                for curve, _ in curves:
                    range_map[group_key] = manager._calculate_range(range_map[group_key], curve.data)

        # Assemble overlays
        overlays = []
        for group_key, curves_data in layout_map.items():
            stations = station_map[group_key]
            style_combinations, has_duplicates, has_style_duplicates = (
                manager._get_style_combinations(stations, curves_data, style_maps)
            )
            styled_curves = [
                manager._apply_curve_styling(
                    curve, row, has_duplicates, has_style_duplicates,
                    style_maps, style_combinations,
                )
                for curve, row in curves_data
            ]

            # With shared_axes=True, rename all curves in this overlay to the
            # same group_vdim so HoloViews doesn't cross-link y-axes between
            # overlays that have different units.  axiswise=True on the Layout
            # keeps each subplot's y-range independent.
            # group_vdim is also used by the hover tooltip to reference the value column.
            group_vdim = f"value__{str(group_key).replace(' ', '_')}"
            if manager.shared_axes:
                _renamed = []
                for curve in styled_curves:
                    try:
                        if curve.vdims:
                            old_vdim = curve.vdims[0].name
                            curve = curve.redim(**{old_vdim: group_vdim})
                    except Exception:
                        pass
                    _renamed.append(curve)
                styled_curves = _renamed
                value_col = group_vdim
            else:
                # shared_axes off — each curve has its own vdim; pick the first
                # curve's vdim as the hover column (all share the same unit group).
                first_curve = next(
                    (el for el in styled_curves if isinstance(el, hv.Curve)),
                    styled_curves[0] if styled_curves else None,
                )
                value_col = (
                    first_curve.vdims[0].name
                    if first_curve is not None and first_curve.vdims
                    else "value"
                )

            # Use list-of-tuples hover_tooltips — this format reliably
            # combines with hover_formatters for datetime column formatting.
            # HTML-string tooltips do NOT reliably respect hover_formatters
            # (Bokeh treats {%F %H:%M} as numeral.js %-percent format).
            # $name in the VALUE position is a Bokeh special field that
            # resolves to the glyph renderer name, which HoloViews sets to
            # the curve's label — i.e. the same text shown in the legend.
            hover_tooltips = [
                ("Time", "@Time{%F %H:%M}"),
                ("", f"$name: @{{{value_col}}}{{0,0.000}}"),
            ]

            overlays.append(
                hv.Overlay(styled_curves)
                .opts(opts.Curve(
                    hover_tooltips=hover_tooltips,
                    hover_formatters={"@Time": "datetime"},
                ))
                .opts(
                    show_legend=manager.show_legend,
                    show_grid=manager.show_gridlines,
                    legend_position=manager.legend_position,
                    ylim=(
                        tuple(range_map[group_key])
                        if range_map[group_key] is not None
                        else (None, None)
                    ),
                    title=title_map[group_key],
                    min_height=400,
                )
            )

        return (
            hv.Layout(overlays)
            .cols(1)
            .opts(
                shared_axes=manager.shared_axes,
                axiswise=True,
                sizing_mode="stretch_both",
            )
        )
