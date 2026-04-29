from .utils import get_unique_short_names
from .dataui import DataUIManager, full_stack
from .actions import PlotAction
from datetime import datetime, timedelta
import warnings
from functools import lru_cache
import os

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
    legend_position = param.Selector(
        objects=["top_right", "top_left", "bottom_right", "bottom_left"],
        default="top_right",
        doc="Legend position",
    )
    fill_gap = param.Integer(
        default=0, doc="Fill gaps in data upto this limit, only when a positive integer"
    )
    do_tidal_filter = param.Boolean(default=False, doc="Apply tidal filter", constant=not _VTOOLS_AVAILABLE)
    irregular_curve_connection = param.Selector(
        objects=["steps-post", "steps-pre", "steps-mid", "linear"],
        default="steps-post",
        doc="Curve connection method for irregular data",
    )
    regular_curve_connection = param.Selector(
        objects=["linear", "steps-pre", "steps-post", "steps-mid"],
        default="steps-pre",
        doc="Curve connection method for regular period type data",
    )
    sensible_range_yaxis = param.Boolean(
        default=False,
        doc="Sensible range (in percentile) or auto range for y axis",
    )
    sensible_percentile_range = param.Range(
        default=(0.01, 0.99), bounds=(0, 1), step=0.01, doc="Percentile range"
    )
    url_num_column = param.String(default="url_num", allow_None=True)
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

    def __init__(self, url_column="url", url_num_column="url_num", **params):
        self._cached_catalog = None
        self.url_column = url_column
        self.url_num_column = url_num_column
        self.display_url_num = False
        # Populate _cached_catalog for subclasses that don't expose data_catalog.
        # Managers with a data_catalog property always rebuild fresh in get_data_catalog().
        if self.data_catalog is None:
            catalog = self.get_data_catalog()
            self._cached_catalog = self._apply_url_num(catalog)
        self.change_color_cycle()
        self.time_range = self.get_time_range(self.get_data_catalog())
        super().__init__(**params)
        table_columns = list(self.get_table_columns())
        # Add blank (None) option at the start
        columns_with_blank = [None] + table_columns
        self.param.marker_cycle_column.objects = columns_with_blank
        self.param.dashed_line_cycle_column.objects = columns_with_blank
        self.param.color_cycle_column.objects = columns_with_blank
        self.param.plot_group_by_column.objects = columns_with_blank

    def _apply_url_num(self, df, catalog=None):
        """Inject a ``url_num`` column when multiple URLs/files are present.

        Called during init (for legacy subclasses) and on every
        ``get_data_catalog()`` rebuild for managers that expose a
        ``data_catalog`` property.  Sets ``self.display_url_num`` as a
        side-effect.

        When *catalog* is provided, each :class:`~dvue.catalog.DataReference`
        in the catalog also receives ``url`` and ``url_num`` as dynamic
        metadata so that :class:`~dvue.catalog.DataCatalog.search` and math
        ref ``search_map`` criteria can filter by source URL index.
        """
        if self.url_column and self.url_column in df.columns:
            unique_urls = [u for u in df[self.url_column].unique() if not pd.isna(u)]
            if len(unique_urls) > 1:
                url_index = {u: i for i, u in enumerate(unique_urls)}
                df = df.copy()
                df[self.url_num_column] = df[self.url_column].apply(
                    lambda x: url_index.get(x, None)
                )
                # Propagate url / url_num into dynamic metadata so catalog.search works.
                if catalog is not None:
                    for _, row in df.iterrows():
                        ref_name = row.get("name") if "name" in df.columns else None
                        if ref_name is None or pd.isna(ref_name):
                            continue
                        try:
                            ref = catalog.get(str(ref_name))
                        except KeyError:
                            continue
                        url_val = row.get(self.url_column)
                        if url_val is not None and not pd.isna(url_val):
                            ref.set_dynamic_metadata("url", url_val)
                            ref.set_dynamic_metadata("url_num", url_index[url_val])
                self.display_url_num = True
                return df
        if self.url_column not in (df.columns if hasattr(df, 'columns') else []):
            self.url_num_column = None
        self.display_url_num = False
        return df

    def get_data_catalog(self):
        # When a live DataCatalog is available, always rebuild from it so that
        # mutations (e.g. catalog.add() in the math-ref editor) are immediately
        # visible without requiring a cache invalidation step.
        if self.data_catalog is not None:
            return self._apply_url_num(super().get_data_catalog(), catalog=self.data_catalog)
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

        Uses ``self.data_catalog`` when available (preferred — avoids rebuilding
        the full display DataFrame).  Falls back to inspecting the ``ref_type``
        column of the cached catalog DataFrame.
        """
        cat = getattr(self, "data_catalog", None)
        if cat is not None:
            return any(getattr(r, "ref_type", "raw") != "raw" for r in cat.list())
        # Fallback: check cached DataFrame
        cached = getattr(self, "_cached_catalog", None)
        if cached is not None and "ref_type" in cached.columns:
            return (cached["ref_type"] != "raw").any()
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
        from .actions import ClearCacheAction
        actions.append(dict(
            name="Clear Cache",
            button_type="light",
            icon="trash",
            action_type="inline",
            callback=ClearCacheAction().callback,
        ))
        return actions

    def get_time_range(self, dfcat):
        raise NotImplementedError("Method get_time_range not implemented")

    def get_table_filters(self):
        raise NotImplementedError("Method get_table_filters not implemented")

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
        tr_start, tr_end = self.time_range if self.time_range else (None, None)
        control_widgets = pn.Column(
            pn.pane.HTML("Change time range of data to display:"),
            pn.Param(
                self.param.time_range,
                widgets={
                    "time_range": {
                        "widget_type": pn.widgets.DatetimeRangeInput,
                        "format": "%Y-%m-%d %H:%M",
                        "start": tr_start,
                        "end": tr_end,
                    }
                },
            ),
        )
        plot_widgets = pn.Column(
            pn.WidgetBox(
                self.param.show_legend,
                self.param.legend_position,
            ),
            pn.WidgetBox(
                self.param.irregular_curve_connection,
                self.param.regular_curve_connection,
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
        tidal_filter_widget = pn.Param(
            self.param.do_tidal_filter,
            widgets={"do_tidal_filter": {"disabled": not _VTOOLS_AVAILABLE}},
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
            name="Clear Cache", button_type="light", icon="trash"
        )
        clear_cache_btn.on_click(_clear_cache_cb)
        transform_widgets = pn.Column(
            self.param.fill_gap,
            tidal_filter_widget,
            pn.Row(self.param.sensible_range_yaxis, self.param.sensible_percentile_range),
            clear_cache_btn,
        )
        widget_tabs = pn.Tabs(
            ("Time", control_widgets),
            ("Plot", plot_widgets),
            ("Transform", transform_widgets),
        )
        return widget_tabs

    def get_mobile_widgets(self):
        """Return a compact widget set for mobile: time range + key plot options."""
        time_widget = pn.Column(
            pn.pane.HTML("Time range:"),
            pn.Param(
                self.param.time_range,
                widgets={
                    "time_range": {
                        "widget_type": pn.widgets.DatetimeRangeInput,
                        "format": "%Y-%m-%d %H:%M",
                    }
                },
            ),
        )
        plot_opts = pn.Column(
            self.param.show_legend,
            self.param.color_cycle_name,
            self.param.shared_axes,
        )
        return pn.Column(time_widget, plot_opts, sizing_mode="stretch_width")

    def get_data(self, df):
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

        # Process each row, updating progress as we go
        for i, (_, r) in enumerate(df.iterrows()):
            if use_catalog:
                data = self.get_data_reference(r).getData(time_range=self.time_range)
            else:
                data, _, _ = self.get_data_for_time_range(r, self.time_range)

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
        return list(self.get_table_column_width_map().keys())

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
        column_width_map = self._get_table_column_width_map()
        if self.url_column:
            column_width_map[self.url_column] = "10%"
            if self.display_url_num and self.url_num_column:
                column_width_map[self.url_num_column] = "5%"
            self.adjust_column_width(column_width_map)
        # Always include ref_type so it is present in the Tabulator data slice
        # (required for hidden-but-filterable behaviour).  Width is small since
        # the column is hidden by default when all refs share the same type.
        column_width_map["ref_type"] = "8%"
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
        data = data[(data.index >= time_range[0]) & (data.index <= time_range[1])]

        # Apply optional data transformations
        if self.fill_gap > 0:
            data = data.interpolate(limit=self.fill_gap)
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
                    data = cosine_lanczos(data_for_filter, "40h")
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"Tidal filter (cosine_lanczos) failed and was skipped: {e}"
                    )

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
        if self.color_cycle_column:
            color_values = df[self.color_cycle_column].unique()
            style_maps["color"] = self.get_color_style_mapping(color_values)

        # Line style map
        if self.dashed_line_cycle_column:
            line_style_values = df[self.dashed_line_cycle_column].unique()
            style_maps["line"] = self.get_line_style_mapping(line_style_values)

        # Marker map
        if self.marker_cycle_column:
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
        return hv.Curve(data.iloc[:, [0]], label=label).opts(
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
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

        # URL-index label map (short unique names per URL when display_url_num is set)
        file_index_map = {}
        if manager.display_url_num:
            local_unique_urls = df[manager.url_column].unique()
            # Math references have NaN for url; exclude them from path shortening.
            valid_files = [f for f in local_unique_urls if not pd.isna(f)]
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

                data = manager._process_curve_data(data, row, time_range)

                file_index = (
                    file_index_map.get(row[manager.url_column], "")
                    if manager.display_url_num
                    else ""
                )
                curve = self.create_curve(data, row, unit, file_index=file_index)
                # Apply curve connection (interpolation) from manager params
                try:
                    connection = (
                        manager.irregular_curve_connection
                        if manager.is_irregular(row)
                        else manager.regular_curve_connection
                    )
                except NotImplementedError:
                    connection = manager.regular_curve_connection
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
            overlays.append(
                hv.Overlay(styled_curves).opts(
                    show_legend=manager.show_legend,
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
