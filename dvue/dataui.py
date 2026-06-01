# organize imports by category
import warnings

warnings.filterwarnings("ignore")
#
import numpy as np
import pandas as pd
import geopandas as gpd
from io import StringIO
from functools import lru_cache

# viz and ui
import hvplot.pandas  # noqa
import holoviews as hv
from holoviews import opts, dim, streams

hv.extension("bokeh")
import cartopy.crs as ccrs
import geoviews as gv

gv.extension("bokeh")
import param
import panel as pn
from panel.io import location

pn.extension("tabulator", notifications=True, design="native")
pn.extension("gridstack")  # for GridStack layout
#
from . import fullscreen
from .actions import (
    PlotAction,
    TabulateAction,
    PermalinkAction,
    DownloadDataAction,
    DownloadDataCatalogAction,
)

from bokeh.models import HoverTool
from bokeh.core.enums import MarkerType

import logging

import urllib.parse
from .utils import full_stack
from .catalog import DataCatalog, DataReference, CatalogView  # noqa: F401 – exposed for subclasses
from .views import ViewsManager, create_views_tab

# ---------------------------------------------------------------------------
# Standard DWR disclaimer — import and assign to disclaimer_text on any
# DataUIManager subclass to display it in the sidebar.
# ---------------------------------------------------------------------------
DWR_DISCLAIMER_TEXT = (
    "All information provided by the Department of Water Resources on its Web "
    "pages and Internet sites is made available to provide immediate access for "
    "the convenience of interested persons. While the Department believes the "
    "information to be reliable, human or mechanical error remains a "
    "possibility. Therefore, the Department does not guarantee the accuracy, "
    "completeness, timeliness, or correct sequencing of the information. "
    "Neither the Department of Water Resources nor any of the sources of the "
    "information shall be responsible for any errors or omissions, or for the "
    "use or results obtained from the use of this information. Other specific "
    "cautionary notices may be included on other Web pages maintained by the "
    "Department."
)

# setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Responsive CSS injected into FastListTemplate
# ---------------------------------------------------------------------------
_RESPONSIVE_CSS = """
/* Tablet: narrower sidebar */
@media (max-width: 1024px) {
    #sidebar {
        width: 330px !important;
        min-width: 280px !important;
    }
}
/* Wrap action buttons on narrow screens */
.action-bar-row {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}
"""

_MOBILE_CSS = """
/* Mobile: tighter spacing, larger touch targets */
.bk-btn {
    min-height: 40px;
    font-size: 14px;
}
.tabulator .tabulator-row {
    min-height: 44px;
}
/* Cards fill width */
.card {
    margin: 4px 0;
}
"""


class DataProvider(param.Parameterized):
    """Data-access base class for DataUI.

    Separates the **data-oriented** contract from view-oriented concerns.
    Subclass this when you only need the data layer without the full
    :class:`DataUIManager` view-configuration API (e.g. scripts, notebooks,
    automated pipelines).

    Catalog integration
    -------------------
    The preferred approach is to override the :attr:`data_catalog` property to
    return a :class:`~dvue.catalog.DataCatalog`.  When set:

    * :meth:`get_data_catalog` automatically calls
      ``data_catalog.to_dataframe().reset_index()``.
    * :meth:`get_data_reference` automatically looks up
      :class:`~dvue.catalog.DataReference` objects by name.
    * :meth:`get_data` automatically yields ``ref.getData()`` for each
      selected row.

    Manual override
    ---------------
    Override :meth:`get_data_catalog` and :meth:`get_data` directly for
    full control without a DataCatalog.  This is the pattern used by
    existing subclasses such as ``TimeSeriesDataUIManager`` subclasses that
    store the catalog as a plain :class:`pandas.DataFrame` attribute and
    override ``get_data_catalog()`` to return it directly — that pattern
    continues to work unchanged.

    Example – catalog-based provider
    ---------------------------------
    ::

        class HydroProvider(DataProvider):
            def __init__(self, data_dir, **params):
                super().__init__(**params)
                self._catalog = (
                    DataCatalog()
                    .add_reader(PatternCSVDirectoryReader("{name}__{stationid}__{source}"))
                    .add_source(data_dir)
                )

            @property
            def data_catalog(self):
                return self._catalog

        provider = HydroProvider("/data/hydro")
        df = provider.get_data_catalog()   # pandas DataFrame from catalog metadata
        ref = provider.get_data_reference(df.iloc[0])
        data = ref.getData()               # actual time-series DataFrame
    """

    @property
    def data_catalog(self) -> DataCatalog | None:
        """Return the underlying :class:`~dvue.catalog.DataCatalog`, or ``None``.

        Override this property to expose a :class:`~dvue.catalog.DataCatalog`
        to the UI layer.  When ``None`` (the default), :meth:`get_data_catalog`
        and :meth:`get_data` must be overridden manually.

        .. note::
            This property is intentionally named ``data_catalog`` (not
            ``catalog``) so that subclasses remain free to use ``self.catalog``
            as a plain instance attribute — which is the established pattern
            for :class:`~dvue.tsdataui.TimeSeriesDataUIManager` subclasses
            that store the catalog as a :class:`pandas.DataFrame`.
        """
        return None

    def get_data_catalog(self) -> pd.DataFrame:
        """Return the data catalog as a :class:`pandas.DataFrame`.

        This DataFrame drives the UI table and optional map.  When
        :attr:`catalog` is set the default implementation calls
        ``catalog.to_dataframe().reset_index()`` so that the reference name
        appears as a regular ``'name'`` column that :meth:`get_data_reference`
        can use for lookup.

        Override to provide or transform the DataFrame directly.

        Raises
        ------
        NotImplementedError
            When neither :attr:`data_catalog` nor a subclass override are provided.
        """
        cat = self.data_catalog
        if cat is not None:
            return cat.to_dataframe().reset_index()
        raise NotImplementedError(
            "Override get_data_catalog() or implement the catalog property in your subclass."
        )

    def get_data_reference(self, row: pd.Series) -> DataReference:
        """Return the :class:`~dvue.catalog.DataReference` for *row*.

        Default: resolves the reference name from the ``'name'`` column of
        *row* (present when the catalog DataFrame was built with
        ``reset_index()``) or from the row's string index value, then returns
        ``catalog.get(name)``.

        Override for non-standard row → reference mappings.

        Parameters
        ----------
        row : pd.Series
            A row from the DataFrame returned by :meth:`get_data_catalog`.

        Returns
        -------
        DataReference

        Raises
        ------
        NotImplementedError
            When :attr:`data_catalog` is not set and this method is not overridden.
        KeyError
            When the resolved name is not in the catalog.
        ValueError
            When the reference name cannot be determined from *row*.
        """
        cat = self.data_catalog
        if cat is None:
            raise NotImplementedError(
                "Override get_data_reference() or implement the catalog property in your subclass."
            )
        # When built via catalog.to_dataframe().reset_index(), 'name' is a column.
        # When built without reset_index(), 'name' is the DataFrame index.
        if "name" in row.index:
            ref_name = row["name"]
        elif isinstance(row.name, str):
            ref_name = row.name
        else:
            raise ValueError(
                "Cannot determine reference name from catalog row. "
                "Ensure the catalog DataFrame has a 'name' column or a string index."
            )
        return cat.get(ref_name)

    def get_data(self, df: pd.DataFrame, time_range=None):
        """Yield data :class:`pandas.DataFrame` objects for each selected row.

        Default: calls :meth:`get_data_reference` then ``.getData()`` for
        each row.  Override for custom retrieval or transformation logic
        (e.g. time-range slicing, unit conversion).

        Parameters
        ----------
        df : pd.DataFrame
            A subset of the catalog DataFrame (selected rows from the UI
            table).
        time_range : tuple of (start, end), optional
            Forwarded to :meth:`~dvue.catalog.DataReference.getData` so that
            only the requested time window is loaded (and cached under the
            same key used by the plot action).

        Yields
        ------
        pd.DataFrame
        """
        for _, row in df.iterrows():
            yield self.get_data_reference(row).getData(time_range=time_range)

    def create_panel(self, df: pd.DataFrame):
        """Return a Panel object for displaying the selected data.

        Override to provide a custom visualisation.

        Parameters
        ----------
        df : pd.DataFrame
            The selected rows from the catalog table.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("Override create_panel() in your subclass.")

    def get_station_ids(self, df: pd.DataFrame) -> list:
        """Return a list of unique station display names from the catalog."""
        return list((df.apply(self.build_station_name, axis=1).astype(str).unique()))

    def build_station_name(self, r: pd.Series) -> str:
        """Build a display name for a station row.  Override in your subclass."""
        raise NotImplementedError("Override build_station_name() in your subclass.")


class DataUIManager(DataProvider):
    """
    Full manager for DataUI: data layer + view layer.

    Combines :class:`DataProvider` (data access) with view-layer configuration
    for table display, map visualisation, and interactive widgets.
    Subclass :class:`DataProvider` alone when you only need the data layer.

    You **must** override all abstract methods in both layers that are relevant
    to your application.  Methods marked as optional may be left as-is or
    overridden for customisation.

    Data-layer methods (inherited from DataProvider)
    -------------------------------------------------
    Override **one** of the following strategies:

    * Set :attr:`~DataProvider.data_catalog` property → automatic DataFrame +
      data retrieval from a :class:`~dvue.catalog.DataCatalog`.
    * Override :meth:`~DataProvider.get_data_catalog` directly → supply your
      own DataFrame (the established pattern for
      :class:`~dvue.tsdataui.TimeSeriesDataUIManager` subclasses).

    Additional overrides available:

    * :meth:`~DataProvider.get_data_reference` – custom row → DataReference
      mapping.
    * :meth:`~DataProvider.get_data` – custom data loading / transformation.
    * :meth:`~DataProvider.create_panel` – custom visualisation panel.
    * :meth:`~DataProvider.build_station_name` – station display name from row.

    View-layer methods (must override)
    -----------------------------------
    * :meth:`get_table_column_width_map`
    * :meth:`get_table_filters`
    * :meth:`get_tooltips`
    * :meth:`get_map_color_columns`
    * :meth:`get_name_to_color`
    * :meth:`get_map_marker_columns`
    * :meth:`get_name_to_marker`

    Optional view-layer overrides
    ------------------------------
    * :meth:`get_widgets`
    * :meth:`get_data_actions`
    * :meth:`get_no_selection_message`
    * ``disclaimer_text`` param — set to any string to show a collapsible
      Disclaimer card at the bottom of the sidebar (use ``DWR_DISCLAIMER_TEXT``
      for the standard DWR disclaimer).
    """

    disclaimer_text = param.String(
        default=None,
        allow_None=True,
        doc=(
            "Text for a collapsible Disclaimer card shown at the bottom of the "
            "sidebar. Set to DWR_DISCLAIMER_TEXT for the standard DWR disclaimer, "
            "or any other string for a custom notice. Leave None to omit."
        ),
    )
    show_permalink = param.Boolean(
        default=False,
        doc="Show the 'Permalink' button in the action bar. Set to False to hide it.",
    )
    show_reset_session_button = param.Boolean(
        default=False,
        doc="Show a 'Reset Session' button at the right end of the action bar.",
    )
    session_cookie_name = param.String(
        default="dvue_user_id",
        doc=(
            "Name of the persistent user-identity cookie cleared by the "
            "'Reset Session' button.  Must match the cookie_name used by "
            "install_session_handler() / SessionManager."
        ),
    )

    # View-layer hint: cartopy CRS for the map panel.  Set as an instance
    # attribute in subclass ``__init__`` (e.g. ``self.crs = ccrs.UTM(10)``).
    # When ``None`` (default), ``serve_session_app`` / ``serve_desktop_app``
    # will try to read it from the manager before falling back to no map.
    crs = None

    @classmethod
    def help(cls):
        """
        Print a summary of required methods and their purpose.
        """
        print(cls.__doc__)
        for name, method in cls.__dict__.items():
            if getattr(method, "__isabstractmethod__", False):
                print(f"- {name}: {method.__doc__}")

    def get_widgets(self) -> dict:
        """
        Return a dict mapping tab label → Panel content Column for the sidebar.

        Override to provide custom widgets.  Return a ``dict`` with string keys
        (tab labels) and Panel layout values (one per sidebar tab).  The base
        implementation returns an empty dict; ``create_view()`` falls back to a
        plain "Options" tab when a non-dict value is returned (backward-compat
        for subclasses that have not yet been migrated).
        """
        return {}

    # ------------------------------------------------------------------
    # Table / view configuration
    # ------------------------------------------------------------------

    def get_table_columns(self) -> list:
        """
        Return the list of columns to display in the table. By default, uses keys from get_table_column_width_map().
        """
        return list(self.get_table_column_width_map().keys())

    def get_table_schema(self, df: pd.DataFrame | None = None) -> dict:
        """Return a formal table schema contract.

        Subclasses can override this to explicitly declare column ownership.
        Supported keys:
        - ``required_columns``: ordered columns always shown when present
        - ``optional_columns``: ordered candidate columns
        - ``hidden_by_default``: columns hidden at startup
        - ``drop_if_all_null``: whether optional all-null columns are dropped
        - ``column_widths``: explicit width map (``{"col": "10%"}``)
        - ``filters``: explicit filter config map

        The default returns an empty schema.  Subclasses should override this
        directly to declare column ownership.  Legacy subclasses that override
        ``get_table_column_width_map()`` and ``get_table_filters()`` still work
        because those public methods are called directly by the framework.
        """
        return {
            "required_columns": [],
            "optional_columns": [],
            "hidden_by_default": [],
            "drop_if_all_null": False,
            "column_widths": {},
            "filters": {},
        }

    def get_hidden_table_columns(self, df: pd.DataFrame | None = None) -> list[str]:
        """Return columns that should be hidden when the table first renders."""
        return []

    def get_table_column_width_map(self) -> dict:
        """Return a dict mapping column names to width strings (e.g. ``'10%'``).

        The default proxies to ``get_table_schema()["column_widths"]``.
        Legacy subclasses that override this method directly still work
        because Python's MRO will call the override, not this base.
        """
        return dict(self.get_table_schema().get("column_widths", {}))

    def get_table_filters(self) -> dict:
        """Return a dict specifying filter widgets for each column.

        The default proxies to ``get_table_schema()["filters"]``.
        Legacy subclasses that override this method directly still work
        because Python's MRO will call the override, not this base.
        """
        return dict(self.get_table_schema().get("filters", {}))

    @lru_cache(maxsize=128)
    def get_no_selection_message(self) -> str:
        """
        Return the message to be displayed when no selection is made.
        Reads from dataui.noselection.html.
        """
        import os

        resource_path = os.path.join(os.path.dirname(__file__), "dataui.noselection.html")
        with open(resource_path, "r") as file:
            no_selection_message = file.read()
        return no_selection_message

    def get_tooltips(self) -> list:
        """
        Return a list of tooltips for map features.

        You must override this in your subclass.
        """
        raise NotImplementedError("You must implement get_tooltips() in your subclass.")

    def get_map_color_columns(self) -> list:
        """
        Return the columns that can be used to color the map.

        You must override this in your subclass.
        """
        raise NotImplementedError("You must implement get_map_color_columns() in your subclass.")

    def get_name_to_color(self) -> dict:
        """
        Return a dictionary mapping column names to color names.

        You must override this in your subclass.
        """
        raise NotImplementedError("You must implement get_name_to_color() in your subclass.")

    def get_map_marker_columns(self) -> list:
        """
        Return the columns that can be used to set map marker types.

        You must override this in your subclass.
        """
        raise NotImplementedError("You must implement get_map_marker_columns() in your subclass.")

    def get_name_to_marker(self) -> dict:
        """
        Return a dictionary mapping column names to marker names. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_name_to_marker().")

    def get_map_option_widgets(self):
        """Return extra Panel widgets to append to the Map Options sidebar tab.

        Override in a subclass to add parameter-type filters, year-range
        sliders, or any other map-side controls.  Return ``None`` (default)
        to add nothing.
        """
        return None

    def get_sidebar_disclaimer(self):
        """Return a Panel pane with disclaimer content for the modal dialog,
        or ``None`` (default) to add nothing.

        Set the ``disclaimer_text`` param to have it rendered automatically,
        or override this method to return a custom Panel component.
        """
        if not self.disclaimer_text:
            return None
        return pn.pane.Markdown(
            f"## Disclaimer\n\n{self.disclaimer_text}",
            sizing_mode="stretch_width",
        )

    def get_version(self) -> str:
        """Return a version string for the running application.

        Override in a subclass to return the app-specific package version.
        The default reads the dvue package version.
        """
        try:
            from dvue._version import version
            return version
        except Exception:
            return "unknown"

    def get_about_text(self) -> str:
        """Return a markdown string shown in the About modal.

        Override in a subclass to provide application-specific information.
        Return an empty string (the default) to show only the auto-generated
        version block.
        """
        return ""

    def get_data_actions(self) -> list:
        """Return a list of default data actions. Override to customize available actions."""
        plot_action = PlotAction()
        tabulate_action = TabulateAction()
        download_action = DownloadDataAction()
        permalink_action = PermalinkAction()
        download_catalog = DownloadDataCatalogAction()
        plot_button = dict(
            name="Plot",
            button_type="primary",
            icon="chart-line",
            action_type="display",
            callback=plot_action.callback,
        )
        tabulate_button = dict(
            name="Tabulate",
            button_type="primary",
            icon="table",
            action_type="display",
            callback=tabulate_action.callback,
        )
        # Combine Download Data + Download Catalog into a single MenuButton.
        download_menu_button = dict(
            name="Download",
            button_type="success",
            icon="download",
            action_type="download_menu",
            items=[
                dict(label="Download Data",    filename="data.csv",    callback=download_action.callback),
                dict(label="Download Catalog", filename="catalog.csv", callback=download_catalog.callback),
            ],
        )
        permalink_button = dict(
            name="Permalink",
            button_type="primary",
            icon="link",
            action_type="link",
            callback=permalink_action.callback,
        )
        actions = [plot_button, tabulate_button, download_menu_button]
        if self.show_permalink:
            actions.append(permalink_button)
        return actions

    def get_mobile_table_columns(self) -> list:
        """Return column names to display in the condensed mobile table.

        Override in subclasses to customize. Defaults to the first 4 columns
        from :meth:`get_table_column_width_map`.
        """
        return list(self.get_table_column_width_map().keys())[:4]

    def get_mobile_actions(self) -> list:
        """Return actions for the mobile action bar.

        Override in subclasses to customize. Defaults to Plot only.
        """
        actions = self.get_data_actions()
        return [a for a in actions if a.get("name") == "Plot"][:1]

    def get_mobile_widgets(self):
        """Return a Panel component with mobile-friendly widget controls.

        Override in subclasses to provide a compact widget set.
        Defaults to None (no extra controls beyond the action bar).
        """
        return None


notifications = pn.state.notifications


class DataUI(param.Parameterized):
    """
    Show table (and map) of data from a catalog. If the catalog manager returns a catalog that is a GeoDataFrame it will display a map of the data.

    Selection on table rows or map will select the corresponding rows in the other view (map or table). It supports 1-to-many mapping of stations to rows in the catalog.

    Actions on the selections are supported via the buttons on the table. These are configurable by the catalog manager.
    """

    view_type = param.Selector(
        objects=["combined", "table", "display"],
        default="combined",
        doc="Type of view to display: combined, table only, or display panel only",
    )

    map_color_category = param.Selector(
        objects=[],
        doc="Options for the map color category selection",
    )
    show_map_colors = param.Boolean(default=True, doc="Show map colors for selected category")
    map_marker_category = param.Selector(
        objects=[],
        doc="Options for the map marker category selection",
    )
    show_map_markers = param.Boolean(default=False, doc="Show map markers for selected category")
    map_default_span = param.Number(default=15000, doc="Default span for map zoom in meters")
    map_non_selection_alpha = param.Number(default=0.2, doc="Non selection alpha")
    map_point_size = param.Number(default=10, doc="Point size for map")

    query = param.String(
        default="",
        doc='Query to filter stations. See <a href="https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html">Pandas Query</a> for details. E.g. max_year <= 2023',
    )
    use_regex_filter = param.Boolean(
        default=False,
        doc="Use regex for table filtering instead of 'like' functionality",
    )
    map_filters_table = param.Boolean(
        default=True,
        doc="When enabled, map clicks filter the table rows instead of highlighting them. The map is unaffected by table changes in this mode.",
    )
    # --- Proxy params: only updated when map_filters_table=False so the map
    # DynamicMap is decoupled from table events while filter mode is active. ---
    _map_sel_proxy = param.List(default=[], precedence=-1)
    _map_filter_proxy = param.Integer(default=0, precedence=-1)

    def __init__(self, dataui_manager, crs=ccrs.PlateCarree(), station_id_column=None, **kwargs):
        self._crs = crs
        self._station_id_column = station_id_column
        super().__init__(**kwargs)
        self._dataui_manager = dataui_manager
        self._dataui_manager._dataui = self  # insert a reference to self in the dataui_manager for progress bar updates for example
        self._dfcat_full = self._dataui_manager.get_data_catalog()
        self._dfcat = self._dfcat_full
        self._views_manager = ViewsManager()
        self.param.map_color_category.objects = self._dataui_manager.get_map_color_columns() or []
        if self.param.map_color_category.objects:
            self.map_color_category = self.param.map_color_category.objects[0]
        self.param.map_marker_category.objects = self._dataui_manager.get_map_marker_columns() or []
        if self.param.map_marker_category.objects:
            self.map_marker_category = self.param.map_marker_category.objects[0]
        self._dfmapcat = self._get_map_catalog()
        self._map_filter_station_ids = None
        self._dfcat_before_map_filter = None

        if isinstance(self._dfcat, gpd.GeoDataFrame):
            self._tmap = gv.tile_sources.CartoLight()
            self.build_map_of_features(self._dfmapcat, crs=self._crs)
            if hasattr(self, "_station_select"):
                self._station_select.source = self._map_features
            else:
                self._station_select = streams.Selection1D(source=self._map_features)
        else:
            warnings.warn("No geolocation data found in catalog. Not displaying map of stations.")

    def _get_map_catalog(self):
        if (
            isinstance(self._station_id_column, str)
            and self._station_id_column in self._dfcat.columns
        ):
            dfx = self._dfcat.groupby(self._station_id_column).first().reset_index()
            if isinstance(dfx, gpd.GeoDataFrame):
                dfx = dfx.dropna(subset=["geometry"])
                dfx = dfx.set_crs(self._dfcat.crs)
            else:
                pass
                # dfx = dfx.dropna(subset=["Latitude", "Longitude"])  # FIXME: ?
        else:
            dfx = self._dfcat
        return dfx

    def build_map_of_features(
        self,
        dfmap,
        crs,
        show_color_by=None,
        color_by=None,
        non_selection_alpha=None,
        point_size=None,
    ):
        # Fall back to current param values when called without explicit arguments (e.g. from __init__)
        if show_color_by is None:
            show_color_by = self.show_map_colors
        if color_by is None:
            color_by = self.map_color_category
        if non_selection_alpha is None:
            non_selection_alpha = self.map_non_selection_alpha
        if point_size is None:
            point_size = self.map_point_size

        tooltips = self._dataui_manager.get_tooltips()
        # if station_id column is defined then consolidate the self._dfcat into a single row per station
        # this is useful when we have multiple rows per station
        hover = HoverTool(tooltips=tooltips)

        # Pre-compute categorical colors as a concrete column BEFORE building the
        # GeoViews element.  Bokeh 3.9+ strictly validates DataSpec properties
        # (fill_color, line_color) and rejects HoloViews dim().categorize()
        # objects.  A plain string field-reference ("_dvue_color") is always
        # accepted by Bokeh.
        _COLOR_COL = "_dvue_color"
        color_dict = None
        column_color = "Category10"  # default; re-resolved below if show_color_by
        if show_color_by:
            name_to_color = self._dataui_manager.get_name_to_color()
            column_color = (
                name_to_color.get(color_by, "Category10")
                if isinstance(name_to_color, dict)
                else name_to_color
            )
            if isinstance(column_color, dict):
                color_dict = column_color
            elif isinstance(column_color, list):
                unique_vals = list(dfmap[color_by].unique()) if color_by in dfmap.columns else []
                color_dict = {v: column_color[i % len(column_color)] for i, v in enumerate(unique_vals)}
            if color_dict is not None:
                dfmap = dfmap.copy()
                dfmap[_COLOR_COL] = dfmap[color_by].map(lambda v: color_dict.get(v, "blue"))

        # check if the dfmap is a geodataframe
        try:
            if isinstance(dfmap, gpd.GeoDataFrame):
                if dfmap.empty:
                    # Keep an empty but valid Points element so map widgets still render.
                    self._map_source_df = pd.DataFrame(columns=["__x__", "__y__"])
                    self._map_features = gv.Points(
                        pd.DataFrame(columns=["__x__", "__y__"]),
                        kdims=["__x__", "__y__"],
                        crs=crs,
                    )
                    return self._map_features
                geom_type = str.lower(str(dfmap.geometry.iloc[0].geom_type))
                if "point" in geom_type:
                    # Passing a GeoDataFrame directly to gv.Points fails in newer
                    # geoviews versions (GeomDictInterface "non-flat" error during
                    # projection).  Extract explicit x/y columns to work around it.
                    #
                    # Pre-project geographic (lat/lon, e.g. EPSG:4326) data to Web
                    # Mercator so GeoViews does not need to re-project at render time.
                    # This avoids two problems:
                    #   1. The ~28 km northing offset that arises when cartopy maps
                    #      PlateCarree (WGS84 ellipsoid) to spherical Web Mercator.
                    #   2. AttributeError: 'Geodetic' has no 'y_limits' in
                    #      geoviews.util.project_extents.
                    _dfmap_pts = dfmap
                    _pts_crs = crs
                    if (
                        isinstance(dfmap, gpd.GeoDataFrame)
                        and dfmap.crs is not None
                        and dfmap.crs.is_geographic
                    ):
                        _dfmap_pts = dfmap.to_crs("EPSG:3857")
                        _pts_crs = ccrs.GOOGLE_MERCATOR
                    dfpts = _dfmap_pts.copy()
                    dfpts["__x__"] = _dfmap_pts.geometry.x
                    dfpts["__y__"] = _dfmap_pts.geometry.y
                    dfpts = dfpts.drop(columns=["geometry"])
                    # Drop columns containing non-scalar objects (e.g. reader instances
                    # stored in the 'source' attribute) that pandas cannot sort/compare.
                    scalar_cols = [
                        c for c in dfpts.columns
                        if dfpts[c].dtype != object
                        or dfpts[c].map(lambda v: isinstance(v, (str, type(None)))).all()
                    ]
                    dfpts = dfpts[scalar_cols]
                    self._map_features = gv.Points(dfpts, kdims=["__x__", "__y__"], crs=_pts_crs)
                    self._map_source_df = dfpts  # one row per feature
                elif "linestring" in geom_type:
                    self._map_features = gv.Path(dfmap, crs=crs)
                    self._map_source_df = pd.DataFrame(dfmap.drop(columns=["geometry"]))  # one row per path
                elif "polygon" in geom_type:
                    self._map_features = gv.Polygons(dfmap, crs=crs)
                    self._map_source_df = pd.DataFrame(dfmap.drop(columns=["geometry"]))  # one row per polygon
                else:  # pragma: no cover
                    raise ValueError("Unknown geometry type " + geom_type)
        except Exception as e:
            logger.error(f"Error building map of features: {e}")
            self._map_source_df = pd.DataFrame(columns=["__x__", "__y__"])
            self._map_features = gv.Points(
                pd.DataFrame(columns=["__x__", "__y__"]),
                kdims=["__x__", "__y__"],
                crs=crs,
            )
        if show_color_by:
            if color_dict is not None:
                # Use pre-computed color column — Bokeh 3.9+ accepts a plain field
                # reference; dim().categorize() is rejected by strict DataSpec validation.
                if isinstance(self._map_features, gv.Points):
                    self._map_features = self._map_features.opts(color=_COLOR_COL)
                elif isinstance(self._map_features, gv.Path):
                    self._map_features = self._map_features.opts(line_color=_COLOR_COL)
                elif isinstance(self._map_features, gv.Polygons):
                    self._map_features = self._map_features.opts(fill_color=_COLOR_COL)
                else:
                    self._map_features = self._map_features.opts(color=_COLOR_COL)
            else:
                # Named colormap string (e.g. "Category10", "Viridis") — HoloViews
                # routes this through a proper Bokeh ColorMapper, which is accepted.
                if isinstance(self._map_features, gv.Points):
                    self._map_features = self._map_features.opts(
                        color=dim(color_by), cmap=column_color
                    )
                elif isinstance(self._map_features, gv.Path):
                    self._map_features = self._map_features.opts(
                        line_color=dim(color_by), cmap=column_color
                    )
                elif isinstance(self._map_features, gv.Polygons):
                    self._map_features = self._map_features.opts(
                        fill_color=dim(color_by), cmap=column_color
                    )
                else:
                    self._map_features = self._map_features.opts(
                        color=dim(color_by), cmap=column_color
                    )
        else:
            if isinstance(self._map_features, gv.Points):
                self._map_features = self._map_features.opts(color="blue")
            elif isinstance(self._map_features, gv.Path):
                self._map_features = self._map_features.opts(line_color="blue")
            elif isinstance(self._map_features, gv.Polygons):
                self._map_features = self._map_features.opts(fill_color="blue")
            else:
                self._map_features = self._map_features.opts(color="blue")
        if isinstance(self._map_features, gv.Points):
            self._map_features = self._map_features.opts(
                opts.Points(
                    tools=["tap", hover, "lasso_select", "box_select"],
                    nonselection_alpha=non_selection_alpha,
                    size=point_size,
                )
            )
        elif isinstance(self._map_features, gv.Path):
            self._map_features = self._map_features.opts(
                opts.Path(
                    tools=["tap", hover, "lasso_select", "box_select"],
                    nonselection_alpha=non_selection_alpha,
                    line_width=2,
                )
            )
        elif isinstance(self._map_features, gv.Polygons):
            self._map_features = self._map_features.opts(
                opts.Polygons(
                    tools=["tap", hover, "lasso_select", "box_select"],
                    nonselection_alpha=non_selection_alpha,
                )
            )
        else:
            raise "Unknown map feature type " + str(type(self._map_features))
        self._map_features = self._map_features.opts(active_tools=["wheel_zoom"], responsive=True)
        return self._map_features

    def update_map_features(
        self,
        show_color_by,
        color_by,
        show_marker_by,
        marker_by,
        query,
        selection,
        map_default_span,
        map_non_selection_alpha,
        map_point_size,
        filters=None,  # not read directly; kept for call-site compatibility
    ):
        """Update the map features based on the selection in the table or filters or query. Also updates if the color or marker by columns are changed"""
        query = query.strip()
        dfs = self._get_map_catalog()
        if self.map_filters_table:
            # In filter mode the map is decoupled from the table: always show the
            # full map catalog (not restricted to display_table.current_view) and
            # never highlight features based on table selection while this mode is active.
            current_view = dfs
            if isinstance(current_view, gpd.GeoDataFrame):
                current_view = current_view.loc[current_view.is_valid]
            current_selection = []
        else:
            # select only those rows in dfs that have station_id_column in self.display_table.current_view
            if (
                self._station_id_column
                and self._station_id_column in self.display_table.current_view.columns
            ):
                current_view = dfs[
                    dfs[self._station_id_column].isin(
                        self.display_table.current_view[self._station_id_column]
                    )
                ]
                # if current_view is a geodataframe, keep only valid geometries
                if isinstance(current_view, gpd.GeoDataFrame):
                    current_view = current_view.loc[current_view.is_valid]
                current_table_selected = self._dfcat.iloc[selection]
                current_selected = current_view[
                    current_view[self._station_id_column].isin(
                        current_table_selected[self._station_id_column]
                    )
                ]
            else:
                current_view = dfs.loc[self.display_table.current_view.index]
                if isinstance(current_view, gpd.GeoDataFrame):
                    current_view = current_view.loc[current_view.is_valid]
                current_table_selected = self._dfcat.iloc[selection]
                current_selected = current_table_selected
            # Filter out -1 entries: math refs with NaN geometry are absent from
            # current_view (filtered by .is_valid above) so get_indexer returns -1
            # for them. Passing -1 to Bokeh's selected= is interpreted as "last row"
            # (Python-style negative index), selecting the wrong geo point on the map.
            current_selection = [
                i for i in current_view.index.get_indexer(current_selected.index).tolist()
                if i >= 0
            ]
        try:
            if len(query) > 0:
                current_view = current_view.query(query)
        except Exception as e:
            str_stack = full_stack()
            logger.error(str_stack)
            notifications.error(f"Error while fetching data for {str_stack}", duration=0)
        # Pass values directly to build_map_of_features instead of assigning to self params,
        # which would fire param events on the same params pn.bind is watching and create a
        # reactive cycle that Panel would suppress.
        self._map_features = self.build_map_of_features(
            current_view,
            self._crs,
            show_color_by=show_color_by,
            color_by=color_by,
            non_selection_alpha=map_non_selection_alpha,
            point_size=map_point_size,
        )
        if isinstance(self._map_features, gv.Points):
            if show_marker_by:
                name_to_marker = self._dataui_manager.get_name_to_marker()
                # get_name_to_marker() returns {column_name: {value: marker}} or a list.
                # Extract the per-column dict before passing to categorize().
                if isinstance(name_to_marker, dict):
                    column_marker = name_to_marker.get(marker_by, {})
                elif isinstance(name_to_marker, list):
                    unique_vals = list(current_view[marker_by].unique()) if marker_by in current_view.columns else []
                    column_marker = {v: name_to_marker[i % len(name_to_marker)] for i, v in enumerate(unique_vals)}
                else:
                    column_marker = {}
                self._map_features = self._map_features.opts(
                    marker=dim(marker_by).categorize(column_marker, default="circle")
                )
            else:
                self._map_features = self._map_features.opts(marker="circle")
        with param.discard_events(self._station_select):
            self._map_features = self._map_features.opts(
                default_span=map_default_span,  # for max zoom this is the default span in meters
                selected=current_selection,
            )
        return self._map_features

    # ------------------------------------------------------------------
    # Map-filter-table helpers
    # ------------------------------------------------------------------

    def _get_table_df_for_display(self, df):
        """Convert *df* (possibly a GeoDataFrame) to a plain DataFrame with the
        same columns as the current Tabulator widget, ready for assignment to
        ``display_table.value``."""
        try:
            tbl_df = pd.DataFrame(df) if isinstance(df, gpd.GeoDataFrame) else df
        except Exception:
            tbl_df = df
        tbl_cols = self.display_table.value.columns
        return tbl_df.reindex(columns=tbl_cols)

    def _refresh_table_with_map_filter(self):
        """Restrict the Tabulator to rows matching ``_map_filter_station_ids``."""
        ids = self._map_filter_station_ids
        if ids is None:
            return
        idcol = self._station_id_column
        if idcol and idcol in self._dfcat.columns:
            filtered = self._dfcat[self._dfcat[idcol].isin(ids)]
        else:
            # ids is a set of index labels when there is no station_id_column
            filtered = self._dfcat.loc[self._dfcat.index.isin(ids)]
        tbl_df = self._get_table_df_for_display(filtered)
        self.display_table.param.update(value=tbl_df, selection=[])

    def _clear_map_filter(self):
        """Remove the active map filter and restore the full catalog in the table."""
        if self._map_filter_station_ids is None:
            return
        self._map_filter_station_ids = None
        tbl_df = self._get_table_df_for_display(self._dfcat)
        self.display_table.param.update(value=tbl_df, selection=[])

    def _on_map_filter_mode_changed(self, event):
        """Restore normal table and re-sync map selection when filter mode is toggled off."""
        if not event.new:
            self._clear_map_filter()
            # Re-sync proxy param with current table selection so map highlights
            # immediately reflect any rows selected while filter mode was active.
            self._map_sel_proxy = list(self.display_table.selection)

    def _apply_map_filter(self, index):
        """Apply a filter to the table based on the *index* of selected map features.

        Called instead of the normal ``select_data_catalog`` path when
        ``map_filters_table`` is True.
        """
        # Empty selection — clear any active filter
        if not index:
            self._clear_map_filter()
            return
        map_df = self._map_features.dframe()
        n_map = len(map_df)
        index = [i for i in index if 0 <= i < n_map]
        if not index:
            self._clear_map_filter()
            return
        idcol = self._station_id_column
        if idcol and idcol in self._dfcat.columns:
            self._map_filter_station_ids = set(map_df.iloc[index][idcol].tolist())
        else:
            # Use the index labels of the matching rows in _dfcat
            dfs = map_df.iloc[index]
            merged_indices = self._dfcat.reset_index().merge(dfs)["index"].to_list()
            self._map_filter_station_ids = set(merged_indices)
        self._refresh_table_with_map_filter()

    def select_data_catalog(self, index=[]):
        """Select the rows in the table that correspond to the selected features in the map"""
        if index is None or (len(index) == 1 and index[0] == -1):
            return

        # In filter mode: filter the table to selected map features instead of
        # highlighting rows.  The map itself is not rebuilt.
        if self.map_filters_table:
            self._apply_map_filter(index)
            return

        idcol = self._station_id_column
        table = self.display_table

        # Guard against stale indices from Selection1D when the map has been redrawn
        # with fewer features (e.g. after a filter change or catalog rebuild).
        # Use the stored source DataFrame (one row per feature) rather than .dframe(),
        # which returns vertex-level rows for gv.Path (many rows per feature) and
        # would map Selection1D path-indices to the wrong catalog rows.
        map_df = getattr(self, "_map_source_df", None)
        if map_df is None:
            map_df = self._map_features.dframe()
        n_map = len(map_df)
        index = [i for i in index if 0 <= i < n_map]
        if not index:
            return

        if idcol and idcol in self._dfcat.columns:
            # get station ids from the _map_features being displayed
            stations_map_selected = map_df.iloc[index][idcol].unique()
            # get the stations selected in table already
            stations_table_selected = table.selected_dataframe[idcol].unique()
            # get stations in stations_map_selected that are not in stations_selected
            stations_to_be_selected = list(
                set(stations_map_selected) - set(stations_table_selected)
            )
            # get the indices of the stations that are not in the selected stations in the current view
            current_view_selected_indices = table.current_view[
                table.current_view[idcol].isin(stations_to_be_selected)
            ].index.to_list()
            # First get the indices of matching rows
            matching_indices = table.selected_dataframe[
                table.selected_dataframe[idcol].isin(stations_map_selected)
            ].index

            # Then convert to integer positions (iloc indices)
            keep_selected_from_map = [i for i in map(int, self._dfcat.index.get_indexer(matching_indices)) if i >= 0]
            i_selected_indices = [i for i in map(int, self._dfcat.index.get_indexer(current_view_selected_indices)) if i >= 0]
            selected_indices = i_selected_indices + list(keep_selected_from_map)
        else:
            dfs = map_df.iloc[index]
            merged_indices = (
                self._dfcat.reset_index().merge(dfs)["index"].to_list()
            )  # index matching
            geo_selected_indices = [
                i for i in self._dfcat.index.get_indexer(merged_indices).tolist()
                if i >= 0
            ]  # positional indices on table; get_indexer returns -1 for unresolved items
            # Preserve currently-selected rows that have no map representation
            # (e.g. math refs with NaN geometry excluded from _map_features).
            # Without this guard, every map click silently deselects them.
            non_geo_positions = []
            if isinstance(self._dfcat, gpd.GeoDataFrame) and table.selection:
                has_no_geo = self._dfcat.geometry.isna()
                non_geo_positions = [
                    i for i in table.selection
                    if i < len(self._dfcat) and has_no_geo.iloc[i]
                ]
            selected_indices = sorted(set(non_geo_positions + geo_selected_indices))
        # with param.discard_events(table.param.selection):
        table.param.update(selection=selected_indices)

    # ------------------------------------------------------------------
    # Map-filter-table helpers
    # ------------------------------------------------------------------

    def _get_table_df_for_display(self, df):
        """Return *df* as a plain DataFrame with the Tabulator's current column set."""
        try:
            tbl_df = pd.DataFrame(df) if isinstance(df, gpd.GeoDataFrame) else df
        except Exception:
            tbl_df = df
        return tbl_df.reindex(columns=self.display_table.value.columns)

    def _refresh_table_with_map_filter(self):
        """Restrict the catalog and table to rows matching ``_map_filter_station_ids``."""
        ids = self._map_filter_station_ids
        if ids is None:
            return
        # Save the pre-filter catalog once so we can restore it on clear.
        if self._dfcat_before_map_filter is None:
            self._dfcat_before_map_filter = self._dfcat
        full = self._dfcat_before_map_filter
        idcol = self._station_id_column
        if idcol and idcol in full.columns:
            filtered = full[full[idcol].isin(ids)]
        else:
            filtered = full.loc[full.index.isin(ids)]
        # Keep _dfcat in sync with display_table.value so _dfcat.iloc[selection]
        # in actions always refers to the correct rows.
        self._dfcat = filtered
        self.display_table.param.update(value=self._get_table_df_for_display(filtered), selection=[])

    def _clear_map_filter(self):
        """Restore the pre-filter catalog and table."""
        if self._map_filter_station_ids is None:
            return
        self._map_filter_station_ids = None
        if self._dfcat_before_map_filter is not None:
            self._dfcat = self._dfcat_before_map_filter
            self._dfcat_before_map_filter = None
        self.display_table.param.update(value=self._get_table_df_for_display(self._dfcat), selection=[])

    def _apply_map_filter(self, index):
        """Filter the table to map features at *index*; clear filter on empty click."""
        if not index:
            self._clear_map_filter()
            return
        map_df = self._map_features.dframe()
        n_map = len(map_df)
        index = [i for i in index if 0 <= i < n_map]
        if not index:
            self._clear_map_filter()
            return
        # Use the pre-filter catalog as the source of truth so successive clicks
        # always filter from the full (view-filtered) catalog, not a sub-filter.
        src = self._dfcat_before_map_filter if self._dfcat_before_map_filter is not None else self._dfcat
        idcol = self._station_id_column
        if idcol and idcol in src.columns:
            self._map_filter_station_ids = set(map_df.iloc[index][idcol].tolist())
        else:
            dfs = map_df.iloc[index]
            merged_indices = src.reset_index().merge(dfs)["index"].to_list()
            self._map_filter_station_ids = set(merged_indices)
        self._refresh_table_with_map_filter()

    def _on_map_filter_mode_changed(self, event):
        """Restore normal table when filter mode is toggled off."""
        if not event.new:  # switched OFF
            self._clear_map_filter()
            # Re-sync proxy so map highlights reflect current table selection immediately.
            self._map_sel_proxy = list(self.display_table.selection)

    def create_data_actions(self, actions):
        action_buttons = []
        for action in actions:
            if action["action_type"] == "download_menu":
                # MenuButton dropdown — each item triggers the matching download
                # callback via a hidden FileDownload widget.  The FileDownload
                # is included in the Row so it is part of the Bokeh document;
                # incrementing _clicks from Python syncs to the browser and
                # triggers the server-side file endpoint.
                items_cfg = action["items"]
                _item_map = {it["label"]: it for it in items_cfg}
                _selected = [items_cfg[0]]  # mutable selection container

                def _dynamic_dl_callback():
                    import asyncio
                    sio = _selected[0]["callback"](None, self)
                    pn.state.curdoc.add_next_tick_callback(
                        lambda: asyncio.create_task(self._hide_progress_after_delay())
                    )
                    return sio

                _trigger_dl = pn.widgets.FileDownload(
                    label="",
                    callback=_dynamic_dl_callback,
                    filename=items_cfg[0]["filename"],
                    embed=False,
                    auto=True,
                    visible=False,
                    width=0,
                    height=0,
                    margin=(0, 0, 0, 0),
                )

                menu_btn = pn.widgets.MenuButton(
                    name=action["name"],
                    items=[it["label"] for it in items_cfg],
                    button_type=action.get("button_type", "success"),
                    icon=action.get("icon", "download"),
                    height=32,
                    margin=(0, 4, 0, 0),
                )

                def _on_dl_menu(event):
                    cfg = _item_map.get(event.new)
                    if cfg is None:
                        return
                    _selected[0] = cfg
                    _trigger_dl.filename = cfg["filename"]
                    _trigger_dl._clicks += 1

                menu_btn.on_click(_on_dl_menu)
                button = pn.Row(menu_btn, _trigger_dl, align="center", margin=(0, 2, 0, 0))
            elif action["action_type"] == "download":
                # Create a closure that captures the current action
                def create_download_callback(current_action):
                    def _download_callback():
                        sio = current_action["callback"](None, self)
                        # Hide progress when download is initiated
                        import asyncio

                        pn.state.curdoc.add_next_tick_callback(
                            lambda: asyncio.create_task(self._hide_progress_after_delay())
                        )
                        if sio:
                            return sio
                        else:
                            return None

                    return _download_callback

                # Pass the current action to create a specific callback function for this action
                button = pn.widgets.FileDownload(
                    label=action["name"],
                    callback=create_download_callback(action),
                    filename=action["filename"],
                    button_type=action["button_type"],
                    icon=action["icon"],
                    embed=False,
                )
            elif action["action_type"] == "menu":
                # MenuButton: each item maps to a named callback.
                # ``event.new`` contains the selected item label.
                button = pn.widgets.MenuButton(
                    name=action["name"],
                    items=action["items"],
                    button_type=action["button_type"],
                    icon=action.get("icon", ""),
                )

                def create_menu_handler(current_action):
                    def on_click(event):
                        cb = current_action["callbacks"].get(event.new)
                        if cb is not None:
                            cb(event, self)

                    return on_click

                button.on_click(create_menu_handler(action))
            else:
                button = pn.widgets.Button(
                    name=action["name"],
                    button_type=action["button_type"],
                    icon=action["icon"],
                )

                # For regular buttons, we can use a function factory to create a proper closure
                def create_click_handler(current_action):
                    def on_click(event):
                        current_action["callback"](event, self)

                    return on_click

                button.on_click(create_click_handler(action))

            action_buttons.append(button)
        return action_buttons

    async def _hide_progress_after_delay(self):
        """Hide the progress bar after a short delay to show completion"""
        import asyncio

        await asyncio.sleep(0.5)
        self.hide_progress()

    @param.depends("use_regex_filter", watch=True)
    def update_data_table_filters(self):
        """Update the table filters based on the use_regex_filter parameter."""
        if self.use_regex_filter:
            # Update filters to use regex
            for column in self.display_table.header_filters:
                # self.display_table.header_filters[column]["type"] = "regex"
                self.display_table.header_filters[column]["func"] = "regex"
        else:
            # Revert to 'like' functionality
            for column in self.display_table.header_filters:
                # self.display_table.header_filters[column]["type"] = "input"
                self.display_table.header_filters[column]["func"] = "like"
        self.display_table.header_filters = self.display_table.header_filters

    def create_data_table(self, dfs):
        column_width_map = self._dataui_manager.get_table_column_width_map()
        all_cols = self._dataui_manager.get_table_columns()
        # Always reindex to all expected columns (fill missing ones with NaN).
        # This guarantees that the Tabulator is created with correct column
        # definitions even for an empty startup catalog — Tabulator.js column
        # definitions are fixed at init time and are not updated when only
        # .value changes later.
        dfs = dfs.reindex(columns=all_cols)
        # GeoDataFrame column slices can still be GeoDataFrames; Tabulator
        # cannot JSON-serialize geometry objects, so force a plain DataFrame.
        try:
            import geopandas as gpd
            if isinstance(dfs, gpd.GeoDataFrame):
                dfs = pd.DataFrame(dfs)
        except ImportError:
            pass
        # Normalize dtypes for Panel/Bokeh compatibility.
        # An empty reindex produces all-NaN float64 columns; pandas 3.x with
        # infer_string=True produces StringDtype columns for populated catalogs.
        # Both must be converted to object so the Tabulator is initialised with
        # text-column definitions and subsequent data-only updates send
        # compatible numpy object arrays (not ExtensionArrays or float64).
        _dtype_convert = {
            c: object for c, dt in dfs.dtypes.items()
            if not isinstance(dt, np.dtype) or (dt.kind == "f" and dfs[c].isna().all())
        }
        if _dtype_convert:
            dfs = dfs.astype(_dtype_convert)
        # Determine which columns to hide initially.  ref_type is hidden when
        # all rows share the same type (homogeneous catalog).
        initial_hidden = list(self._dataui_manager.get_hidden_table_columns(dfs) or [])
        if "ref_type" in dfs.columns:
            from dvue.tsdataui import TimeSeriesDataUIManager
            if not TimeSeriesDataUIManager._has_mixed_ref_types(dfs):
                if "ref_type" not in initial_hidden:
                    initial_hidden.append("ref_type")
        self.display_table = pn.widgets.Tabulator(
            dfs,
            disabled=True,
            widths=column_width_map,
            hidden_columns=initial_hidden,
            show_index=False,
            sizing_mode="stretch_width",
            header_filters=self._dataui_manager.get_table_filters(),
            pagination="local",
            page_size=200,
            configuration={
                "headerFilterLiveFilterDelay": 600,
                "columnDefaults": {"tooltip": True},
            },
        )

        self._display_panel = pn.Row(sizing_mode="stretch_both")
        self._action_panel = pn.Row()
        self._tab_count = 0
        actions = self._dataui_manager.get_data_actions()
        self._registered_actions = actions  # saved for _setup_action_sidebars

        if actions:
            action_buttons = self.create_data_actions(actions)
            # Insert a thin vertical separator between the primary Plot button
            # and the secondary actions (Download, Math Ref, etc.).
            if len(action_buttons) > 1:
                _vsep = pn.pane.HTML(
                    "<div style='width:1px;background:#d8d8d8;margin:6px 4px;"
                    "align-self:stretch'></div>",
                    margin=(0, 0, 0, 0),
                )
                action_buttons = [action_buttons[0], _vsep] + action_buttons[1:]
            self._action_panel.extend(action_buttons)
        self._action_panel.append(pn.layout.HSpacer())
        if self._dataui_manager.show_reset_session_button:
            from dvue.session_persistence import make_reset_session_button
            self._action_panel.append(
                make_reset_session_button(
                    cookie_name=self._dataui_manager.session_cookie_name,
                    sizing_mode="fixed",
                )
            )
        self._display_panel.append(
            pn.pane.HTML(
                self._dataui_manager.get_no_selection_message(),
                sizing_mode="stretch_both",
            )
        )
        gspec = pn.GridStack(sizing_mode="stretch_both", allow_resize=True, allow_drag=False)
        gspec[0:4, 0:10] = fullscreen.FullScreen(pn.Row(self.display_table, scroll=True))
        gspec[5:14, 0:10] = fullscreen.FullScreen(self._display_panel)
        self._main_panel = gspec
        return gspec

    def setup_location_sync(self):
        def _do_sync():
            if pn.state.location:
                pn.state.location.param.watch(self.update_view_from_location, "hash")
                self.update_view_from_location()
        # Defer registration until a session is active so that
        # pn.state.location is guaranteed to be available (it is None
        # when create_view() is called at module level before serving).
        if pn.state.location:
            # Already in a session context (e.g. inside pn.state.onload).
            _do_sync()
        else:
            pn.state.onload(_do_sync)

    def setup_url_sync(self):
        """Sync DataUI view params and manager params with URL query string.

        Bi-directional: on load, URL params restore state; on change, URL is
        updated.  Also restores table selection and filters from URL.
        """
        if not pn.state.location:
            return
        loc = pn.state.location
        query_params = loc.query_params or {}

        # Param → (url_key, default_value) map.  We never write defaults to
        # the URL — they are omitted on first load and cleared (set to "") on
        # revert.  On restore an empty/absent key is treated as "use default".
        _URL_PARAMS = {
            "view_type":        ("vt", self.param["view_type"].default),
            "use_regex_filter": ("rf", self.param["use_regex_filter"].default),
            "query":            ("q",  self.param["query"].default),
        }

        # --- Restore from URL (load) ---
        for p_name, (url_key, default) in _URL_PARAMS.items():
            if p_name not in self.param:
                continue
            raw = query_params.get(url_key)
            if not raw:          # absent or empty string → keep default
                continue
            p_obj = self.param[p_name]
            try:
                if isinstance(p_obj, param.Boolean):
                    val = raw.lower() in ("true", "1", "yes")
                elif isinstance(p_obj, param.Selector):
                    val = raw if raw in p_obj.objects else default
                else:
                    val = type(default)(raw) if default is not None else raw
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

        for p_name, (url_key, default) in _URL_PARAMS.items():
            if p_name in self.param:
                self.param.watch(_make_watcher(url_key, default), p_name)

        # Delegate to the manager's url sync for transform/display params
        mgr = self._dataui_manager
        if hasattr(mgr, "setup_url_sync"):
            mgr.setup_url_sync()
        # Restore table selection from URL query param "sel"
        self._setup_selection_url_sync()
        # Restore table filters from URL query param "flt"
        self._setup_filter_url_sync()
        # Note: we intentionally do NOT write bokeh-session-id to the URL.
        # Session continuity is handled by the dvue_user_id cookie + _APP_REGISTRY
        # in the server entry point.  Pinning the URL to a specific Bokeh session
        # ID would break navigation once that session expires.

    def _setup_selection_url_sync(self):
        """Sync table row selection with the URL ``sel`` query parameter.

        Selections are encoded as a pipe-separated list of row ``name``
        values (the catalog key), e.g. ``sel=STA1_flow_hourly|STA2_ec_daily``.
        """
        loc = pn.state.location
        if not loc:
            return

        # --- Restore on load ---
        query_params = loc.query_params or {}
        sel_str = query_params.get("sel", "")
        if sel_str:
            names = sel_str.split("|")
            if hasattr(self, "_dfcat") and "name" in self._dfcat.columns:
                indices = self._dfcat.index[self._dfcat["name"].isin(names)].tolist()
                if indices and hasattr(self, "display_table"):
                    self.display_table.selection = indices

        # --- Watch for changes and update URL ---
        def _on_selection_change(event):
            if not hasattr(self, "_dfcat") or "name" not in self._dfcat.columns:
                return
            sel = event.new or []
            if not sel:
                # Empty selection → omit the param (don't write sel= to URL)
                return
            # Cap at 30 to avoid URL length issues
            sel = sel[:30]
            selected_names = self._dfcat.iloc[sel]["name"].tolist()
            sel_encoded = "|".join(str(n) for n in selected_names if n)
            loc.update_query(sel=sel_encoded)

        if hasattr(self, "display_table"):
            self.display_table.param.watch(_on_selection_change, "selection")

    def _setup_filter_url_sync(self):
        """Sync table header filter values with the URL ``flt`` query parameter.

        Filters are encoded as ``col1:val1|col2:val2``.  Only restores on
        page load; write-back is not supported because Tabulator header-filter
        changes are client-side and not reflected as Python param events.
        """
        loc = pn.state.location
        if not loc:
            return

        # --- Restore on load ---
        query_params = loc.query_params or {}
        flt_str = query_params.get("flt", "")
        if flt_str and hasattr(self, "display_table"):
            try:
                for pair in flt_str.split("|"):
                    if ":" in pair:
                        col, val = pair.split(":", 1)
                        # Use a callable filter for partial text match.
                        # Capture col/val in default args to avoid late-binding.
                        self.display_table.add_filter(
                            lambda df, c=col, v=val: (
                                df[c].astype(str).str.lower().str.contains(v.lower(), na=False)
                                if c in df.columns
                                else df[c].notna()
                            )
                        )
            except Exception:
                logger.warning("Could not restore filters from URL: %s", flt_str)

    def get_version(self):
        try:
            return self._dataui_manager.get_version()
        except Exception:
            return "unknown"

    def get_about_text(self):
        import textwrap
        from datetime import datetime

        version = self.get_version()
        build_date = datetime.now().strftime("%Y-%m-%d")

        manager_text = ""
        try:
            manager_text = self._dataui_manager.get_about_text()
        except Exception:
            pass

        body = manager_text.strip() if manager_text.strip() else (
            "An application for exploring and visualising time-series data "
            "using HoloViews and Panel."
        )

        text = textwrap.dedent(f"""\
            ## About

            {body}

            ---

            | | |
            |---|---|
            | **Version** | `{version}` |
            | **Date** | {build_date} |
        """)
        return pn.pane.Markdown(text, sizing_mode="stretch_width")

    def create_about_button(self, template):
        about_btn = pn.widgets.Button(name="About App", button_type="primary", icon="info-circle")

        def about_callback(event):
            template.open_modal()

        about_btn.on_click(about_callback)
        return about_btn

    def create_disclaimer_button(self, template, disclaimer_modal_content):
        """Return a button that swaps the modal to the disclaimer text and opens it."""
        disclaimer_btn = pn.widgets.Button(
            name="Disclaimer", button_type="light", icon="alert-circle"
        )

        def disclaimer_callback(event):
            template.modal.clear()
            template.modal.append(disclaimer_modal_content)
            template.open_modal()

        disclaimer_btn.on_click(disclaimer_callback)
        return disclaimer_btn

    def add_header_buttons(self, template):
        """Add About (and optionally Disclaimer) buttons to *template*'s header.

        Use this when DataUI is embedded inside an outer template that supplies
        its own header — the buttons will close over the correct *template* so
        modals open on the servable outer template rather than the inner one
        returned by :meth:`create_view`.
        """
        about_button = self.create_about_button(template)
        disclaimer_content = self._dataui_manager.get_sidebar_disclaimer()
        if disclaimer_content is not None:
            disclaimer_button = self.create_disclaimer_button(template, disclaimer_content)
            template.header.append(disclaimer_button)
        template.header.append(about_button)
        template.modal.append(self.get_about_text())

    def _create_main_view(self):
        """Create the main view content based on the current view_type"""
        if self.view_type == "table":
            gspec = pn.GridStack(sizing_mode="stretch_both", allow_resize=False, allow_drag=False)
            gspec[0:14, 0:10] = fullscreen.FullScreen(pn.Row(self.display_table, scroll=True))
            return pn.Column(self._action_panel, gspec, sizing_mode="stretch_both")
        elif self.view_type == "display":
            gspec = pn.GridStack(sizing_mode="stretch_both", allow_resize=False, allow_drag=False)
            gspec[0:14, 0:10] = fullscreen.FullScreen(self._display_panel)
            return pn.Column(self._action_panel, gspec, sizing_mode="stretch_both")
        else:  # combined — action panel above the resizable GridStack
            return pn.Column(self._action_panel, self._main_panel, sizing_mode="stretch_both")

    def set_progress(self, value, status=None):
        """
        Set the progress bar value and optional status message.

        Parameters:
        -----------
        value : int
            Value between 0-100 for progress percentage, or -1 for indeterminate progress
        status : str or None
            Short status message displayed below the progress bar.  Pass
            ``None`` to leave the current message unchanged.
        """
        self.progress_bar.visible = True
        if value == -1:
            # Set to indeterminate mode
            self.progress_bar.indeterminate = True
        else:
            self.progress_bar.indeterminate = False
            self.progress_bar.value = max(0, min(100, value))  # Ensure value is between 0-100
        if status is not None:
            self._status_label.object = (
                f'<span style="font-size:11px;color:#666;white-space:nowrap;">'
                f"{status}</span>"
            )
            self._status_label.visible = True

    def hide_progress(self):
        """Hide the progress bar and clear the status message."""
        self.progress_bar.visible = False
        self.progress_bar.value = 0
        self.progress_bar.indeterminate = False
        self._status_label.object = ""
        self._status_label.visible = False

    def _setup_action_sidebars(self) -> None:
        """Let registered actions inject sidebar tabs at startup.

        Iterates over ``_registered_actions`` (set in :meth:`create_data_table`)
        and, for any action whose callback object exposes a ``setup_sidebar``
        method, calls that method so the tab appears without requiring a button
        click first.
        """
        for action in getattr(self, "_registered_actions", []):
            cb = action.get("callback")
            action_obj = getattr(cb, "__self__", None) if callable(cb) else None
            if action_obj is not None and hasattr(action_obj, "setup_sidebar"):
                try:
                    action_obj.setup_sidebar(self)
                except Exception as exc:
                    logger.warning(
                        "Action sidebar setup failed for %r: %s", action.get("name"), exc
                    )

    def _refresh_table_from_view(self, event=None) -> None:
        """Update the Tabulator to show only rows matching the active view.

        Called when :attr:`ViewsManager.active_view` changes or when a view
        definition is modified (Apply button in the Views tab).  Resets the
        table selection so stale positional indices don't carry over.
        """
        if not hasattr(self, "display_table"):
            return
        # Discard any active map filter — the view is being replaced.
        self._map_filter_station_ids = None
        self._dfcat_before_map_filter = None
        filtered = self._views_manager.filter_dataframe(self._dfcat_full)
        self._dfcat = filtered
        # Normalise to plain DataFrame (Tabulator can't JSON-serialise geometry)
        try:
            import geopandas as gpd
            tbl_df = pd.DataFrame(filtered) if isinstance(filtered, gpd.GeoDataFrame) else filtered
        except ImportError:
            tbl_df = filtered
        # Reindex to exactly the columns the Tabulator was initialised with
        tbl_cols = self.display_table.value.columns
        tbl_df = tbl_df.reindex(columns=tbl_cols)
        self.display_table.param.update(value=tbl_df, selection=[])

    def show_in_display_panel(self, title, content):
        """Add *content* as a closable tab in the display panel.

        Content is wrapped in a scrollable Column so that tall panels (e.g.
        the Math Ref editor) scroll within the fixed display area instead of
        inflating the display panel height and disrupting other tabs.
        """
        scrollable = pn.Column(content, scroll=True, sizing_mode="stretch_both")
        if len(self._display_panel.objects) > 0 and isinstance(
            self._display_panel.objects[0], pn.Tabs
        ):
            tabs = self._display_panel.objects[0]
            tabs.append((title, scrollable))
            tabs.active = len(tabs) - 1
        else:
            self._display_panel.objects = [
                pn.Tabs((title, scrollable), closable=True,
                        sizing_mode="stretch_both", dynamic=True)
            ]

    def show_map_in_display_panel(self, event):
        """Display the map in the display panel area as a closable tab"""
        try:
            # Set progress indicator while loading the map
            self._display_panel.loading = True
            self.set_progress(-1)  # Start indeterminate progress

            # Create a copy of the map for the display panel
            map_display = pn.Column(
                self._tmap * self._map_function,
                min_width=800,
                min_height=600,
                sizing_mode="stretch_both",
            )

            # Show 90% progress
            self.set_progress(90)

            # Check if there are already tabs in the display panel
            if len(self._display_panel.objects) > 0 and isinstance(
                self._display_panel.objects[0], pn.Tabs
            ):
                # Add to existing tabs
                tabs = self._display_panel.objects[0]
                self._tab_count += 1
                tabs.append((f"Interactive Map {self._tab_count}", map_display))
                tabs.active = len(tabs) - 1  # Activate the new tab
            else:
                # Create a new tabs panel
                self._tab_count = 1
                self._display_panel.objects = [
                    pn.Tabs(
                        (f"Interactive Map {self._tab_count}", map_display),
                        closable=True,
                        sizing_mode="stretch_both",
                        dynamic=True,
                    )
                ]

            # Complete the progress
            self.set_progress(100)
        except Exception as e:
            stack_str = full_stack()
            logger.error(stack_str)
            if pn.state.notifications is not None:
                pn.state.notifications.error("Error displaying map: " + str(stack_str), duration=0)
        finally:
            self._display_panel.loading = False
            # Hide progress after a short delay to show completion
            import asyncio

            pn.state.curdoc.add_next_tick_callback(
                lambda: asyncio.create_task(self._hide_progress_after_delay())
            )

    def create_view_navigation(self):
        """Create a compact Select dropdown for switching between views."""
        nav = pn.widgets.Select(
            name="",
            options=["Combined", "Table", "Display"],
            value="Combined",
            width=120,
            margin=(2, 4, 2, 0),
        )

        def _on_nav_change(event):
            view_name = event.new.lower()
            self._apply_view_type(view_name)
            if pn.state.location:
                pn.state.location.hash = f"#{view_name}"

        nav.param.watch(_on_nav_change, "value")
        self._nav_radio = nav
        return nav

    def _apply_view_type(self, view_name):
        """Switch the main content area to *view_name* ('combined', 'table', or 'display').

        This is the single authoritative place that mutates ``_main_content``.
        Called both from button clicks (direct) and from the hash watcher
        (browser back/forward).  Guards against no-op updates so the layout is
        not recreated unnecessarily.
        """
        if view_name not in ("table", "display"):
            view_name = "combined"
        if self.view_type == view_name and hasattr(self, "_main_content"):
            return  # Already showing this view — nothing to do
        self.view_type = view_name
        if hasattr(self, "_main_content"):
            self._main_content.objects = [self._create_main_view()]
        elif hasattr(self, "_main_view"):
            self._main_view.objects = [self._create_main_view()]

    def update_view_from_location(self, event=None):
        """Update the view based on the URL hash value (browser back/forward)."""
        if not pn.state.location:
            return
        # If view_type is already governed by the URL query param (vt=...),
        # don't let the hash override it.  The hash watcher fires on every
        # reconnect (even with the same value), so without this guard a
        # fresh page load with ?vt=display would be reset to 'combined' by
        # an empty hash before the URL sync could apply.
        query_params = pn.state.location.query_params or {}
        if "vt" in query_params:
            return
        hash_value = pn.state.location.hash.lstrip("#")
        self._apply_view_type(hash_value)
        # Keep the nav Select in sync when navigating via browser back/forward.
        if hasattr(self, "_nav_radio"):
            cap = hash_value.capitalize() if hash_value in ("combined", "table", "display") else "Combined"
            if self._nav_radio.value != cap:
                self._nav_radio.value = cap

    def create_view(self, title="Data User Interface"):
        main_panel = self.create_data_table(self._dfcat)

        # Create progress bar
        self.progress_bar = pn.indicators.Progress(
            name="Progress",
            value=0,
            min_width=400,
            sizing_mode="stretch_width",
            margin=(10, 5, 0, 5),
            bar_color="primary",
            visible=False,
        )
        self._status_label = pn.pane.HTML(
            "",
            height=18,
            margin=(0, 5, 6, 5),
            visible=False,
        )

        table_options = pn.Column(
            self.param.use_regex_filter,
            sizing_mode="stretch_width",
            margin=(4, 8, 4, 4),
        )
        # Column visibility picker — MultiChoice showing all table columns.
        # Checked = visible; unchecked = hidden (but still filterable).
        _all_cols = list(self.display_table.value.columns)
        _initially_hidden = list(self.display_table.hidden_columns or [])
        self._column_picker = pn.widgets.MultiChoice(
            name="Show columns",
            options=_all_cols,
            value=[c for c in _all_cols if c not in _initially_hidden],
            sizing_mode="stretch_width",
        )

        def _on_column_picker_change(event):
            visible = event.new
            self.display_table.hidden_columns = [c for c in list(self.display_table.value.columns) if c not in visible]

        self._column_picker.param.watch(_on_column_picker_change, "value")
        table_options.append(self._column_picker)
        # Build map options panel inside the Map sidebar tab
        if hasattr(self, "_map_features"):
            _extra_map_widgets = self._dataui_manager.get_map_option_widgets()
            _map_option_items = [
                self.param.show_map_colors,
                self.param.map_color_category,
                self.param.show_map_markers,
                self.param.map_marker_category,
                self.param.map_default_span,
                self.param.map_non_selection_alpha,
                self.param.map_point_size,
                self.param.query,
                self.param.map_filters_table,
            ]
            if _extra_map_widgets is not None:
                _map_option_items.append(_extra_map_widgets)
            # Plain column — no collapsible wrapper needed since the options
            # live in their own scrollable area below the map.
            map_options_card = pn.Column(
                *_map_option_items,
                sizing_mode="stretch_width",
                margin=(4, 4, 4, 4),
            )
            # Use HoloViews streams.Params instead of pn.bind so that ALL param
            # changes (including color/marker category) are routed through
            # HoloViews' own rendering pipeline.  pn.bind only triggers a Panel
            # pane swap; it does NOT instruct Bokeh to recreate the
            # CategoricalColorMapper/marker glyph, so color/marker opts changes
            # appear to have no effect.  streams.Params guarantees a full
            # HoloViews renderer refresh on every param change.
            _self_stream = streams.Params(
                parameterized=self,
                parameters=[
                    "show_map_colors",
                    "map_color_category",
                    "show_map_markers",
                    "map_marker_category",
                    "query",
                    "map_default_span",
                    "map_non_selection_alpha",
                    "map_point_size",
                    "_map_sel_proxy",
                    "_map_filter_proxy",
                ],
            )
            # _table_stream is replaced by proxy params above.
            # The proxies are only updated when map_filters_table=False, so the
            # DynamicMap never rebuilds (and never fires a spurious Selection1D)
            # while the map-filter mode is active.

            def _map_callback(
                show_map_colors,
                map_color_category,
                show_map_markers,
                map_marker_category,
                query,
                map_default_span,
                map_non_selection_alpha,
                map_point_size,
                _map_sel_proxy,
                _map_filter_proxy,
            ):
                return self.update_map_features(
                    show_color_by=show_map_colors,
                    color_by=map_color_category,
                    show_marker_by=show_map_markers,
                    marker_by=map_marker_category,
                    query=query,
                    selection=_map_sel_proxy,
                    map_default_span=map_default_span,
                    map_non_selection_alpha=map_non_selection_alpha,
                    map_point_size=map_point_size,
                )

            self._map_function = hv.DynamicMap(_map_callback, streams=[_self_stream])
            self._station_select.source = self._map_function
            self._station_select.param.watch_values(self.select_data_catalog, "index")

            # Wire table events → proxies (gated by map_filters_table mode)
            def _on_tbl_selection(event):
                if not self.map_filters_table:
                    self._map_sel_proxy = list(event.new)

            def _on_tbl_filters(event):
                if not self.map_filters_table:
                    self._map_filter_proxy += 1

            self.display_table.param.watch(_on_tbl_selection, "selection")
            self.display_table.param.watch(_on_tbl_filters, "filters")
            self.param.watch(self._on_map_filter_mode_changed, "map_filters_table")
            map_tooltip = pn.widgets.TooltipIcon(
                value="""Map of geographical features. Click on a feature to see data available in the table. <br/>
                See <a href="https://docs.bokeh.org/en/latest/docs/user_guide/interaction/tools.html">Bokeh Tools</a> for toolbar operation"""
            )

            # Create a button to show map in display panel
            map_display_btn = pn.widgets.Button(
                name="Show Map in Display", button_type="primary", icon="map", width=150
            )
            map_display_btn.on_click(self.show_map_in_display_panel)

            map_view = pn.Column(
                pn.Row(map_display_btn, pn.layout.HSpacer(), map_tooltip),
                # Map pane: fixed portion of the sidebar height.
                pn.Column(
                    self._tmap * self._map_function,
                    sizing_mode="stretch_width",
                    min_height=250,
                    height=350,
                ),
                # Options live in their own scrollable area below the map.
                pn.Column(
                    map_options_card,
                    sizing_mode="stretch_width",
                    styles={"overflow-y": "auto", "flex-shrink": "0"},
                ),
                min_width=300,
                sizing_mode="stretch_width",
            )

            # Build a flat tab list: Time / Transform / Plot / Map / Table / Views
            _ctrl = self._dataui_manager.get_widgets()
            if isinstance(_ctrl, dict) and _ctrl:
                _ctrl_tabs = list(_ctrl.items())
            elif _ctrl:
                _ctrl_tabs = [("Options", _ctrl)]
            else:
                _ctrl_tabs = []
            _ctrl_tabs.append(("Map", map_view))
            _ctrl_tabs.append(("Table", table_options))
            _ctrl_tabs.append(("Views", create_views_tab(self)))
            self._sidebar_tabs = pn.Tabs(*_ctrl_tabs, active=0)
            sidebar_view = pn.Column(
                self._sidebar_tabs,
                self.progress_bar,
                self._status_label,
                sizing_mode="stretch_both",
            )
        else:
            _ctrl = self._dataui_manager.get_widgets()
            if isinstance(_ctrl, dict) and _ctrl:
                _ctrl_tabs = list(_ctrl.items())
            elif _ctrl:
                _ctrl_tabs = [("Options", _ctrl)]
            else:
                _ctrl_tabs = []
            _ctrl_tabs.append(("Table", table_options))
            _ctrl_tabs.append(("Views", create_views_tab(self)))
            self._sidebar_tabs = pn.Tabs(*_ctrl_tabs, active=0)
            sidebar_view = pn.Column(
                self._sidebar_tabs,
                self.progress_bar,
                self._status_label,
                sizing_mode="stretch_both",
            )
        # Wire view switching → table refresh.
        self._views_manager.param.watch(self._refresh_table_from_view, "active_view")
        # Clear map filter when filter mode is toggled off.
        self.param.watch(self._on_map_filter_mode_changed, "map_filters_table")
        # Let actions inject their sidebar tabs now that _sidebar_tabs exists.
        self._setup_action_sidebars()
        # Append the view-navigation widget to the right end of the action
        # panel (after the HSpacer) so it lives on the same row as Plot /
        # Download instead of occupying a dedicated row above them.
        self._action_panel.append(self.create_view_navigation())

        # _main_content is swapped by update_view_from_location on view-type
        # changes; the action panel (which now includes the nav widget) stays
        # pinned inside each _create_main_view() call.
        # Progress bar lives in the sidebar (above the tabs) so it is always
        # visible without causing resize/jitter in the main content area.
        self._main_content = pn.Column(self._create_main_view(), sizing_mode="stretch_both")
        self._main_view = pn.Column(
            self._main_content,
            sizing_mode="stretch_both",
        )

        template = pn.template.FastListTemplate(
            title=title,
            sidebar=[sidebar_view],
            sidebar_width=450,
            header_background="lightgray",
            meta_viewport="width=device-width, initial-scale=1",
            raw_css=[_RESPONSIVE_CSS],
        )

        # About button only in the header (visible in standalone use).
        about_button = self.create_about_button(template)
        disclaimer_text = self._dataui_manager.get_sidebar_disclaimer()
        if disclaimer_text is not None:
            disclaimer_button = self.create_disclaimer_button(template, disclaimer_text)
            template.header.append(disclaimer_button)
        template.header.append(about_button)
        template.main.append(self._main_view)

        # Adding about button
        template.modal.append(self.get_about_text())
        # sidebar_view.append(self.create_about_button(template))
        self._template = template

        # finally sync location views
        self.setup_location_sync()
        # Sync all params (manager transforms + DataUI view) with URL query string
        # so state survives page reload / bookmark / share.
        # Deferred to onload so pn.state.location is available.
        def _deferred_url_sync():
            self.setup_url_sync()
        if pn.state.location:
            _deferred_url_sync()
        else:
            pn.state.onload(_deferred_url_sync)

        return template

    def create_mobile_view(self, title="Data User Interface"):
        """Create a mobile-optimized view with stacked vertical layout.

        Returns a :class:`~panel.template.FastListTemplate` with a condensed
        table, compact action bar, and vertically stacked plot display.
        The sidebar (accessible via hamburger menu) contains the map toggle
        and advanced options.
        """
        # --- Data table (condensed) ---
        mobile_cols = self._dataui_manager.get_mobile_table_columns()
        all_cols = self._dataui_manager.get_table_columns()
        # Ensure requested columns exist in catalog
        mobile_cols = [c for c in mobile_cols if c in all_cols]
        dfs_mobile = self._dfcat[mobile_cols] if mobile_cols else self._dfcat[list(all_cols)[:4]]

        self.display_table = pn.widgets.Tabulator(
            dfs_mobile,
            disabled=True,
            show_index=False,
            sizing_mode="stretch_width",
            page_size=50,
            configuration={
                "headerFilterLiveFilterDelay": 600,
                "columnDefaults": {"tooltip": True},
            },
        )

        # --- Display panel ---
        self._display_panel = pn.Column(sizing_mode="stretch_both", min_height=350)
        self._display_panel.append(
            pn.pane.HTML(
                self._dataui_manager.get_no_selection_message(),
                sizing_mode="stretch_both",
            )
        )

        # --- Action bar (compact) ---
        self._action_panel = pn.Row(styles={"flex-wrap": "wrap", "gap": "4px"})
        actions = self._dataui_manager.get_mobile_actions()
        if actions:
            action_buttons = self.create_data_actions(actions)
            self._action_panel.extend(action_buttons)

        # Advanced options toggle
        mobile_widgets = self._dataui_manager.get_mobile_widgets()
        advanced_panel = None
        if mobile_widgets is not None:
            advanced_panel = pn.Card(
                mobile_widgets,
                title="Options",
                collapsed=True,
                sizing_mode="stretch_width",
            )

        # --- Progress bar ---
        self.progress_bar = pn.indicators.Progress(
            name="Progress",
            value=0,
            sizing_mode="stretch_width",
            margin=(5, 5, 0, 5),
            bar_color="primary",
            visible=False,
        )
        self._status_label = pn.pane.HTML(
            "",
            height=18,
            margin=(0, 5, 6, 5),
            visible=False,
        )

        # --- Main area (stacked cards) ---
        main_items = [
            pn.Card(
                self.display_table,
                title="Catalog",
                sizing_mode="stretch_width",
                max_height=350,
                styles={"overflow-y": "auto"},
            ),
            self._action_panel,
            self.progress_bar,
            self._status_label,
        ]
        if advanced_panel is not None:
            main_items.append(advanced_panel)
        main_items.append(
            pn.Card(
                self._display_panel,
                title="Display",
                sizing_mode="stretch_both",
                min_height=350,
            ),
        )

        # --- Sidebar (hamburger on mobile) ---
        sidebar_items = []
        if hasattr(self, "_map_features"):
            # Rebuild map streams for mobile
            _self_stream = streams.Params(
                parameterized=self,
                parameters=[
                    "show_map_colors",
                    "map_color_category",
                    "show_map_markers",
                    "map_marker_category",
                    "query",
                    "map_default_span",
                    "map_non_selection_alpha",
                    "map_point_size",
                    "_map_sel_proxy",
                    "_map_filter_proxy",
                ],
            )

            def _map_callback(
                show_map_colors, map_color_category,
                show_map_markers, map_marker_category,
                query, map_default_span,
                map_non_selection_alpha, map_point_size,
                _map_sel_proxy, _map_filter_proxy,
            ):
                return self.update_map_features(
                    show_color_by=show_map_colors, color_by=map_color_category,
                    show_marker_by=show_map_markers, marker_by=map_marker_category,
                    query=query, selection=_map_sel_proxy,
                    map_default_span=map_default_span,
                    map_non_selection_alpha=map_non_selection_alpha,
                    map_point_size=map_point_size,
                )

            self._map_function = hv.DynamicMap(
                _map_callback, streams=[_self_stream]
            )
            self._station_select.source = self._map_function
            self._station_select.param.watch_values(self.select_data_catalog, "index")

            # Wire table events → proxies (gated by map_filters_table mode)
            def _on_tbl_selection(event):
                if not self.map_filters_table:
                    self._map_sel_proxy = list(event.new)

            def _on_tbl_filters(event):
                if not self.map_filters_table:
                    self._map_filter_proxy += 1

            self.display_table.param.watch(_on_tbl_selection, "selection")
            self.display_table.param.watch(_on_tbl_filters, "filters")
            self.param.watch(self._on_map_filter_mode_changed, "map_filters_table")

            # Touch-friendly map — tap only, no lasso/box
            map_card = pn.Card(
                pn.Column(
                    self._tmap * self._map_function,
                    sizing_mode="stretch_width",
                    height=350,
                ),
                title="Map",
                collapsed=True,
                sizing_mode="stretch_width",
            )
            sidebar_items.append(map_card)

            sidebar_items.append(
                pn.Card(
                    pn.Column(
                        self.param.show_map_colors,
                        self.param.map_color_category,
                        self.param.map_point_size,
                        self.param.query,
                        self.param.map_filters_table,
                    ),
                    title="Map Options",
                    collapsed=True,
                    sizing_mode="stretch_width",
                )
            )

        control_widgets = self._dataui_manager.get_widgets()
        sidebar_items.append(
            pn.Card(
                control_widgets,
                title="All Options",
                collapsed=True,
                sizing_mode="stretch_width",
            )
        )

        template = pn.template.FastListTemplate(
            title=title,
            sidebar=sidebar_items,
            sidebar_width=300,
            collapsed_sidebar=True,
            header_background="lightgray",
            meta_viewport="width=device-width, initial-scale=1",
            raw_css=[_MOBILE_CSS],
        )

        template.main.extend(main_items)
        about_button = self.create_about_button(template)
        template.header.append(about_button)
        template.modal.append(self.get_about_text())
        self._template = template
        return template

    def create_responsive_view(self, title="Data User Interface"):
        """Auto-detect desktop vs mobile and return the appropriate view.

        Uses URL hash routing:

        * ``#mobile`` → :meth:`create_mobile_view`
        * ``#desktop`` or any other hash → :meth:`create_view`
        * No hash → auto-detect based on client viewport width via a
          small JavaScript snippet that redirects narrow screens to
          ``#mobile``.

        This is the recommended entry-point for apps that want to serve
        both desktop and mobile users from a single URL.
        """
        # We need to build both views lazily inside onload so that
        # pn.state.location is available.
        container = pn.Column(
            pn.indicators.LoadingSpinner(value=True, size=50, name="Loading..."),
            sizing_mode="stretch_both",
        )

        # Placeholder template — will be replaced
        template = pn.template.FastListTemplate(
            title=title,
            meta_viewport="width=device-width, initial-scale=1",
            header_background="lightgray",
        )
        template.main.append(container)

        dataui = self

        def _on_load():
            loc = pn.state.location
            hash_val = (loc.hash or "").lstrip("#").lower() if loc else ""

            if hash_val == "mobile":
                t = dataui.create_mobile_view(title=title)
            elif hash_val:
                # Any explicit hash (desktop, combined, table, display)
                t = dataui.create_view(title=title)
            else:
                # No hash — inject JS auto-detect that redirects narrow
                # viewports to #mobile on next load.  For this first load,
                # default to desktop.
                t = dataui.create_view(title=title)

                # Client-side redirect for narrow screens on future loads
                _detect_js = (
                    "<script>"
                    "if(window.innerWidth<768 && !window.location.hash){"
                    "window.location.hash='#mobile';"
                    "window.location.reload();}"
                    "</script>"
                )
                t.header.append(pn.pane.HTML(_detect_js, width=0, height=0))

            # Re-parent into the outer template
            container.objects = list(t.main)
            template.sidebar[:] = list(t.sidebar)
            template.modal[:] = list(t.modal)
            for obj in t.header:
                template.header.append(obj)
            # Copy template styling
            template.sidebar_width = t.sidebar_width
            if hasattr(t, 'collapsed_sidebar'):
                template.collapsed_sidebar = t.collapsed_sidebar

        pn.state.onload(_on_load)
        self._template = template
        return template
