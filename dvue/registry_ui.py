"""Registry-backed time-series UI manager for dvue.

Provides :class:`RegistryUIManager` — a :class:`~dvue.tsdataui.TimeSeriesDataUIManager`
that starts empty and absorbs any file type whose reader has been registered
with :class:`~dvue.registry.ReaderRegistry`.

Downstream packages can use :class:`RegistryUIManager` directly *or* subclass it
to customise attribute normalisation and per-file side effects::

    from dvue.registry_ui import RegistryUIManager
    from dvue.registry import ReaderRegistry

    # Register your reader (typically at module import time):
    ReaderRegistry.register("myformat", MyReader, extensions=[".xyz"])

    # Then launch the UI with any supported files:
    mgr = RegistryUIManager(files=["data/run1.xyz", "data/run2.xyz"])

Customisation hooks
-------------------
* :meth:`RegistryUIManager.normalize_ref` -- called per-ref after ``scan()``;
  map source-specific attribute names to the common ``station``/``variable`` schema.
* :meth:`RegistryUIManager.on_file_added` -- called once per file after its refs
  are added; override to expand ``time_range``, load geometry, etc.
* :meth:`RegistryPlotAction.format_variable` -- override to apply domain-specific
  variable-name formatting (e.g. ``"FLOW"`` -> ``"Flow"``).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

import pandas as pd
import holoviews as hv

from dvue.catalog import DataCatalog
from dvue.registry import ReaderRegistry
from dvue.tsdataui import TimeSeriesDataUIManager, TimeSeriesPlotAction

if TYPE_CHECKING:  # pragma: no cover
    from dvue.catalog import DataReference

logger = logging.getLogger(__name__)


class RegistryPlotAction(TimeSeriesPlotAction):
    """Generic :class:`~dvue.tsdataui.TimeSeriesPlotAction` for registry-backed catalogs.

    Labels curves as ``station/variable`` (or just ``station`` when only one
    variable is selected).  Override :meth:`format_variable` to apply
    domain-specific variable-name formatting.
    """

    @staticmethod
    def _append_value(new_value: str, existing: str) -> str:
        if str(new_value) not in existing:
            existing += f'{", " if existing else ""}{new_value}'
        return existing

    def format_variable(self, variable: str) -> str:
        """Format a variable name for display.

        The default returns *variable* unchanged.  Override in a subclass to
        apply domain-specific transformations, e.g. title-casing ``"FLOW"``
        to ``"Flow"``.
        """
        return variable

    def render(self, df, refs_and_data, manager):
        self._varying = {
            "station": df["station"].nunique() > 1 if "station" in df.columns else True,
            "variable": df["variable"].nunique() > 1 if "variable" in df.columns else True,
            "source": df["source"].nunique() > 1 if "source" in df.columns else False,
        }
        return super().render(df, refs_and_data, manager)

    def create_curve(self, data, row, unit, file_index=""):
        varying = getattr(self, "_varying", {"station": True, "variable": True})
        file_index_label = f"{file_index}:" if file_index else ""
        station = str(row.get("station") or "unknown")
        variable = str(row.get("variable") or "")
        if row.get("ref_type") == "math":
            crvlabel = f'{file_index_label}{row.get("name", "math_ref")}'
        else:
            parts = [station]
            if varying.get("variable", True):
                parts.append(self.format_variable(variable))
            crvlabel = f'{file_index_label}{"/".join(parts)}'
        ylabel = self.format_variable(variable) + (f" ({unit})" if unit else "")
        crv = hv.Curve(data.iloc[:, [0]], label=crvlabel).redim(value=crvlabel)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    def append_to_title_map(self, title_map, group_key, row):
        value = title_map.get(group_key, ["", ""])
        station = str(row.get("station") or "")
        variable = str(row.get("variable") or "")
        value[0] = self._append_value(self.format_variable(variable), value[0])
        value[1] = self._append_value(station, value[1])
        title_map[group_key] = value

    def create_title(self, title_info) -> str:
        if isinstance(title_info, list) and len(title_info) >= 2:
            return f"{title_info[0]} @ {title_info[1]}"
        return str(title_info)


class RegistryUIManager(TimeSeriesDataUIManager):
    """Time-series UI manager backed by :class:`~dvue.registry.ReaderRegistry`.

    Starts with an empty catalog.  Call :meth:`add_source_files` (or pass
    initial *files*) to populate it.  File type is auto-detected by extension
    via :meth:`~dvue.registry.ReaderRegistry.can_handle`.

    All refs are normalised to a common ``station`` / ``variable`` schema by
    :meth:`normalize_ref` before being added to the catalog.  Subclasses can
    override both :meth:`normalize_ref` and :meth:`on_file_added` for
    domain-specific behaviour without duplicating the scan/add loop.

    Parameters
    ----------
    files : iterable of str, optional
        Initial file paths to load at construction time.
    **kwargs :
        Forwarded to :class:`~dvue.tsdataui.TimeSeriesDataUIManager`.
    """

    def __init__(self, files=(), **kwargs):
        _time_range = kwargs.pop("time_range", None)
        self.station_id_column = "station"
        self._dvue_catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
        self._display_dfcat = pd.DataFrame(
            columns=["name", "station", "variable", "ref_type", "source"]
        )
        super().__init__(**kwargs)
        self.time_range = _time_range
        self.color_cycle_column = "station"
        self.dashed_line_cycle_column = "source"
        self.marker_cycle_column = "variable"
        if files:
            self.add_source_files(*files)

    # ------------------------------------------------------------------
    # Customisation hooks
    # ------------------------------------------------------------------

    def normalize_ref(self, ref: "DataReference") -> None:
        """Normalise *ref* attributes to the ``station`` / ``variable`` schema.

        Called for every ref returned by :meth:`~dvue.registry.ReaderRegistry.scan`
        before it is added to the catalog.

        The default implementation tries the following fallback chain for
        ``station``::

            station (already set) -> id -> NAME -> name -> ""

        And for ``variable``::

            variable (already set) -> VARIABLE (lower-cased) -> ""

        Override in a subclass to use domain-specific attribute names, apply
        further enrichment (e.g. geometry), or transform values.
        """
        if not ref._attributes.get("station"):
            station = (
                ref._attributes.get("id")
                or ref._attributes.get("NAME")
                or ref._attributes.get("name", "")
            )
            ref.set_attribute("station", str(station))
        if not ref._attributes.get("variable"):
            var = ref._attributes.get("VARIABLE", "")
            ref.set_attribute("variable", str(var).lower())

    def on_file_added(self, path: str, refs: List["DataReference"]) -> None:
        """Hook called after a file's refs have been successfully added.

        Default implementation: expands ``self.time_range`` to cover the
        data extent of any newly added refs.  Readers declare their data's
        time extent by storing ``time_extent_start`` and ``time_extent_end``
        attributes (ISO-8601 strings or any ``pd.Timestamp``-coercible value)
        on each :class:`~dvue.catalog.DataReference`.

        Subclasses that need additional side-effects (loading geometry,
        registering external indices, etc.) should call
        ``super().on_file_added(path, refs)`` so that time_range expansion
        still happens.

        Parameters
        ----------
        path :
            Absolute path to the file that was just scanned.
        refs :
            All refs returned by ``ReaderRegistry.scan(path)`` for this file
            (including any that were skipped due to pk collisions).
        """
        starts = []
        ends = []
        seen: set = set()
        for ref in refs:
            s = ref._attributes.get("time_extent_start")
            e = ref._attributes.get("time_extent_end")
            key = (s, e)
            if key in seen or not s:
                continue
            seen.add(key)
            try:
                starts.append(pd.Timestamp(s))
            except Exception:
                pass
            if e:
                try:
                    ends.append(pd.Timestamp(e))
                except Exception:
                    pass
        if not starts:
            return
        new_start = min(starts)
        new_end = max(ends) if ends else new_start + pd.Timedelta(days=366)
        current = self.time_range
        if current is None:
            self.time_range = (new_start, new_end)
        else:
            self.time_range = (
                min(current[0], new_start),
                max(current[1], new_end),
            )

    # ------------------------------------------------------------------
    # Geo / map integration
    # ------------------------------------------------------------------

    def add_geo_source(
        self,
        path: str,
        id_column: str,
        station_column: str = "station",
    ) -> None:
        """Load geographic data and merge it into the live catalog display DataFrame.

        After this call, :meth:`get_data_catalog` returns a
        :class:`geopandas.GeoDataFrame` with a ``geometry`` column.
        If the source file's CRS can be resolved and ``self.crs`` is ``None``,
        ``self.crs`` is also set from the file's CRS.

        The merge is re-applied automatically whenever the catalog grows (e.g.
        when additional files are dropped onto the window after the initial load).

        Parameters
        ----------
        path :
            Path to a CSV, GeoJSON, shapefile, or GeoPackage.  See
            :func:`dvue.utils.load_geo_dataframe` for format details.
        id_column :
            Column in the geo file that contains station identifiers matching
            *station_column* values in the catalog.
        station_column :
            Catalog column to join on.  Default ``"station"``.
        """
        from dvue.utils import load_geo_dataframe

        try:
            geo_df = load_geo_dataframe(path)
        except Exception as exc:
            logger.warning(
                "%s: add_geo_source: could not load %r: %s",
                type(self).__name__,
                path,
                exc,
            )
            return

        self._geo_source_df = geo_df
        self._geo_id_column = id_column
        self._geo_station_column = station_column

        self._apply_geo_merge()

        # Auto-set crs from geo file CRS when not yet declared.
        if self.crs is None and getattr(geo_df, "crs", None) is not None:
            try:
                import cartopy.crs as ccrs
                epsg = geo_df.crs.to_epsg()
                if epsg:
                    self.crs = ccrs.epsg(str(epsg))
            except Exception:
                pass

    def _apply_geo_merge(self) -> None:
        """(Re-)merge stored geo data into ``_display_dfcat``.

        Called by :meth:`add_geo_source` and by :meth:`get_data_catalog` when
        the catalog grows after a geo source has been registered.
        """
        import geopandas as gpd
        import pandas as pd

        geo_df = getattr(self, "_geo_source_df", None)
        if geo_df is None:
            return

        id_col = self._geo_id_column
        station_col = self._geo_station_column
        df = self._display_dfcat

        if id_col not in geo_df.columns:
            logger.warning(
                "%s: _apply_geo_merge: id_column %r not in geo file columns",
                type(self).__name__,
                id_col,
            )
            return
        if station_col not in df.columns:
            return

        # Build a slim geo lookup: id → geometry (+ any extra columns).
        extra_geo_cols = [
            c for c in geo_df.columns if c not in (id_col, "geometry")
        ]
        geo_subset = (
            geo_df[[id_col, "geometry"] + extra_geo_cols]
            .rename(columns={id_col: station_col})
            .drop_duplicates(subset=[station_col])
        )

        # Strip old geometry column to avoid conflicts on re-merge.
        if "geometry" in df.columns:
            df = pd.DataFrame(df.drop(columns=["geometry"]))

        merged = df.merge(geo_subset, on=station_col, how="left")

        if "geometry" in merged.columns:
            self._display_dfcat = gpd.GeoDataFrame(
                merged, geometry="geometry", crs=geo_df.crs
            )
        else:
            self._display_dfcat = merged

    # ------------------------------------------------------------------
    # Catalog / DataReference interface
    # ------------------------------------------------------------------

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def get_data_catalog(self):
        if len(self._dvue_catalog) != len(self._display_dfcat):
            self._display_dfcat = self._dvue_catalog.to_dataframe().reset_index()
            self._apply_geo_merge()
        return self._display_dfcat

    def get_data_reference(self, row):
        if "name" in row.index:
            return self._dvue_catalog.get(row["name"])
        raise KeyError(
            f"No 'name' column in RegistryUIManager row; "
            "ensure the catalog was built with reset_index() before passing rows."
        )

    def add_source_files(self, *paths: str) -> List[str]:
        """Scan *paths* via the registry and add all resulting refs to the catalog.

        Supports optional per-file reader override using ``ref_type:path``.
        Example: ``dsm2_dss:my_file.dss`` forces the ``dsm2_dss`` reader for
        that file, bypassing extension dispatch. Without this prefix,
        extension-based dispatch is used.

        Paths whose extension (or forced ``ref_type``) is not registered are
        logged and skipped. Duplicate refs (same pk) are silently dropped.

        Returns a list of paths from which at least one new ref was added.
        """
        added_paths = []
        for source_spec in paths:
            forced_ref_type, path = ReaderRegistry.parse_source_spec(source_spec)

            # Helpful typo warning for explicit override syntax. We only warn
            # when the left token looks like a ref_type (identifier-like), and
            # avoid false positives for Windows drive paths (e.g., C:\...).
            if forced_ref_type is None and ":" in source_spec and "://" not in source_spec:
                maybe_ref_type, rest = source_spec.split(":", 1)
                looks_like_drive_path = (
                    len(maybe_ref_type) == 1
                    and maybe_ref_type.isalpha()
                    and rest.startswith(("\\", "/"))
                )
                if maybe_ref_type.isidentifier() and not looks_like_drive_path:
                    known = ", ".join(sorted(ReaderRegistry.get_registered_readers().keys()))
                    logger.warning(
                        "%s: unknown reader override prefix %r in %r; "
                        "treating as plain path. Known ref_type values: %s",
                        type(self).__name__,
                        maybe_ref_type,
                        source_spec,
                        known or "(none)",
                    )

            if not ReaderRegistry.can_handle(path, ref_type=forced_ref_type):
                logger.warning(
                    "%s: no registered reader for %s, skipping",
                    type(self).__name__,
                    source_spec,
                )
                continue
            try:
                refs = ReaderRegistry.scan(path, ref_type=forced_ref_type)
            except Exception as exc:
                logger.error(
                    "%s: cannot scan %s: %s",
                    type(self).__name__,
                    source_spec,
                    exc,
                )
                continue

            n_before = len(self._dvue_catalog)
            for ref in refs:
                self.normalize_ref(ref)
                try:
                    self._dvue_catalog.add(ref)
                except ValueError:
                    pass  # pk collision -- ref already present
                except Exception as exc:
                    logger.warning(
                        "%s: skipping ref %s: %s", type(self).__name__, ref.name, exc
                    )

            n_added = len(self._dvue_catalog) - n_before
            if n_added > 0:
                self._display_dfcat = self._dvue_catalog.to_dataframe().reset_index()
                self.on_file_added(path, refs)
                added_paths.append(source_spec)
                logger.info(
                    "%s: added %d refs from %s",
                    type(self).__name__,
                    n_added,
                    source_spec,
                )

        return added_paths

    # ------------------------------------------------------------------
    # TimeSeriesDataUIManager interface
    # ------------------------------------------------------------------

    def _make_plot_action(self):
        return RegistryPlotAction()

    def build_station_name(self, r):
        if r.get("ref_type") == "math":
            name = r.get("name")
            if name and str(name) not in ("nan", "None", ""):
                return str(name)
        station = r.get("station") or ""
        if "source_num" in r.index:
            return f'{r["source_num"]}:{station}'
        return str(station)

    def get_time_range(self, dfcat):
        return self.time_range

    def _get_dynamic_table_df(self):
        try:
            return self.get_data_catalog()
        except Exception:
            return self._display_dfcat

    def _is_effectively_empty_column(self, series: pd.Series) -> bool:
        if series.isna().all():
            return True
        non_na = series.dropna()
        if non_na.empty:
            return True
        # Treat blank strings and common textual null spellings as empty.
        as_text = non_na.astype(str).str.strip().str.lower()
        return as_text.isin({"", "nan", "none", "null"}).all()

    def get_table_schema(self, df: pd.DataFrame | None = None) -> dict:
        if df is None:
            df = self._get_dynamic_table_df()

        required = ["station", "variable", "ref_type"]
        excluded = {"geometry", "source", "source_num", *required}
        optional = [c for c in df.columns if c not in excluded]

        return {
            "required_columns": required,
            "optional_columns": optional,
            "hidden_by_default": ["ref_type"],
            "drop_if_all_null": True,
            "column_widths": {
                "station": "20%",
                "variable": "15%",
                "ref_type": "12%",
            },
            "filters": {
                "station": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "variable": {"type": "input", "func": "like", "placeholder": "Enter match"},
                "ref_type": {"type": "input", "func": "like", "placeholder": "Enter match"},
            },
        }

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

    def get_dynamic_table_columns(self, df: pd.DataFrame) -> list[str]:
        """Return dynamic metadata columns to expose in the table.

        Subclasses should override this method to provide explicit column
        ownership (ordering, inclusion/exclusion).  The default keeps any
        non-empty metadata columns from the current catalog DataFrame.
        """
        schema = self.get_table_schema(df)
        resolved = self._resolve_table_columns_from_schema(df, schema)
        required = set(schema.get("required_columns", []))
        return [c for c in resolved if c not in required]

    def get_dynamic_column_width(self, col: str, series: pd.Series) -> str:
        """Return a default width for a dynamic metadata column.

        Subclasses may override to provide domain-specific sizing.
        """
        dtype = series.dtype
        if pd.api.types.is_bool_dtype(dtype):
            return "7%"
        if pd.api.types.is_numeric_dtype(dtype):
            return "8%"
        return "10%"

    def _iter_dynamic_table_columns(self):
        df = self._get_dynamic_table_df()
        if not hasattr(df, "columns"):
            return []
        return self.get_dynamic_table_columns(df)

    def _get_table_column_width_map(self):
        df = self._get_dynamic_table_df()
        schema = self.get_table_schema(df)
        column_widths = dict(schema.get("column_widths", {}))

        for col in self._resolve_table_columns_from_schema(df, schema):
            if col not in column_widths:
                series = df[col] if hasattr(df, "columns") and col in df.columns else pd.Series(dtype=object)
                column_widths[col] = self.get_dynamic_column_width(col, series)
        return column_widths

    def get_table_filters(self):
        df = self._get_dynamic_table_df()
        schema = self.get_table_schema(df)
        filters = dict(schema.get("filters", {}))
        # Keep filters aligned with dynamically visible columns.
        for col in self._resolve_table_columns_from_schema(df, schema):
            if col in ("geometry",):
                continue
            if col not in filters:
                filters[col] = {
                    "type": "input",
                    "func": "like",
                    "placeholder": "Enter match",
                }
        return filters

    def get_hidden_table_columns(self, df: pd.DataFrame | None = None) -> list[str]:
        if df is None:
            df = self._get_dynamic_table_df()
        schema = self.get_table_schema(df)
        return [c for c in schema.get("hidden_by_default", []) if c in df.columns]

    def is_irregular(self, r):
        return False

    def get_tooltips(self):
        return [
            ("station", "@station"),
            ("variable", "@variable"),
            ("ref_type", "@ref_type"),
        ]

    def get_map_color_category(self):
        return "variable"

    def get_map_color_columns(self):
        return ["variable"]

    def get_map_marker_columns(self):
        return ["variable"]
