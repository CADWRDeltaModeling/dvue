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

        Override to perform file-level side effects such as expanding
        ``self.time_range``, loading geometry from a companion file, or
        registering the source in an external index.

        The default is a no-op.

        Parameters
        ----------
        path :
            Absolute path to the file that was just scanned.
        refs :
            All refs returned by ``ReaderRegistry.scan(path)`` for this file
            (including any that were skipped due to pk collisions).
        """

    # ------------------------------------------------------------------
    # Catalog / DataReference interface
    # ------------------------------------------------------------------

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def get_data_catalog(self):
        if len(self._dvue_catalog) != len(self._display_dfcat):
            self._display_dfcat = self._dvue_catalog.to_dataframe().reset_index()
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

        Only paths whose extension is registered with
        :class:`~dvue.registry.ReaderRegistry` are accepted; others are logged
        and skipped.  Duplicate refs (same pk) are silently dropped.

        Returns a list of paths from which at least one new ref was added.
        """
        added_paths = []
        for path in paths:
            if not ReaderRegistry.can_handle(path):
                logger.warning(
                    "%s: no registered reader for %s, skipping",
                    type(self).__name__,
                    path,
                )
                continue
            try:
                refs = ReaderRegistry.scan(path)
            except Exception as exc:
                logger.error(
                    "%s: cannot scan %s: %s", type(self).__name__, path, exc
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
                added_paths.append(path)
                logger.info(
                    "%s: added %d refs from %s", type(self).__name__, n_added, path
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

    def _get_table_column_width_map(self):
        return {
            "station": "20%",
            "variable": "15%",
            "ref_type": "12%",
        }

    def get_table_filters(self):
        return {
            "station": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "variable": {"type": "input", "func": "like", "placeholder": "Enter match"},
            "ref_type": {"type": "input", "func": "like", "placeholder": "Enter match"},
        }

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
