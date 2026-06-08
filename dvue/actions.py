import asyncio
import os
import threading

import numpy as np
import panel as pn

pn.extension()
import pandas as pd
from io import StringIO
import logging
from .utils import full_stack

logger = logging.getLogger(__name__)


class PlotAction:
    """Base action for visualising selected catalog rows.

    Subclasses override :meth:`render` to build domain-specific panels.
    :meth:`get_refs_and_data` handles data retrieval from the manager's
    DataCatalog and forwards ``time_range`` to each DataReference so that
    time-range-aware readers can load only the requested window.
    """

    def get_refs_and_data(self, df, manager):
        """Yield ``(row, ref, data)`` for each selected row.

        ``manager.time_range`` is forwarded to ``ref.getData()`` so that
        time-range-aware readers can load only the requested window
        efficiently.  Rows whose manager has no
        :class:`~dvue.catalog.DataCatalog` (``get_data_reference`` raises
        :exc:`NotImplementedError`) yield ``(row, None, None)`` instead
        of propagating the error.
        """
        time_range = getattr(manager, "time_range", None)
        for _, row in df.iterrows():
            try:
                ref = manager.get_data_reference(row)
                data = ref.getData(time_range=time_range)
                yield row, ref, data
            except NotImplementedError:
                yield row, None, None

    def get_tab_label(self, tab_count: int) -> str:
        """Return the tab title for the given tab counter.

        Override in subclasses to customise the label (e.g. prefix with
        ``"T"`` for tabulate actions).
        """
        return str(tab_count)

    def render(self, df, refs_and_data, manager):
        """Build and return a Panel/HoloViews object from *refs_and_data*.

        Default: delegates to ``manager.create_panel(df)`` for backward
        compatibility with subclasses that override ``create_panel()``
        directly.  Override to build the visualisation from *refs_and_data*
        without depending on DataUI widget methods.

        Parameters
        ----------
        df : pd.DataFrame
            The selected rows from the catalog table.
        refs_and_data : list of (row, ref, data)
            Pre-loaded triples from :meth:`get_refs_and_data`.
        manager : DataUIManager
            The data/view manager for this UI.
        """
        return manager.create_panel(df)

    def callback(self, event, dataui):
        # Guard: no selection → warn and return immediately (no thread needed).
        if not dataui.display_table.selection or len(dataui.display_table.selection) == 0:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "Please select at least one row from the table.", duration=3000
                )
            logger.warning("No rows selected for plotting")
            return

        # Capture doc + selection snapshot before entering the thread.
        doc = pn.state.curdoc
        # Resolve the selected rows from the *full* catalog (_dfcat) so that
        # all catalog columns (including 'name', 'FILE', etc.) are available
        # to get_data_reference().  display_table.selected_dataframe only
        # contains the columns in display_table.value, which is limited to
        # get_table_columns() — it omits hidden catalog metadata columns.
        #
        # display_table.selection contains positional indices into
        # display_table.value (Panel maps Bokeh ColumnDataSource row numbers
        # back to value positions via _map_indexes).  Using current_view.iloc
        # is wrong when header filters are active because current_view is a
        # subset of value with different positional indices.
        # display_table.value.iloc[selection] is always correct.
        _selection = dataui.display_table.selection
        _sel_index = dataui.display_table.value.iloc[_selection].index
        dfselected = dataui._dfcat.loc[_sel_index].copy()
        manager = dataui._dataui_manager
        total = len(dfselected)

        # Show indeterminate progress in the IO-loop thread (safe here).
        dataui._display_panel.loading = True
        dataui.set_progress(-1, f"Loading 0 of {total}…")

        def _worker():
            try:
                refs_and_data = []
                for i, (_, row) in enumerate(dfselected.iterrows()):
                    name = row.get("name", row.get("station_name", str(i)))
                    # schedule per-item progress update
                    _i, _name = i, name
                    doc.add_next_tick_callback(
                        lambda _i=_i, _name=_name: dataui.set_progress(
                            int(10 + 70 * _i / total),
                            f"Loading {_i + 1} of {total}: {_name}",
                        )
                    )
                    try:
                        ref = manager.get_data_reference(row)
                        time_range = getattr(manager, "time_range", None)
                        data = ref.getData(time_range=time_range)
                        refs_and_data.append((row, ref, data))
                    except NotImplementedError:
                        refs_and_data.append((row, None, None))

                doc.add_next_tick_callback(
                    lambda: dataui.set_progress(85, "Rendering plot…")
                )
                plot_panel = self.render(dfselected, refs_and_data, manager)

                def _update_display():
                    if len(dataui._display_panel.objects) > 0 and isinstance(
                        dataui._display_panel.objects[0], pn.Tabs
                    ):
                        tabs = dataui._display_panel.objects[0]
                        dataui._tab_count += 1
                        tabs.append((self.get_tab_label(dataui._tab_count), plot_panel))
                        tabs.active = len(tabs) - 1
                    else:
                        dataui._tab_count = 0
                        dataui._display_panel.objects = [
                            pn.Tabs((self.get_tab_label(dataui._tab_count), plot_panel), closable=True,
                                    sizing_mode="stretch_both", dynamic=True)
                        ]
                    dataui.set_progress(100, "Done")

                doc.add_next_tick_callback(_update_display)

            except Exception as e:
                stack_str = full_stack()
                logger.error(stack_str)
                short_msg = f"{type(e).__name__}: {e}"

                def _show_error():
                    dataui._display_panel.objects = [
                        pn.pane.Markdown(
                            f"**Error loading data**\n\n`{short_msg}`\n\n"
                            "_See the application log for the full traceback._"
                        )
                    ]
                    if pn.state.notifications is not None:
                        pn.state.notifications.error(short_msg, duration=8000)
                    else:
                        logger.error("Could not display notification: %s", short_msg)

                doc.add_next_tick_callback(_show_error)

            finally:
                doc.add_next_tick_callback(
                    lambda: (
                        setattr(dataui._display_panel, "loading", False),
                        asyncio.create_task(self._hide_progress_after_delay(dataui)),
                    )
                )

        threading.Thread(target=_worker, daemon=True).start()

    async def _hide_progress_after_delay(self, dataui):
        """Hide the progress bar and status label after a short delay."""
        await asyncio.sleep(0.5)
        dataui.hide_progress()


class TabulateAction(PlotAction):
    """Action that loads selected series and displays them as a data table.

    Reuses :class:`PlotAction`'s threaded callback (progress bar, tab
    management) and only overrides :meth:`render` to produce a
    :class:`panel.widgets.Tabulator` instead of a HoloViews plot.

    All selected series are merged into a single wide DataFrame — one column
    per series — indexed by datetime.  The columns are named after the first
    available identifying field in the catalog row (``name``, ``station_name``,
    or ``station_id``).
    """

    def get_tab_label(self, tab_count: int) -> str:
        return f"T{tab_count}"

    def render(self, df, refs_and_data, manager):
        frames = []
        for row, ref, data in refs_and_data:
            if data is None or not isinstance(data, pd.DataFrame):
                continue
            label = None
            for col in ("name", "station_name", "station_id"):
                val = row.get(col) if hasattr(row, "get") else None
                if val and str(val).strip():
                    label = str(val).strip()
                    break
            if label is None:
                label = f"series_{len(frames)}"
            if len(data.columns) == 1:
                data = data.copy()
                data.columns = [label]
            else:
                data = data.copy()
                data.columns = [f"{label}_{c}" for c in data.columns]
            frames.append(data)

        if not frames:
            return pn.pane.Markdown("_No data to display._", sizing_mode="stretch_both")

        combined = pd.concat(frames, axis=1)
        combined.index.name = "datetime"
        combined = combined.reset_index()
        # Round floats to 4 significant figures for readability.
        float_cols = combined.select_dtypes(include="float").columns.tolist()
        if float_cols:
            combined[float_cols] = combined[float_cols].round(4)

        tab = pn.widgets.Tabulator(
            combined,
            pagination="remote",
            page_size=50,
            show_index=False,
            sizing_mode="stretch_both",
        )
        tab.editors = {col: None for col in combined.columns}  # make all columns read-only
        return tab


class ReportAction:
    """Base action for catalog-level report generation.

    Unlike :class:`PlotAction`, a ``ReportAction`` operates on the **full
    catalog** rather than on a row selection.  It is intended for reports that
    summarise coverage, statistics, or data quality across all (or many) series
    — tasks where requiring the user to select rows first would be awkward or
    misleading.

    Subclasses override :meth:`generate` to build and return any Panel-
    renderable object (Markdown, HTML, Tabulator, HoloViews plot, Matplotlib
    figure via ``pn.pane.Matplotlib``, PNG/SVG via ``pn.pane.PNG``/``SVG``, or
    mixed ``pn.Column``/``pn.Row`` layouts).

    **Threading contract** — :meth:`generate` is called inside a daemon worker
    thread, exactly like :meth:`PlotAction.render`.  All Panel / Bokeh state
    mutations (widget updates, ``pn.state.curdoc`` changes, etc.) must be
    routed through ``doc.add_next_tick_callback()``.  The final ``pn.Tabs``
    append and progress-bar update are handled automatically by
    :meth:`callback`.

    Usage::

        class CoverageReportAction(ReportAction):
            def generate(self, catalog_df, manager):
                summary = (
                    catalog_df.groupby("variable")
                    .agg(
                        stations=("station_id", "nunique"),
                        min_year=("min_year", "min"),
                        max_year=("max_year", "max"),
                    )
                    .reset_index()
                )
                return pn.Column(
                    pn.pane.Markdown("## Coverage Report"),
                    pn.widgets.Tabulator(summary, show_index=False),
                    sizing_mode="stretch_both",
                )

    Then register via :meth:`~dvue.DataUIManager.get_data_actions`::

        def get_data_actions(self):
            actions = super().get_data_actions()
            report = CoverageReportAction()
            actions.append(dict(
                name="Report",
                button_type="warning",
                icon="report",
                action_type="display",
                callback=report.callback,
            ))
            return actions
    """

    def get_tab_label(self, tab_count: int) -> str:
        """Return the tab title for the given tab counter.

        Default prefix is ``"R"`` to distinguish report tabs from plot tabs
        (``"P"`` / numbers) in the same display area.  Override to use a
        more descriptive label.
        """
        return f"R{tab_count}"

    def generate(self, catalog_df, manager):
        """Build and return a Panel-renderable object from the full catalog.

        Parameters
        ----------
        catalog_df : pd.DataFrame
            The complete catalog DataFrame (all rows, all columns) from
            ``dataui._dfcat``.  This is the same frame returned by
            ``manager.get_data_catalog()``, but without an extra round-trip.
        manager : DataUIManager
            The data/view manager for this UI.  Use
            ``manager.get_data_reference(row).getData(time_range=...)`` if the
            report needs to load actual time-series data.

        Returns
        -------
        panel.viewable.Viewable
            Any Panel-renderable object.

        Raises
        ------
        NotImplementedError
            Subclasses must override this method.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement generate(catalog_df, manager)"
        )

    def callback(self, event, dataui):
        """Threaded callback — runs generate() and adds the result as a new tab.

        Unlike :class:`PlotAction`, there is no row-selection guard.  The full
        catalog (``dataui._dfcat``) is always passed to :meth:`generate`.
        """
        doc = pn.state.curdoc
        catalog_df = dataui._dfcat.copy()
        manager = dataui._dataui_manager

        dataui._display_panel.loading = True
        dataui.set_progress(-1, "Generating report…")

        def _worker():
            try:
                doc.add_next_tick_callback(
                    lambda: dataui.set_progress(20, "Generating report…")
                )
                report_panel = self.generate(catalog_df, manager)

                def _update_display():
                    if len(dataui._display_panel.objects) > 0 and isinstance(
                        dataui._display_panel.objects[0], pn.Tabs
                    ):
                        tabs = dataui._display_panel.objects[0]
                        dataui._tab_count += 1
                        tabs.append((self.get_tab_label(dataui._tab_count), report_panel))
                        tabs.active = len(tabs) - 1
                    else:
                        dataui._tab_count = 0
                        dataui._display_panel.objects = [
                            pn.Tabs(
                                (self.get_tab_label(dataui._tab_count), report_panel),
                                closable=True,
                                sizing_mode="stretch_both",
                                dynamic=True,
                            )
                        ]
                    dataui.set_progress(100, "Done")

                doc.add_next_tick_callback(_update_display)

            except Exception as e:
                stack_str = full_stack()
                logger.error(stack_str)
                short_msg = f"{type(e).__name__}: {e}"

                def _show_error():
                    dataui._display_panel.objects = [
                        pn.pane.Markdown(
                            f"**Error generating report**\n\n`{short_msg}`\n\n"
                            "_See the application log for the full traceback._"
                        )
                    ]
                    if pn.state.notifications is not None:
                        pn.state.notifications.error(short_msg, duration=8000)
                    else:
                        logger.error("Could not display notification: %s", short_msg)

                doc.add_next_tick_callback(_show_error)

            finally:
                doc.add_next_tick_callback(
                    lambda: (
                        setattr(dataui._display_panel, "loading", False),
                        asyncio.create_task(self._hide_progress_after_delay(dataui)),
                    )
                )

        threading.Thread(target=_worker, daemon=True).start()

    async def _hide_progress_after_delay(self, dataui):
        """Hide the progress bar and status label after a short delay."""
        await asyncio.sleep(0.5)
        dataui.hide_progress()


class DownloadDataAction:
    def callback(self, event, dataui):
        # Guard: no selection → warn immediately without threading.
        if not dataui.display_table.selection or len(dataui.display_table.selection) == 0:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "Please select at least one row from the table.", duration=3000
                )
            logger.warning("No rows selected for download")
            return None

        doc = pn.state.curdoc
        dfselected = dataui.display_table.selected_dataframe.copy()
        total = len(dfselected)

        dataui._display_panel.loading = True
        dataui.set_progress(-1, f"Preparing download of {total} series…")

        # DownloadDataAction.callback must return a file-like for Panel's
        # FileDownload widget.  We run synchronously here because FileDownload
        # needs a return value; we just update status along the way.
        try:
            doc.add_next_tick_callback(
                lambda: dataui.set_progress(30, "Loading data…")
            )
            time_range = getattr(dataui._dataui_manager, "time_range", None)
            dfdata = pd.concat(
                [df for df in dataui._dataui_manager.get_data(dfselected, time_range=time_range)], axis=1
            )
            doc.add_next_tick_callback(
                lambda: dataui.set_progress(80, "Serialising to CSV…")
            )
            sio = StringIO()
            dfdata.to_csv(sio)
            sio.seek(0)
            doc.add_next_tick_callback(
                lambda: dataui.set_progress(100, "Ready")
            )
            return sio
        except Exception as e:
            logger.error("Error downloading data: %s", e)
            if pn.state.notifications is not None:
                pn.state.notifications.error("Error downloading data: " + str(e), duration=0)
            return StringIO()
        finally:
            dataui._display_panel.loading = False
            doc.add_next_tick_callback(
                lambda: asyncio.create_task(_hide_after_delay(dataui))
            )


async def _hide_after_delay(dataui):
    """Hide progress bar and status label after a short completion pause."""
    await asyncio.sleep(0.5)
    dataui.hide_progress()


class DownloadDataCatalogAction:
    def callback(self, event, dataui):
        """Callback to download the currently displayed catalog as a CSV file."""
        dataui._display_panel.loading = True
        try:
            # Show indeterminate progress initially
            dataui.set_progress(-1)

            df = dataui._dataui_manager.get_data_catalog()

            # Update progress to 50%
            dataui.set_progress(50)

            sio = StringIO()
            df.to_csv(sio, index=False)
            sio.seek(0)

            # Indicate completion
            dataui.set_progress(100)

            return sio
        except Exception as e:
            logger.error(f"Error downloading catalog: {e}")
            if pn.state.notifications is not None:
                pn.state.notifications.error("Failed to download catalog")
            return None
        finally:
            dataui._display_panel.loading = False
            # We don't hide the progress bar here as the download might still be in progress


class PermalinkAction:
    def callback(self, event, dataui):
        # Implement permalink action callback here
        pass


class ClearCacheAction:
    """Invalidate the in-memory data cache on every DataReference in the catalog.

    A notification confirms success.  Use this when source files have been
    updated on disk and you want the UI to reload fresh data on the next plot.
    """

    def callback(self, event, dataui):
        try:
            catalog = dataui._dataui_manager.data_catalog
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
        except Exception as e:
            logger.error("Error clearing cache: %s", e)
            if pn.state.notifications is not None:
                pn.state.notifications.error(
                    f"Failed to clear cache: {e}", duration=0
                )


# MathRefEditorAction has moved to dvue.math_ref_editor.  Re-exported here
# for backward compatibility with code that does
# ``from dvue.actions import MathRefEditorAction``.
from .math_ref_editor import MathRefEditorAction  # noqa: E402, F401


class TransformToCatalogAction:
    """Add one ``MathDataReference`` per selected row that encodes the current
    Transform-tab settings as a single chained pandas expression.

    The new refs use ``variable_map`` (direct binding to the original
    :class:`~dvue.catalog.DataReference`) so that no catalog search is needed
    at load time.  Auto-generated names follow the pattern
    ``<original_name>__<transform_tag>`` where *transform_tag* describes the
    active transforms (e.g. ``resample_1D_mean``, ``rolling_24H_mean``, etc.).

    If no transforms are active the action warns the user and does nothing.
    """

    # ------------------------------------------------------------------
    # Expression / name building helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_expression_and_tag(manager) -> tuple[str, str]:
        """Return ``(chained_expression, tag_string)`` for the active transforms.

        The expression is built as a chain starting from the variable ``x``
        (which will be bound to the original :class:`~dvue.catalog.DataReference`
        via ``variable_map``).  Each active transform appends a pandas method
        call.  The *tag* is a concise label used in the auto-generated ref name.

        Tag shorthand:
          - fill gaps N     → ``fillN``
          - tidal filter    → ``tf``
          - resample 1D/mean → ``1D_mean``
          - rolling 24H/mean → ``r24H_mean``
          - diff 1 period   → ``diff``
          - diff N periods  → ``diffN``
          - cumsum          → ``cumsum``
          - scale 2.0       → ``x2.0``
        """
        expr = "x"
        tags = []

        # Fill gaps first — same order as the plot path in tsdataui.py.
        fill_gap = getattr(manager, "fill_gap", 0)
        if fill_gap and fill_gap > 0:
            expr = f"{expr}.interpolate(limit={fill_gap})"
            tags.append(f"fill{fill_gap}")

        # Tidal filter next — operates on raw (gap-filled) sub-daily data.
        # Resampling and rolling are applied to the filtered result.
        if getattr(manager, "do_tidal_filter", False):
            expr = f"cosine_lanczos({expr}, '40h')"
            tags.append("tf")

        period = getattr(manager, "resample_period", "").strip()
        if period:
            agg = getattr(manager, "resample_agg", "mean")
            expr = f"{expr}.resample('{period}').{agg}().dropna(how='all')"
            tags.append(f"{period}_{agg}")

        window = getattr(manager, "rolling_window", "").strip()
        if window:
            agg = getattr(manager, "rolling_agg", "mean")
            expr = f"{expr}.rolling('{window}').{agg}()"
            tags.append(f"r{window}_{agg}")

        if getattr(manager, "do_diff", False):
            periods = getattr(manager, "diff_periods", 1)
            expr = f"{expr}.diff({periods})"
            tags.append("diff" if periods == 1 else f"diff{periods}")

        if getattr(manager, "do_cumsum", False):
            expr = f"{expr}.cumsum()"
            tags.append("cumsum")

        scale = getattr(manager, "scale_factor", 1.0)
        if scale != 1.0:
            expr = f"{expr} * {scale!r}"
            tags.append(f"x{scale!r}")

        tag = "__".join(tags) if tags else ""
        return expr, tag

    @staticmethod
    def _build_ref_name(orig_ref, tag: str, manager) -> str:
        """Build a clean, short catalog key for the derived MathDataReference.

        Format: ``[s{source_num}_]{pk_values}__{tag}``

        The ``s{n}_`` prefix is added only when the catalog has multiple
        sources (``len(catalog._source_index) > 1``).  ``pk_values`` are
        the primary_key column values (excluding ``source_num`` and ``name``)
        joined with ``_``.
        """
        import re
        catalog = getattr(manager, "data_catalog", None)
        parts = []
        if catalog is not None:
            pk = catalog.primary_key
            if "source_num" in pk and len(catalog._source_index) > 1:
                snum = catalog._source_index.get(orig_ref.source)
                if snum is not None:
                    parts.append(f"s{snum}")
            for col in pk:
                if col in ("source_num", "name"):
                    continue
                val = orig_ref.get_attribute(col)
                if val is None:
                    val = orig_ref.get_dynamic_metadata(col)
                if not isinstance(val, (str, int, float, bool)):
                    continue
                if isinstance(val, float) and val != val:  # skip NaN
                    continue
                sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", str(val).strip()).strip("_")
                if sanitized:
                    parts.append(sanitized)
        else:
            parts.append(orig_ref.name)
        # Fallback: if no pk values were found, use the ref's own name.
        if not parts:
            parts.append(orig_ref.name)
        base = "_".join(parts)
        raw = f"{base}__{tag}" if tag else base
        # Dots are NOT valid Python identifier characters; strip them so the
        # generated name can be used directly as a token in further math
        # expressions (e.g. "STA1_flow__tf * 0.5").
        return re.sub(r"[^a-zA-Z0-9_]", "_", raw)

    @staticmethod
    def _get_id_column(manager, catalog) -> str | None:
        """Return the primary-key column used to tag transform-ref identifiers.

        Resolution order
        ----------------
        1. ``manager.transform_id_column`` — explicit opt-in by a subclass.
        2. First primary-key column (excluding ``source_num`` and ``name``)
           whose name contains the substring ``"id"`` (case-insensitive).
        3. First primary-key column (excluding ``source_num`` and ``name``).
        4. ``None`` when the catalog has no usable primary-key columns.

        The returned column's value is modified in the new transform ref's
        attributes (``original_value__tag``) so that catalog searches using
        the original value as a criterion do not match the transform ref.
        """
        pk_cols = [
            c for c in (catalog.primary_key if catalog else [])
            if c not in ("source_num", "name")
        ]
        explicit = getattr(manager, "transform_id_column", None)
        if explicit and explicit in pk_cols:
            return explicit
        for col in pk_cols:
            if "id" in col.lower():
                return col
        return pk_cols[0] if pk_cols else None

    # ------------------------------------------------------------------
    # UI refresh helper (mirrors MathRefEditorAction._on_save)
    # ------------------------------------------------------------------

    @staticmethod
    def _refresh_table(dataui, manager):
        try:
            new_df = manager.get_data_catalog()
            dataui._dfcat = new_df
            new_cols = manager.get_table_columns()
            sliced = dataui._dfcat.reindex(columns=new_cols)
            # Convert pandas ExtensionDtype columns (e.g. StringDtype from pandas 3.x)
            # to plain object dtype.  Panel's Tabulator data-only update path does not
            # handle non-numpy dtypes: the browser column definitions expect numpy-typed
            # arrays, so StringDtype values display as NaN for all but numeric-looking
            # columns (which get coerced to float).
            _ext_cols = {c: object for c, dt in sliced.dtypes.items() if not isinstance(dt, np.dtype)}
            if _ext_cols:
                sliced = sliced.astype(_ext_cols)
            dataui.display_table.value = sliced
            dataui.display_table.widths = manager.get_table_column_width_map()
            dataui.display_table.header_filters = manager.get_table_filters()
        except Exception as e:
            logger.warning("TransformToCatalogAction: table refresh failed: %s", e)

    # ------------------------------------------------------------------
    # Main callback
    # ------------------------------------------------------------------

    def callback(self, event, dataui):
        manager = dataui._dataui_manager
        catalog = getattr(manager, "data_catalog", None)

        # Guard: catalog required
        if catalog is None:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "No catalog attached — cannot add transform refs.", duration=4000
                )
            return

        # Guard: selection required
        if not dataui.display_table.selection:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "Please select at least one row from the table.", duration=3000
                )
            return

        expr, tag = self._build_expression_and_tag(manager)

        # Guard: at least one transform must be active
        if not tag:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "No transforms are active — enable at least one in the Transform tab "
                    "before adding to the catalog.",
                    duration=5000,
                )
            return

        from .math_reference import MathDataReference

        # Resolve selected rows from _dfcat (all catalog columns) via selection
        # indices into display_table.value.  Using selected_dataframe would omit
        # hidden metadata columns that get_data_reference() needs.
        _selection = dataui.display_table.selection
        _sel_index = dataui.display_table.value.iloc[_selection].index
        selected_rows = dataui._dfcat.loc[_sel_index]
        added_names = []
        id_col = self._get_id_column(manager, catalog)

        for _, row in selected_rows.iterrows():
            try:
                orig_ref = manager.get_data_reference(row)
            except Exception as e:
                logger.warning(
                    "TransformToCatalogAction: could not resolve ref for row %s: %s",
                    row.get("name", "?"),
                    e,
                )
                continue

            new_name = self._build_ref_name(orig_ref, tag, manager)

            # Copy attributes from the original ref, excluding:
            #   - "source": file path is not meaningful for a derived ref
            #   - NaN floats: produce literal "nan" tokens in keys and YAML
            def _is_nan(v):
                return isinstance(v, float) and v != v

            inherited_attrs = {
                k: v
                for k, v in orig_ref.attributes.items()
                if k not in ("source",) and not _is_nan(v)
            }

            # Stamp the id column with the tag so the transform ref's value
            # differs from the original (e.g. "STA001" → "STA001__tf").
            # This prevents catalog.search(**x_criteria) from matching both
            # the original ref and the transform ref at getData() time.
            if id_col and id_col in inherited_attrs:
                orig_id_val = str(inherited_attrs[id_col])
                inherited_attrs[id_col] = f"{orig_id_val}__{tag}"

            # Record the transform tag as a dedicated "tag" attribute.
            # This is the clean, domain-agnostic discriminator used in ref_key().
            inherited_attrs["tag"] = tag
            # Mark source as "transform" so the catalog table column is non-blank
            # and transform refs are easy to distinguish and filter.
            inherited_attrs["source"] = "transform"

            # Build search_map criteria for x using the catalog's primary_key columns.
            # Fall back to the catalog name only when no primary_key is set.
            pk_cols = [c for c in catalog.primary_key if c not in ("source_num", "name")]
            if pk_cols:
                x_criteria = {}
                for col in pk_cols:
                    val = orig_ref.get_attribute(col)
                    if val is None:
                        val = orig_ref.get_dynamic_metadata(col)
                    if val is not None and not _is_nan(val):
                        x_criteria[col] = val
            else:
                x_criteria = {"name": orig_ref.name}
            if len(catalog._source_index) > 1:
                snum = catalog._source_index.get(orig_ref.source)
                if snum is not None:
                    x_criteria["source_num"] = snum

            new_ref = MathDataReference(
                expression=expr,
                name=new_name,
                search_map={"x": x_criteria},
                cache=False,
                **inherited_attrs,
            )
            new_ref.set_catalog(catalog)

            # Remove any existing entry with same name so update is idempotent
            try:
                catalog.remove(new_name)
            except Exception:
                pass
            catalog.add(new_ref)
            added_names.append(new_name)

        if added_names:
            self._refresh_table(dataui, manager)
            if pn.state.notifications is not None:
                names_str = ", ".join(f"`{n}`" for n in added_names)
                pn.state.notifications.success(
                    f"Added {len(added_names)} transform ref(s) to catalog: {names_str}",
                    duration=6000,
                )
            logger.info("TransformToCatalogAction: added refs: %s", added_names)


# ---------------------------------------------------------------------------
# Source-compare helpers (pure functions — no Panel dependency, fully testable)
# ---------------------------------------------------------------------------

import re as _re

_METADATA_COLUMNS = frozenset({"ref_type", "expression", "tag", "name"})


def _sanitize_value(v: str) -> str:
    """Sanitise a string the same way ``DataReference.ref_key()`` does."""
    return _re.sub(r"[^a-zA-Z0-9]+", "_", str(v).strip()).strip("_")


def _detect_varying_columns(dfselected: "pd.DataFrame", identity_cols: list) -> list:
    """Return columns that are NOT in *identity_cols* and have >1 unique non-NaN value.

    Parameters
    ----------
    dfselected : pd.DataFrame
        Subset of the catalog DataFrame for the selected rows.
    identity_cols : list of str
        Columns that form the "identity" of a reference (station, parameter, …).

    Returns
    -------
    list of str
        Column names that vary across the selection and are candidates for the
        "vary by" dimension (typically the source / filename).
    """
    exclude = set(identity_cols) | _METADATA_COLUMNS
    varying = []
    for col in dfselected.columns:
        if col in exclude:
            continue
        unique_vals = dfselected[col].dropna().unique()
        if len(unique_vals) > 1:
            varying.append(col)
    return varying


def _group_by_identity(
    dfselected: "pd.DataFrame",
    identity_cols: list,
    vary_by_col: str,
) -> dict:
    """Group selected rows by identity key, keyed by a tuple of identity values.

    Parameters
    ----------
    dfselected : pd.DataFrame
        Selected catalog rows.
    identity_cols : list of str
        Columns that form the identity key.  When empty, all columns except
        *vary_by_col* and known metadata columns are used as the grouping key.
    vary_by_col : str
        The column whose variation we want to compare (excluded from grouping).

    Returns
    -------
    dict
        ``{identity_tuple: group_df}`` where *identity_tuple* is a tuple of
        the values for each identity column.
    """
    if identity_cols:
        group_cols = [c for c in identity_cols if c in dfselected.columns]
    else:
        group_cols = [
            c for c in dfselected.columns
            if c != vary_by_col and c not in _METADATA_COLUMNS
        ]
    if not group_cols:
        return {(): dfselected}
    groups = {}
    for key, grp in dfselected.groupby(group_cols, dropna=False):
        key_tuple = key if isinstance(key, tuple) else (key,)
        groups[key_tuple] = grp
    return groups


def _build_compare_name(identity_key_str: str, source_value: str, operation: str) -> str:
    """Build the catalog name for a source-compare MathDataReference.

    Parameters
    ----------
    identity_key_str : str
        The pre-computed identity key string (from ``_base_key`` or similar).
    source_value : str
        The raw value of the "vary by" column for the non-base reference.
    operation : str
        ``"Diff"`` or ``"Ratio"``.

    Returns
    -------
    str
        Sanitised catalog name, e.g. ``"sac_flow__diff_model"`` or
        ``"sac_flow__ratio_run2"``.
    """
    op_tag = "diff" if operation.lower() == "diff" else "ratio"
    sanitized_source = _sanitize_value(source_value)
    return f"{identity_key_str}__{op_tag}_{sanitized_source}"


def _create_compare_refs(
    manager,
    catalog,
    dfselected: "pd.DataFrame",
    vary_by_col: str,
    base_value,
    operation: str,
) -> int:
    """Create and add cross-source diff/ratio MathDataReferences to *catalog*.

    For every identity group in the selection that contains the *base_value*
    row, creates one ``MathDataReference`` per non-base row:

    * diff  → ``expression = "x - base"``
    * ratio → ``expression = "x / base"``

    Groups with no matching base row are silently skipped.

    Parameters
    ----------
    manager :
        The ``DataUIManager`` / ``TimeSeriesDataUIManager`` instance.
    catalog :
        The live ``DataCatalog``.
    dfselected : pd.DataFrame
        Selected catalog rows (with ``"name"`` column after ``reset_index``).
    vary_by_col : str
        The column that identifies the source dimension.
    base_value :
        The value of *vary_by_col* that is the subtrahend / divisor.
    operation : str
        ``"Diff"`` or ``"Ratio"``.

    Returns
    -------
    int
        Number of new references added to the catalog.
    """
    from .math_reference import MathDataReference

    catalog = getattr(manager, "data_catalog", None)
    identity_cols = [c for c in (catalog.primary_key if catalog is not None else [])
                     if c not in ("source_num", "name", vary_by_col)]
    groups = _group_by_identity(dfselected, identity_cols, vary_by_col)
    expression = "x - base" if operation.lower() == "diff" else "x / base"

    def _is_nan(v):
        return isinstance(v, float) and v != v

    added = 0
    for _key, grp in groups.items():
        # Find the base row in this group
        base_mask = grp[vary_by_col] == base_value
        if not base_mask.any():
            continue  # no base in this group — skip silently
        base_row = grp[base_mask].iloc[0]
        try:
            base_ref = manager.get_data_reference(base_row)
        except Exception as e:
            logger.warning("SourceCompareAction: could not resolve base ref: %s", e)
            continue

        non_base_rows = grp[~base_mask]
        for _, row in non_base_rows.iterrows():
            try:
                orig_ref = manager.get_data_reference(row)
            except Exception as e:
                logger.warning("SourceCompareAction: could not resolve ref: %s", e)
                continue

            # Build identity key string from the primary_key columns
            import re as _re2
            _pk_parts = []
            for _col in identity_cols:
                _val = orig_ref.get_attribute(_col)
                if _val is None:
                    _val = orig_ref.get_dynamic_metadata(_col)
                if not isinstance(_val, (str, int, float, bool)):
                    continue
                if isinstance(_val, float) and _val != _val:
                    continue
                _s = _re2.sub(r"[^a-zA-Z0-9]+", "_", str(_val).strip()).strip("_")
                if _s:
                    _pk_parts.append(_s)
            ident_key_str = "_".join(_pk_parts) if _pk_parts else orig_ref.name
            source_val = row.get(vary_by_col, "")
            new_name = _build_compare_name(ident_key_str, str(source_val), operation)

            # Inherit non-source, non-NaN attributes from the original ref
            inherited_attrs = {
                k: v
                for k, v in orig_ref.attributes.items()
                if k not in ("source",) and not _is_nan(v)
            }
            op_tag = "diff" if operation.lower() == "diff" else "ratio"
            sanitized_source = _sanitize_value(str(source_val))
            inherited_attrs["tag"] = f"{op_tag}_{sanitized_source}"
            # source = operation type so the table column is non-blank and filterable
            inherited_attrs["source"] = op_tag
            # compare_op / compare_source allow build_station_name to produce
            # human-readable labels without parsing the tag string
            inherited_attrs["compare_op"] = op_tag
            inherited_attrs["compare_source"] = str(source_val)

            new_ref = MathDataReference(
                expression=expression,
                name=new_name,
                variable_map={"x": orig_ref, "base": base_ref},
                cache=False,
                **inherited_attrs,
            )
            # Idempotent add
            try:
                catalog.remove(new_name)
            except Exception:
                pass
            catalog.add(new_ref)
            added += 1

    return added


class SourceCompareAction:
    """Open a config form in the display area to generate cross-source diff/ratio refs.

    When triggered, opens a tab in the display panel containing:

    * A **Vary by** Select widget (auto-populated with columns that differ
      across the selection and are not identity columns).
    * A **Base source** Select widget (populated from unique values of the
      chosen vary-by column; updates reactively).
    * A **Diff / Ratio** RadioButtonGroup.
    * A **Create** button that builds ``MathDataReference`` entries for every
      non-base/base pair sharing the same identity key and adds them to the
      live catalog.

    Groups without a matching base row are skipped silently.
    """

    def callback(self, event, dataui):
        manager = dataui._dataui_manager
        catalog = getattr(manager, "data_catalog", None)

        if catalog is None:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "No catalog attached — cannot create compare refs.", duration=4000
                )
            return

        if not dataui.display_table.selection:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "Please select at least one row from the table.", duration=3000
                )
            return

        dfselected = dataui.display_table.selected_dataframe.copy()
        if "name" not in dfselected.columns:
            dfselected = dfselected.reset_index()

        catalog = getattr(manager, "data_catalog", None)
        identity_cols = [c for c in (catalog.primary_key if catalog is not None else [])
                         if c not in ("source_num", "name")]
        varying_cols = _detect_varying_columns(dfselected, identity_cols)

        if not varying_cols:
            if pn.state.notifications is not None:
                pn.state.notifications.warning(
                    "No varying columns detected in the selection — all selected rows "
                    "appear to come from the same source.",
                    duration=5000,
                )
            return

        # --- Build widgets ---
        def _unique_values(col):
            return [v for v in dfselected[col].dropna().unique().tolist()]

        vary_by_select = pn.widgets.Select(
            name="Vary by column",
            options=varying_cols,
            value=varying_cols[0],
            width=200,
        )
        base_select = pn.widgets.Select(
            name="Base source",
            options=_unique_values(varying_cols[0]),
            width=250,
        )
        operation_radio = pn.widgets.RadioButtonGroup(
            name="Operation",
            options=["Diff", "Ratio"],
            value="Diff",
            button_type="default",
        )
        create_btn = pn.widgets.Button(
            name="Create",
            button_type="success",
            icon="arrows-collapse",
            width=120,
        )
        status_pane = pn.pane.Markdown("", width=400)

        # Reactively refresh base_select options when vary_by changes
        def _update_base_options(event):
            base_select.options = _unique_values(event.new)
            if base_select.options:
                base_select.value = base_select.options[0]

        vary_by_select.param.watch(_update_base_options, "value")

        # Create button handler — closes over the live widgets
        def _on_create(evt):
            try:
                n = _create_compare_refs(
                    manager=manager,
                    catalog=catalog,
                    dfselected=dfselected,
                    vary_by_col=vary_by_select.value,
                    base_value=base_select.value,
                    operation=operation_radio.value,
                )
                if n:
                    TransformToCatalogAction._refresh_table(dataui, manager)
                    status_pane.object = f"**Created {n} reference(s).**"
                    logger.info("SourceCompareAction: added %d refs", n)
                else:
                    status_pane.object = (
                        "_No pairs found — check that the base value appears "
                        "in each identity group._"
                    )
            except Exception as e:
                logger.error("SourceCompareAction: error creating refs: %s", e)
                status_pane.object = f"**Error:** {e}"

        create_btn.on_click(_on_create)

        form = pn.Column(
            pn.pane.Markdown(
                "### Source Compare\n"
                "Select the column that distinguishes sources, choose the base, "
                "then click **Create** to add diff/ratio references to the catalog."
            ),
            pn.Row(vary_by_select, base_select, operation_radio),
            pn.Row(create_btn, status_pane),
            sizing_mode="stretch_width",
        )

        dataui.show_in_display_panel("Source Compare", form)


class AddSourceFilesAction:
    """Append source files to the live catalog at runtime.

    Opens a form in the display area with a file-path text input.  On
    confirm, calls ``manager.add_source_files(path)`` and refreshes the
    catalog table.  Supported file types depend on the active manager's
    :meth:`~dvue.tsdataui.TimeSeriesDataUIManager.add_source_files` override.

    In desktop (pywebview) sessions, files can also be dragged directly onto
    the window — :func:`~dvue.session_persistence.serve_desktop_app` wires
    the drop events to call ``add_source_files`` automatically via a
    background queue.  This toolbar button is the manual fallback.
    """

    def callback(self, event, dataui):
        manager = dataui._dataui_manager

        path_input = pn.widgets.TextInput(
            name="File path",
            placeholder="Enter absolute path to a supported file…",
            sizing_mode="stretch_width",
        )
        add_btn = pn.widgets.Button(
            name="Add",
            button_type="success",
            icon="folder-plus",
            width=100,
        )
        status_pane = pn.pane.Markdown("", sizing_mode="stretch_width")

        def _on_add(evt):
            path = (path_input.value or "").strip()
            if not path:
                status_pane.object = "_Please enter a file path._"
                return
            add_btn.disabled = True
            status_pane.object = "_Loading\u2026_"
            curdoc = pn.state.curdoc

            def _do_add():
                try:
                    added = manager.add_source_files(path)
                    err = None
                except Exception as e:
                    added = []
                    err = str(e)

                def _done():
                    add_btn.disabled = False
                    if err:
                        logger.error(
                            "AddSourceFilesAction: error adding %s: %s", path, err
                        )
                        status_pane.object = f"**Error:** {err}"
                        return
                    if added:
                        TransformToCatalogAction._refresh_table(dataui, manager)
                        status_pane.object = (
                            f"**Added {len(added)} reference(s)** from `{path}`."
                        )
                        path_input.value = ""
                        logger.info(
                            "AddSourceFilesAction: added %d refs from %s",
                            len(added),
                            path,
                        )
                    else:
                        status_pane.object = (
                            f"_No references added from `{path}`.  "
                            "Check that the file type is supported by this manager._"
                        )

                curdoc.add_next_tick_callback(_done)

            threading.Thread(target=_do_add, daemon=True).start()

        add_btn.on_click(_on_add)

        # Browse button — only available when tkinter is importable (local process).
        # In a remote-server session tkinter cannot open a dialog on the server
        # machine, so the button is hidden rather than raising an error.
        _tkinter_available = False
        try:
            import tkinter as _tk_probe  # noqa: F401
            _tkinter_available = True
        except Exception:
            pass

        input_row = pn.Row(path_input, add_btn, sizing_mode="stretch_width")

        if _tkinter_available:
            browse_btn = pn.widgets.Button(
                name="Browse…",
                button_type="light",
                icon="folder-open",
                width=120,
            )

            def _on_browse(evt):  # noqa: ARG001
                browse_btn.disabled = True
                status_pane.object = "_Opening file picker…_"
                curdoc = pn.state.curdoc

                def _run_dialog():
                    selected = ()
                    try:
                        import tkinter as tk
                        from tkinter import filedialog
                        root = tk.Tk()
                        root.withdraw()
                        root.wm_attributes("-topmost", True)
                        selected = filedialog.askopenfilenames(
                            title="Select file(s) to add",
                            parent=root,
                        )
                        root.destroy()
                    except Exception as exc:
                        logger.warning(
                            "AddSourceFilesAction: file dialog error: %s", exc
                        )

                    def _apply():
                        browse_btn.disabled = False
                        if not selected:
                            status_pane.object = "_No file selected._"
                            return
                        if len(selected) == 1:
                            # Single file — populate the text input for review.
                            path_input.value = selected[0]
                            status_pane.object = (
                                "_Path set — click **Add** to confirm._"
                            )
                        else:
                            # Multiple files — add immediately.
                            total: list = []
                            errors: list = []
                            for p in selected:
                                try:
                                    total.extend(manager.add_source_files(p))
                                except Exception as exc:
                                    errors.append(
                                        f"`{os.path.basename(p)}`: {exc}"
                                    )
                            if total:
                                TransformToCatalogAction._refresh_table(
                                    dataui, manager
                                )
                            msg = (
                                f"**Added {len(total)} reference(s)** from "
                                f"{len(selected)} file(s)."
                            )
                            if errors:
                                msg += "\n\n**Errors:**\n" + "\n".join(
                                    f"- {e}" for e in errors
                                )
                            status_pane.object = msg

                    curdoc.add_next_tick_callback(_apply)

                threading.Thread(target=_run_dialog, daemon=True).start()

            browse_btn.on_click(_on_browse)
            input_row = pn.Row(path_input, add_btn, browse_btn, sizing_mode="stretch_width")

        form = pn.Column(
            pn.pane.Markdown(
                "### Add Files\n"
                "Enter or browse to a file path to append its catalog entries "
                "to the current view.  "
                "Supported types depend on the active data manager.  "
                "In desktop mode, you can also drag files directly onto the window."
            ),
            input_row,
            status_pane,
            sizing_mode="stretch_width",
        )

        dataui.show_in_display_panel("Add Files", form)
