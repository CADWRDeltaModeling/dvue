import asyncio
import threading

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
        dfselected = dataui._dfcat.iloc[dataui.display_table.selection].copy()
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
                        tabs.append((str(dataui._tab_count), plot_panel))
                        tabs.active = len(tabs) - 1
                    else:
                        dataui._tab_count = 0
                        dataui._display_panel.objects = [
                            pn.Tabs((str(dataui._tab_count), plot_panel), closable=True)
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
        dfselected = dataui._dfcat.iloc[dataui.display_table.selection].copy()
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
            dfdata = pd.concat(
                [df for df in dataui._dataui_manager.get_data(dfselected)], axis=1
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

        # Tidal filter first — operates on raw sub-daily data.
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
    def _identity_key_columns(orig_ref, manager) -> list:
        """Return the identity attribute names to use for naming.

        Fallback chain (first non-empty wins):

        1. ``orig_ref._key_attributes`` — explicitly set on the ref by its
           catalog builder (the preferred, per-ref approach).
        2. ``manager.identity_key_columns`` — manager-level bridge for managers
           that haven’t yet adopted per-ref ``set_key_attributes()``.
        3. Empty list — caller falls back to full ``ref_key()`` (all attrs).
        """
        if orig_ref._key_attributes is not None:
            return list(orig_ref._key_attributes)
        cols = getattr(manager, "identity_key_columns", [])
        if cols:
            return list(cols)
        return []

    @staticmethod
    def _base_key(orig_ref, identity_cols: list) -> str:
        """Return a short sanitised identity key for *orig_ref*.

        If *identity_cols* is non-empty, compute ``ref_key()`` using only
        those columns — without mutating the original ref’s own
        ``_key_attributes``.
        """
        import re
        if identity_cols:
            # Compute ref_key using only the chosen columns, non-destructively.
            parts = []
            for col in identity_cols:
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
            return "_".join(parts)
        # Full ref_key() — may be verbose if set_key_attributes was not called
        return orig_ref.ref_key()

    @staticmethod
    def _build_ref_name(orig_ref, tag: str, manager) -> str:
        """Build a clean, short catalog key for the derived MathDataReference.

        Format: ``[f{url_num}_]{identity_key}__{tag}``

        The file prefix ``f{n}_`` is added only when the catalog contains
        multiple source files (``manager.display_url_num`` is ``True``),
        using the ``url_num`` dynamic metadata on the original ref.
        """
        import re
        identity_cols = TransformToCatalogAction._identity_key_columns(orig_ref, manager)
        base = TransformToCatalogAction._base_key(orig_ref, identity_cols)

        # File prefix — only when multiple source files exist
        prefix = ""
        if getattr(manager, "display_url_num", False):
            url_num = orig_ref.get_dynamic_metadata("url_num")
            if url_num is not None:
                prefix = f"f{url_num}_"

        raw = f"{prefix}{base}__{tag}" if tag else f"{prefix}{base}"
        # Keep alphanumeric, underscores, and dots; collapse everything else to _
        return re.sub(r"[^a-zA-Z0-9_.]", "_", raw)

    # ------------------------------------------------------------------
    # UI refresh helper (mirrors MathRefEditorAction._on_save)
    # ------------------------------------------------------------------

    @staticmethod
    def _refresh_table(dataui, manager):
        try:
            new_df = manager.get_data_catalog()
            dataui._dfcat = new_df
            new_cols = manager.get_table_columns()
            dataui.display_table.value = dataui._dfcat[new_cols]
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

        selected_rows = dataui._dfcat.iloc[dataui.display_table.selection]
        added_names = []

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

            # Record the transform tag as a dedicated "tag" attribute.
            # This is the clean, domain-agnostic discriminator used in ref_key().
            inherited_attrs["tag"] = tag

            # Build search_map criteria for x using the identity key attributes
            # so the YAML is both readable and portable.  Fall back to the
            # catalog name only when no identity columns are known.
            identity_cols = self._identity_key_columns(orig_ref, manager)
            if identity_cols:
                x_criteria = {}
                for col in identity_cols:
                    val = orig_ref.get_attribute(col)
                    if val is None:
                        val = orig_ref.get_dynamic_metadata(col)
                    if val is not None and not _is_nan(val):
                        x_criteria[col] = val
            else:
                x_criteria = {"name": orig_ref.name}

            new_ref = MathDataReference(
                expression=expr,
                name=new_name,
                search_map={"x": x_criteria},
                cache=False,
                **inherited_attrs,
            )
            new_ref.set_catalog(catalog)

            # Set key attributes: identity cols + "tag" so ref_key() on the
            # new math ref is clean and includes the transform signature.
            # Always set even when identity_cols is empty so "tag" is in the key.
            new_ref.set_key_attributes((identity_cols or []) + ["tag"])

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
