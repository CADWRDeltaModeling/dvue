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
            # Mark source as "transform" so the catalog table column is non-blank
            # and transform refs are easy to distinguish and filter.
            inherited_attrs["source"] = "transform"

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

    identity_cols = list(getattr(manager, "identity_key_columns", []) or [])
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

            # Build identity key string from the original ref
            ident_key_str = TransformToCatalogAction._base_key(orig_ref, identity_cols)
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
            new_ref.set_key_attributes((identity_cols or []) + ["tag"])

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

        dfselected = dataui._dfcat.iloc[dataui.display_table.selection].copy()
        if "name" not in dfselected.columns:
            dfselected = dfselected.reset_index()

        identity_cols = list(getattr(manager, "identity_key_columns", []) or [])
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
