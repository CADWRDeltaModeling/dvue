"""Panel-based editor for :class:`~dvue.math_reference.MathDataReference` objects.

The :class:`MathRefEditorAction` class provides a callback that opens an
inline editor inside the DataUI display panel.  Users can:

* Create a new :class:`~dvue.math_reference.MathDataReference` from scratch.
* Edit an existing one selected from the catalog table.
* Save the resulting math references to a YAML file for later reuse.
* Load math references from a YAML file directly into the live catalog.

The editor supports:

* Expression with NumPy math functions.
* Attributes as ``key: value`` lines.
* Search map as ``var: attr=val, attr=val`` lines, with an optional
  ``[multi]`` tag per variable to concat multiple results instead of
  taking only the first (``require_single=False``).
* Per-variable ``require_single`` flag surfaced through the ``[multi]`` syntax.
* YAML save/load via :func:`~dvue.math_reference.save_math_refs` and
  :class:`~dvue.math_reference.MathDataCatalogReader`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import panel as pn

logger = logging.getLogger(__name__)


class MathRefEditorAction:
    """Action that opens a Panel editor for creating / editing MathDataReference objects.

    When the user clicks the "Math Ref" button the action:

    1. Pre-populates the form from the first selected row (if it is a
       :class:`~dvue.math_reference.MathDataReference`); otherwise starts
       with an empty form.
    2. On **Save**, creates or updates a
       :class:`~dvue.math_reference.MathDataReference` in the manager's
       catalog and refreshes the table.
    3. On **Save to YAML**, writes all current math refs in the catalog to a
       YAML file via :func:`~dvue.math_reference.save_math_refs`.
    4. On **Load from YAML**, reads a YAML file via
       :class:`~dvue.math_reference.MathDataCatalogReader` and merges the
       resulting :class:`~dvue.math_reference.MathDataReference` objects into
       the live catalog, then refreshes the table.

    The catalog is retrieved from ``dataui._dataui_manager.data_catalog``.

    Search map
    ----------
    The **Search Map** section shows one row per expression variable.  Each
    row contains:

    * **Alias** — the short identifier used inside the expression (e.g. ``obs``).
    * **Join all** checkbox — when enabled, *all* catalog entries that match
      the criteria are concatenated column-wise instead of taking the first
      result only (equivalent to ``require_single=False``).
    * **Catalog criteria** — comma-separated ``attr=val`` pairs that are
      passed to :meth:`~dvue.catalog.DataCatalog.search` at ``getData()`` time.

    Click **+ Add variable** to append a new row; click the **✕** button on any
    row to remove it.

    Usage
    -----
    ::

        def get_data_actions(self):
            actions = super().get_data_actions()
            actions.append(dict(
                name="Math Ref",
                button_type="warning",
                icon="function",
                action_type="display",
                callback=MathRefEditorAction().callback,
            ))
            return actions
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, default_yaml_path: str = "") -> None:
        """Create the action.

        Parameters
        ----------
        default_yaml_path : str, optional
            Pre-fills the YAML path inputs in the editor so the user does not
            have to type the path manually.
        """
        self._default_yaml_path = default_yaml_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_attrs(text: str) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        for line in text.splitlines():
            line = line.strip()
            if ":" in line:
                k, _, v = line.partition(":")
                attrs[k.strip()] = v.strip()
        return attrs

    @staticmethod
    def _parse_search_map(text: str):
        """Parse ``var[multi]: key=val, key=val`` lines.

        Returns
        -------
        tuple[dict, dict]
            ``(search_map, search_require_single)``
        """
        import re

        sm: Dict[str, Dict[str, str]] = {}
        req: Dict[str, bool] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            var_part, _, criteria_str = line.partition(":")
            var_part = var_part.strip()
            # Check for [multi] tag – case-insensitive.
            multi = bool(re.search(r"\[multi\]", var_part, re.IGNORECASE))
            var = re.sub(r"\[multi\]", "", var_part, flags=re.IGNORECASE).strip()
            criteria: Dict[str, str] = {}
            for part in criteria_str.split(","):
                part = part.strip()
                if "=" in part:
                    k, _, v = part.partition("=")
                    criteria[k.strip()] = v.strip()
            if var and criteria:
                sm[var] = criteria
                req[var] = not multi  # require_single=False when [multi] present
        return sm, req

    @staticmethod
    def _render_search_map(search_map: Dict[str, Any], req: Dict[str, bool]) -> str:
        """Render ``search_map`` + ``search_require_single`` to editor text."""
        lines = []
        for var, criteria in search_map.items():
            require_single = req.get(var, True)
            tag = "" if require_single else "[multi]"
            criteria_str = ", ".join(f"{k}={v}" for k, v in criteria.items())
            lines.append(f"{var}{tag}: {criteria_str}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Main callback
    # ------------------------------------------------------------------

    def callback(self, event: Any, dataui: Any) -> None:  # noqa: C901
        from .math_reference import MathDataReference, MathDataCatalogReader, save_math_refs

        manager = dataui._dataui_manager
        catalog = getattr(manager, "data_catalog", None)
        if catalog is None:
            if pn.state.notifications is not None:
                pn.state.notifications.error(
                    "MathRefEditor requires the manager to expose a 'data_catalog' property.",
                    duration=5000,
                )
            return

        # ── Pre-populate from selected row if it is a MathDataReference ──────
        pre_name = ""
        pre_expr = ""
        pre_attrs = ""
        is_edit = False

        selected = dataui.display_table.selection
        if selected:
            row = dataui.display_table.value.iloc[selected[0]]
            name_val = row.get("name", "") if hasattr(row, "get") else ""
            ref = None
            try:
                ref = catalog.get(str(name_val)) if name_val else None
            except KeyError:
                pass
            if isinstance(ref, MathDataReference):
                is_edit = True
                pre_name = ref.name
                pre_expr = ref.expression
                # Only put primitive values in the text area – non-primitives like
                # Shapely geometry objects cannot round-trip through plain text.
                extra = {
                    k: v
                    for k, v in ref.attributes.items()
                    if k != "expression" and isinstance(v, (str, int, float, bool, type(None)))
                }
                pre_attrs = "\n".join(f"{k}: {v}" for k, v in extra.items())
        # ── Build the editor form ─────────────────────────────────────────────
        title_md = pn.pane.Markdown("### Math Reference Editor", sizing_mode="stretch_width")
        help_md = pn.pane.Markdown(
            "**Expression** — use NumPy functions (`cumsum`, `sqrt`, `abs`, `where`, …) "
            "plus variable aliases defined in the Search Map.\n\n"
            "**Attributes** — one `key: value` pair per line (e.g. `variable: flow`).",
            sizing_mode="stretch_width",
        )
        name_input = pn.widgets.TextInput(
            name="Name (catalog key)",
            value=pre_name,
            placeholder="e.g. bias_RIO001",
        )
        expr_input = pn.widgets.TextAreaInput(
            name="Expression",
            value=pre_expr,
            placeholder="e.g. water_level_usgs - model_RIO001",
            height=90,
        )
        attrs_input = pn.widgets.TextAreaInput(
            name="Attributes  (key: value — one per line)",
            value=pre_attrs,
            placeholder="variable: water_level_bias\nstationid: RIO001\nunit: m",
            height=110,
        )
        # ── Search Map: dynamic row editor ─────────────────────────────────────
        sm_rows: list = []  # list of (var_inp, multi_cb, crit_inp, data_row)

        sm_header_md = pn.pane.Markdown(
            "**Search Map** — one row per expression variable.  "
            "Each alias is resolved against the catalog at `getData()` time using the criteria.",
            sizing_mode="stretch_width",
        )
        sm_col_labels = pn.Row(
            pn.pane.Markdown("**Alias**", width=134, margin=(0, 4, 0, 4)),
            pn.pane.Markdown("**Join all**", width=88, margin=(0, 4, 0, 4)),
            pn.pane.Markdown(
                "**Catalog criteria** (`attr=val, attr=val …`)",
                sizing_mode="stretch_width",
                margin=(0, 4, 0, 4),
            ),
            pn.pane.Markdown("", width=46),
            margin=(2, 0, 0, 0),
        )
        add_var_btn = pn.widgets.Button(
            name="+ Add variable",
            button_type="default",
            icon="plus",
            width=170,
            height=34,
            margin=(6, 4, 4, 4),
        )
        search_map_section = pn.Column(
            sm_header_md,
            sm_col_labels,
            # dynamic rows are inserted before add_var_btn
            add_var_btn,
            sizing_mode="stretch_width",
            styles={"border": "1px solid #ddd", "border-radius": "4px", "padding": "8px"},
        )

        def _add_sm_row(
            var_name: str = "", criteria_str: str = "", keep_single: bool = True
        ) -> None:
            _var_inp = pn.widgets.TextInput(
                value=var_name,
                placeholder="alias (e.g. obs)",
                sizing_mode="fixed",
                width=130,
                margin=(2, 4, 4, 4),
            )
            _multi_cb = pn.widgets.Checkbox(
                name="Join all",
                value=not keep_single,
                width=84,
                margin=(8, 4, 0, 4),
            )
            _crit_inp = pn.widgets.TextInput(
                value=criteria_str,
                placeholder="attr=val, attr=val",
                sizing_mode="stretch_width",
                margin=(2, 4, 4, 4),
            )
            _rm_btn = pn.widgets.Button(
                name="✕",
                button_type="light",
                width=40,
                height=34,
                margin=(4, 4, 4, 4),
                styles={"color": "#c00", "font-weight": "bold"},
            )
            _data_row = pn.Row(
                _var_inp,
                _multi_cb,
                _crit_inp,
                _rm_btn,
                sizing_mode="stretch_width",
                margin=(2, 0),
            )
            _row_ref = [_data_row]

            def _on_rm(ev: Any) -> None:
                for _i, (_v, _m, _c, _r) in enumerate(sm_rows):
                    if _r is _row_ref[0]:
                        sm_rows.pop(_i)
                        break
                search_map_section.objects = [
                    _o for _o in search_map_section.objects if _o is not _row_ref[0]
                ]

            _rm_btn.on_click(_on_rm)
            sm_rows.append((_var_inp, _multi_cb, _crit_inp, _data_row))
            # Insert the new row before the add_var_btn (always the last item)
            search_map_section.insert(len(search_map_section.objects) - 1, _data_row)

        add_var_btn.on_click(lambda _e: _add_sm_row())

        # Pre-populate rows when editing an existing MathDataReference.
        if is_edit and ref is not None and getattr(ref, "_search_map", None):
            for _sm_var, _sm_crit in ref._search_map.items():
                _sm_keep = ref._search_require_single.get(_sm_var, True)
                _sm_crit_str = ", ".join(f"{k}={v}" for k, v in _sm_crit.items())
                _add_sm_row(_sm_var, _sm_crit_str, _sm_keep)

        # ── Catalog attribute browser ─────────────────────────────────────────
        try:
            df_cat = catalog.to_dataframe()
            if not df_cat.empty:
                col_info = []
                for col in df_cat.columns:
                    uniq = df_cat[col].dropna().unique()
                    sample = ", ".join(str(v) for v in uniq[:6])
                    if len(uniq) > 6:
                        sample += f", … ({len(uniq)} total)"
                    col_info.append(f"- **{col}**: {sample}")
                attr_browser_md = pn.pane.Markdown(
                    "**Catalog attributes (for search criteria):**\n\n" + "\n".join(col_info),
                    sizing_mode="stretch_width",
                    styles={"font-size": "0.82em", "max-height": "160px", "overflow-y": "auto"},
                )
            else:
                attr_browser_md = pn.pane.Markdown(
                    "_Catalog is empty._", sizing_mode="stretch_width"
                )
        except Exception:
            attr_browser_md = pn.pane.Markdown(
                "_Could not read catalog attributes._", sizing_mode="stretch_width"
            )

        catalog_names = pn.pane.Markdown(
            "**Available catalog names:**  \n" + "  \n".join(sorted(catalog.list_names())),
            sizing_mode="stretch_width",
            styles={"font-size": "0.82em", "max-height": "140px", "overflow-y": "auto"},
        )

        status_md = pn.pane.Markdown("", sizing_mode="stretch_width")
        save_btn = pn.widgets.Button(name="Save", button_type="success", icon="device-floppy")
        cancel_btn = pn.widgets.Button(name="Cancel", button_type="default", icon="x")
        # Single shared path input used by both Save and Load actions.
        yaml_path_input = pn.widgets.TextInput(
            name="YAML file path",
            value=self._default_yaml_path,
            placeholder="e.g. math_refs.yaml",
            sizing_mode="stretch_width",
        )
        save_yaml_btn = pn.widgets.Button(
            name="Save to YAML",
            button_type="primary",
            icon="file-export",
            width=150,
        )
        load_from_yaml_btn = pn.widgets.Button(
            name="Load from YAML",
            button_type="primary",
            icon="file-import",
            width=150,
        )

        editor_panel = pn.Column(
            title_md,
            pn.layout.Divider(),
            pn.pane.Markdown("#### YAML file"),
            yaml_path_input,
            pn.Row(load_from_yaml_btn, save_yaml_btn),
            pn.layout.Divider(),
            help_md,
            name_input,
            expr_input,
            attrs_input,
            pn.layout.Divider(),
            search_map_section,
            pn.layout.Divider(),
            attr_browser_md,
            catalog_names,
            pn.layout.Divider(),
            status_md,
            pn.Row(save_btn, cancel_btn),
            sizing_mode="stretch_width",
            width=580,
        )

        def _on_save(event: Any) -> None:
            name = name_input.value.strip()
            expr = expr_input.value.strip()
            if not name:
                status_md.object = "⚠️ **Name is required.**"
                return
            if not expr:
                status_md.object = "⚠️ **Expression is required.**"
                return
            try:
                attrs = self._parse_attrs(attrs_input.value)
                # Build search_map from the dynamic row widgets.
                search_map: Dict[str, Dict[str, str]] = {}
                search_req: Dict[str, bool] = {}
                for _vi, _mc, _ci, _dr in sm_rows:
                    _sv = _vi.value.strip()
                    _sc = _ci.value.strip()
                    if not _sv or not _sc:
                        continue
                    _criteria: Dict[str, str] = {}
                    for _part in _sc.split(","):
                        _part = _part.strip()
                        if "=" in _part:
                            _k, _, _v = _part.partition("=")
                            _criteria[_k.strip()] = _v.strip()
                    if _criteria:
                        search_map[_sv] = _criteria
                        search_req[_sv] = not _mc.value  # require_single = not join_all
                # Remove an existing entry with the same name so the update lands cleanly.
                try:
                    catalog.remove(name)
                    action_word = "Updated"
                except KeyError:
                    action_word = "Created"
                new_ref = MathDataReference(
                    expression=expr,
                    name=name,
                    search_map=search_map if search_map else None,
                    search_require_single=search_req if search_req else None,
                    **attrs,
                )
                new_ref.set_catalog(catalog)
                # When updating an existing ref, preserve non-primitive attributes
                # (e.g. Shapely geometry) that cannot round-trip through the text area.
                if is_edit and ref is not None:
                    for _k, _v in ref.attributes.items():
                        if (
                            not isinstance(_v, (str, int, float, bool, type(None)))
                            and _k not in new_ref.attributes
                        ):
                            new_ref.set_attribute(_k, _v)
                catalog.add(new_ref)
                # Refresh the table – guard against get_data_catalog() failures
                # (e.g. if the manager builds a GeoDataFrame and new refs lack geometry).
                if hasattr(dataui, "_dfcat"):
                    try:
                        dataui._dfcat = manager.get_data_catalog()
                        dataui.display_table.value = dataui._dfcat[manager.get_table_columns()]
                    except Exception as _te:
                        logger.warning("Could not refresh table after save: %s", _te)
                status_md.object = f"✅ **{action_word}** `{name}` in catalog."
            except Exception as exc:
                logger.exception("MathRefEditorAction save error")
                status_md.object = f"❌ **Error:** {exc}"

        def _on_save_yaml(event: Any) -> None:
            path = yaml_path_input.value.strip()
            if not path:
                status_md.object = "⚠️ **YAML path is required.**"
                return
            try:
                save_math_refs(catalog, path)
                status_md.object = f"✅ **Saved** math refs to `{path}`."
            except Exception as exc:
                logger.exception("MathRefEditorAction YAML save error")
                status_md.object = f"❌ **YAML save error:** {exc}"

        def _on_load_yaml(event: Any) -> None:
            path = yaml_path_input.value.strip()
            if not path:
                status_md.object = "⚠️ **YAML path is required.**"
                return
            try:
                reader = MathDataCatalogReader(parent_catalog=catalog)
                refs = reader.build(path)
                added = 0
                for r in refs:
                    # Update semantics: remove any existing ref with the same
                    # name before re-adding so callers get the latest version.
                    try:
                        catalog.remove(r.name)
                    except KeyError:
                        pass
                    catalog.add(r)
                    added += 1
                if hasattr(dataui, "_dfcat"):
                    try:
                        dataui._dfcat = manager.get_data_catalog()
                        new_cols = manager.get_table_columns()
                        dataui.display_table.value = dataui._dfcat[new_cols]
                        # Refresh widths and header filters so the expression
                        # column appears correctly after a first-time load.
                        dataui.display_table.widths = manager.get_table_column_width_map()
                        dataui.display_table.header_filters = manager.get_table_filters()
                    except Exception as _te:
                        logger.warning("Could not refresh table after load: %s", _te)
                status_md.object = f"✅ **Loaded** {added} math ref(s) from `{path}`."
            except Exception as exc:
                logger.exception("MathRefEditorAction YAML load error")
                status_md.object = f"❌ **YAML load error:** {exc}"

        def _on_cancel(event: Any) -> None:
            editor_panel.objects = [pn.pane.Markdown("*Editor closed.*")]

        save_btn.on_click(_on_save)
        cancel_btn.on_click(_on_cancel)
        save_yaml_btn.on_click(_on_save_yaml)
        load_from_yaml_btn.on_click(_on_load_yaml)

        # Show editor as a new tab in the display panel.
        if len(dataui._display_panel.objects) > 0 and isinstance(
            dataui._display_panel.objects[0], pn.Tabs
        ):
            tabs = dataui._display_panel.objects[0]
            if not hasattr(dataui, "_tab_count"):
                dataui._tab_count = 0
            dataui._tab_count += 1
            tabs.append((f"Math Ref Editor {dataui._tab_count}", editor_panel))
            tabs.active = len(tabs) - 1
        else:
            dataui._tab_count = 1
            dataui._display_panel.objects = [
                pn.Tabs(("Math Ref Editor", editor_panel), closable=True, dynamic=True)
            ]
