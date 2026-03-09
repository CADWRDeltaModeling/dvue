"""Panel-based editor for :class:`~dvue.math_reference.MathDataReference` objects.

The :class:`MathRefEditorAction` class provides a callback that opens an
inline editor inside the DataUI display panel.  Users can:

* Create a new :class:`~dvue.math_reference.MathDataReference` from scratch.
* Edit an existing one selected from the catalog table.
* Save the resulting math references to a YAML file for later reuse.

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

    The catalog is retrieved from ``dataui._dataui_manager.data_catalog``.

    Search-map syntax
    -----------------
    Each line in the *Search Map* text area has the form::

        var_name: attr=val, attr=val

    Append ``[multi]`` after the variable name to request that **all**
    matching catalog entries are fetched and joined by index (instead of
    taking only the first result)::

        inflow[multi]: variable=discharge, location=upstream
        outflow: variable=discharge, location=downstream

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
        pre_search = ""
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
                extra = {k: v for k, v in ref.attributes.items() if k != "expression"}
                pre_attrs = "\n".join(f"{k}: {v}" for k, v in extra.items())
                if ref._search_map:
                    pre_search = self._render_search_map(
                        ref._search_map, ref._search_require_single
                    )

        # ── Build the editor form ─────────────────────────────────────────────
        title_md = pn.pane.Markdown("### Math Reference Editor", sizing_mode="stretch_width")
        help_md = pn.pane.Markdown(
            "**Expression** — use NumPy functions (`cumsum`, `sqrt`, `abs`, `where`, …) "
            "and variable names that match reference names in the catalog or entries in "
            "the Search Map below.\n\n"
            "**Attributes** — one `key: value` pair per line (e.g. `variable: flow`).\n\n"
            "**Search Map** — one `var: attr=val, attr=val` pair per line.  "
            "Each variable is resolved by searching the catalog at `getData()` time.  "
            "Append `[multi]` to the variable name to join *all* matching results "
            "by index (default: first result only).\n\n"
            "*Example:*\n"
            "```\n"
            "inflow[multi]: variable=discharge, location=upstream\n"
            "outflow: variable=discharge, location=downstream\n"
            "```",
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
        search_map_input = pn.widgets.TextAreaInput(
            name="Search Map  (var[multi]: attr=val, attr=val — one variable per line)",
            value=pre_search,
            placeholder=(
                "inflow[multi]: variable=discharge, location=upstream\n"
                "outflow: variable=discharge, location=downstream"
            ),
            height=120,
        )

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
        yaml_path_input = pn.widgets.TextInput(
            name="YAML file path",
            placeholder="e.g. math_refs.yaml",
            width=320,
        )
        save_yaml_btn = pn.widgets.Button(
            name="Save all math refs to YAML",
            button_type="primary",
            icon="file-export",
            width=220,
        )

        editor_panel = pn.Column(
            title_md,
            help_md,
            name_input,
            expr_input,
            attrs_input,
            search_map_input,
            pn.layout.Divider(),
            attr_browser_md,
            catalog_names,
            pn.layout.Divider(),
            status_md,
            pn.Row(save_btn, cancel_btn),
            pn.layout.Divider(),
            pn.pane.Markdown("#### Save math refs to YAML"),
            pn.Row(yaml_path_input, save_yaml_btn),
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
                search_map, search_req = self._parse_search_map(search_map_input.value)
                # Remove an existing entry with the same name so the update lands cleanly.
                try:
                    catalog.remove(name)
                    action_word = "Updated"
                except KeyError:
                    action_word = "Created"
                ref = MathDataReference(
                    expression=expr,
                    name=name,
                    search_map=search_map if search_map else None,
                    search_require_single=search_req if search_req else None,
                    **attrs,
                )
                ref.set_catalog(catalog)
                catalog.add(ref)
                # Refresh the table
                if hasattr(dataui, "_dfcat"):
                    dataui._dfcat = manager.get_data_catalog()
                    dataui.display_table.value = dataui._dfcat[manager.get_table_columns()]
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

        def _on_cancel(event: Any) -> None:
            editor_panel.objects = [pn.pane.Markdown("*Editor closed.*")]

        save_btn.on_click(_on_save)
        cancel_btn.on_click(_on_cancel)
        save_yaml_btn.on_click(_on_save_yaml)

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
