"""Panel-based editor for :class:`~dvue.math_reference.MathDataReference` objects.

The :class:`MathRefEditorAction` class provides a callback that opens an
inline editor inside the DataUI display panel.  Users can:

* Create a new :class:`~dvue.math_reference.MathDataReference` from scratch.
* Edit an existing one selected from the catalog table.
* Download all current math refs to a YAML file via the **Download YAML** button.
* Upload a YAML file from the client to merge refs into the live catalog via
  the **Upload YAML** button.

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
    3. On **Download YAML**, serialises all current math refs in the catalog to
       a YAML file that is sent to the client browser for download.
    4. On **Upload YAML**, reads a YAML file uploaded from the client and merges
       the resulting :class:`~dvue.math_reference.MathDataReference` objects
       into the live catalog, then refreshes the table.

    The catalog is retrieved from ``dataui._dataui_manager.data_catalog``.

    Search map
    ----------
    The **Search Map** section shows one row per expression variable.  Each
    row contains:

    * **Alias** — the short identifier used inside the expression (e.g. ``obs``).
    * **Match all** checkbox — when enabled, *all* catalog entries that match
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

    def __init__(self, default_yaml_filename: str = "math_refs.yaml") -> None:
        """Create the action.

        Parameters
        ----------
        default_yaml_filename : str, optional
            Suggested filename used when the browser downloads the YAML file.
            Defaults to ``"math_refs.yaml"``.
        """
        self._default_yaml_filename = default_yaml_filename

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
        """Parse ``var[multi]: key=val, key~regex, …`` lines.

        Supports two operators per attribute token:

        * ``attr=value`` — exact match (stored as the plain string ``value``).
        * ``attr~pattern`` — regex fullmatch, case-insensitive (stored as
          ``"~pattern"`` so the tilde prefix is visible in round-trips).

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
                # Detect operator: ~ (regex) takes priority over = (exact).
                tilde_pos = part.find("~")
                eq_pos = part.find("=")
                if tilde_pos != -1 and (eq_pos == -1 or tilde_pos < eq_pos):
                    k, _, v = part.partition("~")
                    k = k.strip()
                    if k:
                        criteria[k] = "~" + v.strip()
                elif eq_pos != -1:
                    k, _, v = part.partition("=")
                    criteria[k.strip()] = v.strip()
            if var and criteria:
                sm[var] = criteria
                req[var] = not multi  # require_single=False when [multi] present
        return sm, req

    @staticmethod
    def _render_search_map(search_map: Dict[str, Any], req: Dict[str, bool]) -> str:
        """Render ``search_map`` + ``search_require_single`` to editor text.

        Regex criteria (stored with a ``~`` prefix) are emitted as
        ``attr~pattern``; exact criteria are emitted as ``attr=value``.
        """
        lines = []
        for var, criteria in search_map.items():
            require_single = req.get(var, True)
            tag = "" if require_single else "[multi]"
            parts = []
            for k, v in criteria.items():
                if isinstance(v, str) and v.startswith("~"):
                    parts.append(f"{k}{v}")  # e.g. "variable~EC.*"
                else:
                    parts.append(f"{k}={v}")
            criteria_str = ", ".join(parts)
            lines.append(f"{var}{tag}: {criteria_str}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # YAML sidebar tab
    # ------------------------------------------------------------------

    def _inject_yaml_sidebar_tab(self, catalog: Any, manager: Any, dataui: Any) -> None:
        """Inject a 'Math YAML' tab into the sidebar the first time the editor opens.

        Idempotent — subsequent calls do nothing so clicking Math Ref multiple
        times does not add duplicate tabs.
        """
        if getattr(dataui, "_math_yaml_sidebar_added", False):
            return
        if not hasattr(dataui, "_sidebar_tabs"):
            return

        yaml_status = pn.pane.Markdown("", sizing_mode="stretch_width", margin=(4, 0, 0, 0))

        def _section(title: str):
            return pn.pane.HTML(
                f"<div style='border-left:3px solid #4a90d9;padding:1px 7px;"
                f"font-size:11px;font-weight:700;color:#333;letter-spacing:.3px;"
                f"margin:8px 0 2px 0'>{title}</div>",
                sizing_mode="stretch_width", margin=(0, 0, 0, 0),
            )

        # ── Upload ────────────────────────────────────────────────────────────
        upload_widget = pn.widgets.FileInput(
            accept=".yaml,.yml",
            name="Choose YAML file",
            sizing_mode="stretch_width",
            margin=(2, 0, 4, 0),
        )
        upload_btn = pn.widgets.Button(
            name="Load YAML",
            button_type="primary",
            icon="file-import",
            sizing_mode="stretch_width",
            height=30,
            margin=(0, 0, 4, 0),
        )

        def _on_upload(event: Any) -> None:
            if upload_widget.value is None:
                yaml_status.object = "⚠️ No file selected."
                return
            try:
                import yaml as _yaml
                from .math_reference import MathDataCatalogReader

                raw = upload_widget.value
                data = _yaml.safe_load(raw)
                refs = MathDataCatalogReader().build_from_data(data, parent_catalog=catalog)
                added = 0
                for r in refs:
                    try:
                        catalog.remove(r.name)
                    except KeyError:
                        pass
                    catalog.add(r)
                    added += 1
                if hasattr(dataui, "_dfcat"):
                    try:
                        dataui.refresh_catalog_table(manager)
                    except Exception as _te:
                        logger.warning("Could not refresh table after YAML upload: %s", _te)
                fname = getattr(upload_widget, "filename", "file")
                yaml_status.object = f"✅ Loaded **{added}** ref(s) from `{fname}`."
            except Exception as exc:
                logger.exception("YAML sidebar upload error")
                yaml_status.object = f"❌ Upload error: {exc}"

        upload_btn.on_click(_on_upload)

        # ── Download ──────────────────────────────────────────────────────────
        def _yaml_download_callback():
            from io import BytesIO
            import tempfile, os
            from .math_reference import save_math_refs

            with tempfile.NamedTemporaryFile(
                suffix=".yaml", delete=False, mode="w", encoding="utf-8"
            ) as tmp:
                tmp_path = tmp.name
            try:
                save_math_refs(catalog, tmp_path)
                with open(tmp_path, "rb") as fh:
                    data = fh.read()
            finally:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            return BytesIO(data)

        import os  # needed in the closure above; also available at module scope
        download_btn = pn.widgets.FileDownload(
            label="Save YAML",
            callback=_yaml_download_callback,
            filename=self._default_yaml_filename,
            button_type="success",
            icon="file-export",
            embed=False,
            sizing_mode="stretch_width",
            height=30,
            margin=(0, 0, 4, 0),
        )

        yaml_tab = pn.Column(
            _section("Upload"),
            pn.pane.HTML(
                "<span style='font-size:10px;color:#999'>Merge math refs from a YAML file "
                "into the live catalog.</span>",
                margin=(0, 0, 4, 0),
            ),
            upload_widget,
            upload_btn,
            _section("Download"),
            pn.pane.HTML(
                "<span style='font-size:10px;color:#999'>Save all current math refs "
                "to a YAML file.</span>",
                margin=(0, 0, 4, 0),
            ),
            download_btn,
            yaml_status,
            sizing_mode="stretch_width",
            margin=(4, 8, 4, 4),
        )

        dataui._sidebar_tabs.append(("Math YAML", yaml_tab))
        dataui._math_yaml_sidebar_added = True

    # ------------------------------------------------------------------
    # Sidebar lifecycle hook
    # ------------------------------------------------------------------

    def setup_sidebar(self, dataui: Any) -> None:
        """Inject the Math YAML sidebar tab at app startup without waiting for a button click.

        Called by :class:`~dvue.dataui.DataUI` after ``_sidebar_tabs`` is
        created so the upload/download tab is visible immediately when the app
        loads.  Idempotent — safe to call multiple times.
        """
        manager = dataui._dataui_manager
        catalog = getattr(manager, "data_catalog", None)
        if catalog is None:
            return
        self._inject_yaml_sidebar_tab(catalog, manager, dataui)

    # ------------------------------------------------------------------
    # Main callback
    # ------------------------------------------------------------------

    def callback(self, event: Any, dataui: Any) -> None:  # noqa: C901
        from .math_reference import MathDataReference, MathDataCatalogReader

        manager = dataui._dataui_manager
        catalog = getattr(manager, "data_catalog", None)
        if catalog is None:
            if pn.state.notifications is not None:
                pn.state.notifications.error(
                    "MathRefEditor requires the manager to expose a 'data_catalog' property.",
                    duration=5000,
                )
            return

        # ── Identifying-attribute helper ──────────────────────────────────────
        _SYSTEM_EXCLUDE = frozenset({
            "source", "name", "ref_type", "expression", "geometry",
            "file", "filename", "start_year", "max_year",
        })

        def _identifying_attrs(row_dict: Dict[str, Any]) -> Dict[str, str]:
            """Return only identifying string attributes for search criteria."""
            result: Dict[str, str] = {}
            for k, v in row_dict.items():
                if k in _SYSTEM_EXCLUDE:
                    continue
                if not isinstance(v, str):
                    continue
                v = v.strip()
                if not v or v.lower() in ("nan", "none"):
                    continue
                result[k] = v
            return result

        # ── Pre-populate from selected row if it is a MathDataReference ──────
        pre_name = ""
        pre_expr = ""
        pre_attrs = ""
        is_edit = [False]  # mutable list so _on_save can reset it
        pre_ref: Optional[MathDataReference] = None

        selected = dataui.display_table.selection
        if selected:
            # Use _dfcat (full catalog DataFrame with 'name' column) rather than
            # display_table.value (display-column subset that strips 'name').
            row = dataui._dfcat.iloc[selected[0]]
            name_val = row.get("name", "") if hasattr(row, "get") else ""
            try:
                pre_ref = catalog.get(str(name_val)) if name_val else None
            except KeyError:
                pass
            if isinstance(pre_ref, MathDataReference):
                is_edit[0] = True
                pre_name = pre_ref.name
                pre_expr = pre_ref.expression
                # Only put primitive values in the text area – non-primitives like
                # Shapely geometry objects cannot round-trip through plain text.
                extra = {
                    k: v
                    for k, v in pre_ref.attributes.items()
                    if k != "expression" and isinstance(v, (str, int, float, bool, type(None)))
                }
                pre_attrs = "\n".join(f"{k}: {v}" for k, v in extra.items())

        # ── Inject (or reuse) the YAML sidebar tab ────────────────────────────
        self._inject_yaml_sidebar_tab(catalog, manager, dataui)

        # ── Section header helper (compact blue accent bar) ───────────────────
        def _section(title: str):
            return pn.pane.HTML(
                f"<div style='border-left:3px solid #4a90d9;padding:1px 7px;"
                f"font-size:11px;font-weight:700;color:#333;letter-spacing:.3px;"
                f"margin:8px 0 2px 0'>{title}</div>",
                sizing_mode="stretch_width", margin=(0, 0, 0, 0),
            )

        # ── Build the editor form ─────────────────────────────────────────────
        _edit_label = f" — Editing: <code>{pre_name}</code>" if is_edit[0] else ""
        title_html = pn.pane.HTML(
            f"<div style='font-size:13px;font-weight:700;align-self:center'>"
            f"Math Reference Editor{_edit_label}</div>",
            margin=(0, 6, 0, 0),
        )
        name_input = pn.widgets.TextInput(
            name="Name (catalog key)",
            value=pre_name,
            placeholder="e.g. bias_RIO001",
            sizing_mode="stretch_width",
        )

        # ── Expression token picker (autocomplete via Select insert) ──────────
        try:
            _all_names = sorted(catalog.list_names())
        except Exception:
            _all_names = []

        token_picker = pn.widgets.AutocompleteInput(
            name="",
            options=_all_names,
            placeholder="Type to search catalog names…",
            case_sensitive=False,
            min_characters=1,
            restrict=False,
            sizing_mode="stretch_width",
            margin=(0, 0, 2, 0),
            height=34,
        )
        token_hint = pn.pane.HTML(
            "<span style='font-size:10px;color:#999'>Type a catalog name to insert it into the expression</span>",
            margin=(0, 0, 4, 0),
        )

        expr_input = pn.widgets.TextAreaInput(
            name="Expression",
            value=pre_expr,
            placeholder="e.g. water_level_usgs - model_RIO001",
            height=60,
            sizing_mode="stretch_width",
        )

        def _on_token_pick(event: Any) -> None:
            token = (event.new or "").strip()
            if not token or token not in _all_names:
                return
            current = expr_input.value.rstrip()
            expr_input.value = f"{current} {token}".lstrip()
            # Clear picker after insertion
            token_picker.value = ""

        token_picker.param.watch(_on_token_pick, "value")

        # ── Alias hint buttons (below expression) ─────────────────────────────
        alias_hint_row = pn.Row(
            pn.pane.HTML("<span style='font-size:10px;color:#666;margin-right:4px'>Aliases:</span>",
                         align=("start", "center"), margin=(0, 4, 0, 0)),
            sizing_mode="stretch_width",
            margin=(0, 0, 4, 0),
        )

        def _refresh_alias_hints() -> None:
            """Rebuild alias hint buttons from current sm_rows variable aliases."""
            btns = [pn.pane.HTML("<span style='font-size:10px;color:#666;margin-right:4px'>Aliases:</span>",
                                  align=("start", "center"), margin=(0, 4, 0, 0))]
            for _vi, _mc, _ci, _rc in sm_rows:
                alias = _vi.value.strip()
                if not alias:
                    continue
                _btn = pn.widgets.Button(
                    name=alias,
                    button_type="light",
                    width=max(50, len(alias) * 9),
                    height=24,
                    margin=(2, 4, 2, 0),
                )

                def _make_insert(a=alias):
                    def _on_insert(ev: Any) -> None:
                        current = expr_input.value
                        expr_input.value = f"{current} {a}".lstrip()
                    return _on_insert

                _btn.on_click(_make_insert())
                btns.append(_btn)
            alias_hint_row.objects = btns

        # ── Copy Attributes button ────────────────────────────────────────────
        copy_attrs_btn = pn.widgets.Button(
            name="Copy from selection",
            button_type="light",
            icon="clipboard-copy",
            height=28,
            margin=(0, 4, 0, 0),
        )

        def _on_copy_attrs(event: Any) -> None:
            """Copy identifying attributes from the first selected table row."""
            sel = dataui.display_table.selection
            if not sel:
                if pn.state.notifications is not None:
                    pn.state.notifications.warning(
                        "Select a table row first to copy its attributes.", duration=3000
                    )
                return
            row = dataui._dfcat.iloc[sel[0]]
            ident = _identifying_attrs(dict(row))
            if ident:
                attrs_input.value = "\n".join(f"{k}: {v}" for k, v in ident.items())
            else:
                if pn.state.notifications is not None:
                    pn.state.notifications.warning(
                        "No identifying string attributes found on selected row.", duration=3000
                    )

        copy_attrs_btn.on_click(_on_copy_attrs)

        attrs_input = pn.widgets.TextAreaInput(
            name="",
            value=pre_attrs,
            placeholder="variable: water_level_bias\nstationid: RIO001\nunit: m",
            height=70,
            sizing_mode="stretch_width",
        )

        # ── Search Map: dynamic row editor ─────────────────────────────────────
        sm_rows: list = []  # list of (var_inp, multi_cb, crit_inp, row_container)

        # Collect catalog attribute names for the picker (exclude internal cols).
        try:
            _df_cat_cols = [
                c for c in catalog.to_dataframe().columns
                if c not in ("source", "ref_type", "expression", "geometry")
            ]
        except Exception:
            _df_cat_cols = []
        _attr_picker_options = [""] + sorted(_df_cat_cols)

        sm_col_labels = pn.Row(
            pn.pane.HTML("<span style='font-size:10px;color:#666;width:100px'>Alias</span>", width=100, margin=(0, 4, 0, 4)),
            pn.pane.HTML("<span style='font-size:10px;color:#666'>Match all</span>", width=80, margin=(0, 4, 0, 4)),
            pn.pane.HTML("<span style='font-size:10px;color:#666'>Catalog criteria (attr=val · attr~regex)</span>", sizing_mode="stretch_width", margin=(0, 4, 0, 4)),
            pn.pane.HTML("<span style='font-size:10px;color:#666'>+attr</span>", width=96, margin=(0, 4, 0, 4)),
            pn.pane.HTML("", width=36),
            pn.pane.HTML("", width=36),
            margin=(2, 0, 0, 0),
        )
        add_var_btn = pn.widgets.Button(
            name="+ Add variable",
            button_type="default",
            icon="plus",
            width=140,
            height=28,
            margin=(4, 4, 2, 4),
        )
        search_map_section = pn.Column(
            sm_col_labels,
            # dynamic rows are inserted before add_var_btn
            add_var_btn,
            sizing_mode="stretch_width",
            styles={"border": "1px solid #e0e0e0", "border-radius": "4px", "padding": "4px 6px"},
        )

        def _add_sm_row(
            var_name: str = "", criteria_str: str = "", keep_single: bool = True
        ) -> None:
            _var_inp = pn.widgets.TextInput(
                value=var_name,
                placeholder="alias",
                sizing_mode="fixed",
                width=96,
                margin=(2, 4, 4, 4),
            )
            _multi_cb = pn.widgets.Checkbox(
                name="Match all",
                value=not keep_single,
                width=76,
                margin=(8, 4, 0, 4),
            )
            _crit_inp = pn.widgets.TextInput(
                value=criteria_str,
                placeholder="attr=val, attr=val",
                sizing_mode="stretch_width",
                margin=(2, 4, 4, 4),
            )
            # Attr-name picker: selecting an attribute appends "attr=" to criteria.
            _attr_sel = pn.widgets.Select(
                value="",
                options=_attr_picker_options,
                width=92,
                margin=(2, 4, 4, 4),
            )

            def _make_attr_append(sel=_attr_sel, inp=_crit_inp):
                def _on_attr_pick(ev: Any) -> None:
                    attr = ev.new
                    if not attr:
                        return
                    current = inp.value.strip()
                    token = f"{attr}="
                    inp.value = f"{current}, {token}" if current else token
                    sel.value = ""  # reset picker
                return _on_attr_pick

            _attr_sel.param.watch(_make_attr_append(), "value")

            # Per-row result pane (hidden until ▶ clicked)
            _row_result_md = pn.pane.Markdown(
                "",
                sizing_mode="stretch_width",
                styles={"font-size": "0.82em", "padding": "2px 6px"},
            )

            # ▶ per-row Test button
            _test_row_btn = pn.widgets.Button(
                name="▶",
                button_type="light",
                width=32,
                height=34,
                margin=(4, 2, 4, 2),
            )

            _rm_btn = pn.widgets.Button(
                name="✕",
                button_type="light",
                width=32,
                height=34,
                margin=(4, 4, 4, 2),
                styles={"color": "#c00", "font-weight": "bold"},
            )
            _data_row = pn.Row(
                _var_inp,
                _multi_cb,
                _crit_inp,
                _attr_sel,
                _test_row_btn,
                _rm_btn,
                sizing_mode="stretch_width",
                margin=(2, 0),
            )
            _row_container = pn.Column(
                _data_row,
                _row_result_md,
                sizing_mode="stretch_width",
                margin=(0, 0),
            )
            _container_ref = [_row_container]

            def _on_row_test(ev: Any) -> None:
                """Test this variable's criteria against the catalog."""
                crit_text = _crit_inp.value.strip()
                if not crit_text:
                    _row_result_md.object = "⚠️ Enter criteria first."
                    return
                criteria: Dict[str, str] = {}
                for part in crit_text.split(","):
                    part = part.strip()
                    # Detect operator: ~ (regex) takes priority over = (exact).
                    tilde_pos = part.find("~")
                    eq_pos = part.find("=")
                    if tilde_pos != -1 and (eq_pos == -1 or tilde_pos < eq_pos):
                        k, _, v = part.partition("~")
                        k = k.strip()
                        if k:
                            criteria[k] = "~" + v.strip()
                    elif eq_pos != -1:
                        k, _, v = part.partition("=")
                        criteria[k.strip()] = v.strip()
                if not criteria:
                    _row_result_md.object = "⚠️ No valid `attr=val` or `attr~regex` pairs found."
                    return
                dataui.set_progress(-1)
                try:
                    results = catalog.search(**criteria)
                except Exception as exc:
                    _row_result_md.object = f"❌ Search error: {exc}"
                    return
                finally:
                    dataui.hide_progress()
                n = len(results)
                if n == 0:
                    _row_result_md.object = "❌ **0 matches** — check criteria."
                elif n == 1:
                    ref_match = results[0]
                    _row_result_md.object = f"✅ **1 match**: `{ref_match.name}`"
                    # Auto-fill Attributes if currently blank
                    if not attrs_input.value.strip():
                        ident = _identifying_attrs(ref_match.attributes)
                        if ident:
                            attrs_input.value = "\n".join(
                                f"{k}: {v}" for k, v in ident.items()
                            )
                else:
                    names = [r.name for r in results[:8]]
                    truncated = f" _…and {n - 8} more_" if n > 8 else ""
                    names_md = ", ".join(f"`{nm}`" for nm in names) + truncated
                    _row_result_md.object = f"⚠️ **{n} matches** — {names_md}"

            _test_row_btn.on_click(_on_row_test)

            # Watch alias input to update hint buttons
            _var_inp.param.watch(lambda ev: _refresh_alias_hints(), "value")

            def _on_rm(ev: Any) -> None:
                for _i, (_v, _m, _c, _rc) in enumerate(sm_rows):
                    if _rc is _container_ref[0]:
                        sm_rows.pop(_i)
                        break
                search_map_section.objects = [
                    _o for _o in search_map_section.objects if _o is not _container_ref[0]
                ]
                _refresh_alias_hints()

            _rm_btn.on_click(_on_rm)
            sm_rows.append((_var_inp, _multi_cb, _crit_inp, _row_container))
            # Insert the new row container before the add_var_btn (always last item)
            search_map_section.insert(len(search_map_section.objects) - 1, _row_container)
            _refresh_alias_hints()

        def _add_sm_row_from_selection(_e: Any) -> None:
            """Add a row, pre-filling criteria from the currently selected table row."""
            crit_str = ""
            try:
                sel = dataui.display_table.selection
                if sel:
                    sel_row = dataui.display_table.value.iloc[sel[0]]
                    ident = _identifying_attrs(dict(sel_row))
                    if ident:
                        crit_str = ", ".join(f"{k}={v}" for k, v in ident.items())
            except Exception:
                pass
            _add_sm_row(criteria_str=crit_str)

        add_var_btn.on_click(_add_sm_row_from_selection)

        # Pre-populate rows when editing an existing MathDataReference.
        if is_edit[0] and pre_ref is not None and getattr(pre_ref, "_search_map", None):
            for _sm_var, _sm_crit in pre_ref._search_map.items():
                _sm_keep = pre_ref._search_require_single.get(_sm_var, True)
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

        status_md = pn.pane.Markdown("", sizing_mode="stretch_width", margin=(0, 0, 0, 0))
        test_result_md = pn.pane.Markdown("", sizing_mode="stretch_width")
        test_btn = pn.widgets.Button(
            name="Test",
            button_type="light",
            icon="player-play",
            width=80,
            height=28,
            margin=(0, 4, 0, 0),
        )
        save_btn = pn.widgets.Button(
            name="Save",
            button_type="success",
            icon="device-floppy",
            height=32,
            margin=(0, 4, 0, 0),
        )
        cancel_btn = pn.widgets.Button(
            name="Cancel",
            button_type="default",
            icon="x",
            height=32,
            margin=(0, 0, 0, 0),
        )

        # Attr browser collapsed so it doesn't eat vertical space
        attr_browser_card = pn.Card(
            attr_browser_md,
            title="Catalog attributes reference",
            collapsed=True,
            sizing_mode="stretch_width",
            margin=(4, 0, 4, 0),
        )

        editor_panel = pn.Column(
            # ── Header: title + action buttons in one row ───────────────
            pn.Row(title_html, pn.layout.HSpacer(), save_btn, cancel_btn,
                   align="center", margin=(0, 0, 4, 0)),
            status_md,
            # ── Reference ────────────────────────────────────────────────
            _section("Reference"),
            name_input,
            expr_input,
            # Token picker: type to search catalog names and click to insert
            token_picker,
            token_hint,
            alias_hint_row,
            # ── Attributes ───────────────────────────────────────────────
            pn.Row(
                _section("Attributes  (key: value — one per line)"),
                copy_attrs_btn,
                align="center",
            ),
            attrs_input,
            # ── Search Map ───────────────────────────────────────────────
            _section("Search Map — resolve aliases at getData() time"),
            search_map_section,
            # ── Test ─────────────────────────────────────────────────────
            pn.Row(test_btn, align="center", margin=(4, 0, 0, 0)),
            test_result_md,
            # ── Catalog reference (collapsed) ────────────────────────────
            attr_browser_card,
            sizing_mode="stretch_width",
            min_width=580,
            margin=(4, 8, 8, 8),
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
                for _vi, _mc, _ci, _rc in sm_rows:
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
                # When renaming (edit mode, name changed), remove the original entry first.
                action_word = "Created"
                if is_edit[0] and pre_ref is not None and pre_ref.name != name:
                    try:
                        catalog.remove(pre_ref.name)
                        action_word = "Renamed"
                    except KeyError:
                        pass
                # Remove any existing entry with the new name (handles same-name update).
                try:
                    catalog.remove(name)
                    if action_word == "Created":
                        action_word = "Updated"
                except KeyError:
                    pass
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
                if is_edit[0] and pre_ref is not None:
                    for _k, _v in pre_ref.attributes.items():
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
                        dataui.refresh_catalog_table(manager)
                        # Auto-show ref_type column when catalog becomes mixed.
                        # Update the column picker to reflect the new state.
                        from dvue.tsdataui import TimeSeriesDataUIManager
                        if (
                            "ref_type" in dataui._dfcat.columns
                            and TimeSeriesDataUIManager._has_mixed_ref_types(dataui._dfcat)
                        ):
                            hidden = list(dataui.display_table.hidden_columns or [])
                            if "ref_type" in hidden:
                                hidden.remove("ref_type")
                                dataui.display_table.hidden_columns = hidden
                            if hasattr(dataui, "_column_picker"):
                                visible = list(dataui._column_picker.value)
                                if "ref_type" not in visible:
                                    dataui._column_picker.value = visible + ["ref_type"]
                    except Exception as _te:
                        logger.warning("Could not refresh table after save: %s", _te)
                status_md.object = f"✅ **{action_word}** `{name}` in catalog."
                # Clear name/expr/attrs for the next entry; keep search map rows.
                name_input.value = ""
                expr_input.value = ""
                attrs_input.value = ""
                is_edit[0] = False
            except Exception as exc:
                logger.exception("MathRefEditorAction save error")
                status_md.object = f"❌ **Error:** {exc}"

        def _on_test(event: Any) -> None:
            """Evaluate the expression against real catalog data and show a preview."""
            expr = expr_input.value.strip()
            if not expr:
                test_result_md.object = "⚠️ **Expression is required to test.**"
                return
            dataui.set_progress(-1)
            try:
                # Build search_map from current widget state (same as _on_save).
                _sm: Dict[str, Dict[str, str]] = {}
                _req: Dict[str, bool] = {}
                for _vi, _mc, _ci, _rc in sm_rows:
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
                        _sm[_sv] = _criteria
                        _req[_sv] = not _mc.value
                tmp_ref = MathDataReference(
                    expression=expr,
                    search_map=_sm if _sm else None,
                    search_require_single=_req if _req else None,
                )
                tmp_ref.set_catalog(catalog)
                time_range = getattr(manager, "time_range", None)
                result = tmp_ref.getData(time_range=time_range)
                head = result.head(5)
                # Format as a simple Markdown table.
                try:
                    table_md = head.to_markdown()
                except Exception:
                    table_md = head.to_string()
                test_result_md.object = (
                    f"✅ **Expression evaluated successfully** — shape `{result.shape}`\n\n"
                    f"```\n{table_md}\n```"
                )
            except Exception as exc:
                test_result_md.object = f"❌ **Test failed:** {exc}"
                if pn.state.notifications is not None:
                    pn.state.notifications.error(f"Test failed: {exc}", duration=8000)
            finally:
                dataui.hide_progress()

        test_btn.on_click(_on_test)

        def _on_cancel(event: Any) -> None:
            editor_panel.objects = [
                pn.pane.HTML(
                    "<span style='color:#999;font-size:12px'>Editor closed. "
                    "Select a row and click Math Ref to open again.</span>",
                )
            ]

        save_btn.on_click(_on_save)
        cancel_btn.on_click(_on_cancel)

        # Wrap editor in a scrollable Column so it never inflates the display
        # area height — tall editors scroll within the available space.
        editor_scrollable = pn.Column(editor_panel, scroll=True, sizing_mode="stretch_both")

        # Show editor as a new tab in the display panel.
        if len(dataui._display_panel.objects) > 0 and isinstance(
            dataui._display_panel.objects[0], pn.Tabs
        ):
            tabs = dataui._display_panel.objects[0]
            if not hasattr(dataui, "_tab_count"):
                dataui._tab_count = 0
            dataui._tab_count += 1
            tabs.append((f"Math Ref Editor {dataui._tab_count}", editor_scrollable))
            tabs.active = len(tabs) - 1
        else:
            dataui._tab_count = 1
            dataui._display_panel.objects = [
                pn.Tabs(("Math Ref Editor", editor_scrollable), closable=True,
                        dynamic=True, sizing_mode="stretch_both")
            ]
