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
            row = dataui.display_table.value.iloc[selected[0]]
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

        # ── Alias hint buttons (below expression) ─────────────────────────────
        alias_hint_row = pn.Row(
            pn.pane.Markdown("**Aliases:**", margin=(6, 4, 0, 4)),
            sizing_mode="stretch_width",
            margin=(0, 0, 4, 0),
        )

        def _refresh_alias_hints() -> None:
            """Rebuild alias hint buttons from current sm_rows variable aliases."""
            btns = [pn.pane.Markdown("**Aliases:**", margin=(6, 4, 0, 4))]
            for _vi, _mc, _ci, _rc in sm_rows:
                alias = _vi.value.strip()
                if not alias:
                    continue
                _btn = pn.widgets.Button(
                    name=alias,
                    button_type="light",
                    width=max(50, len(alias) * 9),
                    height=28,
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

        attrs_input = pn.widgets.TextAreaInput(
            name="Attributes  (key: value — one per line)",
            value=pre_attrs,
            placeholder="variable: water_level_bias\nstationid: RIO001\nunit: m",
            height=110,
        )

        # ── Search Map: dynamic row editor ─────────────────────────────────────
        sm_rows: list = []  # list of (var_inp, multi_cb, crit_inp, row_container)

        sm_header_md = pn.pane.Markdown(
            "**Search Map** — one row per expression variable.  "
            "Each alias is resolved against the catalog at `getData()` time using the criteria.",
            sizing_mode="stretch_width",
        )
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
            pn.pane.Markdown("**Alias**", width=100, margin=(0, 4, 0, 4)),
            pn.pane.Markdown("**Join all**", width=80, margin=(0, 4, 0, 4)),
            pn.pane.Markdown(
                "**Catalog criteria** (`attr=val, attr=val …`)",
                sizing_mode="stretch_width",
                margin=(0, 4, 0, 4),
            ),
            pn.pane.Markdown("**+attr**", width=96, margin=(0, 4, 0, 4)),
            pn.pane.Markdown("", width=36),   # ▶ spacer
            pn.pane.Markdown("", width=36),   # ✕ spacer
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
                placeholder="alias",
                sizing_mode="fixed",
                width=96,
                margin=(2, 4, 4, 4),
            )
            _multi_cb = pn.widgets.Checkbox(
                name="Join all",
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
                    if "=" in part:
                        k, _, v = part.partition("=")
                        criteria[k.strip()] = v.strip()
                if not criteria:
                    _row_result_md.object = "⚠️ No valid `attr=val` pairs found."
                    return
                try:
                    results = catalog.search(**criteria)
                except Exception as exc:
                    _row_result_md.object = f"❌ Search error: {exc}"
                    return
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

        status_md = pn.pane.Markdown("", sizing_mode="stretch_width")
        test_result_md = pn.pane.Markdown("", sizing_mode="stretch_width")
        test_btn = pn.widgets.Button(
            name="Test Expression",
            button_type="light",
            icon="player-play",
            width=160,
            margin=(6, 4, 4, 4),
        )
        save_btn = pn.widgets.Button(name="Save", button_type="success", icon="device-floppy")
        cancel_btn = pn.widgets.Button(name="Cancel", button_type="default", icon="x")

        # ── Client-side YAML upload ───────────────────────────────────────────
        upload_widget = pn.widgets.FileInput(
            accept=".yaml,.yml",
            name="Upload YAML",
            sizing_mode="stretch_width",
        )
        upload_btn = pn.widgets.Button(
            name="Load Uploaded YAML",
            button_type="primary",
            icon="file-import",
            width=180,
        )

        # ── Client-side YAML download ─────────────────────────────────────────
        def _yaml_download_callback():
            """Return an in-memory YAML byte stream using the canonical save_math_refs format."""
            from io import BytesIO
            import tempfile, os
            from .math_reference import save_math_refs

            # save_math_refs writes to a file path; use a temp file then read bytes.
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

        download_yaml_btn = pn.widgets.FileDownload(
            label="Download YAML",
            callback=_yaml_download_callback,
            filename=self._default_yaml_filename,
            button_type="primary",
            icon="file-export",
            embed=False,
        )

        yaml_section = pn.Column(
            pn.pane.Markdown("#### YAML — Upload / Download"),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("**Upload** a YAML file from your computer:"),
                    upload_widget,
                    upload_btn,
                ),
                pn.Column(
                    pn.pane.Markdown("**Download** all math refs as YAML:"),
                    download_yaml_btn,
                ),
                sizing_mode="stretch_width",
            ),
            sizing_mode="stretch_width",
        )

        editor_panel = pn.Column(
            title_md,
            pn.layout.Divider(),
            help_md,
            name_input,
            expr_input,
            alias_hint_row,
            attrs_input,
            pn.layout.Divider(),
            search_map_section,
            pn.layout.Divider(),
            pn.Row(test_btn),
            test_result_md,
            pn.layout.Divider(),
            attr_browser_md,
            pn.layout.Divider(),
            yaml_section,
            pn.layout.Divider(),
            status_md,
            pn.Row(save_btn, cancel_btn),
            sizing_mode="stretch_width",
            min_width=640,
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
                        dataui._dfcat = manager.get_data_catalog()
                        new_cols = manager.get_table_columns()
                        dataui.display_table.value = dataui._dfcat[new_cols]
                        # Also refresh widths and filters — needed when this is
                        # the first math ref added (new expression column appears).
                        dataui.display_table.widths = manager.get_table_column_width_map()
                        dataui.display_table.header_filters = manager.get_table_filters()
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

        def _on_upload_yaml(event: Any) -> None:
            if upload_widget.value is None:
                status_md.object = "⚠️ **No file selected. Please choose a YAML file to upload.**"
                return
            try:
                import yaml as _yaml
                from .math_reference import MathDataReference

                raw = upload_widget.value  # bytes
                data = _yaml.safe_load(raw)
                if isinstance(data, dict):
                    data = data.get("math_refs", [])
                # Build refs from the parsed list — same logic as
                # MathDataCatalogReader.build() but without re-reading a file.
                refs = []
                for entry in data or []:
                    entry = dict(entry)
                    name = entry.pop("name")
                    expression = entry.pop("expression")
                    sm_raw = entry.pop("search_map", None)
                    req: Dict[str, bool] = {}
                    if sm_raw:
                        cleaned = {}
                        for var, crit in sm_raw.items():
                            crit = dict(crit)
                            req[var] = bool(crit.pop("_require_single", True))
                            cleaned[var] = crit
                        sm_raw = cleaned
                    ref = MathDataReference(
                        expression=expression,
                        name=name,
                        search_map=sm_raw if sm_raw else None,
                        search_require_single=req if req else None,
                        **entry,
                    )
                    ref.set_catalog(catalog)
                    refs.append(ref)
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
                        dataui._dfcat = manager.get_data_catalog()
                        new_cols = manager.get_table_columns()
                        dataui.display_table.value = dataui._dfcat[new_cols]
                        dataui.display_table.widths = manager.get_table_column_width_map()
                        dataui.display_table.header_filters = manager.get_table_filters()
                    except Exception as _te:
                        logger.warning("Could not refresh table after upload: %s", _te)
                fname = getattr(upload_widget, "filename", "uploaded file")
                status_md.object = f"✅ **Uploaded** {added} math ref(s) from `{fname}`."
            except Exception as exc:
                logger.exception("MathRefEditorAction YAML upload error")
                status_md.object = f"❌ **YAML upload error:** {exc}"

        upload_btn.on_click(_on_upload_yaml)

        def _on_test(event: Any) -> None:
            """Evaluate the expression against real catalog data and show a preview."""
            expr = expr_input.value.strip()
            if not expr:
                test_result_md.object = "⚠️ **Expression is required to test.**"
                return
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
                result = tmp_ref.getData()
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

        test_btn.on_click(_on_test)

        def _on_cancel(event: Any) -> None:
            editor_panel.objects = [pn.pane.Markdown("*Editor closed.*")]

        save_btn.on_click(_on_save)
        cancel_btn.on_click(_on_cancel)

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
