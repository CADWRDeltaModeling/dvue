"""Named, saveable catalog views for :class:`~dvue.dataui.DataUI`.

A *view* is a named subset of the full catalog table, defined either by a
dict of attribute-match criteria (same syntax as
:meth:`~dvue.catalog.DataCatalog.search`) or by an explicit list of
reference names.  Views are purely table filters — time-range and transform
parameters remain global.

Classes
-------
ViewDefinition
    Lightweight dataclass holding a view's name and filter specification.
ViewsManager
    ``param.Parameterized`` that owns the list of views and the currently
    active view.  Can be watched to react to view switches.

Functions
---------
create_views_tab(dataui)
    Build the Panel column that is injected as the **Views** sidebar tab.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
import param
import panel as pn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ViewDefinition:
    """A named catalog view defined by criteria or an explicit name list.

    Parameters
    ----------
    name : str
        Display name.  Must be unique within a :class:`ViewsManager` and
        must not be ``"All"`` (reserved).
    criteria : dict, optional
        Attribute-match criteria applied to every catalog row.  Supports the
        same syntax as :meth:`~dvue.catalog.DataCatalog.search`:

        * Exact match: ``{"variable": "EC"}``
        * Regex (case-insensitive fullmatch): ``{"station": "~RSAC.*"}``
        * OR list: ``{"station": ["ROLD024", "CHSWP003"]}``

        All criteria are ANDed together.
    names : list[str], optional
        Explicit list of catalog reference names.  When non-empty this takes
        priority over *criteria*.
    """

    name: str
    criteria: Dict[str, Any] = field(default_factory=dict)
    names: List[str] = field(default_factory=list)

    def matches_row(self, row: pd.Series) -> bool:
        """Return ``True`` if *row* passes this view's filter."""
        if self.names:
            row_name = row.get("name", None)
            return row_name in self.names
        if self.criteria:
            return _row_matches_criteria(row, self.criteria)
        return True  # empty definition — show everything


def _row_matches_criteria(row: pd.Series, criteria: Dict[str, Any]) -> bool:
    """Test a catalog DataFrame row against a criteria dict.

    Supports exact match, regex (``"~pattern"``), and list (OR semantics).
    Missing columns always fail.
    """
    for attr, expected in criteria.items():
        if attr not in row.index:
            return False
        actual = row[attr]
        if isinstance(expected, list):
            if actual not in expected:
                return False
        elif isinstance(expected, str) and expected.startswith("~"):
            pattern = expected[1:]
            actual_str = "" if pd.isna(actual) else str(actual)
            if not re.fullmatch(pattern, actual_str, re.IGNORECASE):
                return False
        else:
            # Accept both string equality and Python equality
            if actual != expected and str(actual) != str(expected):
                return False
    return True


# ---------------------------------------------------------------------------
# ViewsManager
# ---------------------------------------------------------------------------


class ViewsManager(param.Parameterized):
    """Owns the list of named catalog views and tracks the active view.

    The implicit ``"All"`` view always shows every row; it cannot be
    added, removed, or renamed.

    Parameters
    ----------
    active_view : str
        Name of the currently active view.  Defaults to ``"All"``.
        Watch this param to react to view switches::

            mgr.param.watch(callback, "active_view")

    _version : int
        Internal counter incremented on structural changes (add / remove /
        rename).  Watch this to rebuild UI elements that enumerate views.
    """

    active_view = param.String(default="All", doc="Name of the currently active view")
    _version = param.Integer(default=0, precedence=-1,
                             doc="Incremented on view list structural changes")

    def __init__(self, **params):
        super().__init__(**params)
        self._views: List[ViewDefinition] = []

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    @property
    def view_names(self) -> List[str]:
        """All view names including the implicit ``"All"``."""
        return ["All"] + [v.name for v in self._views]

    def get_view_def(self, name: str) -> Optional[ViewDefinition]:
        """Return the :class:`ViewDefinition` for *name*, or ``None`` for ``"All"``."""
        if name == "All":
            return None
        return next((v for v in self._views if v.name == name), None)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def add_view(self, view_def: ViewDefinition) -> None:
        """Append *view_def*.

        Raises
        ------
        ValueError
            If the name is ``"All"`` or already exists.
        """
        if view_def.name == "All":
            raise ValueError("'All' is a reserved view name.")
        if any(v.name == view_def.name for v in self._views):
            raise ValueError(f"A view named '{view_def.name}' already exists.")
        self._views.append(view_def)
        self._version += 1

    def remove_view(self, name: str) -> None:
        """Remove the view named *name*.

        If *name* is currently active, ``active_view`` resets to ``"All"``.

        Raises
        ------
        ValueError
            If *name* is ``"All"``.
        KeyError
            If no view with that name exists.
        """
        if name == "All":
            raise ValueError("Cannot remove the 'All' view.")
        idx = next((i for i, v in enumerate(self._views) if v.name == name), None)
        if idx is None:
            raise KeyError(f"No view named '{name!r}'.")
        self._views.pop(idx)
        if self.active_view == name:
            self.active_view = "All"
        self._version += 1

    def rename_view(self, old_name: str, new_name: str) -> None:
        """Rename *old_name* → *new_name* atomically.

        Raises
        ------
        ValueError
            If either name is ``"All"`` or *new_name* already exists.
        KeyError
            If *old_name* does not exist.
        """
        if old_name == "All":
            raise ValueError("Cannot rename the 'All' view.")
        if new_name == "All":
            raise ValueError("'All' is a reserved view name.")
        vdef = self.get_view_def(old_name)
        if vdef is None:
            raise KeyError(f"No view named '{old_name!r}'.")
        if any(v.name == new_name for v in self._views if v.name != old_name):
            raise ValueError(f"A view named '{new_name}' already exists.")
        vdef.name = new_name
        if self.active_view == old_name:
            self.active_view = new_name
        self._version += 1

    def add_to_view(self, name: str, new_names: List[str]) -> int:
        """Append *new_names* to the explicit names list of view *name*.

        Deduplicates: names already present in the view are silently skipped.
        If the view currently uses only ``criteria``, those criteria are left
        untouched; since ``names`` takes priority in :meth:`ViewDefinition.matches_row`
        the criteria become inactive once at least one name is added.

        Parameters
        ----------
        name : str
            Name of an existing user-defined view (not ``"All"``).
        new_names : list[str]
            Reference names to append.

        Returns
        -------
        int
            Count of names actually added (excludes pre-existing duplicates).

        Raises
        ------
        KeyError
            If no view named *name* exists.
        """
        vdef = self.get_view_def(name)
        if vdef is None:
            raise KeyError(f"No view named '{name!r}'.")
        existing = set(vdef.names)
        added = [n for n in new_names if n not in existing]
        vdef.names.extend(added)
        self._version += 1
        return len(added)

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_dataframe(self, df_full: pd.DataFrame) -> pd.DataFrame:
        """Return a filtered slice of *df_full* for the active view.

        Returns *df_full* unchanged when the active view is ``"All"``.
        """
        if self.active_view == "All":
            return df_full
        vdef = self.get_view_def(self.active_view)
        if vdef is None:
            return df_full
        mask = df_full.apply(vdef.matches_row, axis=1)
        return df_full[mask]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def to_yaml_str(self) -> str:
        """Serialize all user-defined views to a YAML string."""
        import yaml  # soft import — yaml is available in any Panel environment

        data: Dict[str, Any] = {"views": []}
        for v in self._views:
            entry: Dict[str, Any] = {"name": v.name}
            if v.names:
                entry["names"] = v.names
            if v.criteria:
                entry["criteria"] = dict(v.criteria)
            data["views"].append(entry)
        return yaml.dump(data, default_flow_style=False, allow_unicode=True)

    def load_from_yaml_str(self, yaml_str: str) -> None:
        """Replace current views with those parsed from *yaml_str*.

        Raises
        ------
        ValueError
            On parse errors or missing required keys.
        """
        import yaml

        try:
            data = yaml.safe_load(yaml_str)
        except Exception as exc:
            raise ValueError(f"Invalid YAML: {exc}") from exc
        if not isinstance(data, dict) or "views" not in data:
            raise ValueError("YAML must have a top-level 'views' key.")
        new_views: List[ViewDefinition] = []
        for entry in data["views"]:
            if not isinstance(entry, dict) or "name" not in entry:
                raise ValueError(
                    f"Each view entry must have a 'name' key. Got: {entry!r}"
                )
            vdef = ViewDefinition(
                name=str(entry["name"]),
                criteria=dict(entry.get("criteria") or {}),
                names=list(entry.get("names") or []),
            )
            new_views.append(vdef)
        self._views = new_views
        if self.active_view not in self.view_names:
            self.active_view = "All"
        self._version += 1


# ---------------------------------------------------------------------------
# Panel UI helpers
# ---------------------------------------------------------------------------


def _section(title: str) -> pn.pane.HTML:
    """Blue-accent section header (same style as MathRefEditorAction)."""
    return pn.pane.HTML(
        f"<div style='border-left:3px solid #4a90d9;padding:1px 7px;"
        f"font-size:11px;font-weight:700;color:#333;letter-spacing:.3px;"
        f"margin:8px 0 2px 0'>{title}</div>",
        sizing_mode="stretch_width",
        margin=(0, 0, 0, 0),
    )


def open_view_editor(dataui: Any, mgr: "ViewsManager") -> None:
    """Open a view builder editor in the main display panel.

    The full catalog table (visible above the display panel) acts as the
    source for adding refs.  The editor shows the current view contents and
    lets the user:

    * **Add Selected Rows** — adds currently selected rows from the catalog
      table.
    * **Add All Filtered** — adds all rows that match the catalog table's
      active header filters (server-side replication of Tabulator filter state).
    * **Remove Selected** — removes selected rows from the view contents.
    * **Clear All** — empties the view.
    * **Save View** — persists changes and switches to the saved view.

    When the active view is ``"All"``, a blank editor opens so the user can
    build a brand-new named view.
    """
    view_name = mgr.active_view
    _view_exists = [view_name != "All"]  # mutable flag for closure

    if view_name == "All":
        existing = mgr.view_names
        base = "New View"
        candidate = base
        n = 1
        while candidate in existing:
            candidate = f"{base} {n}"
            n += 1
        target_name = [candidate]
    else:
        target_name = [view_name]

    vdef = mgr.get_view_def(target_name[0])
    df_full = dataui._dfcat_full
    _has_name = "name" in df_full.columns

    if vdef is not None and _has_name:
        mask = df_full.apply(vdef.matches_row, axis=1)
        _working: List[str] = list(df_full[mask]["name"].dropna().tolist())
    else:
        _working = []

    status = pn.pane.Markdown("", sizing_mode="stretch_width", margin=(4, 0, 0, 0))

    # ── Name input ───────────────────────────────────────────────────────────
    name_input = pn.widgets.TextInput(
        name="View name",
        value="" if view_name == "All" else target_name[0],
        placeholder="Enter view name…",
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )

    # ── View contents table ──────────────────────────────────────────────────
    _mgr_ref = dataui._dataui_manager
    _display_cols: List[str] = list(_mgr_ref.get_table_columns())
    if "name" not in _display_cols:
        _display_cols = ["name"] + _display_cols

    def _build_contents_df() -> pd.DataFrame:
        full = dataui._dfcat_full
        if not _has_name or not _working:
            existing_cols = [c for c in _display_cols if c in full.columns]
            return pd.DataFrame(columns=existing_cols)
        mask = full["name"].isin(set(_working))
        df = full[mask].copy()
        try:
            import geopandas as gpd
            if isinstance(df, gpd.GeoDataFrame):
                df = pd.DataFrame(df.drop(columns=["geometry"], errors="ignore"))
        except ImportError:
            pass
        existing_cols = [c for c in _display_cols if c in df.columns]
        return df[existing_cols].reset_index(drop=True)

    count_md = pn.pane.Markdown(
        f"**{len(_working)} refs** in this view",
        sizing_mode="stretch_width",
        margin=(2, 0, 2, 0),
    )
    contents_table = pn.widgets.Tabulator(
        _build_contents_df(),
        selectable="checkbox",
        show_index=False,
        sizing_mode="stretch_width",
        height=220,
        margin=(2, 0, 4, 0),
    )

    def _refresh_contents() -> None:
        contents_table.value = _build_contents_df()
        count_md.object = f"**{len(_working)} refs** in this view"

    # ── Remove selected ──────────────────────────────────────────────────────
    remove_btn = pn.widgets.Button(
        name="Remove Selected",
        button_type="danger",
        icon="trash",
        width=160,
        margin=(2, 4, 2, 0),
    )

    def _on_remove(event: Any) -> None:
        sel = contents_table.selection
        if not sel:
            status.object = "⚠️ Select rows in the view contents table to remove."
            return
        tbl_df = contents_table.value
        if "name" not in tbl_df.columns:
            return
        to_remove = set(tbl_df.iloc[sel]["name"].dropna().tolist())
        before = len(_working)
        _working[:] = [n for n in _working if n not in to_remove]
        _refresh_contents()
        status.object = f"✓ Removed {before - len(_working)} refs."

    remove_btn.on_click(_on_remove)

    # ── Clear all ────────────────────────────────────────────────────────────
    clear_btn = pn.widgets.Button(
        name="Clear All",
        button_type="light",
        icon="x",
        width=100,
        margin=(2, 0, 2, 4),
    )

    def _on_clear(event: Any) -> None:
        _working.clear()
        _refresh_contents()
        status.object = "✓ View contents cleared."

    clear_btn.on_click(_on_clear)

    # ── Add from main table selection ────────────────────────────────────────
    add_sel_btn = pn.widgets.Button(
        name="Add Selected Rows",
        button_type="primary",
        icon="plus",
        width=160,
        margin=(2, 4, 2, 0),
    )

    def _on_add_selected(event: Any) -> None:
        sel = dataui.display_table.selection
        if not sel:
            status.object = "⚠️ Select rows in the catalog table above first."
            return
        src = dataui._dfcat
        if "name" not in src.columns:
            status.object = "⚠️ Catalog has no 'name' column."
            return
        new_names = src.iloc[sel]["name"].dropna().tolist()
        existing = set(_working)
        added = [n for n in new_names if n not in existing]
        _working.extend(added)
        _refresh_contents()
        already = len(new_names) - len(added)
        msg = f"✓ Added {len(added)} refs."
        if already:
            msg += f" ({already} already in view.)"
        status.object = msg

    add_sel_btn.on_click(_on_add_selected)

    # ── Add via header filters ───────────────────────────────────────────────
    add_filt_btn = pn.widgets.Button(
        name="Add All Filtered",
        button_type="default",
        icon="filter",
        width=160,
        margin=(2, 0, 2, 4),
    )

    def _apply_tabulator_filters(df: pd.DataFrame, filters: list) -> pd.DataFrame:
        """Replicate Tabulator header-filter logic server-side.

        Handles the filter types that Panel Tabulator emits for string columns:
        ``"like"`` (default substring), ``"="`` / ``"eq"`` (exact),
        ``"regex"``, ``"starts"``, ``"ends"``.  Unknown types fall back to
        substring match.
        """
        if not filters:
            return df
        mask = pd.Series(True, index=df.index)
        for f in filters:
            field = f.get("field")
            ftype = (f.get("type") or "like").lower()
            value = f.get("value")
            if not field or field not in df.columns or value is None or str(value) == "":
                continue
            col = df[field].fillna("").astype(str)
            sv = str(value)
            if ftype in ("like", "starts", "ends", "contains"):
                mask &= col.str.contains(sv, case=False, na=False, regex=False)
            elif ftype in ("=", "==", "eq"):
                mask &= col.str.lower() == sv.lower()
            elif ftype == "regex":
                try:
                    mask &= col.str.contains(sv, case=False, na=False, regex=True)
                except Exception:
                    pass
            else:
                mask &= col.str.contains(sv, case=False, na=False, regex=False)
        return df[mask]

    def _on_add_filtered(event: Any) -> None:
        filters = dataui.display_table.filters or []
        if not filters:
            status.object = "⚠️ No header filters are active in the catalog table above."
            return
        src = _apply_tabulator_filters(dataui._dfcat_full, filters)
        if "name" not in src.columns:
            status.object = "⚠️ Catalog has no 'name' column."
            return
        new_names = src["name"].dropna().tolist()
        existing = set(_working)
        added = [n for n in new_names if n not in existing]
        _working.extend(added)
        _refresh_contents()
        already = len(new_names) - len(added)
        msg = f"✓ Added {len(added)} refs from {len(new_names)} filtered rows."
        if already:
            msg += f" ({already} already in view.)"
        status.object = msg

    add_filt_btn.on_click(_on_add_filtered)

    # ── Save / Cancel ────────────────────────────────────────────────────────
    save_btn = pn.widgets.Button(
        name="Save View",
        button_type="success",
        icon="device-floppy",
        width=120,
        margin=(2, 4, 2, 0),
    )
    cancel_btn = pn.widgets.Button(
        name="Discard",
        button_type="light",
        width=100,
        margin=(2, 0, 2, 4),
    )

    editor_panel = pn.Column(sizing_mode="stretch_both")

    def _on_save(event: Any) -> None:
        new_name = name_input.value.strip()
        if not new_name:
            status.object = "⚠️ Enter a view name first."
            return
        old_name = target_name[0]
        if not _view_exists[0]:
            try:
                mgr.add_view(ViewDefinition(name=new_name, names=list(_working)))
            except ValueError as exc:
                status.object = f"⚠️ {exc}"
                return
            _view_exists[0] = True
            target_name[0] = new_name
        else:
            if new_name != old_name:
                try:
                    mgr.rename_view(old_name, new_name)
                except (KeyError, ValueError) as exc:
                    status.object = f"⚠️ {exc}"
                    return
                target_name[0] = new_name
            updated = mgr.get_view_def(new_name)
            updated.names = list(_working)
            updated.criteria = {}  # editor always produces names-based views
            mgr._version += 1
        mgr.active_view = target_name[0]
        dataui._refresh_table_from_view()
        status.object = f"✓ View '{target_name[0]}' saved ({len(_working)} refs)."

    def _on_cancel(event: Any) -> None:
        editor_panel.objects = [
            pn.pane.HTML(
                "<span style='color:#999;font-size:12px'>"
                "View editor closed. Use the Views sidebar tab to reopen.</span>"
            )
        ]

    save_btn.on_click(_on_save)
    cancel_btn.on_click(_on_cancel)

    # ── Assemble ─────────────────────────────────────────────────────────────
    editor_panel.objects = [
        pn.Row(name_input, save_btn, cancel_btn, sizing_mode="stretch_width"),
        status,
        _section("Current view contents"),
        count_md,
        contents_table,
        pn.Row(remove_btn, clear_btn),
        _section("Add from catalog table above"),
        pn.pane.HTML(
            "<span style='font-size:11px;color:#666'>"
            "Select rows in the catalog table above, then click "
            "<b>Add Selected Rows</b>. Or set header filters on the catalog "
            "table and click <b>Add All Filtered</b>.</span>",
            sizing_mode="stretch_width",
            margin=(0, 0, 6, 0),
        ),
        pn.Row(add_sel_btn, add_filt_btn),
    ]

    editor_label = (
        f"View Editor \u2014 {target_name[0]}"
        if view_name != "All"
        else "View Editor \u2014 New"
    )
    dataui.show_in_display_panel(editor_label, editor_panel)


def create_views_tab(dataui: Any) -> pn.Column:
    """Build the **Views** sidebar tab for *dataui*.

    Parameters
    ----------
    dataui : DataUI
        The live :class:`~dvue.dataui.DataUI` instance.  Must have
        ``_views_manager``, ``_dfcat_full``, ``display_table``, and
        ``_refresh_table_from_view`` set (all provided by the framework).

    Returns
    -------
    pn.Column
        The complete Views tab widget tree.
    """
    mgr: ViewsManager = dataui._views_manager
    status = pn.pane.Markdown("", sizing_mode="stretch_width", margin=(4, 0, 0, 0))

    # ── Section A: View switcher ────────────────────────────────────────────
    radio = pn.widgets.RadioButtonGroup(
        options=mgr.view_names,
        value=mgr.active_view,
        button_type="default",
        button_style="outline",
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    add_view_btn = pn.widgets.Button(
        name="+",
        width=34,
        button_type="success",
        margin=(2, 0, 4, 4),
        stylesheets=["button { font-size: 16px; font-weight: bold; }"],
    )
    switcher_row = pn.Row(radio, add_view_btn, sizing_mode="stretch_width")

    # ── Section B: Active view editor (hidden when "All" is selected) ───────
    name_input = pn.widgets.TextInput(
        name="View name",
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    criteria_input = pn.widgets.TextAreaInput(
        name="Criteria (YAML)",
        placeholder="variable: EC\nstation: ~RSAC.*",
        height=110,
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    names_input = pn.widgets.TextAreaInput(
        name="Names (one per line)",
        placeholder="ref_name_1\nref_name_2",
        height=80,
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    apply_btn = pn.widgets.Button(
        name="Apply", button_type="primary", width=80, margin=(2, 4, 2, 0)
    )
    delete_btn = pn.widgets.Button(
        name="Delete", button_type="danger", width=80, margin=(2, 0, 2, 4)
    )
    editor_section = pn.Column(
        _section("Edit Active View"),
        name_input,
        criteria_input,
        names_input,
        pn.Row(apply_btn, delete_btn),
        pn.layout.Divider(),
        sizing_mode="stretch_width",
        visible=False,
    )

    # ── Section C: Create / Append — unified toggled section ────────────────
    mode_toggle = pn.widgets.RadioButtonGroup(
        options=["Create New", "Add to Existing"],
        value="Create New",
        button_type="default",
        button_style="outline",
        sizing_mode="stretch_width",
        margin=(2, 0, 6, 0),
    )

    # -- Create New sub-panel --
    new_view_name_input = pn.widgets.TextInput(
        placeholder="New view name…",
        name="",
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    from_selection_btn = pn.widgets.Button(
        name="From Selection",
        button_type="default",
        icon="list-check",
        sizing_mode="stretch_width",
        margin=(2, 2, 2, 0),
    )
    from_filters_btn = pn.widgets.Button(
        name="From Table Filters",
        button_type="default",
        icon="filter",
        sizing_mode="stretch_width",
        margin=(2, 0, 2, 2),
    )
    create_new_panel = pn.Column(
        new_view_name_input,
        pn.Row(from_selection_btn, from_filters_btn, sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
        visible=True,
    )

    # -- Add to Existing sub-panel --
    _no_views_note = pn.pane.Markdown(
        "_Create a named view first._",
        sizing_mode="stretch_width",
        margin=(0, 0, 4, 0),
        visible=False,
    )
    append_view_select = pn.widgets.Select(
        name="",
        options=[v.name for v in mgr._views],
        disabled=len(mgr._views) == 0,
        sizing_mode="stretch_width",
        margin=(2, 0, 4, 0),
    )
    add_to_view_btn = pn.widgets.Button(
        name="Add Selected to View",
        button_type="primary",
        icon="plus",
        sizing_mode="stretch_width",
        disabled=len(mgr._views) == 0,
        margin=(2, 0, 2, 0),
    )
    add_to_existing_panel = pn.Column(
        _no_views_note,
        append_view_select,
        add_to_view_btn,
        sizing_mode="stretch_width",
        visible=False,
    )

    add_section = pn.Column(
        _section("From Selection"),
        mode_toggle,
        create_new_panel,
        add_to_existing_panel,
        pn.layout.Divider(),
        sizing_mode="stretch_width",
    )

    # ── Section D: Load / Save YAML ─────────────────────────────────────────
    upload_widget = pn.widgets.FileInput(
        accept=".yaml,.yml",
        name="Choose views YAML",
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

    def _yaml_download_callback():
        return io.BytesIO(mgr.to_yaml_str().encode("utf-8"))

    download_btn = pn.widgets.FileDownload(
        label="Save YAML",
        callback=_yaml_download_callback,
        filename="views.yaml",
        button_type="success",
        icon="file-export",
        embed=False,
        sizing_mode="stretch_width",
        height=30,
        margin=(0, 0, 4, 0),
    )
    io_section = pn.Column(
        _section("Load / Save Views"),
        upload_widget,
        pn.Row(upload_btn, download_btn, sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
    )

    # ── Internal helpers ─────────────────────────────────────────────────────

    # Tracks the last named view the user was in so the append dropdown can
    # pre-select it when the user returns after browsing 'All'.
    _last_named_view: List[str] = [""]  # mutable container for closure mutation

    def _rebuild_view_select() -> None:
        """Sync the 'Add to Existing' dropdown to the current view list."""
        names = [v.name for v in mgr._views]
        append_view_select.options = names
        has_views = bool(names)
        append_view_select.disabled = not has_views
        add_to_view_btn.disabled = not has_views
        _no_views_note.visible = not has_views
        if has_views:
            # Pre-select the last named view the user was in, if still available
            preferred = _last_named_view[0]
            append_view_select.value = preferred if preferred in names else names[0]

    def _rebuild_radio() -> None:
        """Sync RadioButtonGroup options and append dropdown to current view list."""
        radio.options = mgr.view_names
        radio.value = mgr.active_view
        _rebuild_view_select()

    def _populate_editor() -> None:
        """Populate editor widgets from the active view definition."""
        vdef = mgr.get_view_def(mgr.active_view)
        if vdef is None:
            editor_section.visible = False
            return
        editor_section.visible = True
        name_input.value = vdef.name
        if vdef.criteria:
            import yaml
            criteria_input.value = yaml.dump(
                vdef.criteria, default_flow_style=False
            ).strip()
        else:
            criteria_input.value = ""
        names_input.value = "\n".join(vdef.names)

    # ── Wiring: view switcher ────────────────────────────────────────────────

    def _on_radio_change(event: Any) -> None:
        # Track the last named (non-"All") view for pre-selecting the append dropdown
        if event.old != "All":
            _last_named_view[0] = event.old
        mgr.active_view = event.new
        _populate_editor()

    radio.param.watch(_on_radio_change, "value")

    def _on_add_click(event: Any) -> None:
        existing = mgr.view_names
        base = "New View"
        candidate = base
        n = 1
        while candidate in existing:
            candidate = f"{base} {n}"
            n += 1
        try:
            vdef = ViewDefinition(name=candidate)
            mgr.add_view(vdef)
            _rebuild_radio()
            radio.value = candidate
            mgr.active_view = candidate
            _populate_editor()
            status.object = ""
        except ValueError as exc:
            status.object = f"⚠️ {exc}"

    add_view_btn.on_click(_on_add_click)

    # ── Wiring: editor Apply / Delete ────────────────────────────────────────

    def _on_apply_click(event: Any) -> None:
        vdef = mgr.get_view_def(mgr.active_view)
        if vdef is None:
            status.object = "⚠️ Cannot edit the 'All' view."
            return
        old_name = vdef.name
        new_name = name_input.value.strip()
        # Parse criteria YAML
        criteria: Dict[str, Any] = {}
        if criteria_input.value.strip():
            try:
                import yaml
                parsed = yaml.safe_load(criteria_input.value)
                if isinstance(parsed, dict):
                    criteria = parsed
            except Exception as exc:
                status.object = f"⚠️ Invalid criteria YAML: {exc}"
                return
        # Parse names list
        names = [n.strip() for n in names_input.value.splitlines() if n.strip()]
        # Rename if needed
        if new_name and new_name != old_name:
            try:
                mgr.rename_view(old_name, new_name)
            except (KeyError, ValueError) as exc:
                status.object = f"⚠️ {exc}"
                return
            vdef = mgr.get_view_def(new_name)
        vdef.criteria = criteria
        vdef.names = names
        _rebuild_radio()
        radio.value = mgr.active_view
        dataui._refresh_table_from_view()
        status.object = f"✓ View '{mgr.active_view}' updated."

    apply_btn.on_click(_on_apply_click)

    def _on_delete_click(event: Any) -> None:
        current = mgr.active_view
        if current == "All":
            status.object = "⚠️ Cannot delete the 'All' view."
            return
        try:
            mgr.remove_view(current)
        except KeyError as exc:
            status.object = f"⚠️ {exc}"
            return
        _rebuild_radio()
        radio.value = mgr.active_view
        _populate_editor()
        dataui._refresh_table_from_view()
        status.object = f"✓ View '{current}' deleted."

    delete_btn.on_click(_on_delete_click)

    # ── Wiring: add from selection / filters ─────────────────────────────────

    def _on_from_selection_click(event: Any) -> None:
        view_name = new_view_name_input.value.strip()
        if not view_name:
            status.object = "⚠️ Enter a name for the new view above."
            return
        sel = dataui.display_table.selection
        if not sel:
            status.object = "⚠️ No rows selected in the table."
            return
        df = dataui._dfcat
        if "name" not in df.columns:
            status.object = "⚠️ Catalog has no 'name' column."
            return
        selected_names = df.iloc[sel]["name"].tolist()
        try:
            vdef = ViewDefinition(name=view_name, names=selected_names)
            mgr.add_view(vdef)
        except ValueError as exc:
            status.object = f"⚠️ {exc}"
            return
        _rebuild_radio()
        radio.value = view_name
        mgr.active_view = view_name
        _populate_editor()
        dataui._refresh_table_from_view()
        new_view_name_input.value = ""
        status.object = (
            f"✓ View '{view_name}' created from selection "
            f"({len(selected_names)} rows)."
        )

    from_selection_btn.on_click(_on_from_selection_click)

    def _on_from_filters_click(event: Any) -> None:
        view_name = new_view_name_input.value.strip()
        if not view_name:
            status.object = "⚠️ Enter a name for the new view above."
            return
        filters = dataui.display_table.filters or []
        criteria: Dict[str, Any] = {}
        for f in filters:
            field_name = f.get("field")
            value = f.get("value")
            if field_name and value is not None and value != "":
                criteria[field_name] = value
        if not criteria:
            status.object = "⚠️ No active table filters to capture."
            return
        try:
            vdef = ViewDefinition(name=view_name, criteria=criteria)
            mgr.add_view(vdef)
        except ValueError as exc:
            status.object = f"⚠️ {exc}"
            return
        _rebuild_radio()
        radio.value = view_name
        mgr.active_view = view_name
        _populate_editor()
        dataui._refresh_table_from_view()
        new_view_name_input.value = ""
        status.object = f"✓ View '{view_name}' created from table filters."

    from_filters_btn.on_click(_on_from_filters_click)

    # ── Wiring: mode toggle ──────────────────────────────────────────────────

    def _on_mode_toggle_change(event: Any) -> None:
        is_create = event.new == "Create New"
        create_new_panel.visible = is_create
        add_to_existing_panel.visible = not is_create
        if not is_create:
            _rebuild_view_select()

    mode_toggle.param.watch(_on_mode_toggle_change, "value")

    # ── Wiring: add to existing view ─────────────────────────────────────────

    def _on_add_to_view_click(event: Any) -> None:
        target = append_view_select.value
        if not target:
            status.object = "⚠️ No view selected."
            return
        sel = dataui.display_table.selection
        if not sel:
            status.object = "⚠️ No rows selected in the table."
            return
        df = dataui._dfcat
        if "name" not in df.columns:
            status.object = "⚠️ Catalog has no 'name' column."
            return
        selected_names = df.iloc[sel]["name"].tolist()
        try:
            n_added = mgr.add_to_view(target, selected_names)
        except KeyError as exc:
            status.object = f"⚠️ {exc}"
            return
        dataui._refresh_table_from_view()
        already = len(selected_names) - n_added
        msg = f"✓ Added {n_added} row(s) to '{target}'."
        if already:
            msg += f" ({already} already present.)"
        status.object = msg

    add_to_view_btn.on_click(_on_add_to_view_click)

    # ── Wiring: YAML upload ──────────────────────────────────────────────────

    def _on_upload_click(event: Any) -> None:
        if upload_widget.value is None:
            status.object = "⚠️ No file selected."
            return
        try:
            raw = upload_widget.value
            yaml_str = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            mgr.load_from_yaml_str(yaml_str)
        except ValueError as exc:
            status.object = f"⚠️ {exc}"
            return
        _rebuild_radio()
        radio.value = mgr.active_view
        _populate_editor()
        dataui._refresh_table_from_view()
        fname = getattr(upload_widget, "filename", "file")
        status.object = f"✓ Loaded {len(mgr._views)} view(s) from {fname}."

    upload_btn.on_click(_on_upload_click)

    # ── Sync mgr state → radio when changed externally ──────────────────────

    def _on_version_change(event: Any) -> None:
        _rebuild_radio()
        _populate_editor()

    def _on_active_view_change(event: Any) -> None:
        if radio.value != event.new:
            radio.value = event.new
        _populate_editor()

    mgr.param.watch(_on_version_change, "_version")
    mgr.param.watch(_on_active_view_change, "active_view")

    # ── View Editor button ────────────────────────────────────────────────────
    edit_view_btn = pn.widgets.Button(
        name="Edit View…",
        button_type="default",
        icon="edit",
        sizing_mode="stretch_width",
        margin=(2, 0, 8, 0),
    )

    def _on_edit_view_click(event: Any) -> None:
        open_view_editor(dataui, mgr)

    edit_view_btn.on_click(_on_edit_view_click)

    return pn.Column(
        _section("Views"),
        switcher_row,
        edit_view_btn,
        editor_section,
        add_section,
        io_section,
        status,
        sizing_mode="stretch_width",
        margin=(4, 4, 4, 4),
    )
