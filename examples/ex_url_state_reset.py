"""
Phase 0: proof-of-concept for cookie-based Panel session persistence.

Two-layer approach
------------------
Layer 1 — live object reuse (server still running):
    A persistent UUID cookie ('dvue_user_id') is set on first visit.
    panel.config.reuse_sessions + session_key_func map that UUID to the
    existing live Bokeh Document, so the browser reconnects to the same
    Python object.  No widget state is lost.

Layer 2 — param restore (after server restart):
    On every selection change the selection is saved to a tiny JSON file
    keyed by the UUID.  On a fresh session pn.state.onload reads the
    cookie, loads the JSON, and restores the selection (and re-draws the
    plot) before the page is shown.

Usage
-----
Run programmatically (required — `panel serve` cannot patch the Tornado
handler before the server starts):

    cd dvue
    python examples/ex_url_state_reset.py

Then open http://localhost:5007 in your browser.

What to verify
--------------
1. First visit: page loads; cookie 'dvue_user_id' appears in DevTools →
   Application → Cookies.  Session ID shown in the banner.
2. Select rows, click Plot.  Plot appears.
3. Close the browser tab.  Reopen http://localhost:5007 (no query string).
   → Same Session ID shown.  Selection AND plot are both restored.
   (Layer 1 — live object reused.)
4. Stop the server (Ctrl-C), restart it, reopen the URL.
   → New Session ID.  Selection is restored from JSON; plot is re-drawn.
   (Layer 2 — param restore from disk.)
5. Open a second browser (or private window).  It gets a different UUID
   and a completely independent session.

Depends only on: panel, holoviews, numpy, pandas
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

import diskcache

import numpy as np
import pandas as pd
import panel as pn
import holoviews as hv

# ---------------------------------------------------------------------------
# Layer 1 — Custom Tornado handler: set UUID cookie, enable session reuse
# ---------------------------------------------------------------------------
# Must happen BEFORE pn.serve() / BokehServer.__init__() — this is why we
# cannot use `panel serve script.py` for this demo.

from bokeh.server.urls import per_app_patterns
from panel.io.server import DocHandler


class _SessionAwareDocHandler(DocHandler):
    """Injects a persistent 'dvue_user_id' UUID cookie on first visit.

    The cookie is injected into self.request.cookies (not just the response)
    so that session_key_func sees it on the very first request — before
    state._sessions has been populated for this user.
    """

    _COOKIE_NAME = "dvue_user_id"

    async def get(self, *args, **kwargs):
        user_id = self.get_cookie(self._COOKIE_NAME)
        if not user_id:
            user_id = uuid4().hex
            # Set response cookie (persists in the browser across restarts)
            self.set_cookie(self._COOKIE_NAME, user_id, expires_days=365, path="/")
            # Inject into the current request's cookie jar so session_key_func
            # can read it immediately (SimpleCookie.__setitem__ with a plain
            # string creates a Morsel with .value == user_id).
            self.request.cookies[self._COOKIE_NAME] = user_id
        await super().get(*args, **kwargs)


# Replace Bokeh/Panel's default per-app doc handler with our custom one.
per_app_patterns[0] = (r"/?", _SessionAwareDocHandler)

# ---------------------------------------------------------------------------
# Layer 2 — diskcache state store: server-restart fallback
# ---------------------------------------------------------------------------
# Panel already depends on diskcache (used by pn.state.as_cached).
# Benefits over hand-rolled JSON: built-in TTL, file-locking, cleaner API.
# Limitation: can only store picklable Python objects (dicts, lists, etc.)
# — NOT live Panel/HoloViews widget objects.

_SESSION_CACHE = diskcache.Cache(str(Path(__file__).parent.parent / ".session_cache"))
_TTL = 30 * 24 * 3600  # 30 days


def _load_state(user_id: str) -> dict:
    return _SESSION_CACHE.get(user_id, default={})


def _save_state(user_id: str, state: dict) -> None:
    _SESSION_CACHE.set(user_id, state, expire=_TTL)


# ---------------------------------------------------------------------------
# Layer 1 — in-memory state registry
# ---------------------------------------------------------------------------
# user_id → {"selection": list[int]}
# A new browser tab (same cookie) reads the same entry — shared state.
# A different browser (different cookie) gets an independent entry.
# Server restart empties the registry; diskcache provides fallback.
_STATE_REGISTRY: dict = {}

# ---------------------------------------------------------------------------
# pn.extension — must happen before any widget is created
# ---------------------------------------------------------------------------
pn.extension("tabulator", sizing_mode="stretch_width")
hv.extension("bokeh")

# ---------------------------------------------------------------------------
# Sample data (module-level — shared, read-only)
# ---------------------------------------------------------------------------
_t = pd.date_range("2020-01-01", periods=365, freq="D")

SERIES = {
    "sine_fast": pd.Series(np.sin(2 * np.pi * _t.dayofyear / 30),  index=_t, name="value"),
    "sine_slow": pd.Series(np.sin(2 * np.pi * _t.dayofyear / 180), index=_t, name="value"),
    "cosine":    pd.Series(np.cos(2 * np.pi * _t.dayofyear / 90),  index=_t, name="value"),
    "trend":     pd.Series(np.linspace(-1, 1, len(_t)),             index=_t, name="value"),
}

catalog_df = pd.DataFrame({
    "name":        list(SERIES.keys()),
    "description": [
        "Fast sine (30-day period)",
        "Slow sine (180-day period)",
        "Cosine (90-day period)",
        "Linear trend (-1 to 1)",
    ],
})

# ---------------------------------------------------------------------------
# App factory — called once per Bokeh session
# ---------------------------------------------------------------------------
# Widgets (table, plot_pane …) are Bokeh models: they belong to exactly one
# Bokeh document and are re-created fresh every session.  Only the plain
# Python state dict in _STATE_REGISTRY is shared across sessions.

def make_app():
    user_id = pn.state.cookies.get("dvue_user_id", "")

    # Resolve or create state entry.
    if user_id and user_id in _STATE_REGISTRY:
        entry = _STATE_REGISTRY[user_id]
    else:
        saved = _load_state(user_id) if user_id else {}
        entry = {"selection": saved.get("selection", [])}
        if user_id:
            _STATE_REGISTRY[user_id] = entry

    # --- Fresh widgets per session (Bokeh models cannot be shared) ---------
    table = pn.widgets.Tabulator(
        catalog_df,
        selectable=True,
        show_index=False,
        disabled=True,
        height=200,
    )

    plot_pane = pn.pane.HoloViews(
        hv.Curve([]).opts(width=750, height=300, title="(press Plot)"),
        sizing_mode="stretch_width",
    )

    info_pane = pn.pane.Markdown("_Loading..._")
    plot_button = pn.widgets.Button(name="Plot", button_type="primary", width=100)

    def _draw_plot(selection):
        if not selection:
            plot_pane.object = hv.Curve([]).opts(width=750, height=300, title="(no selection)")
            return
        curves = []
        for i in selection:
            name = catalog_df.iloc[i]["name"]
            s = SERIES[name]
            df = s.reset_index()
            df.columns = ["datetime", "value"]
            curves.append(hv.Curve(df, "datetime", "value", label=name))
        plot_pane.object = hv.Overlay(curves).opts(
            hv.opts.Curve(width=750, height=300, tools=["hover"]),
            hv.opts.Overlay(legend_position="top_right", title="Selected series"),
        )

    plot_button.on_click(lambda e: _draw_plot(table.selection))

    # --- onload: restore state + wire watchers -----------------------------
    def _on_load():
        try:
            sid = pn.state.curdoc.session_context.id
        except Exception:
            sid = "(unavailable)"

        info_pane.object = (
            f"**User UUID:** `{user_id}`\n\n"
            f"**Session ID:** `{sid}`"
        )

        # Restore selection from registry (or diskcache on first load).
        sel = entry.get("selection", [])
        if sel:
            table.selection = sel
            _draw_plot(sel)

        # Keep registry + diskcache in sync on every change.
        def _on_selection(event):
            entry["selection"] = event.new or []
            if user_id:
                _save_state(user_id, {"selection": entry["selection"]})

        table.param.watch(_on_selection, "selection")

    pn.state.onload(_on_load)

    pn.Column(
        pn.pane.Markdown("""
## Manager-registry session persistence — Phase 0 demo

**Layer 1 (server running — registry hit):**
1. Select rows and click **Plot**.
2. Close this browser tab.
3. Reopen `http://localhost:5007` — same UUID, selection and plot restored
   instantly from in-memory registry (no disk read).

**Layer 2 (server restarted — diskcache fallback):**
1. Select rows and click **Plot**.
2. Stop the server and restart it.
3. Reopen `http://localhost:5007` — new Session ID; selection and plot
   restored from diskcache.
"""),
        info_pane,
        table,
        plot_button,
        plot_pane,
    ).servable(title="Session Persistence Demo")


# ---------------------------------------------------------------------------
# Programmatic launch
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    pn.serve(
        {"app": make_app},
        port=5007,
        show=True,
        unused_session_lifetime_milliseconds=2_592_000_000,
    )
