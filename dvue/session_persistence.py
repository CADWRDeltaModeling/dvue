"""dvue.session_persistence — Two-layer Panel session persistence.

Provides reusable building blocks for persisting UI state across browser
restarts and server restarts in Panel applications built with dvue.

Layer 1 — in-memory registry
    While the server is running, a returning user's browser sends the
    persistent ``dvue_user_id`` cookie → the existing manager and DataUI
    objects are reused from ``_MANAGER_REGISTRY``.  Panel automatically
    mirrors all widget state (table selection, plot tabs, param values) into
    the new Bokeh Document.  No deserialization needed.

Layer 2 — diskcache (server-restart fallback)
    After a server restart the registry is empty.  Picklable params
    (``time_range``, ``selection``) are restored from a ``diskcache.Cache``
    into a freshly constructed manager.

Typical usage in a CLI entry-point::

    from dvue.session_persistence import serve_session_app

    def my_command(files, port=0):
        def build_manager():
            return MyTimeSeriesDataUIManager(*files)

        serve_session_app(build_manager, title="My App", port=port)

The entry-point **must** be launched programmatically (``python run_server.py``
or via a CLI that calls ``pn.serve(callable)``) — **not** via ``panel serve``.
The ``install_session_handler()`` patch must execute before
``BokehServer.__init__()``, which ``panel serve`` does not guarantee.

See Also
--------
``.github/session-management.md`` and ``.github/session-persistence-plan.md``
for the full design rationale.
"""

from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
import panel as pn

logger = logging.getLogger(__name__)

_handler_installed = False  # guard against double-patching


def install_session_handler(cookie_name: str = "dvue_user_id") -> None:
    """Patch Bokeh's first per-app URL handler to set a persistent user-ID cookie.

    Overrides Bokeh's ``DocHandler`` so that:

    * On first visit: a ``uuid4().hex`` value is generated, set as a
      response cookie (365-day lifetime), **and** injected into
      ``request.cookies`` so ``pn.state.cookies`` and Panel's
      ``session_key_func`` see it within the *same* HTTP request — before
      the browser has sent it back.
    * On subsequent visits: the browser sends the cookie in the request and
      it is read normally.

    Must be called **before** ``pn.serve()`` / ``BokehServer.__init__()``.
    Idempotent: safe to call multiple times.

    Parameters
    ----------
    cookie_name:
        Cookie key to use.  Default ``"dvue_user_id"``.  Apps may use their
        own name (e.g. ``"myapp_user_id"``) to avoid collisions on shared
        origins.
    """
    global _handler_installed
    if _handler_installed:
        return
    try:
        from bokeh.server.urls import per_app_patterns
        from panel.io.server import DocHandler

        class _SessionAwareDocHandler(DocHandler):
            _COOKIE_NAME = cookie_name

            async def get(self, *args, **kwargs):
                user_id = self.get_cookie(self._COOKIE_NAME)
                if not user_id:
                    user_id = uuid4().hex
                    self.set_cookie(
                        self._COOKIE_NAME, user_id, expires_days=365, path="/"
                    )
                    # Inject into Tornado SimpleCookie so pn.state.cookies
                    # sees the value on the very first visit, before the
                    # browser has echoed the cookie back.
                    self.request.cookies[self._COOKIE_NAME] = user_id
                await super().get(*args, **kwargs)

        per_app_patterns[0] = (r"/?", _SessionAwareDocHandler)
        _handler_installed = True
    except Exception:
        logger.warning(
            "dvue.session_persistence: could not install session cookie handler; "
            "session persistence will be disabled.",
            exc_info=True,
        )


_TTL = 30 * 24 * 3600  # 30-day diskcache TTL


def snapshot(mgr, ui) -> dict:
    """Return a picklable dict of the current UI state for diskcache storage.

    Only plain Python values are stored (ISO date strings, int lists) — no
    live Panel/HoloViews/Bokeh objects, which are not meaningfully picklable.

    Parameters
    ----------
    mgr:
        A ``DataUIManager`` (or ``TimeSeriesDataUIManager``) instance.
    ui:
        The ``DataUI`` instance wrapping *mgr*.

    Returns
    -------
    dict
        ``{"time_range": [iso_start, iso_end] | None, "selection": [int, ...]}``
    """
    tr = getattr(mgr, "time_range", None)
    tbl = getattr(ui, "display_table", None)
    return {
        "time_range": (
            [pd.Timestamp(tr[0]).isoformat(), pd.Timestamp(tr[1]).isoformat()]
            if tr
            else None
        ),
        "selection": list(tbl.selection or []) if tbl is not None else [],
    }


def restore(mgr, saved: dict) -> None:
    """Apply a diskcache *saved* dict to a freshly created manager.

    Safe for managers without a ``time_range`` param (e.g. those that extend
    ``DataUIManager`` directly rather than ``TimeSeriesDataUIManager``).

    Parameters
    ----------
    mgr:
        Freshly constructed ``DataUIManager`` instance.
    saved:
        Dict previously produced by :func:`snapshot`.
    """
    tr = saved.get("time_range")
    if tr and "time_range" in mgr.param:
        try:
            mgr.time_range = (
                pd.Timestamp(tr[0]).to_pydatetime(),
                pd.Timestamp(tr[1]).to_pydatetime(),
            )
        except Exception:
            logger.debug(
                "dvue.session_persistence: could not restore time_range",
                exc_info=True,
            )


def serve_session_app(
    build_manager_fn,
    title: str,
    port: int = 0,
    crs=None,
    station_id_column: str | None = None,
    cookie_name: str = "dvue_user_id",
    cache_dir: str | Path | None = None,
    persist: bool = False,
    **pn_serve_kwargs,
) -> None:
    """Launch a session-aware Panel app for a single dvue manager.

    Calls :func:`install_session_handler`, builds the per-session Bokeh app
    factory (``make_app``), and hands it to ``pn.serve()``.

    The two-layer persistence contract:

    * **Registry hit** (server running, returning user): existing ``mgr``,
      ``DataUI``, and ``VanillaTemplate`` are reused.  Only per-Document hooks
      (URL/location sync) are re-registered via ``pn.state.onload``.
    * **Registry miss** (new user or server restart): ``build_manager_fn()`` is
      called, and — when *persist* is ``True`` — diskcache params are restored
      and live-save watchers are wired via ``pn.state.onload``.

    Parameters
    ----------
    build_manager_fn:
        Zero-argument callable that constructs and returns a fresh
        ``DataUIManager`` (or subclass) instance.  Called at most once per
        unique user identity (UUID cookie).
    title:
        Browser window/tab title; also used as the URL path key
        (lower-cased, spaces → hyphens).
    port:
        TCP port for the Bokeh server.  ``0`` selects a random available port.
    crs:
        Cartopy CRS passed as ``DataUI(mgr, crs=crs)``.  ``None`` → no map.
    station_id_column:
        Column name passed as ``DataUI(mgr, station_id_column=...)``.
        ``None`` → DataUI default.
    cookie_name:
        Name of the persistent user-identity cookie.  Default
        ``"dvue_user_id"``.  Pass a custom name to avoid collisions when
        multiple dvue apps are served on the same origin.
    cache_dir:
        Directory for the diskcache session store.  Only used when *persist*
        is ``True``.  Defaults to ``~/.dvue_sessions``.
    persist:
        When ``True``, enable Layer 2 disk persistence: save/restore
        ``time_range`` and table ``selection`` across server restarts using
        diskcache.  Defaults to ``False``.
    **pn_serve_kwargs:
        Extra keyword arguments forwarded verbatim to ``pn.serve()``.

    Notes
    -----
    ``num_procs=1`` is required for Layer 1 (registry) to work: the
    ``_registry`` dict is in-process and not shared across OS processes.
    Layer 2 (diskcache) works with any ``num_procs`` because diskcache uses
    file-locking and is safe for concurrent multi-process access.
    """
    install_session_handler(cookie_name=cookie_name)

    # Suppress the harmless Bokeh race-condition noise where the browser sends
    # patch messages for model IDs that no longer exist after a document rebuild.
    class _SuppressUnknownRef(logging.Filter):
        def filter(self, record):
            return "UnknownReferenceError" not in record.getMessage()

    logging.getLogger("bokeh.server.protocol_handler").addFilter(_SuppressUnknownRef())

    # Layer 2: diskcache — only when persist=True
    if persist:
        _cache_dir = Path(cache_dir) if cache_dir else Path.home() / ".dvue_sessions"
        _cache_dir.mkdir(parents=True, exist_ok=True)
        import diskcache
        _session_cache = diskcache.Cache(str(_cache_dir))

        def _load_state(user_id: str) -> dict:
            return _session_cache.get(user_id, default={})

        def _save_state(user_id: str, state: dict) -> None:
            _session_cache.set(user_id, state, expire=_TTL)
    else:
        def _load_state(user_id: str) -> dict:
            return {}

        def _save_state(user_id: str, state: dict) -> None:
            pass

    # In-memory registry: user_id → {"mgr": ..., "ui": ..., "template": ...}
    _registry: dict = {}

    def make_app():
        from dvue.dataui import DataUI

        user_id = pn.state.cookies.get(cookie_name, "")

        if user_id and user_id in _registry:
            # Registry hit: reuse existing objects; only re-register
            # per-Document hooks (location/URL sync binds to curdoc).
            entry = _registry[user_id]
            mgr, ui, tmpl = entry["mgr"], entry["ui"], entry["template"]
            pn.state.onload(lambda: (ui.setup_location_sync(), ui.setup_url_sync()))
            tmpl.servable()
            return

        # New user or server restart: build a fresh manager.
        mgr = build_manager_fn()
        saved = _load_state(user_id) if user_id else {}
        if saved:
            restore(mgr, saved)

        dataui_kwargs: dict = {}
        if crs is not None:
            dataui_kwargs["crs"] = crs
        if station_id_column is not None:
            dataui_kwargs["station_id_column"] = station_id_column

        ui = DataUI(mgr, **dataui_kwargs)
        tmpl = ui.create_view(title=title)
        tmpl.servable()

        if user_id:
            _registry[user_id] = {"mgr": mgr, "ui": ui, "template": tmpl}

        sel = saved.get("selection", [])

        def _on_load():
            # Replay saved selection → re-trigger Plot action.  Only
            # meaningful after a server restart when diskcache had a saved
            # selection; fresh users have no saved state.
            if sel and hasattr(ui, "display_table") and hasattr(ui, "_registered_actions"):
                plot_cb = next(
                    (
                        a["callback"]
                        for a in ui._registered_actions
                        if a.get("name") == "Plot"
                    ),
                    None,
                )
                if plot_cb:
                    n_rows = len(ui.display_table.value) if ui.display_table.value is not None else 0
                    valid_sel = [i for i in sel if i < n_rows]
                    if valid_sel:
                        ui.display_table.selection = valid_sel
                    pn.state.curdoc.add_next_tick_callback(
                        lambda: plot_cb(None, ui)
                    )

            # Wire live-persistence watchers so any param change is
            # immediately flushed to diskcache (only when persist=True).
            if persist:
                def _do_save(event=None):
                    if user_id:
                        _save_state(user_id, snapshot(mgr, ui))

                if "time_range" in mgr.param:
                    mgr.param.watch(_do_save, "time_range")
                if hasattr(ui, "display_table"):
                    ui.display_table.param.watch(_do_save, "selection")

        pn.state.onload(_on_load)

    # Sanitize title into a URL-safe Bokeh app-route key.
    app_key = title.lower().replace(" ", "-")

    pn.serve(
        {app_key: make_app},
        port=port,
        show=True,
        unused_session_lifetime_milliseconds=2_592_000_000,
        **pn_serve_kwargs,
    )
