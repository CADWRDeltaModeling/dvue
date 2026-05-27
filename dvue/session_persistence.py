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

        # Locate DocHandler by class rather than by fixed index.
        # Older Bokeh/Panel placed DocHandler at index 0; newer Panel
        # rewrites per_app_patterns so that WSHandler is at index 0 and
        # DocHandler moves to the last position.  Patching index 0 in the
        # new layout overwrote WSHandler, causing BokehTornado to raise
        # "Couldn't find websocket path".
        for _i, _p in enumerate(per_app_patterns):
            if _p[1] is DocHandler:
                per_app_patterns[_i] = (_p[0], _SessionAwareDocHandler)
                _handler_installed = True
                break
        else:
            logger.warning(
                "dvue.session_persistence: DocHandler not found in "
                "per_app_patterns; session persistence will be disabled."
            )
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

    # Probe manager once for view-layer hints (crs, station_id_column) when
    # the caller did not provide them.  Caller-provided values always win.
    _effective_crs = crs
    _effective_sic = station_id_column
    if _effective_crs is None or _effective_sic is None:
        _probe = build_manager_fn()
        if _effective_crs is None:
            _effective_crs = getattr(_probe, "crs", None)
        if _effective_sic is None:
            _effective_sic = getattr(_probe, "station_id_column", None)
        del _probe

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
        if _effective_crs is not None:
            dataui_kwargs["crs"] = _effective_crs
        if _effective_sic is not None:
            dataui_kwargs["station_id_column"] = _effective_sic

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


# ---------------------------------------------------------------------------
# Desktop-mode helpers
# ---------------------------------------------------------------------------

def _find_free_port() -> int:
    """Return an available TCP port assigned by the OS."""
    import socket
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_server(port: int, timeout: float = 15.0) -> None:
    """Block until a TCP listener is ready on localhost:*port*.

    Raises
    ------
    TimeoutError
        If the server does not respond within *timeout* seconds.
    """
    import socket
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", port), timeout=0.5):
                return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    raise TimeoutError(
        f"Panel server did not become ready on port {port} within {timeout:.0f}s"
    )


def serve_desktop_app(
    build_manager_fn,
    title: str,
    port: int = 0,
    server_timeout: float = 15.0,
    crs=None,
    station_id_column: str | None = None,
    cookie_name: str = "dvue_user_id",
    cache_dir: str | Path | None = None,
    persist: bool = False,
    **pn_serve_kwargs,
) -> None:
    """Launch a session-aware Panel app in a native desktop window via pywebview.

    Drop-in replacement for :func:`serve_session_app` that opens the app in a
    native OS window (using ``pywebview``) instead of a browser tab.  All
    session-persistence behaviour (cookie, in-memory registry, optional
    diskcache) is identical.

    The window starts maximised so it fills the available screen space on all
    platforms.  The user can resize it freely after launch.

    Parameters
    ----------
    build_manager_fn:
        Zero-argument callable returning a fresh ``DataUIManager`` instance.
    title:
        Window title; also used as the URL path key (lower-cased, spaces →
        hyphens).
    port:
        TCP port for the Bokeh/Panel server.  ``0`` (default) selects a free
        port automatically — the port is reserved *before* the server thread
        starts so the URL is known for the webview call.
    server_timeout:
        Seconds to wait for the Panel server to become ready before raising
        ``TimeoutError``.  Default ``15.0``.
    crs:
        Cartopy CRS passed as ``DataUI(mgr, crs=crs)``.  ``None`` → no map.
    station_id_column:
        Column name passed as ``DataUI(mgr, station_id_column=...)``.
    cookie_name:
        Name of the persistent user-identity cookie.  Default
        ``"dvue_user_id"``.
    cache_dir:
        Directory for the diskcache session store.  Only used when *persist*
        is ``True``.  Defaults to ``~/.dvue_sessions``.
    persist:
        Enable Layer 2 disk persistence across server restarts via diskcache.
        Default ``False``.
    **pn_serve_kwargs:
        Extra keyword arguments forwarded verbatim to ``pn.serve()``.

    Raises
    ------
    ImportError
        If ``pywebview`` is not installed.
    TimeoutError
        If the Panel server is not ready within *server_timeout* seconds.

    Notes
    -----
    * Must be launched as a regular Python script, **not** via ``panel serve``.
      ``install_session_handler()`` must run before ``BokehServer.__init__()``.
    * ``num_procs=1`` is required for the in-memory registry (Layer 1) to work.
    * The Panel server thread is a **daemon** — it exits automatically when the
      webview window is closed.

    Examples
    --------
    Minimal usage::

        from dvue.session_persistence import serve_desktop_app

        def build_manager():
            return MyTimeSeriesDataUIManager(*files)

        serve_desktop_app(build_manager, title="My App")
    """
    try:
        import webview
    except ImportError as exc:
        raise ImportError(
            "pywebview is required for desktop mode.  Install it with:\n"
            "    pip install pywebview"
        ) from exc

    import threading
    import tornado.ioloop as _tornado_ioloop

    # Reserve a free port before starting the server thread so we know the URL.
    if port == 0:
        port = _find_free_port()

    # Mutable dict shared between the pywebview GUI thread and the Panel/Tornado
    # event loop.  Populated once the first Panel session starts (_on_load).
    _app_state: dict = {}  # keys: "mgr", "ui", "ioloop"

    def _on_drag(e):  # noqa: ARG001
        """Intercept dragenter/dragover to prevent browser's default behaviour."""

    def _on_file_drop(e):
        """Receive pywebview drop event; schedule a confirmation form on the IOLoop."""
        paths = [
            f["pywebviewFullPath"]
            for f in (e.get("dataTransfer") or {}).get("files", [])
            if f.get("pywebviewFullPath")
        ]
        if not paths:
            return
        ioloop = _app_state.get("ioloop")
        if ioloop is None:
            logger.warning("serve_desktop_app: drop received before session ready; ignoring")
            return
        # add_callback is documented as thread-safe in Tornado — no queue needed.
        ioloop.add_callback(_show_drop_confirm, paths)

    def _show_drop_confirm(paths: list) -> None:
        """Build and show a confirmation form for dropped file paths.

        Runs inside the Panel/Tornado event loop (scheduled via add_callback).
        """
        import os
        ui = _app_state.get("ui")
        mgr = _app_state.get("mgr")
        if ui is None or mgr is None:
            return

        labels = [os.path.basename(p) for p in paths]
        header = pn.pane.Markdown(
            "### Add dropped file(s)\n\n"
            + "\n".join(f"- `{p}`" for p in paths),
            sizing_mode="stretch_width",
        )
        add_btn = pn.widgets.Button(
            name="Add", button_type="success", icon="folder-plus", width=120
        )
        cancel_btn = pn.widgets.Button(
            name="Cancel", button_type="light", width=100
        )
        status = pn.pane.Markdown("", sizing_mode="stretch_width")

        def _on_confirm(evt):  # noqa: ARG001
            add_btn.disabled = True
            cancel_btn.disabled = True
            status.object = "_Loading\u2026_"
            curdoc = pn.state.curdoc

            def _do_add():
                total_added: list = []
                errors: list = []
                for path in paths:
                    try:
                        added = mgr.add_source_files(path)
                        total_added.extend(added)
                    except Exception as exc:
                        errors.append(f"`{os.path.basename(path)}`: {exc}")

                def _done():
                    if total_added:
                        try:
                            from dvue.actions import TransformToCatalogAction
                            TransformToCatalogAction._refresh_table(ui, mgr)
                        except Exception as exc:
                            logger.warning(
                                "serve_desktop_app: table refresh failed: %s", exc
                            )
                        msg = (
                            f"**Added {len(total_added)} reference(s)** from "
                            f"{', '.join(f'`{l}`' for l in labels)}."
                        )
                    else:
                        msg = "_No new references were added._"
                    if errors:
                        msg += "\n\n**Errors:**\n" + "\n".join(
                            f"- {e}" for e in errors
                        )
                    status.object = msg

                curdoc.add_next_tick_callback(_done)

            import threading as _th
            _th.Thread(target=_do_add, daemon=True).start()

        def _on_cancel(evt):  # noqa: ARG001
            ui.show_in_display_panel("Cancelled", pn.pane.Markdown("_Drop cancelled._"))

        add_btn.on_click(_on_confirm)
        cancel_btn.on_click(_on_cancel)
        form = pn.Column(
            header,
            pn.Row(add_btn, cancel_btn),
            status,
            sizing_mode="stretch_width",
        )
        ui.show_in_display_panel("File Dropped", form)

    def _bind_drop_events(window):
        """Register document-level drag-and-drop handlers after the window loads."""
        try:
            from webview.dom import DOMEventHandler
            window.dom.document.events.dragenter += DOMEventHandler(
                _on_drag, True, True
            )
            window.dom.document.events.dragover += DOMEventHandler(
                _on_drag, True, True, debounce=200
            )
            window.dom.document.events.drop += DOMEventHandler(
                _on_file_drop, True, True
            )
        except Exception as _exc:  # pragma: no cover
            logger.warning("Could not bind drop events: %s", _exc)

    # --- Build the same session-aware make_app factory as serve_session_app ---
    install_session_handler(cookie_name=cookie_name)

    class _SuppressUnknownRef(logging.Filter):
        def filter(self, record):
            return "UnknownReferenceError" not in record.getMessage()

    logging.getLogger("bokeh.server.protocol_handler").addFilter(_SuppressUnknownRef())

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

    # Probe manager once for view-layer hints (crs, station_id_column) when
    # the caller did not provide them.  Caller-provided values always win.
    _effective_crs = crs
    _effective_sic = station_id_column
    if _effective_crs is None or _effective_sic is None:
        _probe = build_manager_fn()
        if _effective_crs is None:
            _effective_crs = getattr(_probe, "crs", None)
        if _effective_sic is None:
            _effective_sic = getattr(_probe, "station_id_column", None)
        del _probe

    _registry: dict = {}

    def make_app():
        from dvue.dataui import DataUI

        user_id = pn.state.cookies.get(cookie_name, "")

        if user_id and user_id in _registry:
            entry = _registry[user_id]
            mgr, ui, tmpl = entry["mgr"], entry["ui"], entry["template"]
            pn.state.onload(lambda: (ui.setup_location_sync(), ui.setup_url_sync()))
            tmpl.servable()
            return

        mgr = build_manager_fn()
        saved = _load_state(user_id) if user_id else {}
        if saved:
            restore(mgr, saved)

        dataui_kwargs: dict = {}
        if _effective_crs is not None:
            dataui_kwargs["crs"] = _effective_crs
        if _effective_sic is not None:
            dataui_kwargs["station_id_column"] = _effective_sic

        ui = DataUI(mgr, **dataui_kwargs)
        tmpl = ui.create_view(title=title)
        tmpl.servable()

        # Expose mgr/ui to the drop-event handler as soon as they exist.
        _app_state["mgr"] = mgr
        _app_state["ui"] = ui

        if user_id:
            _registry[user_id] = {"mgr": mgr, "ui": ui, "template": tmpl}

        sel = saved.get("selection", [])

        def _on_load():
            # Capture the running Tornado IOLoop so the pywebview GUI thread can
            # schedule confirmation callbacks via IOLoop.add_callback (thread-safe).
            _app_state["ioloop"] = _tornado_ioloop.IOLoop.current()

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

            if persist:
                def _do_save(event=None):
                    if user_id:
                        _save_state(user_id, snapshot(mgr, ui))

                if "time_range" in mgr.param:
                    mgr.param.watch(_do_save, "time_range")
                if hasattr(ui, "display_table"):
                    ui.display_table.param.watch(_do_save, "selection")

        pn.state.onload(_on_load)

    app_key = title.lower().replace(" ", "-")
    url = f"http://localhost:{port}/{app_key}"

    # Start Panel server in a background daemon thread (show=False).
    server_thread = threading.Thread(
        target=lambda: pn.serve(
            {app_key: make_app},
            port=port,
            show=False,
            unused_session_lifetime_milliseconds=2_592_000_000,
            **pn_serve_kwargs,
        ),
        daemon=True,
        name="panel-server",
    )
    server_thread.start()

    logger.info("Waiting for Panel server on port %d...", port)
    _wait_for_server(port, server_timeout)
    logger.info("Panel server ready — opening desktop window at %s", url)

    # Open the app in a native window, maximised to fill the screen.
    _win = webview.create_window(title, url, maximized=True)
    # Resolve the bundled dvue icon (falls back gracefully if missing).
    _icon_path = Path(__file__).parent / "assets" / "icon.ico"
    _icon = str(_icon_path) if _icon_path.is_file() else None
    # Pass _bind_drop_events so pywebview calls it once the window is ready,
    # registering document-level drag-and-drop handlers.
    webview.start(_bind_drop_events, _win, icon=_icon)  # blocks until window is closed


def make_reset_session_button(
    cookie_name: str = "dvue_user_id",
    label: str = "Reset Session",
    button_type: str = "warning",
    on_reset=None,
    **kwargs,
) -> "pn.widgets.Button":
    """Create a Panel button that resets the persistent user-session cookie.

    Clicking the button:

    1. Calls *on_reset* server-side (optional) — use this to remove stale
       registry or diskcache entries for the old user identity.
    2. Deletes the *cookie_name* cookie in the browser via JavaScript.
    3. Reloads the page so the server assigns a fresh UUID, starting a
       completely clean session with no saved state.

    Parameters
    ----------
    cookie_name : str
        Name of the cookie to clear.  Must match the *cookie_name* passed to
        :func:`install_session_handler` / :func:`serve_session_app`.
        Default ``"dvue_user_id"``.
    label : str
        Button label shown in the UI.  Default ``"Reset Session"``.
    button_type : str
        Panel button styling (``"default"``, ``"primary"``, ``"warning"``,
        ``"danger"``).  Default ``"warning"``.
    on_reset : callable or None
        Optional zero-argument callable invoked server-side when the button is
        clicked.  Use it to evict the current user's entry from an in-memory
        registry or a diskcache store before the browser reloads.

        .. note::
            The server-side callback and the browser-side reload run
            concurrently (Panel sends the Python event; JavaScript fires the
            cookie-deletion and ``window.location.reload()`` independently).
            The callback is best-effort: if the session closes before it
            completes the old entry will remain until the server restarts or
            the cache TTL expires.
    **kwargs
        Extra keyword arguments forwarded verbatim to ``pn.widgets.Button``.

    Returns
    -------
    pn.widgets.Button

    Examples
    --------
    Minimal usage — no server-side cleanup::

        reset_btn = make_reset_session_button()
        template.header.append(reset_btn)

    With registry cleanup::

        reset_btn = make_reset_session_button(
            on_reset=lambda: _APP_REGISTRY.pop(reg_key, None)
        )
    """
    btn = pn.widgets.Button(name=label, button_type=button_type, **kwargs)

    # Client-side: expire the cookie immediately then reload.
    btn.js_on_click(
        args={},
        code=(
            f"document.cookie = '{cookie_name}=; "
            f"expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';"
            f"window.location.reload();"
        ),
    )

    # Server-side: optional cleanup before the browser reloads.
    if on_reset is not None:
        btn.on_click(lambda event: on_reset())

    return btn


class SessionManager:
    """Two-layer Panel session persistence manager.

    Encapsulates the in-memory registry (Layer 1) and optional diskcache
    persistence (Layer 2) used by dvue session-aware apps.  Create one
    instance per server process and share it across all app factories.

    Parameters
    ----------
    cookie_name : str
        Name of the persistent user-identity cookie.  Must match the value
        passed to :func:`install_session_handler`.  Default ``"dvue_user_id"``.
    cache_dir : str or Path or None
        Directory for the diskcache store.  Only used when *persist* is
        ``True``.  Defaults to ``~/.dvue_sessions``.
    ttl : int
        Diskcache entry lifetime in seconds.  Default 30 days.
    persist : bool
        Enable Layer 2 disk persistence (survive server restarts).
        Default ``False``.

    Examples
    --------
    Typical setup in a server entry-point::

        from dvue.session_persistence import install_session_handler, SessionManager

        install_session_handler()
        _session_mgr = SessionManager(cache_dir=".session_cache", persist=True)

        def make_app():
            user_id = _session_mgr.current_user_id
            reg_key  = _session_mgr.make_reg_key(user_id, "myapp")
            entry    = _session_mgr.get_entry(reg_key)
            if entry:
                ...  # reuse
            else:
                mgr = build_manager()
                saved = _session_mgr.load_state(user_id)
                ...
                _session_mgr.set_entry(reg_key, {"mgr": mgr, ...})
                def _save(event=None):
                    _session_mgr.save_state(user_id, snapshot(mgr, ui))
    """

    def __init__(
        self,
        cookie_name: str = "dvue_user_id",
        cache_dir: "str | Path | None" = None,
        ttl: int = 30 * 24 * 3600,
        persist: bool = False,
    ) -> None:
        self.cookie_name = cookie_name
        self._ttl = ttl
        self._registry: dict = {}

        if persist:
            _cache_dir = Path(cache_dir) if cache_dir else Path.home() / ".dvue_sessions"
            _cache_dir.mkdir(parents=True, exist_ok=True)
            try:
                import diskcache as _dc
                self._session_cache = _dc.Cache(str(_cache_dir))
            except ImportError:
                logger.warning(
                    "dvue.session_persistence: diskcache not installed; "
                    "Layer 2 persistence disabled."
                )
                self._session_cache = None
        else:
            self._session_cache = None

    # ── Cookie helpers ────────────────────────────────────────────────────────

    @property
    def current_user_id(self) -> str:
        """Return the current user's UUID from the Panel session cookie."""
        return pn.state.cookies.get(self.cookie_name, "")

    def make_reg_key(self, user_id: str, suffix: str) -> str:
        """Return ``"{user_id}:{suffix}"`` or ``""`` when *user_id* is empty."""
        return f"{user_id}:{suffix}" if user_id else ""

    # ── Registry ──────────────────────────────────────────────────────────────

    def get_entry(self, reg_key: str) -> "dict | None":
        """Return the registry entry for *reg_key*, or ``None``."""
        return self._registry.get(reg_key) if reg_key else None

    def set_entry(self, reg_key: str, entry: dict) -> None:
        """Store *entry* in the registry under *reg_key*."""
        if reg_key:
            self._registry[reg_key] = entry

    def evict(self, reg_key: str) -> None:
        """Remove *reg_key* from the registry (no-op if absent)."""
        self._registry.pop(reg_key, None)

    # ── Diskcache ─────────────────────────────────────────────────────────────

    def load_state(self, user_id: str) -> dict:
        """Return the persisted state dict for *user_id*, or ``{}``."""
        if self._session_cache and user_id:
            return self._session_cache.get(user_id, default={})
        return {}

    def save_state(self, user_id: str, state: dict) -> None:
        """Persist *state* for *user_id* with the configured TTL."""
        if self._session_cache and user_id:
            self._session_cache.set(user_id, state, expire=self._ttl)

    # ── Reset button ──────────────────────────────────────────────────────────

    def make_reset_button(self, reg_key: str = "", **kwargs) -> "pn.widgets.Button":
        """Return a reset-session button bound to this manager.

        Clicking the button evicts *reg_key* from the in-memory registry,
        clears the user-identity cookie in the browser, and reloads the page.

        Parameters
        ----------
        reg_key : str
            Registry key to evict on click (e.g. ``"{user_id}:myapp"``).
            Pass ``""`` to skip server-side eviction.
        **kwargs
            Forwarded to :func:`make_reset_session_button`.
        """
        on_reset = (lambda: self.evict(reg_key)) if reg_key else None
        return make_reset_session_button(
            cookie_name=self.cookie_name,
            on_reset=on_reset,
            **kwargs,
        )
