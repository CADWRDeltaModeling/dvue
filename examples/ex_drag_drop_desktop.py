"""
Drag-and-drop file preview demo using pywebview + Panel.

Demonstrates how pywebview exposes real OS file paths when a user drops
files onto a Panel application hosted in a native desktop window.  Dropped
files are read as DataFrames and displayed as an interactive table preview.

This is *not* possible in a regular browser: the HTML5 File API deliberately
hides OS paths for security.  pywebview injects ``pywebviewFullPath`` on each
dropped ``File`` object, making the full disk path available in Python.

Architecture
------------
::

    webview thread          panel/tornado thread
    ──────────────          ──────────────────────
    on_drop(e)              drain_queue()   ← periodic callback (500 ms)
      └─ queue.put(path)      └─ load_file(path)
                                  └─ updates status_pane / preview_pane

The drop handler and the Panel session run on different threads.  Panel's
Tornado event loop is not thread-safe, so paths are transferred via a
``queue.Queue`` and consumed by a periodic callback that executes safely
inside the Bokeh document context.

Requirements
------------
    pip install pywebview pandas openpyxl  # openpyxl for .xlsx support

Usage
-----
    python examples/ex_drag_drop_desktop.py
"""

import queue
import socket
import threading
import time
from pathlib import Path

try:
    import webview
    from webview.dom import DOMEventHandler
except ImportError as exc:
    raise ImportError(
        "pywebview is required.  Install it with:\n    pip install pywebview"
    ) from exc

import pandas as pd
import panel as pn


# ---------------------------------------------------------------------------
# Thread-safe channel: webview drop handler → Panel periodic callback
# ---------------------------------------------------------------------------

_dropped_queue: "queue.Queue[str]" = queue.Queue()


# ---------------------------------------------------------------------------
# pywebview DOM event handlers
# ---------------------------------------------------------------------------

def _on_drag(e) -> None:  # noqa: ANN001
    """No-op; registered for dragenter/dragover to enable the drop target."""


def on_drop(e) -> None:  # noqa: ANN001
    """Receive a ``drop`` DOM event from pywebview and queue each file path.

    pywebview enhances the standard ``drop`` event by injecting a
    ``pywebviewFullPath`` property on every ``File`` object in
    ``e['dataTransfer']['files']``.  This value is the absolute OS path —
    not available when running in a regular browser.
    """
    files = e.get("dataTransfer", {}).get("files", [])
    for file in files:
        path = file.get("pywebviewFullPath")
        if path:
            _dropped_queue.put(path)


def bind(window) -> None:  # noqa: ANN001
    """Register drag-and-drop DOM event handlers once the window has loaded."""
    window.dom.document.events.dragenter += DOMEventHandler(_on_drag, True, True)
    window.dom.document.events.dragstart += DOMEventHandler(_on_drag, True, True)
    # debounce=200 prevents a flood of dragover events while dragging.
    window.dom.document.events.dragover += DOMEventHandler(
        _on_drag, True, True, debounce=200
    )
    window.dom.document.events.drop += DOMEventHandler(on_drop, True, True)


# ---------------------------------------------------------------------------
# Panel app factory
# ---------------------------------------------------------------------------

_SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt", ".xlsx", ".xls", ".parquet"}
_MAX_PREVIEW_ROWS = 200


def make_app():
    """Create one Panel app instance per Bokeh session."""
    # -- Status bar -----------------------------------------------------------
    status_pane = pn.pane.Str(
        "No files dropped yet — drag a tabular file anywhere onto this window.",
        styles={"font-size": "0.9em", "color": "#555", "padding": "4px 0"},
    )

    # -- DataFrame preview table ----------------------------------------------
    preview_pane = pn.widgets.Tabulator(
        pd.DataFrame(),
        height=420,
        sizing_mode="stretch_width",
        show_index=True,
        theme="simple",
    )

    # -- Visual drop-zone cue (purely cosmetic — webview intercepts at         #    document level, so any region of the window works as a drop target) ---
    drop_zone = pn.pane.HTML(
        f"""
        <div style="
            border: 3px dashed #90caf9;
            border-radius: 8px;
            padding: 28px 32px;
            text-align: center;
            color: #1e88e5;
            font-size: 1.15em;
            background: #e3f2fd;
            user-select: none;
            margin-bottom: 8px;
        ">
            &#x1F4C2;&nbsp; Drop files anywhere in this window
            <br>
            <small style="color: #555; font-size: 0.72em;">
                Supported: {", ".join(sorted(_SUPPORTED_EXTENSIONS))}
                &nbsp;|&nbsp; First {_MAX_PREVIEW_ROWS} rows shown
                &nbsp;|&nbsp; Real OS paths via pywebview — not available in browsers
            </small>
        </div>
        """,
        sizing_mode="stretch_width",
    )

    # -- File loader ----------------------------------------------------------
    def load_file(path: str) -> None:
        """Read *path* as a tabular file and update the preview pane."""
        p = Path(path)
        status_pane.object = f"\u23F3  Loading: {path}"
        try:
            suffix = p.suffix.lower()
            if suffix in {".csv", ".txt"}:
                df = pd.read_csv(path, nrows=_MAX_PREVIEW_ROWS)
            elif suffix == ".tsv":
                df = pd.read_csv(path, sep="\t", nrows=_MAX_PREVIEW_ROWS)
            elif suffix in {".xlsx", ".xls"}:
                df = pd.read_excel(path, nrows=_MAX_PREVIEW_ROWS)
            elif suffix == ".parquet":
                df = pd.read_parquet(path).head(_MAX_PREVIEW_ROWS)
            else:
                # Attempt CSV as a best-effort fallback for unknown extensions.
                df = pd.read_csv(path, nrows=_MAX_PREVIEW_ROWS)

            rows_loaded = len(df)
            note = f"(first {rows_loaded} rows)" if rows_loaded == _MAX_PREVIEW_ROWS else f"({rows_loaded} rows)"
            status_pane.object = (
                f"\u2705  {p.name}  \u2014  {rows_loaded} \xd7 {len(df.columns)} columns  {note}"
                f"\n     Full path: {path}"
            )
            preview_pane.value = df

        except Exception as exc:  # noqa: BLE001
            status_pane.object = f"\u274C  Error loading \u2018{p.name}\u2019: {exc}"
            preview_pane.value = pd.DataFrame()

    # -- Periodic callback: drain cross-thread queue --------------------------
    def drain_queue() -> None:
        """Consume all queued file paths and load the most recently dropped one."""
        path = None
        while not _dropped_queue.empty():
            try:
                path = _dropped_queue.get_nowait()
            except queue.Empty:
                break
        # Load only the last path so rapid multi-drop doesn't re-render N times.
        if path is not None:
            load_file(path)

    def on_load() -> None:
        pn.state.add_periodic_callback(drain_queue, period=500)

    pn.state.onload(on_load)

    return pn.Column(
        pn.pane.Markdown("## \U0001F4E5  Drag & Drop — File Preview"),
        drop_zone,
        status_pane,
        pn.layout.Divider(),
        preview_pane,
        sizing_mode="stretch_width",
        margin=(12, 16),
    )


# ---------------------------------------------------------------------------
# Server and window helpers
# ---------------------------------------------------------------------------

def find_free_port() -> int:
    """Return an available TCP port assigned by the OS."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 15.0) -> None:
    """Block until a TCP listener is ready on localhost:*port*.

    Raises
    ------
    TimeoutError
        If the server does not respond within *timeout* seconds.
    """
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = find_free_port()

    server_thread = threading.Thread(
        target=lambda: pn.serve(
            {"drag_drop": make_app},
            port=port,
            show=False,
        ),
        daemon=True,
        name="panel-server",
    )
    server_thread.start()

    print(f"Waiting for Panel server on port {port}...")
    wait_for_server(port)
    print(f"Panel server ready — opening desktop window at http://localhost:{port}/drag_drop")

    window = webview.create_window(
        "Drag & Drop — File Preview",
        f"http://localhost:{port}/drag_drop",
        width=1050,
        height=780,
        min_size=(600, 450),
    )
    webview.start(bind, window)
    # webview.start() blocks until the window is closed.
    # The daemon server_thread is killed automatically when the main thread exits.
