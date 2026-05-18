"""
Desktop wrapper for ex_tsdataui.py using pywebview.

Launches the existing ``ex_tsdataui`` Panel application inside a native
desktop window instead of a browser tab.  Everything in ``ex_tsdataui.py``
(catalog, classes, session persistence) is reused by importing that module.

Requirements
------------
    pip install pywebview

Usage
-----
    # From the dvue project root:
    python examples/ex_tsdataui_desktop.py

Do NOT use ``panel serve`` — the ``install_session_handler()`` call in
ex_tsdataui.py must execute before ``BokehServer.__init__()``.  Importing
ex_tsdataui here satisfies that constraint because the module-level code
(including ``install_session_handler()``) runs at import time, well before
``pn.serve()`` is called.

New canonical pattern
---------------------
For apps that use :func:`dvue.session_persistence.serve_session_app` (which
is the standard pattern for all dsm2ui managers), you can replace it with
:func:`dvue.session_persistence.serve_desktop_app` directly::

    from dvue.session_persistence import serve_desktop_app

    def build_manager():
        return MyTimeSeriesDataUIManager(*files)

    serve_desktop_app(build_manager, title="My App")

The ``dsm2ui.session`` module re-exports ``serve_desktop_app`` with dsm2ui
defaults, and all ``show_*_ui`` commands accept ``--desktop`` / ``desktop=True``
to open in a native window::

    from dsm2ui.dsm2ui import show_dsm2_output_ui
    show_dsm2_output_ui(echo_files, desktop=True)

    # or via CLI:
    #   dsm2ui output-ui myfile.inp --desktop

This example file predates ``serve_desktop_app`` and keeps its own manual
port/thread/webview setup to illustrate the underlying mechanics.
"""

import socket
import sys
import threading
import time
from pathlib import Path

# Ensure the examples/ directory is importable when running from the project
# root or from within the examples/ directory itself.
_examples_dir = str(Path(__file__).parent)
if _examples_dir not in sys.path:
    sys.path.insert(0, _examples_dir)

try:
    import webview
except ImportError as exc:
    raise ImportError(
        "pywebview is required for desktop mode.  Install it with:\n"
        "    pip install pywebview"
    ) from exc

# ---------------------------------------------------------------------------
# Import the Panel app factory from ex_tsdataui.
#
# Side-effects at import time (all desired):
#   1. install_session_handler() patches Bokeh's per_app_patterns.
#   2. DataCatalog is built and populated with synthetic station data.
#   3. ExampleTimeSeriesDataUIManager and ExampleTimeSeriesPlotAction are
#      defined.
#   4. SessionManager is created.
# ---------------------------------------------------------------------------
import ex_tsdataui  # noqa: E402  (must follow sys.path manipulation above)
from ex_tsdataui import make_app  # noqa: E402

import panel as pn  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_free_port() -> int:
    """Return an available TCP port assigned by the OS."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 15.0) -> None:
    """Block until a TCP listener is ready on localhost:*port*.

    Parameters
    ----------
    port:
        The port number to poll.
    timeout:
        Maximum seconds to wait before raising ``TimeoutError``.

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
            {"tsdataui": make_app},
            port=port,
            show=False,
            unused_session_lifetime_milliseconds=2_592_000_000,
        ),
        daemon=True,
        name="panel-server",
    )
    server_thread.start()

    print(f"Waiting for Panel server on port {port}...")
    wait_for_server(port)
    print(f"Panel server ready — opening desktop window at http://localhost:{port}/tsdataui")

    window = webview.create_window(
        "dvue — TimeSeries Data UI",
        f"http://localhost:{port}/tsdataui",
        width=1400,
        height=900,
        min_size=(800, 600),
    )
    webview.start()
    # webview.start() blocks until the window is closed.
    # The daemon server_thread is killed automatically when the main thread exits.
