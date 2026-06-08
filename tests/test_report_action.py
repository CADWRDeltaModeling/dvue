"""Tests for ReportAction in dvue.actions.

These tests cover:
- ReportAction base class raises NotImplementedError on generate()
- get_tab_label returns "R{n}" prefix
- callback() with no row selection still calls generate() (no abort)
- callback() passes the full catalog (dataui._dfcat) to generate()
- Result is appended as a new tab in the display panel
"""

import asyncio
import threading
import pandas as pd
import panel as pn
import pytest

from dvue.actions import ReportAction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_catalog_df(n=4):
    """Return a small fake catalog DataFrame."""
    variables = ["flow", "stage", "precipitation", "salinity"]
    units      = ["cfs",  "ft",    "mm",            "ppt"]
    return pd.DataFrame({
        "name":       [f"ref_{i}" for i in range(n)],
        "station_id": [f"STA{i}" for i in range(n)],
        "variable":   [variables[i % len(variables)] for i in range(n)],
        "unit":       [units[i % len(units)]          for i in range(n)],
    })


class _FakeTabs(pn.Tabs):
    """Panel Tabs subclass that records appended items."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._appended = []

    def append(self, item):
        self._appended.append(item)
        super().append(item)


class _FakeDisplayPanel:
    """Minimal stand-in for dataui._display_panel."""

    def __init__(self):
        self.loading = False
        self.objects = []


class _FakeDataUI:
    """Minimal stand-in for a DataUI instance used by action callbacks."""

    def __init__(self, catalog_df=None):
        self._dfcat = catalog_df if catalog_df is not None else _make_catalog_df()
        self._display_panel = _FakeDisplayPanel()
        self._tab_count = -1
        self._dataui_manager = None
        # Mimic an empty table selection (no rows selected).
        class _Table:
            selection = []
        self.display_table = _Table()

    def set_progress(self, value, status=None):
        pass

    def hide_progress(self):
        pass


# ---------------------------------------------------------------------------
# Synchronous helpers to run callback() inline (avoids real thread waiting)
# ---------------------------------------------------------------------------


def _run_callback_synchronously(action, dataui, monkeypatch):
    """Invoke action.callback() and wait for the worker thread to finish.

    Monkeypatches:
    - ``pn.state.curdoc``: replaced with a fake doc whose
      ``add_next_tick_callback`` calls the callback immediately.
    - ``asyncio.create_task``: replaced with a no-op so the async progress
      cleanup doesn't raise "no event loop" in tests.
    """

    class _FakeDoc:
        def add_next_tick_callback(self, fn):
            fn()

    monkeypatch.setattr(pn.state, "curdoc", _FakeDoc(), raising=False)

    # Replace create_task with a no-op; close the coroutine to avoid the
    # "coroutine was never awaited" RuntimeWarning.
    def _noop_create_task(coro):
        coro.close()
        return None

    monkeypatch.setattr(asyncio, "create_task", _noop_create_task, raising=False)

    # Track threads spawned by callback.
    spawned = []
    _orig_start = threading.Thread.start

    def _patched_start(self, *args, **kwargs):
        spawned.append(self)
        _orig_start(self, *args, **kwargs)

    monkeypatch.setattr(threading.Thread, "start", _patched_start)

    action.callback(None, dataui)

    # Wait for every spawned thread to finish.
    for t in spawned:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in spawned), "Worker thread(s) did not finish in time"


# ---------------------------------------------------------------------------
# Tests: base class contract
# ---------------------------------------------------------------------------


class TestReportActionBase:
    def test_generate_raises_not_implemented(self):
        """Unsubclassed ReportAction.generate() must raise NotImplementedError."""
        action = ReportAction()
        with pytest.raises(NotImplementedError):
            action.generate(pd.DataFrame(), manager=None)

    def test_tab_label_prefix(self):
        action = ReportAction()
        assert action.get_tab_label(0) == "R0"
        assert action.get_tab_label(3) == "R3"
        assert action.get_tab_label(10) == "R10"

    def test_tab_label_override(self):
        class MyReport(ReportAction):
            def get_tab_label(self, n):
                return f"Coverage R{n}"
            def generate(self, catalog_df, manager):
                return pn.pane.Markdown("")

        action = MyReport()
        assert action.get_tab_label(0) == "Coverage R0"


# ---------------------------------------------------------------------------
# Tests: callback() behaviour
# ---------------------------------------------------------------------------


class _MarkdownReport(ReportAction):
    """Concrete subclass that records what catalog_df was passed."""

    def __init__(self):
        super().__init__()
        self.received_catalog_df = None
        self.received_manager = None

    def generate(self, catalog_df, manager):
        self.received_catalog_df = catalog_df.copy()
        self.received_manager = manager
        return pn.pane.Markdown("## Report")


class TestReportActionCallback:
    def test_callback_with_no_selection_still_runs(self, monkeypatch):
        """callback() must call generate() even when display_table.selection is empty."""
        action = _MarkdownReport()
        dataui = _FakeDataUI()
        dataui.display_table.selection = []  # no rows selected

        _run_callback_synchronously(action, dataui, monkeypatch)

        assert action.received_catalog_df is not None, "generate() was never called"

    def test_callback_passes_full_catalog(self, monkeypatch):
        """generate() receives the full _dfcat, not a selection subset."""
        catalog_df = _make_catalog_df(n=6)
        action = _MarkdownReport()
        dataui = _FakeDataUI(catalog_df=catalog_df)
        dataui.display_table.selection = [0, 1]  # only 2 rows selected — irrelevant

        _run_callback_synchronously(action, dataui, monkeypatch)

        assert len(action.received_catalog_df) == 6, (
            "generate() should receive all 6 catalog rows, not just the 2 selected"
        )

    def test_callback_creates_tab_in_display_panel(self, monkeypatch):
        """The result of generate() must appear as a tab in _display_panel."""
        action = _MarkdownReport()
        dataui = _FakeDataUI()

        _run_callback_synchronously(action, dataui, monkeypatch)

        assert len(dataui._display_panel.objects) == 1
        assert isinstance(dataui._display_panel.objects[0], pn.Tabs)

    def test_callback_tab_label_uses_prefix(self, monkeypatch):
        """First tab added by ReportAction should carry the 'R0' label."""
        action = _MarkdownReport()
        dataui = _FakeDataUI()

        _run_callback_synchronously(action, dataui, monkeypatch)

        tabs = dataui._display_panel.objects[0]
        # Panel stores tab labels in _names; indexing tabs[i] yields the content object.
        assert tabs._names[0] == "R0", f"Expected tab label 'R0', got {tabs._names[0]!r}"

    def test_callback_appends_second_tab(self, monkeypatch):
        """Calling callback() twice appends a second tab rather than replacing."""
        action = _MarkdownReport()
        dataui = _FakeDataUI()

        _run_callback_synchronously(action, dataui, monkeypatch)
        _run_callback_synchronously(action, dataui, monkeypatch)

        tabs = dataui._display_panel.objects[0]
        assert len(tabs) == 2

    def test_callback_error_shows_error_pane(self, monkeypatch):
        """If generate() raises, the display panel shows an error pane (not a crash)."""
        class _BrokenReport(ReportAction):
            def generate(self, catalog_df, manager):
                raise ValueError("simulated error")

        action = _BrokenReport()
        dataui = _FakeDataUI()

        _run_callback_synchronously(action, dataui, monkeypatch)

        # Display panel should show a Markdown error pane, not be empty.
        assert len(dataui._display_panel.objects) == 1
        obj = dataui._display_panel.objects[0]
        assert isinstance(obj, pn.pane.Markdown)
        assert "Error" in obj.object or "error" in obj.object
