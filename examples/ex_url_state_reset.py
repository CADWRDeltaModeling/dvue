"""
Minimal demonstration of Panel UI state reset on page reload.

Depends only on: panel, holoviews, numpy, pandas

Run with:
    panel serve examples/ex_url_state_reset.py --show --port 5007

Select rows, click Plot, then reload — the selection is restored but the plot is gone.
"""

import numpy as np
import pandas as pd
import panel as pn
import holoviews as hv

pn.extension("tabulator", sizing_mode="stretch_width")
hv.extension("bokeh")

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------
t = pd.date_range("2020-01-01", periods=365, freq="D")

SERIES = {
    "sine_fast": pd.Series(np.sin(2 * np.pi * t.dayofyear / 30),  index=t, name="value"),
    "sine_slow": pd.Series(np.sin(2 * np.pi * t.dayofyear / 180), index=t, name="value"),
    "cosine":    pd.Series(np.cos(2 * np.pi * t.dayofyear / 90),  index=t, name="value"),
    "trend":     pd.Series(np.linspace(-1, 1, len(t)),             index=t, name="value"),
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
# Widgets
# ---------------------------------------------------------------------------
table = pn.widgets.Tabulator(
    catalog_df,
    selectable=True,
    show_index=False,
    disabled=True,
    height=200,
)

plot_button = pn.widgets.Button(name="Plot", button_type="primary", width=100)

plot_pane = pn.pane.HoloViews(hv.Curve([]).opts(width=750, height=300, title="(press Plot)"))

session_id_pane = pn.pane.Markdown("_Session ID: (loading...)_")

# ---------------------------------------------------------------------------
# Plot callback (triggered only by the button)
# ---------------------------------------------------------------------------
def _on_plot(event):
    selection = table.selection
    if not selection:
        plot_pane.object = hv.Text(0.5, 0.5, "No rows selected").opts(width=750, height=300)
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

plot_button.on_click(_on_plot)

# ---------------------------------------------------------------------------
# URL sync setup
# ---------------------------------------------------------------------------
def _setup_url_sync():
    loc = pn.state.location
    if loc is None:
        return

    # Display session ID to prove each reload is a new session
    try:
        sid = pn.state.curdoc.session_context.id
    except Exception:
        sid = "(unavailable)"
    session_id_pane.object = f"**Session ID:** `{sid}`  _(changes on every reload)_"

    # Restore selection from ?sel=0,1
    raw = (loc.query_params or {}).get("sel", "")
    if raw:
        try:
            table.selection = [int(x) for x in raw.split(",") if x.strip().isdigit()]
        except Exception:
            pass

    # Write selection back to URL whenever it changes
    def _write_selection(event):
        sel = table.selection or []
        loc.update_query(sel=",".join(str(i) for i in sel) if sel else None)

    table.param.watch(_write_selection, "selection")


pn.state.onload(_setup_url_sync)

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
pn.Column(
    pn.pane.Markdown("""
## URL state reset demo

1. Select rows in the table and click **Plot**.
2. Note the `?sel=` param added to the URL.
3. Reload the page — selection is restored from the URL, but **the plot is gone**
   because Panel starts a new Python session on every reload and the button was
   never clicked in the new session.
"""),
    table,
    plot_button,
    session_id_pane,
    plot_pane,
).servable(title="URL State Reset Demo")
