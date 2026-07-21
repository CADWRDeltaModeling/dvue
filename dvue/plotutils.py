import pandas as pd
import numpy as np
import hvplot.pandas
import holoviews as hv
from holoviews import opts, dim


def cdf(df):
    """Return the cumulative distribution function of a dataframe."""
    df = df.apply(lambda x: x.sort_values().reset_index(drop=True), axis=0)
    df.index = np.linspace(0, 100, len(df))
    return df


# %%
def customize_legend(
    fig, legend_labels, legend_position="upper center", bbox_to_anchor=(0.5, 1.15)
):
    axes = fig.get_axes()[0]
    # put the legend outside the plot at the top centered
    axes.legend(
        legend_labels,
        loc=legend_position,
        bbox_to_anchor=bbox_to_anchor,
        ncols=len(legend_labels),
    )
    return fig


def rectangle_around_plot(fig, edgecolor="black", facecolor="none"):
    import matplotlib.patches as patches

    # fig.subplots_adjust(left=0.1, right=0.9, wspace=0.4, hspace=0.4)
    rect = patches.Rectangle(
        (0, 0.3),
        1,
        0.4,
        linewidth=1,
        edgecolor=edgecolor,
        facecolor=facecolor,
        transform=fig.transFigure,
        figure=fig,
    )

    # Add the patch to the figure
    fig.add_artist(rect)
    return fig


def exceedance_plot(
    df,
    ylabel,
    xlabel,
    line_styles=[
        (0, (5, 1, 2, 1)),
        "-",
        ":",
        "--",
        "-.",
        ":",
    ],
    legend_position="upper center",
):
    edf = cdf(df)
    line_plot = edf.hvplot.line(
        linestyle=line_styles,
        ylabel=ylabel,
        xlabel=xlabel,
        grid=True,
        legend="top",
    )
    line_plot.opts(fig_inches=5, show_frame=True).opts(opts.Layout(tight=True)).opts(
        backend_opts={
            "legend.frame_on": False,
        }
    )
    fig = hvplot.render(line_plot, backend="matplotlib")
    fig = customize_legend(fig, df.columns, legend_position=legend_position)
    fig = rectangle_around_plot(fig)
    return fig


def save_figure(fig, filename):
    fig.savefig(filename, dpi=300, bbox_inches="tight")


# ---------------------------------------------------------------------------
# PSU reference-line utilities for EC (µS/cm) plots
# ---------------------------------------------------------------------------

import math as _math


def _nice_psu_levels(lo_psu: float, hi_psu: float, max_lines: int = 7) -> list:
    """Return PSU values at 'nice' intervals within [lo_psu, hi_psu].

    Uses standard nice-number algorithm: step is 1, 2, or 5 × 10ⁿ.
    """
    span = hi_psu - lo_psu
    if span <= 0:
        return []
    raw_step = span / max_lines
    magnitude = 10 ** _math.floor(_math.log10(raw_step)) if raw_step > 0 else 1
    step = magnitude * 10  # fallback
    for mult in (1, 2, 5, 10):
        candidate = mult * magnitude
        if span / candidate <= max_lines:
            step = candidate
            break
    decimals = max(0, -int(_math.floor(_math.log10(step)))) if step < 1 else 0
    start_idx = _math.ceil(lo_psu / step)
    levels = []
    idx = start_idx
    while True:
        val = round(idx * step, decimals + 1)
        if val > hi_psu + step * 1e-6:
            break
        if val >= 0:
            levels.append(val)
        idx += 1
    return levels


def make_psu_reference_lines_hook(lo_ec: float, hi_ec: float,
                                   ec_unit_scale: float = 1.0):
    """Return a Bokeh hook that draws PSU isopleths on an EC plot.

    Horizontal dotted lines are positioned at the exact EC values that
    correspond to round PSU levels.  The conversion uses the full
    non-linear PSP-78 / Hill-corrected ``ec_psu_25c`` formula from vtools,
    so tick positions are physically correct.

    Parameters
    ----------
    lo_ec, hi_ec : float
        Y-axis range in the displayed EC unit.
    ec_unit_scale : float
        Multiply axis values by this factor to convert to µS/cm before
        the PSU conversion.  Default ``1.0`` when the axis IS already in
        µS/cm.  Pass ``1000.0`` for axes in mS/cm (e.g. NOAA conductivity).

    Returns
    -------
    callable
        A Bokeh plot hook ``hook(plot, element)``.
    """
    def hook(plot, element, _lo=lo_ec, _hi=hi_ec, _scale=ec_unit_scale):
        try:
            from bokeh.models import Span, Label
            from vtools.functions.unit_conversions import ec_psu_25c, psu_ec_25c
        except ImportError:
            return
        fig = plot.handles.get("plot")
        if fig is None or _lo is None or _hi is None or _lo >= _hi:
            return

        lo_psu = float(ec_psu_25c(max(_lo * _scale, 0.0)))
        hi_psu = float(ec_psu_25c(max(_hi * _scale, 0.0)))
        psu_levels = _nice_psu_levels(lo_psu, hi_psu)

        for psu in psu_levels:
            try:
                ec_val = float(psu_ec_25c(float(psu))) / _scale
            except Exception:
                continue
            if not (_lo <= ec_val <= _hi):
                continue
            label_text = f"{int(psu)} PSU" if psu == int(psu) else f"{psu:.1f} PSU"
            fig.add_layout(Span(
                location=ec_val,
                dimension="width",
                line_color="#888888",
                line_dash="dotted",
                line_width=1,
                line_alpha=0.6,
            ))
            fig.add_layout(Label(
                x=6,
                y=ec_val,
                text=label_text,
                text_font_size="9px",
                text_color="#555555",
                x_units="screen",
                y_units="data",
                text_align="left",
                text_baseline="bottom",
            ))

    return hook


def make_psu_dual_axis_hook(lo_ec: float, hi_ec: float,
                             ec_unit_scale: float = 1.0):
    """Return a Bokeh hook that adds a PSU reference axis to an EC plot.

    A right-side ``LinearAxis`` is added whose tick marks sit at the exact
    EC axis positions that correspond to round PSU levels (computed via the
    full non-linear PSP-78 / Hill-corrected ``ec_psu_25c`` formula).  The
    axis shares the primary ``y_range`` so every tick is physically aligned:
    the EC value on the left axis and its PSU equivalent on the right sit at
    the same height.

    Parameters
    ----------
    lo_ec, hi_ec : float
        Y-axis range in the displayed EC unit.
    ec_unit_scale : float
        Multiply axis values by this to convert to µS/cm before the PSU
        conversion.  Default ``1.0`` (axis is already in µS/cm).
        Pass ``1000.0`` for mS/cm axes (NOAA conductivity).

    Returns
    -------
    callable
        A Bokeh plot hook ``hook(plot, element)``.
    """
    def hook(plot, element, _lo=lo_ec, _hi=hi_ec, _scale=ec_unit_scale):
        try:
            from bokeh.models import LinearAxis, FixedTicker, CustomJSTickFormatter
            from vtools.functions.unit_conversions import ec_psu_25c, psu_ec_25c
        except ImportError:
            return
        fig = plot.handles.get("plot")
        if fig is None or _lo is None or _hi is None or _lo >= _hi:
            return

        lo_psu = float(ec_psu_25c(max(_lo * _scale, 0.0)))
        hi_psu = float(ec_psu_25c(max(_hi * _scale, 0.0)))
        psu_levels = _nice_psu_levels(lo_psu, hi_psu)
        if not psu_levels:
            return

        ec_ticks, psu_labels = [], []
        for psu in psu_levels:
            try:
                ec_val = float(psu_ec_25c(float(psu))) / _scale
            except Exception:
                continue
            if _lo <= ec_val <= _hi:
                ec_ticks.append(ec_val)
                psu_labels.append(
                    str(int(psu)) if psu == int(psu) else f"{psu:.1f}"
                )
        if not ec_ticks:
            return

        # CustomJSTickFormatter: the tick *position* is an EC value; return
        # the pre-computed PSU label for that position.  We use a tolerance
        # check rather than exact equality to guard against any floating-point
        # rounding that Bokeh may apply to tick values.
        formatter = CustomJSTickFormatter(
            args=dict(ec_ticks=ec_ticks, psu_labels=psu_labels),
            code="""
            var best = 0, min_diff = Infinity;
            for (var i = 0; i < ec_ticks.length; i++) {
                var d = Math.abs(tick - ec_ticks[i]);
                if (d < min_diff) { min_diff = d; best = i; }
            }
            return psu_labels[best];
            """,
        )
        fig.add_layout(
            LinearAxis(
                y_range_name="default",
                axis_label="PSU",
                ticker=FixedTicker(ticks=ec_ticks),
                formatter=formatter,
            ),
            "right",
        )

    return hook
