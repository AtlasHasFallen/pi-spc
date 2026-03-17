"""SPC visualization functions built on Altair (and optionally Plotly).

Provides Pareto charts, Xbar/S control charts, and I-MR charts using Altair
for static/interactive browser rendering.  The :func:`batch_timeline` function
requires Plotly, which is available via the ``pi-spc[plotly]`` extra.
"""

from __future__ import annotations

import altair as alt
import polars as pl

try:
    import plotly.express as px
    import plotly.graph_objects as go
    _HAS_PLOTLY = True
except ImportError:
    _HAS_PLOTLY = False

# SPC constants for individuals charts (d2=1.128 for n=2)
IMR_SIGMA_MULTIPLIER = 2.66      # UCL_I = Xbar ± 2.66 * MR_bar
MR_UCL_MULTIPLIER = 3.267        # UCL_MR = 3.267 * MR_bar  (D4 for n=2)
S_UCL_MULTIPLIER = 2.266         # UCL_S = 2.266 * S_bar

__all__ = [
    "pareto_chart",
    "batch_timeline",
    "xbar_s_chart",
    "imr_chart",
    "assign_stem_levels",
    "IMR_SIGMA_MULTIPLIER",
    "MR_UCL_MULTIPLIER",
    "S_UCL_MULTIPLIER",
]


def assign_stem_levels(
    midpoints: list,
    range_fraction: float = 0.12,
    *,
    initial_levels: int = 4,
    max_levels: int = 16,
    outer_bound: float = 4.5,
    inner_bound: float = 0.6,
) -> list[float]:
    """Assign vertical stem levels for a lollipop/timeline chart.

    Greedily places each point above or below a centerline, alternating
    sides and growing the number of available levels when all slots are
    occupied by close neighbours — up to a hard cap.

    Args:
        midpoints: Sequence of datetime-like objects (must support
            ``.timestamp()``).  One per event to place.
        range_fraction: Fraction of the total time range that defines the
            "collision zone" around each placed point.
        initial_levels: Starting number of levels per side (above/below).
        max_levels: Hard cap on levels per side.
        outer_bound: Outermost stem distance from centerline.
        inner_bound: Closest stem distance to centerline.

    Returns:
        List of float stem levels (positive = above, negative = below),
        one per element in *midpoints*.
    """
    if len(midpoints) < 2:
        return [2.0] * len(midpoints)

    _ts = [mp.timestamp() for mp in midpoints]
    _gap_s = (max(_ts) - min(_ts)) * range_fraction

    def _make_slots(n):
        step = (outer_bound - inner_bound) / max(n - 1, 1) if n > 1 else 0
        return [round(outer_bound - step * k, 2) for k in range(n)]

    _pos = _make_slots(initial_levels)
    _neg = [-v for v in _pos]

    _levels, _history = [], []
    for i, ts in enumerate(_ts):
        _occupied = {lvl for _prev_ts, lvl in _history if abs(ts - _prev_ts) < _gap_s}
        _pool = _pos + _neg if i % 2 == 0 else _neg + _pos
        _chosen = next((l for l in _pool if l not in _occupied), None)

        if _chosen is None and len(_pos) < max_levels:
            _pos = _make_slots(len(_pos) + 1)
            _neg = [-v for v in _pos]
            _pool = _pos + _neg if i % 2 == 0 else _neg + _pos
            _chosen = next((l for l in _pool if l not in _occupied), None)

        if _chosen is None:
            _last_used = {}
            for _prev_ts, lvl in _history:
                if lvl not in _last_used or _prev_ts > _last_used[lvl]:
                    _last_used[lvl] = _prev_ts
            _chosen = min(_pool, key=lambda l: _last_used.get(l, 0))

        _levels.append(_chosen)
        _history.append((ts, _chosen))
    return _levels


def pareto_chart(
    df: pl.DataFrame,
    category: str = "category",
    count: str = "count",
    color: str | None = None,
    color_map: dict | None = None,
    title: str = "Pareto Chart",
    width: int | str = "container",
    height: int = 400,
    total_count: int | None = None,
) -> alt.LayerChart:
    """Create a Pareto chart (bars + cumulative % line) from a Polars DataFrame.

    Args:
        df: DataFrame with at least a category column and a count column.
        category: Name of the column containing category labels.
        count: Name of the column containing counts.
        color: Optional column name to color bars by (e.g. "equipment").
        color_map: Optional mapping {value: hex} to force colors for the color
            dimension so it matches other charts (e.g. the timeline).
        title: Chart title.
        width: Chart width in pixels, or "container" to fill available width.
        height: Chart height in pixels.
        total_count: Optional total count for cumulative % denominator. If None,
            uses the sum of the count column (default behavior).

    Returns:
        alt.LayerChart with bars sorted descending and a cumulative percentage line.
    """
    # Cumulative % is always based on per-category totals (one row per category)
    cat_totals = (
        df.group_by(category)
        .agg(pl.col(count).sum())
        .sort(count, descending=True)
    )
    total = total_count if total_count is not None else cat_totals[count].sum()
    cat_totals = cat_totals.with_columns(
        (pl.col(count).cum_sum() / total * 100).alias("cumulative_pct"),
    )
    cat_order = cat_totals[category].to_list()

    # Bars — optionally split by color dimension
    if color:
        bar_df = (
            df.group_by([category, color])
            .agg(pl.col(count).sum())
        ).to_pandas()

        if color_map:
            domain = list(color_map.keys())
            range_colors = list(color_map.values())
            color_enc = alt.Color(f"{color}:N", scale=alt.Scale(domain=domain, range=range_colors))
        else:
            color_enc = alt.Color(f"{color}:N")

        bar_chart = (
            alt.Chart(bar_df)
            .mark_bar()
            .encode(
                x=alt.X(
                    f"{category}:N", sort=cat_order, title=None,
                    axis=alt.Axis(labelAngle=-45),
                ),
                y=alt.Y(f"{count}:Q", title="Count"),
                color=color_enc,
                tooltip=[
                    alt.Tooltip(f"{category}:N", title="Category"),
                    alt.Tooltip(f"{color}:N", title=color),
                    alt.Tooltip(f"{count}:Q", title="Count"),
                ],
            )
        )
    else:
        bar_chart = (
            alt.Chart(cat_totals.to_pandas())
            .mark_bar(color="#4c78a8")
            .encode(
                x=alt.X(
                    f"{category}:N", sort=cat_order, title=None,
                    axis=alt.Axis(labelAngle=-45),
                ),
                y=alt.Y(f"{count}:Q", title="Count"),
                tooltip=[
                    alt.Tooltip(f"{category}:N", title="Category"),
                    alt.Tooltip(f"{count}:Q", title="Count"),
                    alt.Tooltip("cumulative_pct:Q", title="Cumulative %", format=".1f"),
                ],
            )
        )

    line = (
        alt.Chart(cat_totals.to_pandas())
        .mark_line(color="#e45756", strokeWidth=2, point=True)
        .encode(
            x=alt.X(f"{category}:N", sort=cat_order, title=None),
            y=alt.Y(
                "cumulative_pct:Q", title="Cumulative %",
                scale=alt.Scale(domain=[0, 100]),
            ),
            tooltip=[
                alt.Tooltip(f"{category}:N", title="Category"),
                alt.Tooltip("cumulative_pct:Q", title="Cumulative %", format=".1f"),
            ],
        )
    )

    chart = alt.layer(bar_chart, line).resolve_scale(y="independent").properties(
        title=title,
        width=width,
        height=height,
    )

    return chart


def batch_timeline(
    df: pl.DataFrame,
    start: str = "Start",
    end: str = "End",
    label: str = "Label",
    category: str = "Category",
    color_by: str | None = None,
    title: str = "Batch Timeline",
    height: int | None = None,
    color_map: dict[str, str] | None = None,
    highlight_labels: list[str] | None = None,
) -> go.Figure:
    """Create an interactive Gantt-style timeline from a Polars DataFrame.

    Each row becomes a horizontal bar spanning from *start* to *end*.  Rows
    are bucketed on the y-axis by *category* (e.g. "Downtime",
    "Alarm (stops equipment)"), while *label* appears in the hover tooltip so
    the user can identify individual items without cluttering the axis.

    Args:
        df: DataFrame with start/end datetime columns, a label column
            (shown on hover), and a category column (y-axis grouping).
        start: Column name for interval start (datetime).
        end: Column name for interval end (datetime).
        label: Column name shown in hover tooltip (e.g. alarm description).
        category: Column name for y-axis grouping.
        color_by: Optional column name to color bars by (e.g. equipment).
        title: Chart title.
        height: Chart height in pixels.  ``None`` auto-sizes based on
            the number of unique categories.
        color_map: Optional ``{value: hex_color}`` override for the color_by
            dimension.
        highlight_labels: Optional list of label values to highlight.
            When set, matching bars keep full opacity and others are dimmed.

    Returns:
        plotly ``Figure`` with interactive zoom/pan.
    """
    if not _HAS_PLOTLY:
        raise ImportError(
            "plotly is required for batch_timeline. "
            "Install with: pip install pi-spc[plotly]"
        )

    pdf = df.to_pandas()

    color_arg = color_by if color_by else category
    hover_cols = [label] + ([color_by] if color_by else [])

    fig = px.timeline(
        pdf,
        x_start=start,
        x_end=end,
        y=category,
        color=color_arg,
        color_discrete_map=color_map or {},
        title=title,
        hover_data=hover_cols,
    )

    n_categories = pdf[category].nunique()
    auto_height = max(300, n_categories * 80 + 120) if height is None else height
    fig.update_layout(
        height=auto_height,
        xaxis_title=None,
        yaxis_title=None,
        yaxis=dict(autorange="reversed"),
        legend_title_text=color_arg,
        bargroupgap=0.1,
    )
    fig.update_traces(
        hovertemplate=(
            f"<b>%{{customdata[0]}}</b><br>"
            "%{base|%Y-%m-%d %H:%M:%S} → %{x|%Y-%m-%d %H:%M:%S}"
            "<extra></extra>"
        )
    )

    if highlight_labels:
        hl_set = set(highlight_labels)
        for trace in fig.data:
            if trace.customdata is None:
                continue
            # customdata[0] corresponds to the 'label' column because we set hover_data with label first
            opacities = [
                1.0 if cd[0] in hl_set else 0.05
                for cd in trace.customdata
            ]
            trace.marker.opacity = opacities

    return fig


def xbar_s_chart(
    df: pl.DataFrame,
    batch_col: str = "Batch",
    mean_col: str = "Mean",
    sigma_col: str = "Sigma",
    title: str = "Xbar / S Control Chart",
    xbar_y_title: str = "X\u0304",
    width: int | str = "container",
    height_per_panel: int = 150,
    spec_limits: tuple[float | None, float | None] = (None, None),
    as_panels: bool = False,
    show_y_title: bool = True,
    dispersion_label: str = "S",
    show_dispersion_limits: bool = True,
    selection: "alt.SelectionParameter | None" = None,
) -> "alt.VConcatChart | tuple[alt.LayerChart, alt.LayerChart, alt.LayerChart]":
    """Three-panel Xbar / MR / S control chart from subgroup summary stats.

    Panel order: X-bar, MR (moving range), S (within-subgroup sigma).

    When *as_panels* is True, returns the three panels as a tuple so the
    caller can lay them out with mo.vstack / mo.hstack (each panel is a
    simple layered chart that supports ``width="container"``).
    """
    # ── Enrich with MR + control-limit columns ──
    vals = df[mean_col].to_list()
    gm = float(df[mean_col].mean())
    sb = float(df[sigma_col].mean())
    mr = [None] + [abs(vals[i] - vals[i - 1]) for i in range(1, len(vals))]
    mr_ok = [v for v in mr if v is not None]
    mb = sum(mr_ok) / len(mr_ok) if mr_ok else 0.0
    lcl = gm - IMR_SIGMA_MULTIPLIER * mb

    chart_df = df.with_columns(
        MR=pl.Series("MR", mr, dtype=pl.Float64),
        xbar_cl=pl.lit(gm),
        xbar_ucl=pl.lit(gm + IMR_SIGMA_MULTIPLIER * mb),
        xbar_lcl=pl.lit(lcl if lcl > 0 else None, dtype=pl.Float64),
        s_cl=pl.lit(sb),
        s_ucl=pl.lit(sb * S_UCL_MULTIPLIER),
        mr_cl=pl.lit(mb),
        mr_ucl=pl.lit(MR_UCL_MULTIPLIER * mb),
    )

    lsl, usl = spec_limits
    chart_df = chart_df.with_columns(
        spec_lsl=pl.lit(lsl, dtype=pl.Float64),
        spec_usl=pl.lit(usl, dtype=pl.Float64),
        _order=pl.arange(0, chart_df.height, eager=True),
    )

    _pw = width
    mr_df = chart_df.filter(pl.col("MR").is_not_null())
    _x_sort = alt.SortField("_order")

    # ── Xbar y-axis: domain from data + control limits + nearby spec limits ──
    _y_min = float(chart_df[mean_col].min())
    _y_max = float(chart_df[mean_col].max())
    _lcl_vals = chart_df["xbar_lcl"].drop_nulls()
    if _lcl_vals.len() > 0:
        _y_min = min(_y_min, float(_lcl_vals.min()))
    _y_max = max(_y_max, float(chart_df["xbar_ucl"].max()))
    # Include spec limits in domain only if they're within ~2x the CL range
    _cl_range = _y_max - _y_min
    if lsl is not None and lsl >= _y_min - 2 * _cl_range:
        _y_min = min(_y_min, lsl)
    if usl is not None and usl <= _y_max + 2 * _cl_range:
        _y_max = max(_y_max, usl)
    _y_pad = (_y_max - _y_min) * 0.08
    _xbar_domain = [_y_min - _y_pad, _y_max + _y_pad]
    _xbar_scale = alt.Scale(zero=False, domain=_xbar_domain)

    # ── Helper: build limit rules from encoded columns ──
    def _limit_rules(data, cl_col, ucl_col, lcl_col=None, scale=alt.Undefined):
        layers = [
            alt.Chart(data).mark_rule(color="green", strokeDash=[4, 4], opacity=0.7, clip=True)
            .encode(y=alt.Y(f"mean({cl_col}):Q", scale=scale)),
            alt.Chart(data).mark_rule(color="red", strokeDash=[6, 3], opacity=0.7, clip=True)
            .encode(y=alt.Y(f"mean({ucl_col}):Q", scale=scale)),
        ]
        if lcl_col:
            layers.append(
                alt.Chart(data).mark_rule(color="red", strokeDash=[6, 3], opacity=0.7, clip=True)
                .encode(y=alt.Y(f"mean({lcl_col}):Q", scale=scale))
            )
        return layers

    # ── Xbar panel ──
    _sel_encode = {}
    if selection is not None:
        _sel_encode["opacity"] = alt.condition(selection, alt.value(1), alt.value(0.3))
    xbar_line = (
        alt.Chart(chart_df)
        .mark_line(point=alt.OverlayMarkDef(size=40), color="steelblue", clip=True)
        .encode(
            x=alt.X(f"{batch_col}:N", sort=_x_sort, title="",
                     axis=alt.Axis(labels=False, ticks=False)),
            y=alt.Y(f"{mean_col}:Q", title=xbar_y_title if show_y_title else "", scale=_xbar_scale),
            tooltip=[
                alt.Tooltip(f"{batch_col}:N"),
                alt.Tooltip(f"{mean_col}:Q", format=".4f", title="Mean"),
                alt.Tooltip(f"{sigma_col}:Q", format=".4f", title="\u03c3"),
            ],
            **_sel_encode,
        )
    )
    if selection is not None:
        xbar_line = xbar_line.add_params(selection)
    xbar_layers = xbar_line
    for r in _limit_rules(chart_df, "xbar_cl", "xbar_ucl", "xbar_lcl", scale=_xbar_scale):
        xbar_layers = xbar_layers + r
    if lsl is not None:
        xbar_layers = xbar_layers + (
            alt.Chart(chart_df).mark_rule(strokeDash=[2, 2], color="purple", opacity=0.5, clip=True)
            .encode(y=alt.Y("mean(spec_lsl):Q", scale=_xbar_scale))
        )
    if usl is not None:
        xbar_layers = xbar_layers + (
            alt.Chart(chart_df).mark_rule(strokeDash=[2, 2], color="purple", opacity=0.5, clip=True)
            .encode(y=alt.Y("mean(spec_usl):Q", scale=_xbar_scale))
        )
    xbar_panel = xbar_layers.properties(width=_pw, height=height_per_panel)

    # ── MR panel (middle) ──
    mr_line = (
        alt.Chart(mr_df)
        .mark_line(point=alt.OverlayMarkDef(size=40), color="#7b3294")
        .encode(
            x=alt.X(f"{batch_col}:N", sort=_x_sort, title="",
                     axis=alt.Axis(labels=False, ticks=False)),
            y=alt.Y("MR:Q", title="MR" if show_y_title else ""),
            tooltip=[
                alt.Tooltip(f"{batch_col}:N"),
                alt.Tooltip("MR:Q", format=".4f", title="Moving Range"),
            ],
            **_sel_encode,
        )
    )
    if selection is not None:
        mr_line = mr_line.add_params(selection)
    mr_layers = mr_line
    for r in _limit_rules(mr_df, "mr_cl", "mr_ucl"):
        mr_layers = mr_layers + r
    mr_panel = mr_layers.properties(width=_pw, height=height_per_panel)

    # ── S panel (bottom) ──
    s_line = (
        alt.Chart(chart_df)
        .mark_line(point=alt.OverlayMarkDef(size=40), color="darkorange")
        .encode(
            x=alt.X(f"{batch_col}:N", sort=_x_sort, title="Batch",
                     axis=alt.Axis(labelAngle=-45, labelLimit=80)),
            y=alt.Y(f"{sigma_col}:Q", title=dispersion_label if show_y_title else ""),
            tooltip=[
                alt.Tooltip(f"{batch_col}:N"),
                alt.Tooltip(f"{sigma_col}:Q", format=".4f", title=dispersion_label),
            ],
            **_sel_encode,
        )
    )
    if selection is not None:
        s_line = s_line.add_params(selection)
    s_layers = s_line
    if show_dispersion_limits:
        for r in _limit_rules(chart_df, "s_cl", "s_ucl"):
            s_layers = s_layers + r
    s_panel = s_layers.properties(width=_pw, height=height_per_panel)

    if as_panels:
        # Strip explicit width so marimo auto-applies "container" per-chart
        return (
            xbar_layers.properties(height=height_per_panel),
            mr_layers.properties(height=height_per_panel),
            s_layers.properties(height=height_per_panel),
        )

    return (
        alt.vconcat(xbar_panel, mr_panel, s_panel)
        .resolve_scale(x="shared")
        .configure_concat(spacing=2)
        .properties(title=title)
    )


def imr_chart(
    df: pl.DataFrame,
    x_col: str = "Cycle",
    timestamp_col: str = "Timestamp",
    value_col: str = "Value",
    batch_col: str | None = "Batch",
    title: str = "I-MR Chart",
    y_title: str = "Value",
    x_title: str = "Cycle",
    height_per_panel: int = 200,
    width: int | str = "container",
    spec_min: float | None = None,
    spec_max: float | None = None,
) -> alt.VConcatChart:
    """Two-panel Individuals / Moving Range chart.

    *x_col* drives the x-axis (typically an integer cycle count).
    *timestamp_col* is included in the tooltip only.
    Moving range resets at batch boundaries when *batch_col* is provided.
    Control limits: UCL_I = X̄ ± 2.66·MR̄, UCL_MR = 3.267·MR̄.
    Optional *spec_min*/*spec_max* draw orange dashed spec limit lines on the I panel.
    """
    df = df.sort([batch_col, x_col] if batch_col else [x_col])

    if batch_col:
        df = df.with_columns(
            MR=(pl.col(value_col) - pl.col(value_col).shift(1).over(batch_col))
            .abs()
        )
    else:
        df = df.with_columns(
            MR=(pl.col(value_col) - pl.col(value_col).shift(1)).abs()
        )

    gm = float(df[value_col].mean())
    _mr_vals = df["MR"].drop_nulls()
    mr_bar = float(_mr_vals.mean()) if len(_mr_vals) > 0 else 0.0
    ucl_i = gm + IMR_SIGMA_MULTIPLIER * mr_bar
    lcl_i = gm - IMR_SIGMA_MULTIPLIER * mr_bar
    ucl_mr = MR_UCL_MULTIPLIER * mr_bar

    df = df.with_columns(
        _i_cl=pl.lit(gm),
        _i_ucl=pl.lit(ucl_i),
        _i_lcl=pl.lit(lcl_i if lcl_i > 0 else None, dtype=pl.Float64),
        _mr_cl=pl.lit(mr_bar),
        _mr_ucl=pl.lit(ucl_mr),
    )
    mr_df = df.filter(pl.col("MR").is_not_null())

    _color = alt.Color(f"{batch_col}:N", legend=alt.Legend(title="Batch")) if batch_col else alt.value("steelblue")
    _has_ts = timestamp_col in df.columns

    def _limit_rules(data, cl_col, ucl_col, lcl_col=None):
        layers = [
            alt.Chart(data).mark_rule(color="green", strokeDash=[4, 4], opacity=0.7)
            .encode(y=alt.Y(f"mean({cl_col}):Q")),
            alt.Chart(data).mark_rule(color="red", strokeDash=[6, 3], opacity=0.7)
            .encode(y=alt.Y(f"mean({ucl_col}):Q")),
        ]
        if lcl_col:
            layers.append(
                alt.Chart(data).mark_rule(color="red", strokeDash=[6, 3], opacity=0.7)
                .encode(y=alt.Y(f"mean({lcl_col}):Q"))
            )
        return layers

    # Y-only zoom: scroll zooms Y, X stays locked
    _zoom_y = alt.selection_interval(bind="scales", encodings=["y"])

    # I panel
    _i_line = (
        alt.Chart(df)
        .mark_line(point=alt.OverlayMarkDef(size=30), clip=True)
        .encode(
            x=alt.X(f"{x_col}:Q", title="", axis=alt.Axis(labels=False, ticks=False)),
            y=alt.Y(f"{value_col}:Q", title=y_title, scale=alt.Scale(zero=False),
                    axis=alt.Axis(tickCount=5)),
            color=_color,
            tooltip=[
                *(([alt.Tooltip(f"{batch_col}:N")] if batch_col else [])),
                alt.Tooltip(f"{x_col}:Q", title="Cycle"),
                *(([alt.Tooltip(f"{timestamp_col}:T", format="%Y-%m-%d %H:%M:%S")] if _has_ts else [])),
                alt.Tooltip(f"{value_col}:Q", format=".2f", title=y_title),
            ],
        )
    )
    i_layers = _i_line
    for r in _limit_rules(df, "_i_cl", "_i_ucl", "_i_lcl"):
        i_layers = i_layers + r
    if spec_min is not None or spec_max is not None:
        _spec_vals = [v for v in [spec_min, spec_max] if v is not None]
        _spec_labels = [lbl for v, lbl in [(spec_min, "LSL"), (spec_max, "USL")] if v is not None]
        _spec_df = pl.DataFrame({"y": _spec_vals, "label": _spec_labels})
        _spec_rules = (
            alt.Chart(_spec_df)
            .mark_rule(color="orange", strokeDash=[8, 4], opacity=0.9, strokeWidth=1.5)
            .encode(y=alt.Y("y:Q"))
        )
        _spec_text = (
            alt.Chart(_spec_df)
            .mark_text(color="orange", fontSize=9, align="left", dx=4, fontWeight="bold")
            .encode(
                y=alt.Y("y:Q"),
                x=alt.value(0),
                text="label:N",
            )
        )
        i_layers = i_layers + _spec_rules + _spec_text
    i_panel = i_layers.add_params(_zoom_y).properties(
        width=width, height=height_per_panel,
        title=alt.TitleParams("Individuals", anchor="start", fontSize=11),
    )

    # Distribution panel — horizontal histogram sharing Y with I panel
    # Compute explicit bin step from data range so tight distributions get proper resolution
    _v_min = float(df[value_col].min())
    _v_max = float(df[value_col].max())
    _v_range = _v_max - _v_min if _v_max != _v_min else 1.0
    _bin_step = _v_range / 60
    dist_panel = (
        alt.Chart(df)
        .mark_bar(opacity=0.6, color="steelblue")
        .encode(
            y=alt.Y(
                f"{value_col}:Q",
                bin=alt.Bin(step=_bin_step),
                title="",
                axis=alt.Axis(labels=False, ticks=False, domain=False),
                scale=alt.Scale(zero=False),
            ),
            x=alt.X("count():Q", title="", axis=alt.Axis(labels=False, ticks=False, domain=False, grid=False)),
        )
        .properties(width=60, height=height_per_panel)
    )

    i_row = (
        alt.hconcat(i_panel, dist_panel)
        .resolve_scale(y="shared")
    )

    # MR panel
    _mr_line = (
        alt.Chart(mr_df)
        .mark_line(point=alt.OverlayMarkDef(size=30), clip=True)
        .encode(
            x=alt.X(f"{x_col}:Q", title=x_title),
            y=alt.Y("MR:Q", title="MR", scale=alt.Scale(zero=True)),
            color=_color,
            tooltip=[
                *(([alt.Tooltip(f"{batch_col}:N")] if batch_col else [])),
                alt.Tooltip(f"{x_col}:Q", title="Cycle"),
                *(([alt.Tooltip(f"{timestamp_col}:T", format="%Y-%m-%d %H:%M:%S")] if _has_ts else [])),
                alt.Tooltip("MR:Q", format=".2f", title="Moving Range"),
            ],
        )
    )
    mr_layers = _mr_line
    for r in _limit_rules(mr_df, "_mr_cl", "_mr_ucl"):
        mr_layers = mr_layers + r
    mr_panel = mr_layers.properties(width=width, height=height_per_panel // 2)

    return (
        alt.vconcat(i_row, mr_panel)
        .resolve_scale(x="shared", color="shared")
        .properties(title=title)
        .configure_view(stroke=None)
        .configure_concat(spacing=2)
    )
