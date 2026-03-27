# src/quick_insight/web/pages/earnings_dashboard_helpers/charts.py
from __future__ import annotations

import altair as alt
import pandas as pd
import uuid

def chart_dims(*, per_row: int, aspect_w_over_h: float) -> tuple[int, int]:
    w = 460 if per_row == 3 else 720 if per_row == 2 else 1080
    return w, int(w / aspect_w_over_h)


def line_with_mean(ts: pd.DataFrame, *, y_title: str, width: int, height: int) -> alt.Chart:
    ts = ts.dropna(subset=["as_of_date", "metric_value"]).copy()
    if ts.empty:
        raise ValueError("No datapoints to plot.")
    mean_val = float(ts["metric_value"].mean())

    base = alt.Chart(ts).encode(
        x=alt.X("as_of_date:T", title="Snapshot date"),
        tooltip=[alt.Tooltip("as_of_date:T", title="Date"), alt.Tooltip("metric_value:Q", title=y_title, format=",.6g")],
    )
    series = base.mark_line(point=True).encode(y=alt.Y("metric_value:Q", title=y_title))
    mean_rule = alt.Chart(pd.DataFrame({"mean_value": [mean_val]})).mark_rule(strokeDash=[6, 6], color="red").encode(
        y="mean_value:Q"
    )
    return alt.layer(series, mean_rule).properties(width=width, height=height).interactive()


def indexed_log_chart(
    df: pd.DataFrame,
    *,
    width: int,
    height: int,
    title: str = "",
    color_domain=None,
    color_range=None,
    legend_orient="bottom",
    selection_name: str | None = None,
):
    if color_domain is None:
        color_domain = sorted(df["series_name"].unique())

    if color_range is None:
        color_range = ["#1f77b4"] * len(color_domain)

    df = df.dropna(subset=["as_of_date", "series_name", "index_value"]).copy()
    df = df[df["index_value"] > 0]
    if df.empty:
        raise ValueError("No positive indexed values available to plot on log scale.")

    if selection_name is None:
        selection_name = f"legend_sel_{uuid.uuid4().hex}"

    # Legend filter selection (your original)
    sel = alt.selection_point(
        fields=["series_name"],
        bind="legend",
        name=selection_name,
    )

    # Zoom/pan selection with UNIQUE name (replaces .interactive())
    zoom = alt.selection_interval(
        bind="scales",
        name=f"zoom_{selection_name}",
    )

    legend = alt.Legend(
        orient=legend_orient,
        direction="vertical",
        title=None,
        labelLimit=500,
        symbolLimit=0,
        padding=6,
    )

    return (
        alt.Chart(df)
        .add_params(sel, zoom)          # <-- add both params explicitly
        .transform_filter(sel)
        .mark_line(point=True)
        .encode(
            x=alt.X("as_of_date:T", title="Snapshot date"),
            y=alt.Y(
                "index_value:Q",
                title="Relative growth (index=100 at start) — log scale",
                scale=alt.Scale(type="log"),
            ),
            color=alt.Color(
                "series_name:N",
                title="",
                legend=legend,
                scale=alt.Scale(domain=color_domain, range=color_range),
            ),
            tooltip=[
                alt.Tooltip("as_of_date:T", title="Date"),
                alt.Tooltip("series_name:N", title="Series"),
                alt.Tooltip("index_value:Q", title="Index", format=",.4g"),
                alt.Tooltip("raw_value:Q", title="Raw", format=",.6g"),
            ],
        )
        .properties(width=width, height=height, title=title)
    )




def legend_only(*, label: str, width: int) -> alt.Chart:
    df = pd.DataFrame({"series_name": [label], "x": [0], "y": [0]})
    return (
        alt.Chart(df)
        .mark_point(opacity=0)
        .encode(
            x=alt.X("x:Q", axis=None),
            y=alt.Y("y:Q", axis=None),
            color=alt.Color(
                "series_name:N",
                legend=alt.Legend(orient="bottom", direction="horizontal", title=None, labelLimit=500, symbolLimit=0),
            ),
        )
        .properties(width=width, height=30)
    )
