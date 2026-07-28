"""
Plotly figure builders. Deliberately free of Streamlit so the charts can be
rendered and inspected outside the app, and unit-tested.
"""

import pandas as pd
import plotly.graph_objects as go
from theme import PATTERNS, ink_on, style_axes

STAGE_ORDER = ["W", "REM", "N1", "N2", "N3"]
STAGE_LABELS = ["Wake", "REM", "N1", "N2", "N3"]
BANDS = [
    ("Delta", "avg_delta_power", "0.5-4 Hz"),
    ("Theta", "avg_theta_power", "4-8 Hz"),
    ("Alpha", "avg_alpha_power", "8-12 Hz"),
    ("Sigma", "avg_sigma_power", "12-16 Hz"),
    ("Beta", "avg_beta_power", "16-30 Hz"),
]


def hypnogram_figure(epochs: pd.DataFrame, onset_idx: int, colours: dict) -> go.Figure:
    """
    Sleep stage against time. One series, so no legend: the heading names it.
    Drawn as a step line because a stage holds for the whole of its epoch.
    """
    positions = {stage: index for index, stage in enumerate(STAGE_ORDER)}
    minutes = (epochs["epoch_idx"] - onset_idx) * 0.5

    figure = go.Figure(
        go.Scatter(
            x=minutes,
            y=epochs["sleep_stage"].map(positions),
            mode="lines",
            line=dict(color=colours["series"], width=2, shape="hv"),
            customdata=epochs["sleep_stage"],
            hovertemplate="%{customdata}<br>%{x:.0f} min after onset<extra></extra>",
            name="Sleep stage",
        )
    )
    style_axes(figure, colours, x_title="Minutes after sleep onset")
    figure.update_yaxes(
        tickmode="array",
        tickvals=list(range(len(STAGE_ORDER))),
        ticktext=STAGE_LABELS,
        autorange="reversed",
    )
    figure.update_layout(height=300, showlegend=False)
    return figure


def architecture_figure(metrics: pd.Series, textured: bool, colours: dict) -> go.Figure:
    """
    Sleep architecture as one part-to-whole bar. Three segments, so a legend is
    always present, every segment is directly labelled, and an optional hatch
    gives a third identity channel beyond colour and position.
    """
    segments = [
        ("Deep (N3)", metrics["deep_sleep_percentage"], metrics["deep_sleep_minutes"]),
        (
            "Light (N1+N2)",
            metrics["light_sleep_percentage"],
            metrics["light_sleep_minutes"],
        ),
        ("REM", metrics["rem_sleep_percentage"], metrics["rem_sleep_minutes"]),
    ]

    figure = go.Figure()
    for index, (label, share, minutes) in enumerate(segments):
        fill = colours["categorical"][index]
        figure.add_trace(
            go.Bar(
                x=[share * 100],
                y=["Sleep"],
                orientation="h",
                name=label,
                marker=dict(
                    color=fill,
                    # A 2px gap in the surface colour separates touching
                    # segments. A spacer, not a contrasting border.
                    line=dict(color=colours["surface"], width=1),
                    pattern=dict(
                        shape=PATTERNS[index] if textured else "",
                        # Hatch in the surface colour, not in the label ink:
                        # matching the two makes the in-segment percentage sit
                        # on busy dark hatching and become hard to read.
                        fgcolor=colours["surface"],
                        solidity=0.22,
                        size=7,
                    ),
                ),
                # Plotly fonts carry no weight property, so the bold comes from
                # <b> markup in the text itself.
                text=[f"<b>{share * 100:.0f}%</b>"],
                # "auto" measures the label and moves it outside rather than
                # letting a narrow segment clip it. Anchoring inside labels to
                # the middle keeps the final segment's label off the plot edge,
                # where the default "end" anchor clips it.
                textposition="auto",
                insidetextanchor="middle",
                # 19px bold qualifies as WCAG large text, which needs 3:1 rather
                # than 4.5:1. That matters for the blue slot: neither the light
                # nor the dark ink token clears 4.5:1 against it (4.46 and 4.42),
                # so at body size this label would sit just under AA.
                insidetextfont=dict(
                    color=ink_on(fill), size=19, family="system-ui, sans-serif"
                ),
                outsidetextfont=dict(color=colours["ink_muted"], size=13),
                cliponaxis=False,
                hovertemplate=(
                    f"{label}<br>%{{x:.1f}}% of sleep<br>{minutes:.0f} min<extra></extra>"
                ),
            )
        )

    style_axes(figure, colours, x_title="Share of total sleep time")
    figure.update_yaxes(showgrid=False, showticklabels=False, showline=False, ticks="")
    figure.update_xaxes(range=[0, 100], ticksuffix="%")
    figure.update_layout(
        barmode="stack",
        height=210,
        bargap=0.62,
        showlegend=True,
        legend=dict(
            orientation="h",
            # Plotly reverses legend order for stacked horizontal bars; force
            # normal order so the legend reads left-to-right like the bar.
            traceorder="normal",
            yanchor="top",
            y=-0.5,
            x=0,
            font=dict(color=colours["ink_muted"], size=12),
        ),
    )
    return figure


def band_power_figure(metrics: pd.Series, colours: dict) -> go.Figure:
    """
    Mean power per frequency band. The hue ramp encodes the frequency ordering
    (delta is the slowest band, beta the fastest), not the bar heights -- these
    are decibels, and roughly a third of subjects run negative in the faster
    bands, so bars grow either side of a marked zero baseline.
    """
    labels = [name for name, _, _ in BANDS]
    values = [float(metrics[column]) for _, column, _ in BANDS]
    ranges = [span for _, _, span in BANDS]

    figure = go.Figure(
        go.Bar(
            x=labels,
            y=values,
            marker=dict(color=list(colours["ordinal"]), cornerradius=4),
            # Thin marks: leave most of each category band as air rather than
            # filling it with a heavy block.
            width=0.22,
            text=[f"{value:.1f}" for value in values],
            # Outside placement puts labels above positive caps and below
            # negative ones with no special-casing.
            textposition="outside",
            textfont=dict(color=colours["ink_muted"], size=12),
            cliponaxis=False,
            customdata=ranges,
            hovertemplate="%{x} (%{customdata})<br>%{y:.2f} dB<extra></extra>",
            name="Band power",
        )
    )
    style_axes(figure, colours, y_title="Mean power (dB)")
    figure.update_xaxes(showgrid=False)
    # The zero baseline carries meaning here, so it is drawn one step stronger
    # than the grid.
    figure.update_yaxes(zeroline=True, zerolinecolor=colours["axis"], zerolinewidth=1)
    figure.update_layout(height=290, showlegend=False)
    return figure
