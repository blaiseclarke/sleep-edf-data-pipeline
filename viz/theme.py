"""
Colour tokens and chart chrome for the dashboard.

Every palette here was checked with the data-viz validator rather than chosen by
eye, in both light and dark modes:

  categorical (3 slots)  light PASS · dark PASS
                         worst adjacent CVD dE 9.2 light / 9.4 dark (>= 8)
                         aqua sits at 2.74:1 on the light surface, below 3:1, so
                         the relief rule applies -- every categorical chart ships
                         visible direct labels and a table view, and identity is
                         never carried by colour alone.
  ordinal ramp (5 steps) light PASS · dark PASS
                         monotone lightness, single hue, light end clears 2:1.

Dark is a selected set of steps for the dark surface, not an automatic flip of
the light values.

The validator surfaces (#fcfcfb, #1a1a19) are slightly off Streamlit's actual
ones (#ffffff, #0e1117). Checked both: every mark and text colour scores equal
or better against the real surfaces than against the validated ones -- worst
mark contrast 2.06 -> 2.11 light and 3.23 -> 3.50 dark -- so the gates hold as
deployed.
"""

LIGHT = {
    "surface": "#fcfcfb",
    "ink": "#0b0b0b",
    "ink_muted": "#52514e",
    "grid": "#e6e5e1",
    "axis": "#c9c8c3",
    # Single-series charts use categorical slot 1.
    "series": "#2a78d6",
    # Slots 1-3, in fixed order. Never cycled, never reassigned by rank.
    "categorical": ("#2a78d6", "#eb6834", "#1baf7a"),
    # One hue, light -> dark. Encodes frequency order, not magnitude.
    "ordinal": ("#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"),
}

DARK = {
    "surface": "#1a1a19",
    "ink": "#ffffff",
    "ink_muted": "#c3c2b7",
    "grid": "#2e2e2c",
    "axis": "#464643",
    "series": "#3987e5",
    "categorical": ("#3987e5", "#d95926", "#199e70"),
    # Stops at step 500 rather than running to 600. Rendering the darker
    # variant showed the last two bars sinking into the dark surface; this
    # keeps the dark end at 3.23:1 instead of 2.15:1 and still clears the
    # adjacent-lightness gap.
    "ordinal": ("#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf"),
}

# Secondary encoding for the stacked bar, so the three segments stay
# distinguishable in greyscale, in print, and under full colour blindness.
# Plain counts as one of the three states, so only two slots carry a texture --
# hatching every segment adds noise without adding information.
PATTERNS = ("", "/", ".")


def palette(mode: str) -> dict:
    return DARK if mode == "dark" else LIGHT


def _relative_luminance(hex_colour: str) -> float:
    """WCAG relative luminance for a #rrggbb string."""
    channels = []
    for offset in (1, 3, 5):
        value = int(hex_colour[offset : offset + 2], 16) / 255
        channels.append(
            value / 12.92 if value <= 0.03928 else ((value + 0.055) / 1.055) ** 2.4
        )
    red, green, blue = channels
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue


def ink_on(fill: str) -> str:
    """
    Pick white or near-black for text sitting inside a coloured fill, by whichever
    actually clears contrast against it. Guessing per-hue is how in-bar labels end
    up unreadable on the lighter slots.
    """
    luminance = _relative_luminance(fill)
    on_white = 1.05 / (luminance + 0.05)
    on_black = (luminance + 0.05) / 0.05
    return "#ffffff" if on_white >= on_black else "#0b0b0b"


def style_axes(figure, colours: dict, *, x_title=None, y_title=None) -> None:
    """Recessive hairline grid and axes, transparent surface, no chart junk."""
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            color=colours["ink_muted"],
            size=13,
            family=(
                "ui-monospace, SFMono-Regular, Menlo, Consolas, "
                "'Liberation Mono', monospace"
            ),
        ),
        margin=dict(l=8, r=8, t=8, b=8),
        # Charts must not animate for readers who asked not to see motion.
        transition_duration=0,
        hoverlabel=dict(
            bgcolor=colours["surface"],
            bordercolor=colours["axis"],
            font=dict(color=colours["ink"], size=13),
        ),
    )
    axis_common = dict(
        showgrid=True,
        gridcolor=colours["grid"],
        gridwidth=1,
        griddash="solid",
        linecolor=colours["axis"],
        linewidth=1,
        zeroline=False,
        ticks="outside",
        tickcolor=colours["axis"],
        ticklen=4,
        title_font=dict(color=colours["ink_muted"], size=12),
    )
    figure.update_xaxes(**axis_common, title_text=x_title)
    figure.update_yaxes(**axis_common, title_text=y_title)


# Plotly toolbar: keep the useful controls, drop the rest.
PLOTLY_CONFIG = {
    "displaylogo": False,
    "displayModeBar": False,
    "scrollZoom": False,
    "responsive": True,
}
