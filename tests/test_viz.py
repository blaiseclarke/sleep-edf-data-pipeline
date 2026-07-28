import pandas as pd
import pytest
from charts import STAGE_ORDER, architecture_figure, band_power_figure, hypnogram_figure
from theme import DARK, LIGHT, _relative_luminance, ink_on, palette


def contrast(foreground: str, background: str) -> float:
    """WCAG contrast ratio between two hex colours."""
    first, second = _relative_luminance(foreground), _relative_luminance(background)
    lighter, darker = max(first, second), min(first, second)
    return (lighter + 0.05) / (darker + 0.05)


@pytest.fixture
def metrics():
    """A summary row with the fields the charts read."""
    return pd.Series(
        {
            "deep_sleep_percentage": 0.34,
            "light_sleep_percentage": 0.47,
            "rem_sleep_percentage": 0.19,
            "deep_sleep_minutes": 110.0,
            "light_sleep_minutes": 152.0,
            "rem_sleep_minutes": 62.0,
            "avg_delta_power": 21.06,
            "avg_theta_power": 12.83,
            "avg_alpha_power": 7.05,
            "avg_sigma_power": 2.66,
            "avg_beta_power": 0.49,
        }
    )


@pytest.fixture
def epochs():
    return pd.DataFrame(
        {
            "epoch_idx": [10, 11, 12, 13, 14],
            "sleep_stage": ["W", "N1", "N2", "N3", "REM"],
        }
    )


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_every_in_segment_label_clears_contrast(mode):
    """
    The percentage sits inside a coloured fill, so it must clear contrast against
    that fill rather than against the page. Labels are rendered at 19px bold,
    which is WCAG large text and therefore needs 3:1.
    """
    for fill in palette(mode)["categorical"]:
        assert contrast(ink_on(fill), fill) >= 3.0


def test_in_segment_labels_are_wcag_large_text(metrics):
    """
    The 3:1 floor above only holds while the labels qualify as WCAG large text.
    They once claimed 19px bold but rendered 19px regular: at normal-text size
    the blue slot's 4.46:1 fails the 4.5:1 AA floor this test suite waived.
    Plotly fonts carry no weight property, so the bold must come from markup.
    """
    for trace in architecture_figure(metrics, textured=False, colours=LIGHT).data:
        assert trace.insidetextfont.size == 19
        assert trace.text[0].startswith("<b>") and trace.text[0].endswith("</b>")


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_body_text_clears_aa_against_the_surface(mode):
    colours = palette(mode)
    for role in ("ink", "ink_muted"):
        assert contrast(colours[role], colours["surface"]) >= 4.5


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_ordinal_ramp_stays_visible_against_its_surface(mode):
    """
    Rendering the dark ramp exposed the last two bars sinking into the surface.
    Every step has to stay separable from the background it is drawn on.
    """
    colours = palette(mode)
    for step in colours["ordinal"]:
        assert contrast(step, colours["surface"]) >= 2.0


def test_ink_on_picks_the_higher_contrast_option():
    for fill in LIGHT["categorical"] + DARK["categorical"]:
        chosen = ink_on(fill)
        rejected = "#0b0b0b" if chosen == "#ffffff" else "#ffffff"
        assert contrast(chosen, fill) >= contrast(rejected, fill)


def test_light_and_dark_are_separately_chosen():
    """Dark is its own set of steps, not an automatic flip of the light values."""
    assert LIGHT["categorical"] != DARK["categorical"]
    assert LIGHT["ordinal"] != DARK["ordinal"]
    assert set(LIGHT) == set(DARK)


def test_hypnogram_positions_stages_and_inverts_the_axis(epochs):
    figure = hypnogram_figure(epochs, onset_idx=10, colours=LIGHT)

    (trace,) = figure.data
    # Wake at the top, N3 at the bottom, which means a reversed axis.
    assert figure.layout.yaxis.autorange == "reversed"
    assert list(trace.y) == [STAGE_ORDER.index(s) for s in epochs["sleep_stage"]]
    # Time is measured from onset, in minutes, at two epochs per minute.
    assert list(trace.x) == [0.0, 0.5, 1.0, 1.5, 2.0]
    # A stage holds for its whole epoch, so the line steps rather than slopes.
    assert trace.line.shape == "hv"
    # One series needs no legend box; the heading names it.
    assert figure.layout.showlegend is False


def test_architecture_segments_match_the_legend_order(metrics):
    figure = architecture_figure(metrics, textured=False, colours=LIGHT)

    assert [trace.name for trace in figure.data] == [
        "Deep (N3)",
        "Light (N1+N2)",
        "REM",
    ]
    # Plotly reverses legend order for stacked horizontal bars by default, which
    # would read right-to-left against the bar.
    assert figure.layout.legend.traceorder == "normal"
    assert figure.layout.showlegend is True
    assert [trace.marker.color for trace in figure.data] == list(LIGHT["categorical"])


def test_architecture_shares_sum_to_the_whole(metrics):
    figure = architecture_figure(metrics, textured=False, colours=LIGHT)
    assert sum(trace.x[0] for trace in figure.data) == pytest.approx(100.0)


def test_architecture_labels_are_centred_not_pinned_to_the_edge(metrics):
    """
    The default "end" anchor pushed the final segment's label against the plot
    boundary, where it was clipped.
    """
    for trace in architecture_figure(metrics, textured=False, colours=LIGHT).data:
        assert trace.insidetextanchor == "middle"
        assert trace.cliponaxis is False


def test_patterns_are_opt_in_and_use_the_surface_colour(metrics):
    plain = architecture_figure(metrics, textured=False, colours=LIGHT)
    assert all(trace.marker.pattern.shape == "" for trace in plain.data)

    textured = architecture_figure(metrics, textured=True, colours=LIGHT)
    shapes = [trace.marker.pattern.shape for trace in textured.data]
    # What matters is that the three fills are distinguishable without colour.
    # Plain is itself one of the three states, so the first slot stays unhatched
    # rather than every segment carrying a texture.
    assert len(set(shapes)) == len(shapes)
    # Hatching in the label ink makes the percentage sit on busy dark lines.
    for trace in textured.data:
        assert trace.marker.pattern.fgcolor == LIGHT["surface"]


def test_band_power_applies_the_ramp_in_frequency_order(metrics):
    figure = band_power_figure(metrics, LIGHT)
    (trace,) = figure.data

    assert list(trace.x) == ["Delta", "Theta", "Alpha", "Sigma", "Beta"]
    assert list(trace.marker.color) == list(LIGHT["ordinal"])
    assert figure.layout.showlegend is False


def test_band_power_handles_negative_decibels(metrics):
    """
    Roughly a third of subjects run negative in the faster bands. Bars grow
    either side of a zero baseline, and labels sit outside the cap so they land
    above positive bars and below negative ones without special-casing.
    """
    metrics = metrics.copy()
    metrics["avg_beta_power"] = -5.9
    metrics["avg_sigma_power"] = -6.0

    figure = band_power_figure(metrics, LIGHT)
    (trace,) = figure.data

    assert min(trace.y) < 0
    assert trace.textposition == "outside"
    assert trace.text[-1] == "-5.9"
    # The zero line carries meaning here, so it must be drawn.
    assert figure.layout.yaxis.zeroline is True


def test_charts_do_not_animate(metrics, epochs):
    """Motion is suppressed for readers who asked not to see it."""
    figures = [
        hypnogram_figure(epochs, 10, LIGHT),
        architecture_figure(metrics, False, LIGHT),
        band_power_figure(metrics, LIGHT),
    ]
    for figure in figures:
        assert figure.layout.transition.duration == 0


def test_gridlines_are_solid_hairlines(metrics, epochs):
    """Dashed grids read as thresholds; these are just grids."""
    for figure in (
        hypnogram_figure(epochs, 10, LIGHT),
        band_power_figure(metrics, LIGHT),
    ):
        assert figure.layout.xaxis.griddash == "solid"
        assert figure.layout.yaxis.gridwidth == 1
