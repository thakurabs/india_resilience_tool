"""
Unit tests for viz.colors.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.viz.colors import (
    NO_DATA_FILL_HEX,
    FLOOD_SEVERITY_CLASS_COLORS,
    apply_fillcolor,
    apply_fillcolor_classed,
    apply_fillcolor_binned,
    build_compact_binned_legend_card_html,
    build_compact_categorical_legend_card_html,
    build_vertical_categorical_legend_block_html,
    build_vertical_binned_legend_block_html,
    build_vertical_gradient_legend_html,
    compute_robust_range,
    get_binned_cmap_hex_list,
    get_cmap_hex_list,
)


def test_compute_robust_range_min_pad_respected() -> None:
    # Degenerate (single-value) range: the padding floor must honour min_pad so a
    # fraction metric does not balloon to +-1.0 (CHG-0120).
    single = pd.Series([0.0085])

    vmin_d, vmax_d = compute_robust_range(single)  # default floor 1.0 (backward-compat)
    assert vmin_d == 0.0085 - 1.0
    assert vmax_d == 0.0085 + 1.0

    vmin_s, vmax_s = compute_robust_range(single, min_pad=0.01)  # scaled floor
    assert vmin_s == 0.0085 - 0.01
    assert vmax_s == 0.0085 + 0.01


def test_get_cmap_hex_list_length() -> None:
    colors = get_cmap_hex_list("Reds", nsteps=16)
    assert isinstance(colors, list)
    assert len(colors) == 16
    assert all(isinstance(c, str) and c.startswith("#") for c in colors)


def test_apply_fillcolor_nan_defaults() -> None:
    df = pd.DataFrame({"x": [1.0, None, 3.0]})
    out = apply_fillcolor(df, "x", vmin=1.0, vmax=3.0, cmap_name="Reds")
    assert "fillColor" in out.columns
    assert out.loc[1, "fillColor"] == NO_DATA_FILL_HEX
    # valid rows should be hex strings
    assert isinstance(out.loc[0, "fillColor"], str) and out.loc[0, "fillColor"].startswith("#")
    assert isinstance(out.loc[2, "fillColor"], str) and out.loc[2, "fillColor"].startswith("#")


def test_build_legend_html_contains_labels() -> None:
    html = build_vertical_gradient_legend_html(
        pretty_metric_label="Summer Days",
        vmin=10.0,
        vmax=20.0,
        cmap_name="Reds",
        map_width=780,
        map_height=700,
    )
    assert "Summer Days" in html
    assert "20.0" in html
    assert "10.0" in html


def test_apply_fillcolor_binned_handles_nan_and_limits() -> None:
    df = pd.DataFrame({"x": [0.0, 1.5, 3.0, None]})
    out = apply_fillcolor_binned(df, "x", vmin=0.0, vmax=3.0, cmap_name="Reds", nlevels=3)

    assert "fillColor" in out.columns
    assert out.loc[3, "fillColor"] == NO_DATA_FILL_HEX

    # vmin maps to first bin color; vmax maps to last bin color. Bin colors
    # come from the shared sampler so the map matches the legend (CHG-0252).
    colors = get_binned_cmap_hex_list("Reds", nlevels=3)
    assert out.loc[0, "fillColor"] == colors[0]
    assert out.loc[2, "fillColor"] == colors[-1]


def test_irt_domain_ramps_are_monotone_in_oklab_lightness() -> None:
    # Scientific-correctness gate for the SDG-anchored ramps: class lightness
    # must decrease monotonically (perceptual ordering) with steps big enough
    # to keep adjacent classes distinguishable (min deltaL >= 0.06), and the
    # composite ramp must satisfy the same gate.
    from india_resilience_tool.viz.colors import (
        IRT_COMPOSITE_CMAP,
        IRT_RAMP_ANCHORS,
        _srgb_to_oklab,
    )

    for name in list(IRT_RAMP_ANCHORS) + [IRT_COMPOSITE_CMAP]:
        hexes = get_binned_cmap_hex_list(name, nlevels=7)
        assert len(hexes) == 7, name
        lightness = [_srgb_to_oklab(h)[0] for h in hexes]
        deltas = [lightness[i] - lightness[i + 1] for i in range(len(lightness) - 1)]
        assert all(d > 0 for d in deltas), f"{name}: lightness not monotone: {lightness}"
        assert min(deltas) >= 0.06, f"{name}: weakest class step deltaL={min(deltas):.3f}"


def test_irt_ramps_hold_their_sdg_anchor_hue() -> None:
    # Brand contract: each domain ramp is built with the anchor's OKLab hue
    # held fixed, so the saturated middle/dark classes must sit on the
    # anchor's hue angle (small tolerance for sRGB gamut clipping).
    import math

    from india_resilience_tool.viz.colors import IRT_RAMP_ANCHORS, _srgb_to_oklab

    def hue_deg(hex_color: str) -> float:
        _, a, b = _srgb_to_oklab(hex_color)
        return math.degrees(math.atan2(b, a)) % 360

    for name, anchor in IRT_RAMP_ANCHORS.items():
        anchor_hue = hue_deg(anchor)
        ramp = get_binned_cmap_hex_list(name, nlevels=7)
        # Saturated middle/dark classes only — skip the pale low end, where the
        # hue angle is numerically unstable at near-zero chroma.
        for h in ramp[len(ramp) // 3 : -1]:
            diff = abs(hue_deg(h) - anchor_hue) % 360
            diff = min(diff, 360 - diff)
            assert diff <= 6.0, f"{name}: class {h} hue off anchor by {diff:.1f} deg"


def test_get_binned_cmap_hex_list_clips_sequential_top_not_diverging() -> None:
    # Sequential ramps drop the near-black top (CHG-0252); diverging ramps keep
    # their full symmetric range so the midpoint stays neutral.
    import matplotlib as mpl
    import matplotlib.colors as mcolors

    raw_reds = [mcolors.to_hex(mpl.colormaps.get_cmap("Reds")(i / 6)) for i in range(7)]
    binned_reds = get_binned_cmap_hex_list("Reds", nlevels=7)
    assert binned_reds[0] == raw_reds[0]
    assert binned_reds[-1] != raw_reds[-1]
    assert binned_reds[-1] != mcolors.to_hex(mpl.colormaps.get_cmap("Reds")(1.0))

    raw_rdbu = [mcolors.to_hex(mpl.colormaps.get_cmap("RdBu_r")(i / 6)) for i in range(7)]
    binned_rdbu = get_binned_cmap_hex_list("RdBu_r", nlevels=7)
    assert binned_rdbu == raw_rdbu


def test_irt_cmap_names_work_for_gradients_and_legacy_fillcolor() -> None:
    from india_resilience_tool.viz.colors import IRT_COMPOSITE_CMAP

    composite = get_cmap_hex_list(IRT_COMPOSITE_CMAP, nsteps=9)
    assert len(composite) == 9
    assert all(color.startswith("#") for color in composite)

    html = build_vertical_gradient_legend_html(
        pretty_metric_label="Composite",
        vmin=0.0,
        vmax=100.0,
        cmap_name=IRT_COMPOSITE_CMAP,
    )
    assert "Composite" in html

    df = pd.DataFrame({"x": [0.0, 50.0, 100.0, None]})
    out = apply_fillcolor(df, "x", vmin=0.0, vmax=100.0, cmap_name="irt:heat")
    assert out.loc[3, "fillColor"] == NO_DATA_FILL_HEX
    assert all(str(out.loc[i, "fillColor"]).startswith("#") for i in (0, 1, 2))


def test_sdg_anchored_ramp_handles_single_and_clamped_multi_step() -> None:
    from india_resilience_tool.viz.colors import _sdg_anchored_ramp

    one = _sdg_anchored_ramp("#ffffee", 1)
    assert len(one) == 1
    assert one[0].startswith("#")

    five = _sdg_anchored_ramp("#ffffee", 5)
    assert len(five) == 5
    assert all(color.startswith("#") for color in five)


def test_build_binned_legend_block_contains_min_max_and_title() -> None:
    html = build_vertical_binned_legend_block_html(
        pretty_metric_label="Δ TM Mean",
        vmin=0.96,
        vmax=1.11,
        cmap_name="RdBu_r",
        nlevels=15,
        map_height=700,
    )
    assert "Δ TM Mean" in html
    assert "1.11" in html
    assert "0.96" in html


def test_build_compact_binned_legend_card_contains_no_data_and_shared_colors() -> None:
    html = build_compact_binned_legend_card_html(
        legend_title="°C",
        vmin=0.0,
        vmax=7.0,
        cmap_name="Reds",
        nlevels=7,
    )

    # Positioning is owned by the Leaflet bottomright control that hosts the
    # card, so the card itself must NOT carry fixed positioning.
    assert "position:fixed" not in html
    assert "width:240px" in html
    assert "pointer-events:none" in html
    assert "No data" in html
    assert NO_DATA_FILL_HEX in html
    for color in get_binned_cmap_hex_list("Reds", nlevels=7):
        assert color in html


def test_apply_fillcolor_classed_uses_fixed_class_colors() -> None:
    df = pd.DataFrame({"x": [1.0, 3.0, 5.0, None]})
    out = apply_fillcolor_classed(
        df,
        "x",
        value_to_color={index: color for index, color in enumerate(FLOOD_SEVERITY_CLASS_COLORS, start=1)},
    )

    assert out.loc[0, "fillColor"] == FLOOD_SEVERITY_CLASS_COLORS[0]
    assert out.loc[1, "fillColor"] == FLOOD_SEVERITY_CLASS_COLORS[2]
    assert out.loc[2, "fillColor"] == FLOOD_SEVERITY_CLASS_COLORS[4]
    assert out.loc[3, "fillColor"] == NO_DATA_FILL_HEX


def test_build_compact_categorical_legend_card_contains_no_data() -> None:
    html = build_compact_categorical_legend_card_html(
        legend_title="Flood Severity Index (RP-100)",
        labels=["VeryLow", "Low", "Moderate", "High", "Extreme"],
        colors=FLOOD_SEVERITY_CLASS_COLORS,
    )

    assert "Flood Severity Index (RP-100)" in html
    assert "Extreme" in html
    assert "No data" in html
    assert NO_DATA_FILL_HEX in html


def test_build_categorical_legend_block_contains_labels_and_title() -> None:
    html = build_vertical_categorical_legend_block_html(
        legend_title="Flood Severity Index (RP-100)",
        labels=["VeryLow", "Low", "Moderate", "High", "Extreme"],
        colors=FLOOD_SEVERITY_CLASS_COLORS,
        map_height=700,
    )

    assert "Flood Severity Index (RP-100)" in html
    assert "VeryLow" in html
    assert "Extreme" in html


def test_symmetric_diverging_range_anchors_midpoint_on_zero() -> None:
    from india_resilience_tool.viz.colors import symmetric_diverging_range

    # The defect: a data-driven delta range like -2..+8 puts the diverging
    # ramp's neutral midpoint at +3, so zero-change units render cool.
    vmin, vmax = symmetric_diverging_range(-2.0, 8.0)
    assert (vmin, vmax) == (-8.0, 8.0)
    assert (vmin + vmax) / 2.0 == 0.0

    # Largest absolute bound wins regardless of which side it is on.
    assert symmetric_diverging_range(-9.0, 3.0) == (-9.0, 9.0)
    # Single-signed ranges still straddle zero.
    assert symmetric_diverging_range(1.0, 4.0) == (-4.0, 4.0)


def test_symmetric_diverging_range_handles_non_finite_and_zero() -> None:
    import math

    from india_resilience_tool.viz.colors import symmetric_diverging_range

    # One finite bound is preserved as the magnitude.
    assert symmetric_diverging_range(float("nan"), 5.0) == (-5.0, 5.0)
    assert symmetric_diverging_range(-6.0, math.inf) == (-6.0, 6.0)
    # Degenerate cases fall back to a unit domain rather than collapsing.
    assert symmetric_diverging_range(0.0, 0.0) == (-1.0, 1.0)
    assert symmetric_diverging_range(float("nan"), float("nan")) == (-1.0, 1.0)


def test_zero_gets_the_neutral_class_on_a_symmetric_delta_domain() -> None:
    # End-to-end intent of CHG-0277: apply the anchoring step to a realistic
    # asymmetric delta range, then assert 0.0 lands on the ramp's middle class.
    # Both halves matter — on the raw -2..+8 domain, 0.0 is not the midpoint.
    from india_resilience_tool.viz.colors import (
        DEFAULT_CHOROPLETH_NLEVELS,
        get_binned_cmap_hex_list,
        symmetric_diverging_range,
    )

    vmin, vmax = symmetric_diverging_range(-2.0, 8.0)
    assert (vmin, vmax) == (-8.0, 8.0)

    gdf = pd.DataFrame({"delta": [-8.0, 0.0, 8.0]})
    out = apply_fillcolor_binned(
        gdf,
        "delta",
        vmin,
        vmax,
        cmap_name="RdBu_r",
        nlevels=DEFAULT_CHOROPLETH_NLEVELS,
    )

    palette = get_binned_cmap_hex_list("RdBu_r", nlevels=DEFAULT_CHOROPLETH_NLEVELS)
    neutral = palette[DEFAULT_CHOROPLETH_NLEVELS // 2]
    colors = list(out["fillColor"])
    assert colors[1].lower() == neutral.lower()
    # ...and the signed extremes still take opposite ends of the ramp.
    assert colors[0].lower() != colors[2].lower()
