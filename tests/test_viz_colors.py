"""
Unit tests for viz.colors.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.viz.colors import (
    FLOOD_SEVERITY_CLASS_COLORS,
    apply_fillcolor,
    apply_fillcolor_classed,
    apply_fillcolor_binned,
    build_vertical_categorical_legend_block_html,
    build_vertical_binned_legend_block_html,
    build_vertical_gradient_legend_html,
    get_cmap_hex_list,
)


def test_get_cmap_hex_list_length() -> None:
    colors = get_cmap_hex_list("Reds", nsteps=16)
    assert isinstance(colors, list)
    assert len(colors) == 16
    assert all(isinstance(c, str) and c.startswith("#") for c in colors)


def test_apply_fillcolor_nan_defaults() -> None:
    df = pd.DataFrame({"x": [1.0, None, 3.0]})
    out = apply_fillcolor(df, "x", vmin=1.0, vmax=3.0, cmap_name="Reds")
    assert "fillColor" in out.columns
    assert out.loc[1, "fillColor"] == "#cccccc"
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
    assert out.loc[3, "fillColor"] == "#cccccc"

    # vmin maps to first bin color; vmax maps to last bin color
    colors = get_cmap_hex_list("Reds", nsteps=3)
    assert out.loc[0, "fillColor"] == colors[0]
    assert out.loc[2, "fillColor"] == colors[-1]


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
    assert out.loc[3, "fillColor"] == "#cccccc"


def test_build_categorical_legend_block_contains_labels_and_title() -> None:
    html = build_vertical_categorical_legend_block_html(
        legend_title="Flood Severity Index (RP-100)",
        labels=["VeryLow/No", "Low", "Moderate", "High", "Extreme"],
        colors=FLOOD_SEVERITY_CLASS_COLORS,
        map_height=700,
    )

    assert "Flood Severity Index (RP-100)" in html
    assert "VeryLow/No" in html
    assert "Extreme" in html
