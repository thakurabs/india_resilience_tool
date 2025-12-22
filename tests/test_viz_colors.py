"""
Unit tests for viz.colors.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.viz.colors import (
    apply_fillcolor,
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
