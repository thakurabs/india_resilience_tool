"""Regression tests for compact map legend HTML."""

from india_resilience_tool.app.map_pipeline import _build_legend_title
from india_resilience_tool.viz.colors import build_vertical_binned_legend_block_html


def test_build_legend_title_uses_metric_units_only() -> None:
    """Legend title should use only the metric unit text."""
    assert _build_legend_title({"unit": "°C", "label": "Annual Mean Temperature"}) == "°C"


def test_build_legend_title_omits_missing_units() -> None:
    """Metrics without units should produce an empty legend title."""
    assert _build_legend_title({"label": "Unitless Metric"}) == ""


def test_build_vertical_binned_legend_block_html_omits_empty_title() -> None:
    """Legend HTML should not render a vertical title block when no title is supplied."""
    html = build_vertical_binned_legend_block_html(
        legend_title="",
        vmin=1.0,
        vmax=5.0,
        cmap_name="Reds",
        map_height=560,
    )
    assert "writing-mode: vertical-rl" not in html


def test_build_vertical_binned_legend_block_html_renders_compact_title() -> None:
    """Legend HTML should render the compact title text when a unit is provided."""
    html = build_vertical_binned_legend_block_html(
        legend_title="°C",
        vmin=1.0,
        vmax=5.0,
        cmap_name="Reds",
        map_height=560,
    )
    assert ">°C</div>" in html


def test_build_vertical_binned_legend_block_html_centers_full_legend() -> None:
    """Legend HTML should center the entire legend assembly in the iframe."""
    html = build_vertical_binned_legend_block_html(
        legend_title="°C",
        vmin=1.0,
        vmax=5.0,
        cmap_name="Reds",
        map_height=560,
    )
    assert "height: 100%; width: 100%; display: flex; align-items: center; justify-content: center;" in html
