from __future__ import annotations

import math

from india_resilience_tool.viz.formatting import (
    format_metric_compact,
    format_metric_number,
    format_metric_value,
    get_metric_display_units,
)


def test_format_metric_number_rounds_population_total_to_whole_persons() -> None:
    assert format_metric_number(1931513.75, metric_slug="population_total") == "1,931,514"


def test_format_metric_value_keeps_population_density_decimal() -> None:
    assert format_metric_value(123.456, metric_slug="population_density", units="people/km2") == "123.46 people/km2"


def test_format_metric_number_renders_nan_as_na_dash() -> None:
    assert format_metric_number(math.nan, metric_slug="population_total") == "—"


def test_jrc_flood_extent_scales_fraction_for_display() -> None:
    assert format_metric_number(0.34, metric_slug="jrc_flood_extent_rp100") == "34"
    assert format_metric_value(0.34, metric_slug="jrc_flood_extent_rp100") == "34%"
    assert format_metric_compact(0.34, metric_slug="jrc_flood_extent_rp100") == "34%"
    assert get_metric_display_units(metric_slug="jrc_flood_extent_rp100") == "%"
