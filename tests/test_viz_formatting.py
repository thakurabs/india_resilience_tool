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


def test_jrc_flood_severity_formats_integer_classes_with_label_and_score() -> None:
    assert format_metric_value(4.0, metric_slug="jrc_flood_depth_index_rp100") == "High (4)"
    assert format_metric_compact(5.0, metric_slug="jrc_flood_depth_index_rp100") == "Extreme (5)"
    assert format_metric_number(4.0, metric_slug="jrc_flood_depth_index_rp100") == "4"
    assert get_metric_display_units(metric_slug="jrc_flood_depth_index_rp100") == ""


def test_jrc_flood_severity_formats_non_integer_aggregates_numerically() -> None:
    assert format_metric_value(4.23, metric_slug="jrc_flood_depth_index_rp100") == "4.2 / 5"
    assert format_metric_compact(3.75, metric_slug="jrc_flood_depth_index_rp100") == "3.8 / 5"


def test_label_only_deterioration_renders_bare_delta_label() -> None:
    # Deterioration uses class_display_mode="label_only": a bare delta label with
    # NO numeric suffix, unlike the scarcity label_with_score mode.
    for code, expected in [(0, "No change"), (1, "Worsens by 1 class"), (3, "Worsens by 3 classes")]:
        out = format_metric_compact(code, metric_slug="water_scarcity_deterioration_2050")
        assert out == expected
        assert "(" not in out  # asserts the negative: no " (n)" suffix


def test_label_with_score_scarcity_keeps_numeric_suffix() -> None:
    assert format_metric_compact(3, metric_slug="water_scarcity_percapita") == "Scarcity (3)"
    assert format_metric_value(4, metric_slug="water_scarcity_percapita") == "Absolute scarcity (4)"


def test_class_display_modes_suppress_units() -> None:
    assert get_metric_display_units(metric_slug="water_scarcity_percapita") == ""
    assert get_metric_display_units(metric_slug="water_scarcity_deterioration_2050") == ""
