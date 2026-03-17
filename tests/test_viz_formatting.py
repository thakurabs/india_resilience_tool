from __future__ import annotations

from india_resilience_tool.viz.formatting import format_metric_number, format_metric_value


def test_format_metric_number_rounds_population_total_to_whole_persons() -> None:
    assert format_metric_number(1931513.75, metric_slug="population_total") == "1,931,514"


def test_format_metric_value_keeps_population_density_decimal() -> None:
    assert format_metric_value(123.456, metric_slug="population_density", units="people/km2") == "123.46 people/km2"
