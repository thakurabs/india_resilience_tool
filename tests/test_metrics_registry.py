"""
Unit tests for the shared metrics registry.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.config.metrics_registry import (
    METRICS_BY_SLUG,
    PIPELINE_METRICS,
    PIPELINE_METRICS_RAW,
    find_duplicate_slugs,
    validate_registry_against_pipeline,
)


def test_pipeline_metrics_present() -> None:
    assert isinstance(PIPELINE_METRICS_RAW, list)
    assert len(PIPELINE_METRICS_RAW) > 0
    assert len(PIPELINE_METRICS) == len(PIPELINE_METRICS_RAW)


def test_default_periods_metric_col_matches_value_col() -> None:
    for spec in PIPELINE_METRICS:
        if spec.value_col:
            assert spec.periods_metric_col == spec.value_col


def test_duplicate_detection_is_stable() -> None:
    dupes = find_duplicate_slugs(PIPELINE_METRICS_RAW)
    # Current pipeline list includes at least one duplicate slug; we preserve behavior.
    assert "tasmin_tropical_nights_gt20" in dupes


def test_validate_registry_against_pipeline_reports_duplicates_but_no_mismatch() -> None:
    issues = validate_registry_against_pipeline(METRICS_BY_SLUG, PIPELINE_METRICS_RAW)
    # Expect a duplicate warning line
    assert any("Duplicate pipeline metric slugs detected" in s for s in issues)
    # Should not report the fragile mismatch we care about
    assert not any("periods_metric_col" in s and "value_col" in s for s in issues)
