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
    build_registry_from_pipeline,
    find_duplicate_slugs,
    get_default_domain,
    get_default_pillar,
    get_bundles,
    get_domain_description,
    get_domains_for_pillar,
    get_metrics_for_bundle,
    get_pillar_for_domain,
    get_pillars,
    get_pipeline_bundles,
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
    # Do not rely on real registry contents having duplicates; validate the helper
    # against a small synthetic list.
    pipeline = [
        {"slug": "a", "value_col": "a_val"},
        {"slug": "b", "value_col": "b_val"},
        {"slug": "a", "value_col": "a_val"},
        {"slug": ""},  # ignored
        {},  # ignored
    ]
    dupes = find_duplicate_slugs(pipeline)
    assert dupes == ["a"]


def test_validate_registry_against_pipeline_reports_duplicates_but_no_mismatch() -> None:
    pipeline = [
        {"slug": "x", "name": "X", "var": "tas", "value_col": "x_val"},
        {"slug": "y", "name": "Y", "var": "tas", "value_col": "y_val"},
        {"slug": "x", "name": "X duplicate", "var": "tas", "value_col": "x_val"},
    ]
    reg = build_registry_from_pipeline(pipeline)
    issues = validate_registry_against_pipeline(reg, pipeline)
    assert any("Duplicate pipeline metric slugs detected" in s for s in issues)
    assert not any("periods_metric_col" in s and "value_col" in s for s in issues)


def test_wbd_metrics_registered() -> None:
    assert "wbd_le_3" in METRICS_BY_SLUG
    assert "wbd_le_6" in METRICS_BY_SLUG

    severe = METRICS_BY_SLUG["wbd_le_3"]
    humid = METRICS_BY_SLUG["wbd_le_6"]

    assert severe.compute == "wet_bulb_depression_days_le_threshold_stull"
    assert humid.compute == "wet_bulb_depression_days_le_threshold_stull"
    assert severe.value_col == "wbd_le_3_days"
    assert humid.value_col == "wbd_le_6_days"


def test_dashboard_only_metrics_do_not_leak_into_pipeline_bundles() -> None:
    pipeline_bundles = get_pipeline_bundles()
    dashboard_only = {
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
    }
    assert dashboard_only.isdisjoint({slug for slugs in pipeline_bundles.values() for slug in slugs})
    for slug in dashboard_only:
        assert slug in METRICS_BY_SLUG


def test_aqueduct_metric_is_context_limited_to_supported_views() -> None:
    admin_district_metrics = set(get_metrics_for_bundle("Aqueduct Water Risk", spatial_family="admin", level="district"))
    admin_block_metrics = set(get_metrics_for_bundle("Aqueduct Water Risk", spatial_family="admin", level="block"))
    assert "Aqueduct Water Risk" in get_bundles(spatial_family="hydro", level="basin")
    hydro_metrics = set(get_metrics_for_bundle("Aqueduct Water Risk", spatial_family="hydro", level="sub_basin"))
    assert "Aqueduct Water Risk" in get_bundles(spatial_family="admin", level="district")
    assert {
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
    }.issubset(hydro_metrics)
    assert {
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
    }.issubset(admin_district_metrics)
    assert {
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
    }.issubset(admin_block_metrics)


def test_taxonomy_exposes_climate_and_biophysical_pillars() -> None:
    pillars = get_pillars(spatial_family="admin", level="district")
    assert "Climate Hazards" in pillars
    assert "Bio-physical Hazards" in pillars
    assert "Exposure" in pillars
    assert get_default_pillar(spatial_family="admin", level="district") == "Climate Hazards"


def test_aqueduct_domain_lives_under_biophysical_hazards() -> None:
    domains = get_domains_for_pillar("Bio-physical Hazards", spatial_family="hydro", level="basin")
    assert domains == ["Aqueduct Water Risk"]
    assert get_pillar_for_domain("Aqueduct Water Risk") == "Bio-physical Hazards"
    assert get_pillar_for_domain("Water Risk") == "Bio-physical Hazards"
    assert "Aqueduct" in get_domain_description("Aqueduct Water Risk")


def test_default_domain_remains_heat_risk_for_climate_hazards() -> None:
    assert get_default_domain(
        pillar="Climate Hazards",
        spatial_family="admin",
        level="district",
    ) == "Heat Risk"


def test_population_exposure_domain_is_admin_only() -> None:
    admin_domains = get_domains_for_pillar("Exposure", spatial_family="admin", level="district")
    assert admin_domains == ["Population Exposure"]
    admin_metrics = set(get_metrics_for_bundle("Population Exposure", spatial_family="admin", level="block"))
    assert admin_metrics == {"population_total", "population_density"}

    hydro_pillars = get_pillars(spatial_family="hydro", level="basin")
    assert "Exposure" not in hydro_pillars
