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


def test_tropical_nights_gt25_metric_is_registered_for_heat_risk() -> None:
    assert "tasmin_tropical_nights_gt25" in METRICS_BY_SLUG

    metric = METRICS_BY_SLUG["tasmin_tropical_nights_gt25"]
    assert metric.value_col == "tropical_nights_gt_25C"
    assert metric.params["thresh_k"] == 25.0 + 273.15

    heat_risk_metrics = set(get_metrics_for_bundle("Heat Risk", spatial_family="admin", level="district"))
    assert "tasmin_tropical_nights_gt25" in heat_risk_metrics
    assert "tasmin_tropical_nights_gt20" not in heat_risk_metrics


def test_heat_stress_metrics_and_bundle_membership_are_registered() -> None:
    assert "twb_summer_mean" in METRICS_BY_SLUG
    assert "tasmin_tropical_nights_gt28" in METRICS_BY_SLUG
    assert "wbd_gt3_le6" in METRICS_BY_SLUG
    assert "wbd_le_3_consecutive_days" in METRICS_BY_SLUG
    assert "twb_days_ge_28" in METRICS_BY_SLUG

    summer = METRICS_BY_SLUG["twb_summer_mean"]
    tropical_nights = METRICS_BY_SLUG["tasmin_tropical_nights_gt28"]
    moderate = METRICS_BY_SLUG["wbd_gt3_le6"]
    consecutive = METRICS_BY_SLUG["wbd_le_3_consecutive_days"]
    threshold = METRICS_BY_SLUG["twb_days_ge_28"]

    assert summer.compute == "wet_bulb_seasonal_mean_stull"
    assert summer.params["months"] == [3, 4, 5]
    assert tropical_nights.params["thresh_k"] == 28.0 + 273.15
    assert moderate.compute == "wet_bulb_depression_days_range_stull"
    assert moderate.params["lower_c"] == 3.0
    assert moderate.params["upper_c"] == 6.0
    assert consecutive.compute == "wet_bulb_depression_longest_run_le_threshold_stull"
    assert consecutive.params["min_spell_days"] == 3
    assert threshold.compute == "wet_bulb_days_ge_threshold_stull"
    assert threshold.params["thresh_c"] == 28.0

    heat_stress_metrics = get_metrics_for_bundle("Heat Stress", spatial_family="admin", level="district")
    assert heat_stress_metrics == [
        "twb_annual_mean",
        "twb_summer_mean",
        "twb_annual_max",
        "twb_days_ge_30",
        "wbd_le_3",
        "wbd_gt3_le6",
        "tasmin_tropical_nights_gt28",
        "tn90p_warm_nights_pct",
        "wbd_le_3_consecutive_days",
        "wsdi_warm_spell_days",
        "twb_days_ge_28",
    ]
    assert "wbd_le_6" not in heat_stress_metrics


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


def test_groundwater_domain_is_admin_district_only() -> None:
    admin_domains = get_domains_for_pillar("Bio-physical Hazards", spatial_family="admin", level="district")
    assert admin_domains == ["Aqueduct Water Risk", "Groundwater Status & Availability"]
    admin_metrics = set(
        get_metrics_for_bundle("Groundwater Status & Availability", spatial_family="admin", level="district")
    )
    assert admin_metrics == {
        "gw_stage_extraction_pct",
        "gw_future_availability_ham",
        "gw_extractable_resource_ham",
        "gw_total_extraction_ham",
    }

    block_domains = get_domains_for_pillar("Bio-physical Hazards", spatial_family="admin", level="block")
    assert block_domains == ["Aqueduct Water Risk"]
