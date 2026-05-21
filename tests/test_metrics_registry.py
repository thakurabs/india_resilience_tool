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
    get_domains_for_metric,
    get_domains_for_pillar,
    get_metrics_for_domain,
    get_metrics_for_bundle,
    get_pillar_for_domain,
    get_pillars,
    get_pipeline_bundles,
    validate_registry_against_pipeline,
)
from india_resilience_tool.compute.extreme_rainfall_gridfirst import (
    EXTREME_RAINFALL_GRIDFIRST_SLUGS,
    R95P_BASELINE_YEARS,
    R95P_QUANTILE_METHOD,
    R95P_STRICT_EXCEEDANCE,
)
from india_resilience_tool.compute.heat_risk_gridfirst import HEAT_RISK_GRIDFIRST_SLUGS
from india_resilience_tool.compute.heat_stress_gridfirst import HEAT_STRESS_GRIDFIRST_SLUGS


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


def test_heat_risk_warm_percentile_metrics_use_linear_quantile_and_strict_exceedance() -> None:
    audited = {
        "tx90p_hot_days_pct",
        "tn90p_warm_nights_pct",
        "wsdi_warm_spell_days",
        "hwfi_tmean_90p",
        "hwfi_events_tmean_90p",
        "hwa_heatwave_amplitude",
    }

    assert audited < HEAT_RISK_GRIDFIRST_SLUGS
    for slug in audited:
        spec = METRICS_BY_SLUG[slug]
        assert spec.params["quantile_method"] == "linear"
        assert spec.params["exceed_ge"] is False


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
        "composite_heat_stress",
        "twb_annual_mean",
        "twb_summer_mean",
        "twb_annual_max",
        "twb_days_ge_28",
        "twb_days_ge_30",
        "tasmin_tropical_nights_gt28",
        "tn90p_warm_nights_pct",
        "wsdi_warm_spell_days",
    ]
    assert "wbd_le_6" not in heat_stress_metrics


def test_heat_stress_gridfirst_slugs_do_not_overlap_heat_risk_gridfirst_slugs() -> None:
    assert HEAT_STRESS_GRIDFIRST_SLUGS & HEAT_RISK_GRIDFIRST_SLUGS == set()


def test_wbgt_and_swbgt_metrics_registered_but_not_under_heat_stress() -> None:
    expected = {
        "wbgt_shade_stull_annual_mean": (
            "Shaded WBGT (Annual Mean)",
            "wbgt_shade_stull_annual_mean",
            "wbgt_shade_stull_annual_mean_C",
        ),
        "wbgt_shade_stull_days_ge_28": (
            "Shaded WBGT Days (≥ 28°C)",
            "wbgt_shade_stull_days_ge_threshold",
            "wbgt_shade_stull_days_ge_28_days",
        ),
        "wbgt_shade_stull_days_ge_30": (
            "Shaded WBGT Days (≥ 30°C)",
            "wbgt_shade_stull_days_ge_threshold",
            "wbgt_shade_stull_days_ge_30_days",
        ),
        "wbgt_shade_stull_days_ge_32": (
            "Shaded WBGT Days (≥ 32°C)",
            "wbgt_shade_stull_days_ge_threshold",
            "wbgt_shade_stull_days_ge_32_days",
        ),
        "swbgt_empirical_annual_mean": (
            "Outdoor WBGT (Annual Mean)",
            "swbgt_empirical_annual_mean",
            "swbgt_empirical_annual_mean_C",
        ),
        "swbgt_empirical_days_ge_28": (
            "Outdoor WBGT Days (≥ 28°C)",
            "swbgt_empirical_days_ge_threshold",
            "swbgt_empirical_days_ge_28_days",
        ),
        "swbgt_empirical_days_ge_30": (
            "Outdoor WBGT Days (≥ 30°C)",
            "swbgt_empirical_days_ge_threshold",
            "swbgt_empirical_days_ge_30_days",
        ),
        "swbgt_empirical_days_ge_32": (
            "Outdoor WBGT Days (≥ 32°C)",
            "swbgt_empirical_days_ge_threshold",
            "swbgt_empirical_days_ge_32_days",
        ),
    }

    heat_stress_metrics = set(get_metrics_for_bundle("Heat Stress", spatial_family="admin", level="district"))

    for slug, (label, compute, value_col) in expected.items():
        assert slug in METRICS_BY_SLUG
        assert slug not in heat_stress_metrics

        spec = METRICS_BY_SLUG[slug]
        assert spec.label == label
        assert spec.var == "tas"
        assert list(spec.vars or []) == ["tas", "hurs"]
        assert spec.compute == compute
        assert spec.value_col == value_col
        assert spec.description is not None
        assert "tas" in spec.description
        assert "hurs" in spec.description
        if slug.startswith("wbgt_shade"):
            assert "shaded" in spec.description.lower()
            assert "no-direct-sun" in spec.description.lower()
            assert "direct solar radiation" in spec.description.lower()
            assert "radiant heat load" in spec.description.lower()
        else:
            assert "outdoor heat-stress screening indicator" in spec.description.lower()
            assert "wbgt-style estimate" in spec.description.lower()
            assert "vapour pressure" in spec.description
            assert "solar radiation" in spec.description.lower()
            assert "wind speed" in spec.description.lower()
            assert "black-globe temperature" in spec.description.lower()


def test_wbd_legacy_metrics_registered_but_not_under_heat_stress() -> None:
    legacy_slugs = {"wbd_le_3", "wbd_gt3_le6", "wbd_le_3_consecutive_days", "wbd_le_6"}
    heat_stress_metrics = set(get_metrics_for_bundle("Heat Stress", spatial_family="admin", level="district"))

    assert legacy_slugs <= set(METRICS_BY_SLUG)
    assert legacy_slugs.isdisjoint(heat_stress_metrics)


def test_cold_risk_metrics_and_bundle_membership_are_registered() -> None:
    assert "tasmin_winter_min" in METRICS_BY_SLUG
    assert "tnle10_cold_nights" in METRICS_BY_SLUG
    assert "tnle5_severe_cold_nights" in METRICS_BY_SLUG
    assert "txle15_cold_days" in METRICS_BY_SLUG
    assert "tnle10_consecutive_cold_nights" in METRICS_BY_SLUG

    winter_min = METRICS_BY_SLUG["tasmin_winter_min"]
    cold_nights = METRICS_BY_SLUG["tnle10_cold_nights"]
    severe_nights = METRICS_BY_SLUG["tnle5_severe_cold_nights"]
    cold_days = METRICS_BY_SLUG["txle15_cold_days"]
    consecutive = METRICS_BY_SLUG["tnle10_consecutive_cold_nights"]

    assert winter_min.compute == "seasonal_min"
    assert winter_min.params["months"] == [12, 1, 2]
    assert cold_nights.compute == "count_days_le_threshold"
    assert cold_nights.params["thresh_k"] == 10.0 + 273.15
    assert severe_nights.params["thresh_k"] == 5.0 + 273.15
    assert cold_days.compute == "count_days_le_threshold"
    assert cold_days.params["thresh_k"] == 15.0 + 273.15
    assert consecutive.compute == "longest_consecutive_run_le_threshold"
    assert consecutive.params["min_len"] == 1

    cold_risk_metrics = get_metrics_for_bundle("Cold Risk", spatial_family="admin", level="district")
    assert cold_risk_metrics == [
        "composite_cold_risk",
        "tas_winter_mean",
        "tasmin_winter_mean",
        "tnn_annual_min",
        "tasmin_winter_min",
        "tnle10_cold_nights",
        "tnle5_severe_cold_nights",
        "txle15_cold_days",
        "tx10p_cool_days_pct",
        "tn10p_cool_nights_pct",
        "csdi_cold_spell_days",
        "tnle10_consecutive_cold_nights",
    ]
    assert "fd_frost_days" not in cold_risk_metrics
    assert "tnlt2_cold_nights" not in cold_risk_metrics


def test_drought_risk_metrics_and_bundle_membership_are_registered() -> None:
    from india_resilience_tool.compute.drought_risk_gridfirst import DROUGHT_GRIDFIRST_SLUGS

    assert "spi3_count_events_lt_minus1" in METRICS_BY_SLUG
    assert "spi6_count_events_lt_minus1" in METRICS_BY_SLUG
    assert "spi12_count_events_lt_minus1" in METRICS_BY_SLUG

    spi3 = METRICS_BY_SLUG["spi3_count_events_lt_minus1"]
    spi6 = METRICS_BY_SLUG["spi6_count_events_lt_minus1"]
    spi12 = METRICS_BY_SLUG["spi12_count_events_lt_minus1"]

    assert spi3.compute == "standardised_precipitation_index"
    assert spi3.params["scale_months"] == 3
    assert spi3.params["annual_aggregation"] == "count_events_lt"
    assert spi3.params["threshold"] == -1.0
    assert spi6.params["scale_months"] == 6
    assert spi12.params["scale_months"] == 12

    drought_metrics = get_metrics_for_bundle("Drought Risk", spatial_family="admin", level="district")
    assert drought_metrics == [
        "composite_drought_risk",
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "spi6_max_spell_lt_minus1",
        "spi12_max_spell_lt_minus1",
    ]
    for slug in drought_metrics[1:]:
        params = METRICS_BY_SLUG[slug].params
        assert int(params["min_event_months"]) > 0
    for slug in ("spi3_max_spell_lt_minus1", "spi6_max_spell_lt_minus1", "spi12_max_spell_lt_minus1"):
        assert METRICS_BY_SLUG[slug].params["period_rollup"] == "period_max"
    assert set(drought_metrics[1:]) == set(DROUGHT_GRIDFIRST_SLUGS)


def test_flood_bundle_membership_remains_the_current_six_metric_set() -> None:
    flood_metrics = get_metrics_for_bundle(
        "Extreme Rainfall | Flash Flood Risk",
        spatial_family="admin",
        level="district",
    )
    assert flood_metrics == [
        "composite_flood_extreme_rainfall_risk",
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "r20mm_very_heavy_precip_days",
        "r95p_very_wet_precip",
        "r95ptot_contribution_pct",
        "cwd_consecutive_wet_days",
    ]
    assert set(flood_metrics[1:]) == set(EXTREME_RAINFALL_GRIDFIRST_SLUGS)


def test_extreme_rainfall_v2_keeps_registry_and_admin_semantics_explicit() -> None:
    r20 = METRICS_BY_SLUG["r20mm_very_heavy_precip_days"]
    rainy = METRICS_BY_SLUG["rain_gt_2p5mm"]
    r95p = METRICS_BY_SLUG["r95p_very_wet_precip"]
    r95ptot = METRICS_BY_SLUG["r95ptot_contribution_pct"]

    assert r20.params["thresh_mm"] == 20.0
    assert r20.params["exceed_ge"] is True
    assert "exceed_ge" not in rainy.params
    assert rainy.params["thresh_mm"] == 2.5
    assert r95p.params["baseline_years"] == (1981, 2010)
    assert r95p.params["quantile_method"] == "nearest"
    assert r95p.params["exceed_ge"] is True
    assert r95ptot.params["baseline_years"] == (1981, 2010)
    assert r95ptot.params["quantile_method"] == "nearest"
    assert r95ptot.params["exceed_ge"] is True
    assert R95P_BASELINE_YEARS == (1990, 2010)
    assert R95P_QUANTILE_METHOD == "linear"
    assert R95P_STRICT_EXCEEDANCE is True


def test_proposal_pipeline_metrics_are_registered_without_changing_dashboard_domains() -> None:
    assert "r99p_extreme_wet_precip" in METRICS_BY_SLUG
    assert "pr_2day_heavy_rainfall_events_ge150mm" in METRICS_BY_SLUG

    r99p = METRICS_BY_SLUG["r99p_extreme_wet_precip"]
    heavy_rain = METRICS_BY_SLUG["pr_2day_heavy_rainfall_events_ge150mm"]

    assert r99p.compute == "percentile_precipitation_total"
    assert r99p.params["percentile"] == 99
    assert r99p.params["quantile_method"] == "nearest"
    assert r99p.params["exceed_ge"] is True
    assert heavy_rain.compute == "consecutive_heavy_rainfall_events"
    assert heavy_rain.params["daily_thresh_mm"] == 150.0
    assert heavy_rain.params["min_event_days"] == 2

    flood_metrics = get_metrics_for_bundle(
        "Extreme Rainfall | Flash Flood Risk",
        spatial_family="admin",
        level="district",
    )
    assert "r99p_extreme_wet_precip" not in flood_metrics
    assert "pr_2day_heavy_rainfall_events_ge150mm" not in flood_metrics


def test_agricultural_risk_bundle_membership_uses_final_seven_metric_mix_and_legacy_alias() -> None:
    agriculture_metrics = get_metrics_for_bundle(
        "Agricultural Risk",
        spatial_family="admin",
        level="district",
    )
    assert agriculture_metrics == [
        "composite_agricultural_risk",
        "txx_annual_max",
        "txge35_extreme_heat_days",
        "wsdi_warm_spell_days",
        "spi3_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "pr_max_5day_precip",
        "tnle10_cold_nights",
    ]
    assert get_metrics_for_bundle(
        "Agriculture & Growing Conditions",
        spatial_family="admin",
        level="district",
    ) == agriculture_metrics


def test_visible_glance_composites_are_first_in_admin_domains_and_hidden_from_hydro() -> None:
    expected = {
        "Heat Risk": "composite_heat_risk",
        "Drought Risk": "composite_drought_risk",
        "Extreme Rainfall | Flash Flood Risk": "composite_flood_extreme_rainfall_risk",
        "Heat Stress": "composite_heat_stress",
        "Cold Risk": "composite_cold_risk",
    }

    for domain, slug in expected.items():
        district_metrics = get_metrics_for_bundle(domain, spatial_family="admin", level="district")
        block_metrics = get_metrics_for_bundle(domain, spatial_family="admin", level="block")
        assert district_metrics[0] == slug
        assert block_metrics[0] == slug
        assert slug not in get_metrics_for_bundle(domain, spatial_family="hydro", level="basin")

        spec = METRICS_BY_SLUG[slug]
        assert spec.source_type == "derived"
        assert spec.selection_mode == "scenario_period"
        assert spec.supported_statistics == ("mean",)
        assert spec.supported_spatial_families == ("admin",)
        assert spec.supported_levels == ("district", "block")
        assert spec.supports_yearly_trend is False
        assert spec.supports_baseline_comparison is False
        assert spec.supports_scenario_comparison is False


def test_dashboard_only_metrics_do_not_leak_into_pipeline_bundles() -> None:
    pipeline_bundles = get_pipeline_bundles()
    dashboard_only = {
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
        "jrc_flood_depth_index_rp100",
        "jrc_flood_extent_rp100",
        "jrc_flood_depth_rp10",
        "jrc_flood_depth_rp50",
        "jrc_flood_depth_rp100",
        "jrc_flood_depth_rp500",
    }
    assert dashboard_only.isdisjoint({slug for slugs in pipeline_bundles.values() for slug in slugs})
    for slug in dashboard_only:
        assert slug in METRICS_BY_SLUG


def test_sector_wise_domains_expand_to_composite_plus_source_metrics_in_exact_order() -> None:
    assert get_metrics_for_domain(
        "Infrastructure Risk",
        spatial_family="admin",
        level="district",
    ) == [
        "composite_infrastructure_risk",
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "txx_annual_max",
    ]
    assert get_metrics_for_domain(
        "Infrastructure Risk",
        spatial_family="admin",
        level="block",
    ) == [
        "composite_infrastructure_risk",
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "txx_annual_max",
    ]
    assert get_metrics_for_domain(
        "Health Risk",
        spatial_family="admin",
        level="district",
    ) == [
        "composite_health_risk",
        "txx_annual_max",
        "wsdi_warm_spell_days",
        "tnx_annual_max",
        "pr_max_1day_precip",
        "cwd_consecutive_wet_days",
    ]


def test_sector_wise_domains_remain_hidden_in_unsupported_contexts() -> None:
    assert get_metrics_for_domain(
        "Health Risk",
        spatial_family="admin",
        level="basin",
    ) == []
    assert get_metrics_for_domain(
        "Health Risk",
        spatial_family="hydro",
        level="basin",
    ) == []


def test_sector_wise_reverse_membership_is_context_aware() -> None:
    district_domains = get_domains_for_metric(
        "pr_max_1day_precip",
        spatial_family="admin",
        level="district",
    )
    assert "Extreme Rainfall | Flash Flood Risk" in district_domains
    assert "Health Risk" in district_domains
    assert "Industrial Risk" in district_domains
    assert "Infrastructure Risk" in district_domains
    assert "Life & Livelihood Loss Risk" in district_domains
    assert "Agricultural Risk" in get_domains_for_metric(
        "pr_max_5day_precip",
        spatial_family="admin",
        level="district",
    )

    block_domains = get_domains_for_metric(
        "pr_max_1day_precip",
        spatial_family="admin",
        level="block",
    )
    assert "Life & Livelihood Loss Risk" in block_domains

    hydro_domains = get_domains_for_metric(
        "pr_max_1day_precip",
        spatial_family="hydro",
        level="basin",
    )
    assert "Health Risk" not in hydro_domains
    assert "Infrastructure Risk" not in hydro_domains


def test_hydropower_sector_bundle_includes_helper_metric() -> None:
    assert get_metrics_for_domain(
        "Asset Risk (Hydropower Plants)",
        spatial_family="admin",
        level="district",
    ) == [
        "composite_asset_risk_hydropower",
        "pr_max_5day_precip",
        "pr_consecutive_dry_days_lt1mm",
        "r95p_interannual_variability",
    ]


def test_get_pipeline_bundles_remains_static_for_sector_wise_domains() -> None:
    pipeline_bundles = get_pipeline_bundles()
    assert "Health Risk" not in pipeline_bundles
    assert "Infrastructure Risk" not in pipeline_bundles
    assert "Life & Livelihood Loss Risk" not in pipeline_bundles


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


def test_aqueduct_domain_is_unassigned_from_pillars() -> None:
    domains = get_domains_for_pillar("Bio-physical Hazards", spatial_family="hydro", level="basin")
    assert domains == []
    assert get_pillar_for_domain("Aqueduct Water Risk") == ""
    assert get_pillar_for_domain("Water Risk") == ""
    assert "Aqueduct" in get_domain_description("Aqueduct Water Risk")


def test_default_domain_remains_heat_risk_for_climate_hazards() -> None:
    assert get_default_domain(
        pillar="Climate Hazards",
        spatial_family="admin",
        level="district",
    ) == "Heat Risk"


def test_population_exposure_domain_is_admin_only() -> None:
    admin_domains = get_domains_for_pillar("Exposure", spatial_family="admin", level="district")
    assert admin_domains == [
        "Population Exposure",
        "Rural Facilities Exposure",
        "Built-up Area Exposure",
        "Agricultural LULC Exposure",
    ]
    admin_metrics = set(get_metrics_for_bundle("Population Exposure", spatial_family="admin", level="block"))
    assert admin_metrics == {"population_total", "population_density"}
    rural_metrics = set(get_metrics_for_bundle("Rural Facilities Exposure", spatial_family="admin", level="block"))
    assert "rural_facilities_total_count" in rural_metrics
    assert "rural_facilities_total_count_per_100k" in rural_metrics
    built_metrics = set(get_metrics_for_bundle("Built-up Area Exposure", spatial_family="admin", level="block"))
    assert built_metrics == {"built_up_area_km2", "built_up_area_share_pct"}
    assert METRICS_BY_SLUG["built_up_area_km2"].fixed_scenario == "snapshot"
    assert METRICS_BY_SLUG["built_up_area_share_pct"].fixed_period == "Current"

    hydro_pillars = get_pillars(spatial_family="hydro", level="basin")
    assert "Exposure" not in hydro_pillars


def test_groundwater_domain_is_admin_district_only() -> None:
    admin_domains = get_domains_for_pillar("Bio-physical Hazards", spatial_family="admin", level="district")
    assert admin_domains == [
        "Groundwater Status & Availability",
        "Riverine Flood",
    ]
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
    assert block_domains == ["Riverine Flood"]


def test_jrc_flood_depth_domain_is_admin_only_and_telangana_restricted() -> None:
    district_metrics = get_metrics_for_bundle(
        "Riverine Flood",
        spatial_family="admin",
        level="district",
    )
    block_metrics = get_metrics_for_bundle(
        "Riverine Flood",
        spatial_family="admin",
        level="block",
    )
    assert district_metrics == [
        "composite_flood_jrc_depth",
        "jrc_flood_depth_index_rp100",
        "jrc_flood_extent_rp100",
        "jrc_flood_depth_rp100",
    ]
    assert block_metrics == district_metrics
    # Composite is first so Deep Dive opens the same choropleth as Glance.
    assert district_metrics[0] == "composite_flood_jrc_depth"
    assert get_metrics_for_bundle(
        "Riverine Flood",
        spatial_family="hydro",
        level="basin",
    ) == []
    extent_spec = METRICS_BY_SLUG["jrc_flood_extent_rp100"]
    assert extent_spec.units == "fraction"
    assert extent_spec.display_units == "%"
    assert extent_spec.display_scale == 100.0
