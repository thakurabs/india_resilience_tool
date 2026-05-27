"""Tests for proposal bundle configuration.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.config.dashboard_bundles import SECTOR_WISE_DASHBOARD_BUNDLES
from india_resilience_tool.config.metrics_registry import DOMAINS, PIPELINE_SLUGS
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES,
    get_proposal_bundle_source_metric_slugs,
    get_proposal_bundle_spec_by_slug,
    is_proposal_bundle_slug,
    proposal_available_rule_count_column,
    proposal_available_rule_weight_fraction_column,
    proposal_bundle_mean_column,
    proposal_rule_abs_score_column,
    proposal_rule_chg_score_column,
    proposal_rule_imp_score_column,
    proposal_rule_score_column,
    validate_proposal_bundle_specs,
)
from india_resilience_tool.config.variables import VARIABLES


def test_proposal_bundle_specs_validate_cleanly() -> None:
    assert validate_proposal_bundle_specs() == []


def test_proposal_rules_use_continuous_phase_one_pressure_schema() -> None:
    for spec in PROPOSAL_BUNDLES:
        for rule in spec.rules:
            assert rule.rule_type in {"blended", "trend"}
            assert rule.direction == "higher_worse"
            if rule.rule_type == "blended":
                assert rule.absolute_weight + rule.change_weight + rule.impact_weight > 0.0


def test_proposal_bundle_labels_slugs_and_rule_order_are_exact() -> None:
    expected = [
        (
            "Agricultural Risk",
            "composite_agricultural_risk",
            [
                "txx_peak_crop_heat",
                "txge35_damaging_heat_days",
                "wsdi_persistent_heat",
                "spi3_drought_episodes",
                "spi3_longest_drought_spell",
                "rx5day_heavy_rainfall",
                "tnle10_cold_nights",
            ],
        ),
        (
            "Health Risk",
            "composite_health_risk",
            ["txx_ge_45", "wsdi_ge_5", "tnx_ge_30", "rx1day_ge_200", "cwd_ge_5"],
        ),
        (
            "Industrial Risk",
            "composite_industrial_risk",
            ["rx1day_ge_150", "rx5day_ge_250", "cdd_ge_30", "txx_ge_45"],
        ),
        (
            "Investment / Financial Risk",
            "composite_investment_financial_risk",
            [
                "rx1day_positive_trend",
                "rx5day_positive_trend",
                "r99p_positive_trend",
                "cdd_change_gt_20pct_vs_baseline",
                "hwfi_positive_trend",
            ],
        ),
        (
            "Infrastructure Risk",
            "composite_infrastructure_risk",
            ["rx1day_ge_200", "rx5day_ge_400", "txx_ge_45"],
        ),
        (
            "Asset Risk (Thermal Power Plants)",
            "composite_asset_risk_thermal_power",
            ["cdd_ge_30", "txx_ge_45", "spi3_low_flow_proxy_norm"],
        ),
        (
            "Asset Risk (Hydropower Plants)",
            "composite_asset_risk_hydropower",
            ["rx5day_ge_500", "cdd_ge_60", "r95p_interannual_variability_norm"],
        ),
        (
            "Life & Livelihood Loss Risk",
            "composite_life_livelihood_loss_risk",
            ["rx1day_ge_200", "rx5day_livelihood_pressure", "cdd_ge_40", "wsdi_ge_5"],
        ),
    ]
    observed = [
        (spec.bundle_label, spec.composite_slug, [rule.rule_slug for rule in spec.rules])
        for spec in PROPOSAL_BUNDLES
    ]
    assert observed == expected


def test_proposal_bundle_rule_display_labels_are_exact() -> None:
    observed = {
        spec.bundle_label: {rule.rule_slug: rule.display_label for rule in spec.rules}
        for spec in PROPOSAL_BUNDLES
    }
    assert observed == {
        "Agricultural Risk": {
            "txx_peak_crop_heat": "Peak crop heat",
            "txge35_damaging_heat_days": "Damaging heat days",
            "wsdi_persistent_heat": "Persistent heat",
            "spi3_drought_episodes": "Drought episodes",
            "spi3_longest_drought_spell": "Longest drought spell",
            "rx5day_heavy_rainfall": "5-day heavy rainfall",
            "tnle10_cold_nights": "Cold nights",
        },
        "Health Risk": {
            "txx_ge_45": "Extreme daytime heat pressure",
            "wsdi_ge_5": "Warm-spell duration pressure",
            "tnx_ge_30": "Night-time heat pressure",
            "rx1day_ge_200": "1-day rainfall disruption pressure",
            "cwd_ge_5": "Consecutive wet-day pressure",
        },
        "Industrial Risk": {
            "rx1day_ge_150": "1-day rainfall disruption pressure",
            "rx5day_ge_250": "5-day rainfall disruption pressure",
            "cdd_ge_30": "Dry-spell water-stress pressure",
            "txx_ge_45": "Extreme heat operations pressure",
        },
        "Investment / Financial Risk": {
            "rx1day_positive_trend": "1-day rainfall intensity trend pressure",
            "rx5day_positive_trend": "5-day rainfall intensity trend pressure",
            "r99p_positive_trend": "Extreme wet precipitation trend pressure",
            "cdd_change_gt_20pct_vs_baseline": "Dry-spell change pressure",
            "hwfi_positive_trend": "Heatwave frequency trend pressure",
        },
        "Infrastructure Risk": {
            "rx1day_ge_200": "1-day rainfall design pressure",
            "rx5day_ge_400": "5-day rainfall design pressure",
            "txx_ge_45": "Extreme heat asset pressure",
        },
        "Asset Risk (Thermal Power Plants)": {
            "cdd_ge_30": "Dry-spell cooling-water pressure",
            "txx_ge_45": "Extreme heat cooling-efficiency pressure",
            "spi3_low_flow_proxy_norm": "Low-flow drought proxy pressure",
        },
        "Asset Risk (Hydropower Plants)": {
            "rx5day_ge_500": "5-day rainfall operations pressure",
            "cdd_ge_60": "Dry-spell flow pressure",
            "r95p_interannual_variability_norm": "Very wet precipitation variability pressure",
        },
        "Life & Livelihood Loss Risk": {
            "rx1day_ge_200": "1-day rainfall exposure pressure",
            "rx5day_livelihood_pressure": "5-day rainfall exposure pressure",
            "cdd_ge_40": "Dry-spell livelihood pressure",
            "wsdi_ge_5": "Warm-spell livelihood pressure",
        },
    }


def test_sector_wise_dashboard_catalog_matches_proposal_bundle_order() -> None:
    observed = [(spec.bundle_label, spec.composite_slug) for spec in PROPOSAL_BUNDLES]
    expected = [(spec.canonical_bundle, spec.composite_slug) for spec in SECTOR_WISE_DASHBOARD_BUNDLES]
    assert expected == observed


def test_get_proposal_bundle_spec_by_slug_returns_expected_bundle() -> None:
    spec = get_proposal_bundle_spec_by_slug("composite_health_risk")

    assert spec is not None
    assert spec.bundle_label == "Health Risk"
    assert [rule.rule_slug for rule in spec.rules] == ["txx_ge_45", "wsdi_ge_5", "tnx_ge_30", "rx1day_ge_200", "cwd_ge_5"]


def test_is_proposal_bundle_slug_matches_catalog() -> None:
    assert is_proposal_bundle_slug("composite_health_risk") is True
    assert is_proposal_bundle_slug("composite_heat_risk") is False


def test_get_proposal_bundle_source_metric_slugs_are_exact_and_deduplicated() -> None:
    assert get_proposal_bundle_source_metric_slugs("composite_agricultural_risk") == (
        "txx_annual_max",
        "txge35_extreme_heat_days",
        "wsdi_warm_spell_days",
        "spi3_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "pr_max_5day_precip",
        "tnle10_cold_nights",
    )
    assert get_proposal_bundle_source_metric_slugs("composite_infrastructure_risk") == (
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "txx_annual_max",
    )
    assert get_proposal_bundle_source_metric_slugs("composite_health_risk") == (
        "txx_annual_max",
        "wsdi_warm_spell_days",
        "tnx_annual_max",
        "pr_max_1day_precip",
        "cwd_consecutive_wet_days",
    )


def test_get_proposal_bundle_source_metric_slugs_returns_empty_tuple_for_unknown_slug() -> None:
    assert get_proposal_bundle_source_metric_slugs("unknown_slug") == ()


def test_proposal_persisted_column_helpers_are_exact() -> None:
    assert proposal_rule_score_column("txx_ge_45", "ssp585", "2040-2060") == "txx_ge_45__ssp585__2040-2060__score"
    assert (
        proposal_rule_abs_score_column("txx_ge_45", "ssp585", "2040-2060")
        == "txx_ge_45__ssp585__2040-2060__abs_score"
    )
    assert (
        proposal_rule_chg_score_column("txx_ge_45", "ssp585", "2040-2060")
        == "txx_ge_45__ssp585__2040-2060__chg_score"
    )
    assert (
        proposal_rule_imp_score_column("txx_ge_45", "ssp585", "2040-2060")
        == "txx_ge_45__ssp585__2040-2060__imp_score"
    )
    assert (
        proposal_bundle_mean_column("composite_health_risk", "ssp585", "2040-2060")
        == "composite_health_risk__ssp585__2040-2060__mean"
    )
    assert (
        proposal_available_rule_count_column("composite_health_risk", "ssp585", "2040-2060")
        == "composite_health_risk__ssp585__2040-2060__available_rule_count"
    )
    assert (
        proposal_available_rule_weight_fraction_column("composite_health_risk", "ssp585", "2040-2060")
        == "composite_health_risk__ssp585__2040-2060__available_rule_weight_fraction"
    )


def test_agricultural_risk_uses_explicit_normalized_rule_weights() -> None:
    spec = get_proposal_bundle_spec_by_slug("composite_agricultural_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70
    assert [rule.rule_weight for rule in spec.rules] == [0.10, 0.10, 0.10, 0.15, 0.15, 0.20, 0.20]
    assert all(rule.absolute_weight == 1.0 and rule.change_weight == 0.0 and rule.impact_weight == 0.0 for rule in spec.rules)


def test_sector_wise_dashboard_bundles_have_matching_proposal_specs() -> None:
    for spec in SECTOR_WISE_DASHBOARD_BUNDLES:
        proposal_spec = get_proposal_bundle_spec_by_slug(spec.composite_slug)
        assert proposal_spec is not None
        assert proposal_spec.bundle_label == spec.canonical_bundle


def test_sector_wise_proposal_bundles_are_exposed_in_dashboard_registry_but_not_pipeline() -> None:
    proposal_slugs = {spec.composite_slug for spec in PROPOSAL_BUNDLES}
    domain_slugs = {slug for slugs in DOMAINS.values() for slug in slugs}
    assert proposal_slugs.issubset(domain_slugs)
    assert proposal_slugs.issubset(set(VARIABLES))
    assert proposal_slugs.isdisjoint(PIPELINE_SLUGS)
