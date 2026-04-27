from __future__ import annotations

from india_resilience_tool.config.dashboard_bundles import SECTOR_WISE_DASHBOARD_BUNDLES
from india_resilience_tool.config.metrics_registry import DOMAINS, PIPELINE_SLUGS
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES,
    get_proposal_bundle_spec_by_slug,
    is_proposal_bundle_slug,
    proposal_available_rule_count_column,
    proposal_bundle_mean_column,
    proposal_rule_score_column,
    validate_proposal_bundle_specs,
)
from india_resilience_tool.config.variables import VARIABLES


def test_proposal_bundle_specs_validate_cleanly() -> None:
    assert validate_proposal_bundle_specs() == []


def test_proposal_bundle_labels_slugs_and_rule_order_are_exact() -> None:
    expected = [
        (
            "Agricultural Risk",
            "composite_agricultural_risk",
            [
                "rx1day_ge_200",
                "rx5day_ge_300",
                "cdd_ge_20",
                "txx_ge_40",
                "r95p_change_gt_20pct_vs_baseline",
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
            ["rx1day_ge_200", "heavy_rain_2day_event_ge_1", "cdd_ge_40", "wsdi_ge_5"],
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
            "rx1day_ge_200": "1-day rainfall >= 200 mm",
            "rx5day_ge_300": "5-day rainfall >= 300 mm",
            "cdd_ge_20": "Consecutive dry days >= 20",
            "txx_ge_40": "Annual max temperature >= 40 degC",
            "r95p_change_gt_20pct_vs_baseline": "Very wet precipitation change > 20% vs baseline",
        },
        "Health Risk": {
            "txx_ge_45": "Annual max temperature >= 45 degC",
            "wsdi_ge_5": "Warm spell days >= 5",
            "tnx_ge_30": "Annual max nighttime temperature >= 30 degC",
            "rx1day_ge_200": "1-day rainfall >= 200 mm",
            "cwd_ge_5": "Consecutive wet days >= 5",
        },
        "Industrial Risk": {
            "rx1day_ge_150": "1-day rainfall >= 150 mm",
            "rx5day_ge_250": "5-day rainfall >= 250 mm",
            "cdd_ge_30": "Consecutive dry days >= 30",
            "txx_ge_45": "Annual max temperature >= 45 degC",
        },
        "Investment / Financial Risk": {
            "rx1day_positive_trend": "Increasing 1-day rainfall intensity",
            "rx5day_positive_trend": "Increasing 5-day rainfall intensity",
            "r99p_positive_trend": "Increasing extreme wet precipitation",
            "cdd_change_gt_20pct_vs_baseline": "Consecutive dry days change > 20% vs baseline",
            "hwfi_positive_trend": "Increasing heatwave frequency",
        },
        "Infrastructure Risk": {
            "rx1day_ge_200": "1-day rainfall >= 200 mm",
            "rx5day_ge_400": "5-day rainfall >= 400 mm",
            "txx_ge_45": "Annual max temperature >= 45 degC",
        },
        "Asset Risk (Thermal Power Plants)": {
            "cdd_ge_30": "Consecutive dry days >= 30",
            "txx_ge_45": "Annual max temperature >= 45 degC",
            "spi3_low_flow_proxy_norm": "Low-flow drought proxy severity",
        },
        "Asset Risk (Hydropower Plants)": {
            "rx5day_ge_500": "5-day rainfall >= 500 mm",
            "cdd_ge_60": "Consecutive dry days >= 60",
            "r95p_interannual_variability_norm": "Very wet precipitation variability severity",
        },
        "Life & Livelihood Loss Risk": {
            "rx1day_ge_200": "1-day rainfall >= 200 mm",
            "heavy_rain_2day_event_ge_1": "2-day heavy rainfall events >= 1",
            "cdd_ge_40": "Consecutive dry days >= 40",
            "wsdi_ge_5": "Warm spell days >= 5",
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


def test_proposal_persisted_column_helpers_are_exact() -> None:
    assert proposal_rule_score_column("txx_ge_45", "ssp585", "2040-2060") == "txx_ge_45__ssp585__2040-2060__score"
    assert (
        proposal_bundle_mean_column("composite_health_risk", "ssp585", "2040-2060")
        == "composite_health_risk__ssp585__2040-2060__mean"
    )
    assert (
        proposal_available_rule_count_column("composite_health_risk", "ssp585", "2040-2060")
        == "composite_health_risk__ssp585__2040-2060__available_rule_count"
    )


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
