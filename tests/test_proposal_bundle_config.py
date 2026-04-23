from __future__ import annotations

from india_resilience_tool.config.composite_metrics import get_visible_glance_composite_slugs
from india_resilience_tool.config.metrics_registry import DOMAINS
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES,
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


def test_proposal_bundles_do_not_leak_into_ui_surfaces() -> None:
    proposal_slugs = {spec.composite_slug for spec in PROPOSAL_BUNDLES}
    domain_slugs = {slug for slugs in DOMAINS.values() for slug in slugs}
    assert proposal_slugs.isdisjoint(domain_slugs)
    assert proposal_slugs.isdisjoint(set(VARIABLES))
    assert proposal_slugs.isdisjoint(set(get_visible_glance_composite_slugs()))
