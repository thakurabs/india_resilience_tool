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
            assert rule.rule_type == "blended"
            assert rule.direction == "higher_worse"
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
            "rx1day_positive_trend": "1-day rainfall disruption pressure",
            "rx5day_positive_trend": "5-day rainfall accumulation pressure",
            "r99p_positive_trend": "Extreme wet precipitation concentration pressure",
            "cdd_change_gt_20pct_vs_baseline": "Dry-spell water-stress pressure",
            "hwfi_positive_trend": "Heatwave persistence pressure",
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


def test_agricultural_risk_matches_lens_dossier_section_12() -> None:
    """Pin Agricultural Risk to lens dossier section 12 (CHG-0032).

    Locks weight mode, coverage gate, per-rule lens weights, impact bands,
    change modes, and rule weights against docs/lens_scoring_methodology.md
    sections 12.1-12.8. Drift in either direction (code vs dossier) fails here.
    """
    spec = get_proposal_bundle_spec_by_slug("composite_agricultural_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70

    rules_by_slug = {rule.rule_slug: rule for rule in spec.rules}
    assert list(rules_by_slug.keys()) == [
        "txx_peak_crop_heat",
        "txge35_damaging_heat_days",
        "wsdi_persistent_heat",
        "spi3_drought_episodes",
        "spi3_longest_drought_spell",
        "rx5day_heavy_rainfall",
        "tnle10_cold_nights",
    ]

    expected = {
        "txx_peak_crop_heat": {
            "lens_weights": (0.40, 0.30, 0.30),  # 12.1 — self-derived, medium confidence
            "impact_band": (35.0, 45.0),
            "change_mode": "absolute_delta",
            "rule_weight": 0.15,
        },
        "txge35_damaging_heat_days": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.2 — self-derived, low confidence
            "impact_band": (15.0, 60.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.10,
        },
        "wsdi_persistent_heat": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.3 — reuses Health 6.2; low confidence
            "impact_band": (6.0, 18.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.10,
        },
        "spi3_drought_episodes": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.4 — self-derived, low confidence
            "impact_band": (3.0, 12.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.15,
        },
        "spi3_longest_drought_spell": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.5 — self-derived, low confidence
            "impact_band": (3.0, 12.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.15,
        },
        "rx5day_heavy_rainfall": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.6 — reuses Industrial 7.2; low confidence
            "impact_band": (250.0, 500.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.20,
        },
        "tnle10_cold_nights": {
            "lens_weights": (0.45, 0.40, 0.15),  # 12.7 — peninsular default; low confidence
            "impact_band": (10.0, 30.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.15,
        },
    }

    for slug, expected_fields in expected.items():
        rule = rules_by_slug[slug]
        observed_lens = (rule.absolute_weight, rule.change_weight, rule.impact_weight)
        assert observed_lens == expected_fields["lens_weights"], slug
        assert (rule.impact_low, rule.impact_high) == expected_fields["impact_band"], slug
        assert rule.change_mode == expected_fields["change_mode"], slug
        assert rule.rule_weight == expected_fields["rule_weight"], slug

    # Rule weights must sum to 1.0 (validator enforces this on explicit_normalized bundles).
    assert sum(rule.rule_weight for rule in spec.rules) == 1.0


def test_industrial_risk_matches_lens_dossier_section_7() -> None:
    """Pin Industrial Risk to lens dossier section 7 (CHG-0028).

    Locks weight mode, coverage gate, per-rule lens weights, impact bands,
    change modes, and rule weights against docs/lens_scoring_methodology.md
    sections 7.1-7.5. Drift in either direction (code vs dossier) fails here.
    """
    spec = get_proposal_bundle_spec_by_slug("composite_industrial_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70

    rules_by_slug = {rule.rule_slug: rule for rule in spec.rules}
    assert list(rules_by_slug.keys()) == ["rx1day_ge_150", "rx5day_ge_250", "cdd_ge_30", "txx_ge_45"]

    # Per-rule (absolute, change, impact) weights, impact band, change mode, rule weight.
    expected = {
        "rx1day_ge_150": {
            "lens_weights": (0.40, 0.25, 0.35),  # section 7.1 — IMD external, high confidence
            "impact_band": (115.6, 204.5),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
        "rx5day_ge_250": {
            "lens_weights": (0.45, 0.40, 0.15),  # section 7.2 — self-derived, low confidence
            "impact_band": (250.0, 500.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.15,
        },
        "cdd_ge_30": {
            "lens_weights": (0.40, 0.30, 0.30),  # section 7.3 — IMD-anchored, medium confidence
            "impact_band": (30.0, 90.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.20,
        },
        "txx_ge_45": {
            "lens_weights": (0.40, 0.25, 0.35),  # section 7.4 — IMD external, high confidence
            "impact_band": (40.0, 45.0),
            "change_mode": "absolute_delta",
            "rule_weight": 0.40,
        },
    }

    for slug, expected_fields in expected.items():
        rule = rules_by_slug[slug]
        observed_lens = (rule.absolute_weight, rule.change_weight, rule.impact_weight)
        assert observed_lens == expected_fields["lens_weights"], slug
        assert (rule.impact_low, rule.impact_high) == expected_fields["impact_band"], slug
        assert rule.change_mode == expected_fields["change_mode"], slug
        assert rule.rule_weight == expected_fields["rule_weight"], slug

    # Rule weights must sum to 1.0 (validator enforces this on explicit_normalized bundles).
    assert sum(rule.rule_weight for rule in spec.rules) == 1.0


def test_investment_financial_risk_matches_lens_dossier_section_8() -> None:
    """Pin Investment / Financial Risk to lens dossier section 8 (CHG-0033)."""
    spec = get_proposal_bundle_spec_by_slug("composite_investment_financial_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70

    rules_by_slug = {rule.rule_slug: rule for rule in spec.rules}
    assert list(rules_by_slug.keys()) == [
        "rx1day_positive_trend",
        "rx5day_positive_trend",
        "r99p_positive_trend",
        "cdd_change_gt_20pct_vs_baseline",
        "hwfi_positive_trend",
    ]

    expected = {
        "rx1day_positive_trend": {
            "lens_weights": (0.40, 0.25, 0.35),
            "impact_band": (115.6, 204.5),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
        "rx5day_positive_trend": {
            "lens_weights": (0.45, 0.40, 0.15),
            "impact_band": (250.0, 500.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.15,
        },
        "r99p_positive_trend": {
            "lens_weights": (0.40, 0.60, 0.0),
            "impact_band": (None, None),
            "change_mode": "relative_pct",
            "rule_weight": 0.10,
        },
        "cdd_change_gt_20pct_vs_baseline": {
            "lens_weights": (0.40, 0.30, 0.30),
            "impact_band": (30.0, 90.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
        "hwfi_positive_trend": {
            "lens_weights": (0.45, 0.40, 0.15),
            "impact_band": (5.0, 15.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
    }

    for slug, expected_fields in expected.items():
        rule = rules_by_slug[slug]
        observed_lens = (rule.absolute_weight, rule.change_weight, rule.impact_weight)
        assert observed_lens == expected_fields["lens_weights"], slug
        assert (rule.impact_low, rule.impact_high) == expected_fields["impact_band"], slug
        assert rule.change_mode == expected_fields["change_mode"], slug
        assert rule.rule_weight == expected_fields["rule_weight"], slug

    assert sum(rule.rule_weight for rule in spec.rules) == 1.0


def test_infrastructure_risk_matches_lens_dossier_section_9() -> None:
    """Pin Infrastructure Risk to lens dossier section 9 (CHG-0034)."""
    spec = get_proposal_bundle_spec_by_slug("composite_infrastructure_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70

    rules_by_slug = {rule.rule_slug: rule for rule in spec.rules}
    assert list(rules_by_slug.keys()) == ["rx1day_ge_200", "rx5day_ge_400", "txx_ge_45"]

    expected = {
        "rx1day_ge_200": {
            "lens_weights": (0.40, 0.25, 0.35),
            "impact_band": (115.6, 204.5),
            "change_mode": "relative_pct",
            "rule_weight": 0.45,
        },
        "rx5day_ge_400": {
            "lens_weights": (0.45, 0.40, 0.15),
            "impact_band": (250.0, 500.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.30,
        },
        "txx_ge_45": {
            "lens_weights": (0.40, 0.25, 0.35),
            "impact_band": (40.0, 45.0),
            "change_mode": "absolute_delta",
            "rule_weight": 0.25,
        },
    }

    for slug, expected_fields in expected.items():
        rule = rules_by_slug[slug]
        observed_lens = (rule.absolute_weight, rule.change_weight, rule.impact_weight)
        assert observed_lens == expected_fields["lens_weights"], slug
        assert (rule.impact_low, rule.impact_high) == expected_fields["impact_band"], slug
        assert rule.change_mode == expected_fields["change_mode"], slug
        assert rule.rule_weight == expected_fields["rule_weight"], slug

    assert sum(rule.rule_weight for rule in spec.rules) == 1.0


def test_life_livelihood_loss_risk_matches_lens_dossier_section_13() -> None:
    """Pin Life & Livelihood Loss Risk to lens dossier section 13 (CHG-0037)."""
    spec = get_proposal_bundle_spec_by_slug("composite_life_livelihood_loss_risk")

    assert spec is not None
    assert spec.weight_mode == "explicit_normalized"
    assert spec.min_available_rule_weight_fraction == 0.70

    rules_by_slug = {rule.rule_slug: rule for rule in spec.rules}
    assert list(rules_by_slug.keys()) == [
        "rx1day_ge_200",
        "rx5day_livelihood_pressure",
        "cdd_ge_40",
        "wsdi_ge_5",
    ]

    expected = {
        "rx1day_ge_200": {
            "lens_weights": (0.40, 0.25, 0.35),  # 13.1 — HIGH external (IMD)
            "impact_band": (115.6, 204.5),
            "change_mode": "relative_pct",
            "rule_weight": 0.30,
        },
        "rx5day_livelihood_pressure": {
            "lens_weights": (0.40, 0.30, 0.30),  # 13.2 — reuses Industrial 7.2 band; LOW
            "impact_band": (250.0, 500.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
        "cdd_ge_40": {
            "lens_weights": (0.40, 0.30, 0.30),  # 13.3 — MEDIUM (IMD + ICAR + NDMA anchored)
            "impact_band": (60.0, 120.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.20,
        },
        "wsdi_ge_5": {
            "lens_weights": (0.40, 0.30, 0.30),  # 13.4 — MEDIUM (mortality-lit anchored)
            "impact_band": (6.0, 18.0),
            "change_mode": "relative_pct",
            "rule_weight": 0.25,
        },
    }

    for slug, expected_fields in expected.items():
        rule = rules_by_slug[slug]
        observed_lens = (rule.absolute_weight, rule.change_weight, rule.impact_weight)
        assert observed_lens == expected_fields["lens_weights"], slug
        assert (rule.impact_low, rule.impact_high) == expected_fields["impact_band"], slug
        assert rule.change_mode == expected_fields["change_mode"], slug
        assert rule.rule_weight == expected_fields["rule_weight"], slug

    assert sum(rule.rule_weight for rule in spec.rules) == 1.0


def test_explicit_weight_bundle_inventory_is_exact() -> None:
    observed = {
        spec.composite_slug
        for spec in PROPOSAL_BUNDLES
        if spec.weight_mode == "explicit_normalized"
    }
    assert observed == {
        "composite_agricultural_risk",
        "composite_health_risk",
        "composite_industrial_risk",
        "composite_infrastructure_risk",
        "composite_investment_financial_risk",
        "composite_life_livelihood_loss_risk",
    }


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
