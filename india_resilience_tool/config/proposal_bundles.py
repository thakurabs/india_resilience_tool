"""Declarative config for proposal climate-risk bundles.

These bundles are intentionally offline/admin-only in v1 and must not be added
to UI-facing registry surfaces such as DOMAINS, VARIABLES, or landing bundles.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProposalRuleSpec:
    """One deterministic rule within a proposal bundle."""

    rule_slug: str
    metric_slug: str
    rule_type: str
    threshold: float | None = None
    source_mode: str = "master"


@dataclass(frozen=True)
class ProposalBundleSpec:
    """One proposal bundle and its ordered scoring rules."""

    bundle_label: str
    composite_slug: str
    supported_levels: tuple[str, ...]
    rules: tuple[ProposalRuleSpec, ...]


PROPOSAL_BUNDLES: tuple[ProposalBundleSpec, ...] = (
    ProposalBundleSpec(
        bundle_label="Agricultural Risk",
        composite_slug="composite_agricultural_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx1day_ge_200", "pr_max_1day_precip", "threshold", threshold=200.0),
            ProposalRuleSpec("rx5day_ge_300", "pr_max_5day_precip", "threshold", threshold=300.0),
            ProposalRuleSpec("cdd_ge_20", "pr_consecutive_dry_days_lt1mm", "threshold", threshold=20.0),
            ProposalRuleSpec("txx_ge_40", "txx_annual_max", "threshold", threshold=40.0),
            ProposalRuleSpec("r95p_change_gt_20pct_vs_baseline", "r95p_very_wet_precip", "change_vs_baseline"),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Health Risk",
        composite_slug="composite_health_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("txx_ge_45", "txx_annual_max", "threshold", threshold=45.0),
            ProposalRuleSpec("wsdi_ge_5", "wsdi_warm_spell_days", "threshold", threshold=5.0),
            ProposalRuleSpec("tnx_ge_30", "tnx_annual_max", "threshold", threshold=30.0),
            ProposalRuleSpec("rx1day_ge_200", "pr_max_1day_precip", "threshold", threshold=200.0),
            ProposalRuleSpec("cwd_ge_5", "cwd_consecutive_wet_days", "threshold", threshold=5.0),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Industrial Risk",
        composite_slug="composite_industrial_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx1day_ge_150", "pr_max_1day_precip", "threshold", threshold=150.0),
            ProposalRuleSpec("rx5day_ge_250", "pr_max_5day_precip", "threshold", threshold=250.0),
            ProposalRuleSpec("cdd_ge_30", "pr_consecutive_dry_days_lt1mm", "threshold", threshold=30.0),
            ProposalRuleSpec("txx_ge_45", "txx_annual_max", "threshold", threshold=45.0),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Investment / Financial Risk",
        composite_slug="composite_investment_financial_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx1day_positive_trend", "pr_max_1day_precip", "trend", source_mode="yearly"),
            ProposalRuleSpec("rx5day_positive_trend", "pr_max_5day_precip", "trend", source_mode="yearly"),
            ProposalRuleSpec("r99p_positive_trend", "r99p_extreme_wet_precip", "trend", source_mode="yearly"),
            ProposalRuleSpec("cdd_change_gt_20pct_vs_baseline", "pr_consecutive_dry_days_lt1mm", "change_vs_baseline"),
            ProposalRuleSpec("hwfi_positive_trend", "hwfi_tmean_90p", "trend", source_mode="yearly"),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Infrastructure Risk",
        composite_slug="composite_infrastructure_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx1day_ge_200", "pr_max_1day_precip", "threshold", threshold=200.0),
            ProposalRuleSpec("rx5day_ge_400", "pr_max_5day_precip", "threshold", threshold=400.0),
            ProposalRuleSpec("txx_ge_45", "txx_annual_max", "threshold", threshold=45.0),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Asset Risk (Thermal Power Plants)",
        composite_slug="composite_asset_risk_thermal_power",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("cdd_ge_30", "pr_consecutive_dry_days_lt1mm", "threshold", threshold=30.0),
            ProposalRuleSpec("txx_ge_45", "txx_annual_max", "threshold", threshold=45.0),
            ProposalRuleSpec("spi3_low_flow_proxy_norm", "spi3_count_months_lt_minus1", "continuous_proxy"),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Asset Risk (Hydropower Plants)",
        composite_slug="composite_asset_risk_hydropower",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx5day_ge_500", "pr_max_5day_precip", "threshold", threshold=500.0),
            ProposalRuleSpec("cdd_ge_60", "pr_consecutive_dry_days_lt1mm", "threshold", threshold=60.0),
            ProposalRuleSpec(
                "r95p_interannual_variability_norm",
                "r95p_interannual_variability",
                "continuous_proxy",
                source_mode="helper_master",
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Life & Livelihood Loss Risk",
        composite_slug="composite_life_livelihood_loss_risk",
        supported_levels=("district", "block"),
        rules=(
            ProposalRuleSpec("rx1day_ge_200", "pr_max_1day_precip", "threshold", threshold=200.0),
            ProposalRuleSpec(
                "heavy_rain_2day_event_ge_1",
                "pr_2day_heavy_rainfall_events_ge150mm",
                "threshold",
                threshold=1.0,
            ),
            ProposalRuleSpec("cdd_ge_40", "pr_consecutive_dry_days_lt1mm", "threshold", threshold=40.0),
            ProposalRuleSpec("wsdi_ge_5", "wsdi_warm_spell_days", "threshold", threshold=5.0),
        ),
    ),
)

PROPOSAL_BUNDLES_BY_SLUG: dict[str, ProposalBundleSpec] = {
    spec.composite_slug: spec for spec in PROPOSAL_BUNDLES
}


def get_proposal_bundle_specs() -> tuple[ProposalBundleSpec, ...]:
    """Return the ordered proposal-bundle definitions."""
    return PROPOSAL_BUNDLES


def validate_proposal_bundle_specs() -> list[str]:
    """Return validation issues for the proposal bundle config."""
    issues: list[str] = []
    seen_bundle_labels: set[str] = set()
    seen_composite_slugs: set[str] = set()
    for spec in PROPOSAL_BUNDLES:
        if spec.bundle_label in seen_bundle_labels:
            issues.append(f"Duplicate proposal bundle label: {spec.bundle_label!r}")
        seen_bundle_labels.add(spec.bundle_label)
        if spec.composite_slug in seen_composite_slugs:
            issues.append(f"Duplicate proposal composite slug: {spec.composite_slug!r}")
        seen_composite_slugs.add(spec.composite_slug)
        if spec.supported_levels != ("district", "block"):
            issues.append(f"Proposal bundle {spec.composite_slug!r} must support exactly district/block.")
        if not spec.rules:
            issues.append(f"Proposal bundle {spec.composite_slug!r} has no rules.")
            continue
        seen_rule_slugs: set[str] = set()
        for rule in spec.rules:
            if rule.rule_slug in seen_rule_slugs:
                issues.append(
                    f"Proposal bundle {spec.composite_slug!r} repeats rule slug {rule.rule_slug!r}."
                )
            seen_rule_slugs.add(rule.rule_slug)
            if rule.rule_type not in {"threshold", "change_vs_baseline", "trend", "continuous_proxy"}:
                issues.append(
                    f"Proposal bundle {spec.composite_slug!r} uses unsupported rule type {rule.rule_type!r}."
                )
            if rule.rule_type == "threshold" and rule.threshold is None:
                issues.append(
                    f"Proposal bundle {spec.composite_slug!r} threshold rule {rule.rule_slug!r} is missing a threshold."
                )
    return issues
