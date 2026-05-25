"""Declarative compute-owned config for proposal climate-risk bundles.

Proposal bundles are Phase-1 sector climate hazard-pressure scores. They are
not full sectoral risk scores because exposure, vulnerability, and adaptive
capacity are not included in this compute path.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass


VALID_RULE_TYPES = {"blended", "trend"}
VALID_DIRECTIONS = {"higher_worse", "lower_worse"}
VALID_CHANGE_MODES = {"auto", "absolute_delta", "relative_pct"}
VALID_WEIGHT_MODES = {"equal", "explicit_normalized"}
EXPLICIT_WEIGHT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ProposalRuleSpec:
    """One deterministic rule within a proposal bundle.

    Missing source data for a rule returns NaN at build time and reduces the
    available-rule count for the affected geography. Continuous component scores
    are 0-100, with higher values representing higher sector hazard pressure.
    """

    rule_slug: str
    display_label: str
    metric_slug: str
    rule_type: str
    threshold: float | None = None
    source_mode: str = "master"
    direction: str = "higher_worse"
    absolute_weight: float = 1.0
    change_weight: float = 0.0
    impact_weight: float = 0.0
    impact_low: float | None = None
    impact_high: float | None = None
    change_mode: str = "auto"
    rule_weight: float | None = None
    method_note: str = ""


@dataclass(frozen=True)
class ProposalBundleSpec:
    """One proposal bundle and its ordered scoring rules."""

    bundle_label: str
    composite_slug: str
    supported_levels: tuple[str, ...]
    rules: tuple[ProposalRuleSpec, ...]
    weight_mode: str = "equal"
    min_available_rule_weight_fraction: float = 0.0


def _pressure_rule(
    rule_slug: str,
    display_label: str,
    metric_slug: str,
    *,
    absolute_weight: float = 0.65,
    change_weight: float = 0.35,
    impact_weight: float = 0.0,
    impact_low: float | None = None,
    impact_high: float | None = None,
    change_mode: str = "auto",
    source_mode: str = "master",
    method_note: str = "",
    rule_weight: float | None = None,
) -> ProposalRuleSpec:
    """Return a continuous sector-hazard-pressure rule specification."""
    return ProposalRuleSpec(
        rule_slug=rule_slug,
        display_label=display_label,
        metric_slug=metric_slug,
        rule_type="blended",
        source_mode=source_mode,
        absolute_weight=absolute_weight,
        change_weight=change_weight,
        impact_weight=impact_weight,
        impact_low=impact_low,
        impact_high=impact_high,
        change_mode=change_mode,
        rule_weight=rule_weight,
        method_note=method_note,
    )


def _absolute_pressure_rule(
    rule_slug: str,
    display_label: str,
    metric_slug: str,
    *,
    rule_weight: float,
    method_note: str = "",
) -> ProposalRuleSpec:
    """Return a pure absolute-pressure rule with an explicit bundle weight."""
    return _pressure_rule(
        rule_slug,
        display_label,
        metric_slug,
        absolute_weight=1.0,
        change_weight=0.0,
        impact_weight=0.0,
        change_mode="auto",
        rule_weight=rule_weight,
        method_note=method_note,
    )


def _trend_rule(rule_slug: str, display_label: str, metric_slug: str) -> ProposalRuleSpec:
    """Return a continuous adverse-trend pressure rule specification."""
    return ProposalRuleSpec(
        rule_slug=rule_slug,
        display_label=display_label,
        metric_slug=metric_slug,
        rule_type="trend",
        source_mode="yearly",
        method_note="Continuous adverse yearly-slope pressure within the selected future period.",
    )


PROPOSAL_BUNDLES: tuple[ProposalBundleSpec, ...] = (
    ProposalBundleSpec(
        bundle_label="Agricultural Risk",
        composite_slug="composite_agricultural_risk",
        supported_levels=("district", "block"),
        weight_mode="explicit_normalized",
        min_available_rule_weight_fraction=0.70,
        rules=(
            _absolute_pressure_rule(
                "txx_peak_crop_heat",
                "Peak crop heat",
                "txx_annual_max",
                rule_weight=0.10,
                method_note=(
                    "Pure current/future absolute-pressure score. The earlier TXx 40-45 degC "
                    "impact band and baseline-change blend are intentionally retired."
                ),
            ),
            _absolute_pressure_rule(
                "txge35_damaging_heat_days",
                "Damaging heat days",
                "txge35_extreme_heat_days",
                rule_weight=0.10,
            ),
            _absolute_pressure_rule(
                "wsdi_persistent_heat",
                "Persistent heat",
                "wsdi_warm_spell_days",
                rule_weight=0.10,
            ),
            _absolute_pressure_rule(
                "spi3_drought_episodes",
                "Drought episodes",
                "spi3_count_events_lt_minus1",
                rule_weight=0.15,
            ),
            _absolute_pressure_rule(
                "spi3_longest_drought_spell",
                "Longest drought spell",
                "spi3_max_spell_lt_minus1",
                rule_weight=0.15,
            ),
            _absolute_pressure_rule(
                "rx5day_heavy_rainfall",
                "5-day heavy rainfall",
                "pr_max_5day_precip",
                rule_weight=0.20,
            ),
            _absolute_pressure_rule(
                "tnle10_cold_nights",
                "Cold nights",
                "tnle10_cold_nights",
                rule_weight=0.20,
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Health Risk",
        composite_slug="composite_health_risk",
        supported_levels=("district", "block"),
        weight_mode="explicit_normalized",
        min_available_rule_weight_fraction=0.70,
        rules=(
            _pressure_rule(
                "txx_ge_45",
                "Extreme daytime heat pressure",
                "txx_annual_max",
                absolute_weight=0.40,
                change_weight=0.25,
                impact_weight=0.35,
                impact_low=40.0,
                impact_high=45.0,
                change_mode="absolute_delta",
                rule_weight=0.30,
                method_note=(
                    "Impact band 40-45 degC: IMD plains heatwave (declared >=40, severe >=45). "
                    "External, high confidence. Plains/national default (zone refinement: BL-0020)."
                ),
            ),
            _pressure_rule(
                "wsdi_ge_5",
                "Warm-spell duration pressure",
                "wsdi_warm_spell_days",
                absolute_weight=0.45,
                change_weight=0.40,
                impact_weight=0.15,
                impact_low=6.0,
                impact_high=18.0,
                rule_weight=0.12,
                method_note=(
                    "Impact band 6-18 days/yr: self-derived, LOW confidence. Onset=6 (WSDI min "
                    "qualifying spell, past the ~4-day added-mortality threshold); saturation=18 "
                    "(high annual warm-spell burden). Small impact weight by design."
                ),
            ),
            _pressure_rule(
                "tnx_ge_30",
                "Night-time heat pressure",
                "tnx_annual_max",
                absolute_weight=0.40,
                change_weight=0.25,
                impact_weight=0.35,
                impact_low=28.0,
                impact_high=32.0,
                change_mode="absolute_delta",
                rule_weight=0.18,
                method_note=(
                    "Impact band 28-32 degC (absolute level): self-derived, MEDIUM confidence. "
                    "Onset=28 (India hot-night mortality inflection ~+9.8%/degC); saturation=32 "
                    "(upper envelope of Indian warmest nights). Supersedes the withdrawn IMD "
                    "warm-night departure band. Plains/national default (BL-0020)."
                ),
            ),
            _pressure_rule(
                "rx1day_ge_200",
                "1-day rainfall disruption pressure",
                "pr_max_1day_precip",
                absolute_weight=0.40,
                change_weight=0.25,
                impact_weight=0.35,
                impact_low=115.6,
                impact_high=204.5,
                change_mode="relative_pct",
                rule_weight=0.25,
                method_note=(
                    "Impact band 115.6-204.5 mm/day: IMD very-heavy (115.6-204.4) to "
                    "extremely-heavy (>=204.5) rainfall. External, high confidence; national "
                    "categories (zone-invariant)."
                ),
            ),
            _pressure_rule(
                "cwd_ge_5",
                "Consecutive wet-day pressure",
                "cwd_consecutive_wet_days",
                absolute_weight=0.45,
                change_weight=0.40,
                impact_weight=0.15,
                impact_low=7.0,
                impact_high=15.0,
                rule_weight=0.15,
                method_note=(
                    "Impact band 7-15 days: self-derived, LOW confidence. Onset=7 (week of "
                    "standing water spans the mosquito aquatic cycle / waterlogging onset); "
                    "saturation=15 (prolonged saturation). Local-hydrology dependent; small "
                    "impact weight. Zone/hydrology refinement: BL-0020."
                ),
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Industrial Risk",
        composite_slug="composite_industrial_risk",
        supported_levels=("district", "block"),
        rules=(
            _pressure_rule("rx1day_ge_150", "1-day rainfall disruption pressure", "pr_max_1day_precip"),
            _pressure_rule("rx5day_ge_250", "5-day rainfall disruption pressure", "pr_max_5day_precip"),
            _pressure_rule("cdd_ge_30", "Dry-spell water-stress pressure", "pr_consecutive_dry_days_lt1mm"),
            _pressure_rule(
                "txx_ge_45",
                "Extreme heat operations pressure",
                "txx_annual_max",
                absolute_weight=0.40,
                change_weight=0.25,
                impact_weight=0.35,
                impact_low=40.0,
                impact_high=45.0,
                change_mode="absolute_delta",
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Investment / Financial Risk",
        composite_slug="composite_investment_financial_risk",
        supported_levels=("district", "block"),
        rules=(
            _trend_rule("rx1day_positive_trend", "1-day rainfall intensity trend pressure", "pr_max_1day_precip"),
            _trend_rule("rx5day_positive_trend", "5-day rainfall intensity trend pressure", "pr_max_5day_precip"),
            _trend_rule("r99p_positive_trend", "Extreme wet precipitation trend pressure", "r99p_extreme_wet_precip"),
            _pressure_rule(
                "cdd_change_gt_20pct_vs_baseline",
                "Dry-spell change pressure",
                "pr_consecutive_dry_days_lt1mm",
                absolute_weight=0.30,
                change_weight=0.70,
                change_mode="relative_pct",
            ),
            _trend_rule("hwfi_positive_trend", "Heatwave frequency trend pressure", "hwfi_tmean_90p"),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Infrastructure Risk",
        composite_slug="composite_infrastructure_risk",
        supported_levels=("district", "block"),
        rules=(
            _pressure_rule(
                "rx1day_ge_200",
                "1-day rainfall design pressure",
                "pr_max_1day_precip",
                absolute_weight=0.70,
                change_weight=0.30,
            ),
            _pressure_rule(
                "rx5day_ge_400",
                "5-day rainfall design pressure",
                "pr_max_5day_precip",
                absolute_weight=0.70,
                change_weight=0.30,
            ),
            _pressure_rule(
                "txx_ge_45",
                "Extreme heat asset pressure",
                "txx_annual_max",
                absolute_weight=0.45,
                change_weight=0.25,
                impact_weight=0.30,
                impact_low=40.0,
                impact_high=45.0,
                change_mode="absolute_delta",
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Asset Risk (Thermal Power Plants)",
        composite_slug="composite_asset_risk_thermal_power",
        supported_levels=("district", "block"),
        rules=(
            _pressure_rule(
                "cdd_ge_30",
                "Dry-spell cooling-water pressure",
                "pr_consecutive_dry_days_lt1mm",
                absolute_weight=0.50,
                change_weight=0.50,
            ),
            _pressure_rule(
                "txx_ge_45",
                "Extreme heat cooling-efficiency pressure",
                "txx_annual_max",
                absolute_weight=0.45,
                change_weight=0.25,
                impact_weight=0.30,
                impact_low=40.0,
                impact_high=45.0,
                change_mode="absolute_delta",
            ),
            _pressure_rule(
                "spi3_low_flow_proxy_norm",
                "Low-flow drought proxy pressure",
                "spi3_count_months_lt_minus1",
                absolute_weight=1.0,
                change_weight=0.0,
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Asset Risk (Hydropower Plants)",
        composite_slug="composite_asset_risk_hydropower",
        supported_levels=("district", "block"),
        rules=(
            _pressure_rule(
                "rx5day_ge_500",
                "5-day rainfall operations pressure",
                "pr_max_5day_precip",
                absolute_weight=0.50,
                change_weight=0.50,
            ),
            _pressure_rule(
                "cdd_ge_60",
                "Dry-spell flow pressure",
                "pr_consecutive_dry_days_lt1mm",
                absolute_weight=0.50,
                change_weight=0.50,
            ),
            _pressure_rule(
                "r95p_interannual_variability_norm",
                "Very wet precipitation variability pressure",
                "r95p_interannual_variability",
                absolute_weight=1.0,
                change_weight=0.0,
                source_mode="helper_master",
            ),
        ),
    ),
    ProposalBundleSpec(
        bundle_label="Life & Livelihood Loss Risk",
        composite_slug="composite_life_livelihood_loss_risk",
        supported_levels=("district", "block"),
        rules=(
            _pressure_rule(
                "rx1day_ge_200",
                "1-day rainfall exposure pressure",
                "pr_max_1day_precip",
                absolute_weight=0.70,
                change_weight=0.30,
            ),
            _pressure_rule(
                "rx5day_livelihood_pressure",
                "5-day rainfall exposure pressure",
                "pr_max_5day_precip",
                absolute_weight=0.70,
                change_weight=0.30,
            ),
            _pressure_rule(
                "cdd_ge_40",
                "Dry-spell livelihood pressure",
                "pr_consecutive_dry_days_lt1mm",
                absolute_weight=0.60,
                change_weight=0.40,
            ),
            _pressure_rule(
                "wsdi_ge_5",
                "Warm-spell livelihood pressure",
                "wsdi_warm_spell_days",
                absolute_weight=0.70,
                change_weight=0.30,
            ),
        ),
    ),
)

PROPOSAL_BUNDLES_BY_SLUG: dict[str, ProposalBundleSpec] = {
    spec.composite_slug: spec for spec in PROPOSAL_BUNDLES
}


def get_proposal_bundle_specs() -> tuple[ProposalBundleSpec, ...]:
    """Return the ordered proposal-bundle definitions."""
    return PROPOSAL_BUNDLES


def get_proposal_bundle_spec_by_slug(composite_slug: str) -> ProposalBundleSpec | None:
    """Return one proposal-bundle spec by composite slug."""
    return PROPOSAL_BUNDLES_BY_SLUG.get(str(composite_slug or "").strip())


def is_proposal_bundle_slug(composite_slug: str) -> bool:
    """Return whether a composite slug belongs to the proposal-bundle catalog."""
    return get_proposal_bundle_spec_by_slug(composite_slug) is not None


def get_proposal_bundle_source_metric_slugs(composite_slug: str) -> tuple[str, ...]:
    """Return declared source metric slugs for one proposal bundle in stable order."""
    spec = get_proposal_bundle_spec_by_slug(composite_slug)
    if spec is None:
        return ()
    ordered: list[str] = []
    seen: set[str] = set()
    for rule in spec.rules:
        metric_slug = str(rule.metric_slug).strip()
        if not metric_slug or metric_slug in seen:
            continue
        seen.add(metric_slug)
        ordered.append(metric_slug)
    return tuple(ordered)


def proposal_rule_score_column(rule_slug: str, scenario: str, period: str) -> str:
    """Return the persisted score-column name for one proposal rule selection."""
    return f"{str(rule_slug).strip()}__{str(scenario).strip()}__{str(period).strip()}__score"


def proposal_bundle_mean_column(composite_slug: str, scenario: str, period: str) -> str:
    """Return the persisted composite mean-column name for one proposal bundle selection."""
    return f"{str(composite_slug).strip()}__{str(scenario).strip()}__{str(period).strip()}__mean"


def proposal_available_rule_count_column(composite_slug: str, scenario: str, period: str) -> str:
    """Return the persisted available-rule-count column name for one proposal bundle selection."""
    return f"{str(composite_slug).strip()}__{str(scenario).strip()}__{str(period).strip()}__available_rule_count"


def proposal_available_rule_weight_fraction_column(composite_slug: str, scenario: str, period: str) -> str:
    """Return the persisted available-rule-weight-fraction column name for one proposal bundle selection."""
    return f"{str(composite_slug).strip()}__{str(scenario).strip()}__{str(period).strip()}__available_rule_weight_fraction"


def _validate_weight(value: float, *, field_name: str, rule_slug: str, issues: list[str]) -> None:
    """Append a validation issue if a score-component weight is invalid."""
    if value < 0.0:
        issues.append(f"Proposal rule {rule_slug!r} has negative {field_name}: {value!r}")


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
        if spec.weight_mode not in VALID_WEIGHT_MODES:
            issues.append(f"Proposal bundle {spec.composite_slug!r} has unsupported weight mode {spec.weight_mode!r}.")
        if not (0.0 <= spec.min_available_rule_weight_fraction <= 1.0):
            issues.append(
                f"Proposal bundle {spec.composite_slug!r} has invalid minimum available weight fraction "
                f"{spec.min_available_rule_weight_fraction!r}."
            )
        if not spec.rules:
            issues.append(f"Proposal bundle {spec.composite_slug!r} has no rules.")
            continue
        explicit_rule_weight_sum = 0.0
        seen_rule_slugs: set[str] = set()
        for rule in spec.rules:
            if rule.rule_slug in seen_rule_slugs:
                issues.append(
                    f"Proposal bundle {spec.composite_slug!r} has duplicate rule slug {rule.rule_slug!r}."
                )
            seen_rule_slugs.add(rule.rule_slug)
            if not rule.rule_slug.strip():
                issues.append(f"Proposal bundle {spec.composite_slug!r} has a blank rule slug.")
            if not rule.display_label.strip():
                issues.append(f"Proposal rule {rule.rule_slug!r} has a blank display label.")
            if not rule.metric_slug.strip():
                issues.append(f"Proposal rule {rule.rule_slug!r} has a blank metric slug.")
            if rule.rule_type not in VALID_RULE_TYPES:
                issues.append(f"Proposal rule {rule.rule_slug!r} has unsupported type {rule.rule_type!r}.")
            if rule.direction not in VALID_DIRECTIONS:
                issues.append(f"Proposal rule {rule.rule_slug!r} has unsupported direction {rule.direction!r}.")
            if rule.change_mode not in VALID_CHANGE_MODES:
                issues.append(f"Proposal rule {rule.rule_slug!r} has unsupported change mode {rule.change_mode!r}.")
            _validate_weight(
                rule.absolute_weight,
                field_name="absolute_weight",
                rule_slug=rule.rule_slug,
                issues=issues,
            )
            _validate_weight(rule.change_weight, field_name="change_weight", rule_slug=rule.rule_slug, issues=issues)
            _validate_weight(rule.impact_weight, field_name="impact_weight", rule_slug=rule.rule_slug, issues=issues)
            if spec.weight_mode == "explicit_normalized":
                if rule.rule_weight is None or rule.rule_weight <= 0.0 or rule.rule_weight > 1.0:
                    issues.append(
                        f"Proposal rule {rule.rule_slug!r} must declare 0 < rule_weight <= 1 "
                        f"for explicit_normalized bundles."
                    )
                else:
                    explicit_rule_weight_sum += float(rule.rule_weight)
            elif rule.rule_weight is not None and rule.rule_weight <= 0.0:
                issues.append(f"Proposal rule {rule.rule_slug!r} has non-positive rule_weight: {rule.rule_weight!r}")
            active_weight = rule.absolute_weight + rule.change_weight + rule.impact_weight
            if rule.rule_type == "blended" and active_weight <= 0.0:
                issues.append(f"Proposal rule {rule.rule_slug!r} must have at least one active blended component.")
            if rule.rule_type == "trend" and rule.source_mode != "yearly":
                issues.append(f"Proposal trend rule {rule.rule_slug!r} must use yearly source mode.")
            if rule.impact_weight > 0.0 and (rule.impact_low is None or rule.impact_high is None):
                issues.append(f"Proposal rule {rule.rule_slug!r} has impact_weight but incomplete impact thresholds.")
            if rule.impact_low is not None and rule.impact_high is not None and rule.impact_low == rule.impact_high:
                issues.append(f"Proposal rule {rule.rule_slug!r} impact thresholds must differ.")
        if spec.weight_mode == "explicit_normalized" and not abs(explicit_rule_weight_sum - 1.0) <= EXPLICIT_WEIGHT_TOLERANCE:
            issues.append(
                f"Proposal bundle {spec.composite_slug!r} explicit rule weights must sum to 1.0; "
                f"got {explicit_rule_weight_sum!r}."
            )
    return issues
