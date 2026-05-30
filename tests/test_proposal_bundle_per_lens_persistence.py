"""Per-lens persistence and helper-frame alignment tests for proposal bundles.

Locks the CHG-0027 contract:

A. Blended rules persist columns only for the lenses that are active on the
   rule (sparse Option B). Pure-absolute rules persist ``__score`` and
   ``__abs_score`` only.
B. The persisted blended score equals the weighted mean of the persisted
   active-lens columns row-wise.
C. When the change lens is active but the historical baseline column is
   missing, ``__chg_score`` is all-NaN and the blended score renormalizes
   over the remaining active lenses.
D. When change_mode='relative_pct' and the per-row baseline is zero,
   ``__chg_score`` is NaN on those rows and the blended score for those
   rows renormalizes over the remaining active lenses.
E. Trend rules and proxy rules persist only ``__score`` and ``__abs_score``;
   ``__abs_score`` equals ``__score`` row-wise.
F. The R95p variability helper-frame alignment is canonical-key based —
   shuffling helper-frame rows must not change per-district scores.
G. Pre/post regression: Health Risk ``txx_ge_45`` blended decomposition on
   known inputs matches hand-computed expected values.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import india_resilience_tool.compute.proposal_bundles as proposal_bundle_module
from india_resilience_tool.compute.proposal_bundles import compute_proposal_bundle_master_frame
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES_BY_SLUG,
    proposal_bundle_mean_column,
    proposal_rule_abs_score_column,
    proposal_rule_chg_score_column,
    proposal_rule_imp_score_column,
    proposal_rule_score_column,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_canonical_units(
    monkeypatch: pytest.MonkeyPatch,
    *,
    district_df: pd.DataFrame | None = None,
    block_df: pd.DataFrame | None = None,
) -> None:
    def _loader(*, level: str, state_name: str, data_dir: Path) -> pd.DataFrame:
        _ = state_name, data_dir
        if level == "district":
            assert district_df is not None
            return district_df.copy()
        assert block_df is not None
        return block_df.copy()

    monkeypatch.setattr(proposal_bundle_module, "_load_canonical_unit_frame", _loader)


def _metric_base(slug: str) -> str:
    spec = METRICS_BY_SLUG[slug]
    return spec.periods_metric_col or spec.value_col or slug


def _write_district_master(
    tmp_path: Path,
    *,
    slug: str,
    state_name: str,
    df: pd.DataFrame,
) -> None:
    root = tmp_path / "processed" / slug / state_name
    root.mkdir(parents=True, exist_ok=True)
    df.to_csv(root / "master_metrics_by_district.csv", index=False)


def _district_ids(state_name: str = "Telangana") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )


def _seed_health_risk_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    target_period: str = "2020-2040",
    target_scenario: str = "ssp245",
    baseline_period: str = "1995-2014",
    txx_currents: tuple[float, float, float] = (46.0, 42.0, 44.0),
    txx_baselines: tuple[float, float, float] = (43.0, 41.0, 42.0),
    other_currents: tuple[float, float, float] = (12.0, 8.0, 10.0),
    other_baselines: tuple[float, float, float] = (10.0, 8.0, 9.0),
    include_txx_baseline: bool = True,
) -> None:
    """Seed all five Health Risk metric masters in canonical 4-token schema.

    The ``txx_*`` slug carries the deliberately-chosen trio used by Test G's
    hand-computed expected values. The other four metrics get filler that
    keeps the rules computable (not asserted against).
    """
    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_health_risk"]
    for rule in bundle.rules:
        slug = rule.metric_slug
        base = _metric_base(slug)
        df = ids.copy()
        if slug == "txx_annual_max":
            df[f"{base}__{target_scenario}__{target_period}__mean"] = list(txx_currents)
            if include_txx_baseline:
                df[f"{base}__historical__{baseline_period}__mean"] = list(txx_baselines)
        else:
            df[f"{base}__{target_scenario}__{target_period}__mean"] = list(other_currents)
            df[f"{base}__historical__{baseline_period}__mean"] = list(other_baselines)
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


def _seed_agricultural_risk_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    target_period: str = "2020-2040",
    target_scenario: str = "ssp245",
    baseline_period: str = "1995-2014",
    values: tuple[float, float, float] = (30.0, 10.0, 20.0),
    baseline_values: tuple[float, float, float] = (5.0, 5.0, 5.0),
) -> None:
    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"]
    for rule in bundle.rules:
        slug = rule.metric_slug
        base = _metric_base(slug)
        df = ids.copy()
        df[f"{base}__{target_scenario}__{target_period}__mean"] = list(values)
        df[f"{base}__historical__{baseline_period}__mean"] = list(baseline_values)
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


def _run_health_risk(
    tmp_path: Path,
    state_name: str = "Telangana",
) -> pd.DataFrame:
    return compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_health_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )


def _run_agricultural_risk(
    tmp_path: Path,
    state_name: str = "Telangana",
) -> pd.DataFrame:
    return compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )


def _seed_investment_risk_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    target_period: str = "2020-2040",
    target_scenario: str = "ssp245",
    baseline_period: str = "1995-2014",
    current_by_slug: dict[str, tuple[float, float, float]] | None = None,
    baseline_by_slug: dict[str, tuple[float, float, float]] | None = None,
) -> None:
    current_defaults = {
        "pr_max_1day_precip": (120.0, 80.0, 100.0),
        "pr_max_5day_precip": (320.0, 180.0, 250.0),
        "r99p_extreme_wet_precip": (400.0, 150.0, 250.0),
        "pr_consecutive_dry_days_lt1mm": (60.0, 30.0, 45.0),
        "hwfi_tmean_90p": (12.0, 6.0, 9.0),
    }
    baseline_defaults = {
        "pr_max_1day_precip": (60.0, 50.0, 55.0),
        "pr_max_5day_precip": (160.0, 140.0, 150.0),
        "r99p_extreme_wet_precip": (200.0, 120.0, 160.0),
        "pr_consecutive_dry_days_lt1mm": (30.0, 25.0, 30.0),
        "hwfi_tmean_90p": (6.0, 5.0, 6.0),
    }
    if current_by_slug:
        current_defaults.update(current_by_slug)
    if baseline_by_slug:
        baseline_defaults.update(baseline_by_slug)

    for slug, values in current_defaults.items():
        base = _metric_base(slug)
        df = ids.copy()
        df[f"{base}__{target_scenario}__{target_period}__mean"] = list(values)
        df[f"{base}__historical__{baseline_period}__mean"] = list(baseline_defaults[slug])
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


def _seed_infrastructure_risk_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    target_period: str = "2020-2040",
    target_scenario: str = "ssp245",
    baseline_period: str = "1995-2014",
    current_by_slug: dict[str, tuple[float, float, float]] | None = None,
    baseline_by_slug: dict[str, tuple[float, float, float]] | None = None,
) -> None:
    current_defaults = {
        "pr_max_1day_precip": (180.0, 120.0, 150.0),
        "pr_max_5day_precip": (420.0, 260.0, 340.0),
        "txx_annual_max": (46.0, 42.0, 44.0),
    }
    baseline_defaults = {
        "pr_max_1day_precip": (100.0, 80.0, 90.0),
        "pr_max_5day_precip": (280.0, 220.0, 240.0),
        "txx_annual_max": (43.0, 41.0, 42.0),
    }
    if current_by_slug:
        current_defaults.update(current_by_slug)
    if baseline_by_slug:
        baseline_defaults.update(baseline_by_slug)

    for slug, values in current_defaults.items():
        base = _metric_base(slug)
        df = ids.copy()
        df[f"{base}__{target_scenario}__{target_period}__mean"] = list(values)
        df[f"{base}__historical__{baseline_period}__mean"] = list(baseline_defaults[slug])
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


def _run_investment_risk(
    tmp_path: Path,
    state_name: str = "Telangana",
) -> pd.DataFrame:
    return compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_investment_financial_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )


def _run_infrastructure_risk(
    tmp_path: Path,
    state_name: str = "Telangana",
) -> pd.DataFrame:
    return compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_infrastructure_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )


# ---------------------------------------------------------------------------
# Test A — sparse persistence policy
# ---------------------------------------------------------------------------


def test_blended_rule_persists_active_lens_columns_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_health_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_health_risk(tmp_path)

    # Health Risk's txx_ge_45 has all three lenses active.
    for lens_col in (
        proposal_rule_abs_score_column("txx_ge_45", "ssp245", "2020-2040"),
        proposal_rule_chg_score_column("txx_ge_45", "ssp245", "2020-2040"),
        proposal_rule_imp_score_column("txx_ge_45", "ssp245", "2020-2040"),
    ):
        assert lens_col in out.columns, f"missing active-lens column {lens_col!r}"

    # Post-CHG-0032: every Agricultural Risk rule is three-lens-active and
    # must emit __abs_score, __chg_score, __imp_score columns. (Negative
    # sparse-policy case is now covered by test_trend_and_proxy_rules_
    # persist_only_abs_score on the Asset Thermal Power proxy rule.)
    _seed_agricultural_risk_masters(tmp_path, ids=ids, state_name=state_name)
    ag_out = _run_agricultural_risk(tmp_path)
    for rule in PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"].rules:
        for lens_col in (
            proposal_rule_abs_score_column(rule.rule_slug, "ssp245", "2020-2040"),
            proposal_rule_chg_score_column(rule.rule_slug, "ssp245", "2020-2040"),
            proposal_rule_imp_score_column(rule.rule_slug, "ssp245", "2020-2040"),
        ):
            assert lens_col in ag_out.columns, (
                f"missing Agricultural lens column {lens_col!r}"
            )

    _seed_infrastructure_risk_masters(tmp_path, ids=ids, state_name=state_name)
    infra_out = _run_infrastructure_risk(tmp_path)
    for rule_slug in ("rx1day_ge_200", "rx5day_ge_400"):
        for lens_col in (
            proposal_rule_abs_score_column(rule_slug, "ssp245", "2020-2040"),
            proposal_rule_chg_score_column(rule_slug, "ssp245", "2020-2040"),
            proposal_rule_imp_score_column(rule_slug, "ssp245", "2020-2040"),
        ):
            assert lens_col in infra_out.columns, (
                f"missing Infrastructure lens column {lens_col!r}"
            )


# ---------------------------------------------------------------------------
# Test B — blended equals weighted mean of persisted active-lens columns
# ---------------------------------------------------------------------------


def test_blended_score_equals_weighted_mean_of_persisted_active_lens_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_health_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_health_risk(tmp_path)

    rule = PROPOSAL_BUNDLES_BY_SLUG["composite_health_risk"].rules[0]  # txx_ge_45
    abs_col = proposal_rule_abs_score_column(rule.rule_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(rule.rule_slug, "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column(rule.rule_slug, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(rule.rule_slug, "ssp245", "2020-2040")

    weights = {
        abs_col: float(rule.absolute_weight),
        chg_col: float(rule.change_weight),
        imp_col: float(rule.impact_weight),
    }
    components = out[[abs_col, chg_col, imp_col]]
    valid = components.notna()
    weighted = components.multiply([weights[c] for c in components.columns], axis=1).where(valid)
    denominator = valid.multiply([weights[c] for c in components.columns], axis=1).sum(axis=1)
    expected = (weighted.sum(axis=1, skipna=True) / denominator.replace(0.0, np.nan)).astype(float)

    pd.testing.assert_series_equal(
        out[score_col].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


# ---------------------------------------------------------------------------
# Test C — change lens active but baseline column missing
# ---------------------------------------------------------------------------


def test_lens_renormalization_when_change_lens_baseline_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_health_risk_masters(
        tmp_path,
        ids=ids,
        state_name=state_name,
        include_txx_baseline=False,
    )

    out = _run_health_risk(tmp_path)

    chg_col = proposal_rule_chg_score_column("txx_ge_45", "ssp245", "2020-2040")
    abs_col = proposal_rule_abs_score_column("txx_ge_45", "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column("txx_ge_45", "ssp245", "2020-2040")
    score_col = proposal_rule_score_column("txx_ge_45", "ssp245", "2020-2040")

    # The change lens is still active by spec → column is present but all-NaN.
    assert chg_col in out.columns
    assert out[chg_col].isna().all()

    # Blended renormalizes row-wise across abs+imp (weights 0.40 and 0.35).
    expected = (
        0.40 * out[abs_col].astype(float) + 0.35 * out[imp_col].astype(float)
    ) / (0.40 + 0.35)
    pd.testing.assert_series_equal(
        out[score_col].astype(float).reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


# ---------------------------------------------------------------------------
# Test D — relative_pct change with zero baseline on some rows
# ---------------------------------------------------------------------------


def test_lens_renormalization_when_relative_pct_baseline_is_zero_for_some_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """rx1day_ge_200 uses change_mode='relative_pct'. Per-row zero baseline
    forces chg-lens NaN on those rows and the blended score renormalizes."""
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)

    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_health_risk"]
    # District A: baseline=0 -> chg NaN for A; B and C have valid baselines.
    baselines = (0.0, 50.0, 80.0)
    _seed_health_risk_masters(
        tmp_path,
        ids=ids,
        state_name=state_name,
        other_baselines=baselines,
    )
    # Override only pr_max_1day_precip baseline column to the zero-mixed series.
    rx1_slug = "pr_max_1day_precip"
    base = _metric_base(rx1_slug)
    df = ids.copy()
    df[f"{base}__ssp245__2020-2040__mean"] = [120.0, 150.0, 180.0]
    df[f"{base}__historical__1995-2014__mean"] = list(baselines)
    _write_district_master(tmp_path, slug=rx1_slug, state_name=state_name, df=df)

    out = _run_health_risk(tmp_path)

    chg_col = proposal_rule_chg_score_column("rx1day_ge_200", "ssp245", "2020-2040")
    abs_col = proposal_rule_abs_score_column("rx1day_ge_200", "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column("rx1day_ge_200", "ssp245", "2020-2040")
    score_col = proposal_rule_score_column("rx1day_ge_200", "ssp245", "2020-2040")

    assert chg_col in out.columns
    # District A (baseline=0) -> chg NaN; districts B, C -> chg finite.
    a_mask = out["district"] == "A"
    assert out.loc[a_mask, chg_col].isna().all()
    assert out.loc[~a_mask, chg_col].notna().all()

    # District A's blended score should use only abs and imp lenses
    # (weights 0.40 and 0.35 from the spec); districts B and C use all three.
    row_a = out.loc[a_mask].iloc[0]
    expected_a = (0.40 * row_a[abs_col] + 0.35 * row_a[imp_col]) / (0.40 + 0.35)
    assert np.isclose(row_a[score_col], expected_a)


# ---------------------------------------------------------------------------
# Test E — trend and proxy rules persist only abs_score
# ---------------------------------------------------------------------------


def test_trend_and_proxy_rules_persist_only_abs_score(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asset Thermal Power's spi3_low_flow_proxy_norm is a pure-absolute proxy
    rule; it must persist __score and __abs_score only."""
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)

    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_thermal_power"]
    for rule in bundle.rules:
        slug = rule.metric_slug
        base = _metric_base(slug)
        df = ids.copy()
        df[f"{base}__ssp245__2020-2040__mean"] = [30.0, 10.0, 20.0]
        df[f"{base}__historical__1995-2014__mean"] = [10.0, 8.0, 9.0]
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)

    out = compute_proposal_bundle_master_frame(
        bundle,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    proxy_slug = "spi3_low_flow_proxy_norm"
    abs_col = proposal_rule_abs_score_column(proxy_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(proxy_slug, "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column(proxy_slug, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(proxy_slug, "ssp245", "2020-2040")

    assert score_col in out.columns
    assert abs_col in out.columns
    assert chg_col not in out.columns
    assert imp_col not in out.columns
    pd.testing.assert_series_equal(
        out[abs_col].astype(float).reset_index(drop=True),
        out[score_col].astype(float).reset_index(drop=True),
        check_names=False,
    )


def test_investment_r99p_persists_only_active_abs_and_chg_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_investment_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_investment_risk(tmp_path)

    rule_slug = "r99p_positive_trend"
    abs_col = proposal_rule_abs_score_column(rule_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(rule_slug, "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column(rule_slug, "ssp245", "2020-2040")

    assert abs_col in out.columns
    assert chg_col in out.columns
    assert imp_col not in out.columns


def test_investment_r99p_score_equals_weighted_mean_of_abs_and_chg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_investment_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_investment_risk(tmp_path)

    rule_slug = "r99p_positive_trend"
    rule = next(
        rule for rule in PROPOSAL_BUNDLES_BY_SLUG["composite_investment_financial_risk"].rules
        if rule.rule_slug == rule_slug
    )
    abs_col = proposal_rule_abs_score_column(rule_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(rule_slug, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(rule_slug, "ssp245", "2020-2040")
    denominator = float(rule.absolute_weight) + float(rule.change_weight)
    expected = (
        float(rule.absolute_weight) * out[abs_col].astype(float)
        + float(rule.change_weight) * out[chg_col].astype(float)
    ) / denominator
    pd.testing.assert_series_equal(
        out[score_col].astype(float).reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


def test_infrastructure_rx1day_score_equals_weighted_mean_of_abs_chg_and_imp_from_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_infrastructure_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_infrastructure_risk(tmp_path)

    rule_slug = "rx1day_ge_200"
    rule = next(
        rule for rule in PROPOSAL_BUNDLES_BY_SLUG["composite_infrastructure_risk"].rules
        if rule.rule_slug == rule_slug
    )
    abs_col = proposal_rule_abs_score_column(rule_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(rule_slug, "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column(rule_slug, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(rule_slug, "ssp245", "2020-2040")
    denom = float(rule.absolute_weight) + float(rule.change_weight) + float(rule.impact_weight)
    expected = (
        float(rule.absolute_weight) * out[abs_col].astype(float)
        + float(rule.change_weight) * out[chg_col].astype(float)
        + float(rule.impact_weight) * out[imp_col].astype(float)
    ) / denom
    pd.testing.assert_series_equal(
        out[score_col].astype(float).reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


# ---------------------------------------------------------------------------
# Test F — variability proxy aligns to canonical key under shuffled helper rows
# ---------------------------------------------------------------------------


def _build_hydropower_helper_frame(
    ids: pd.DataFrame,
    *,
    target_scenario: str = "ssp245",
    target_period: str = "2020-2040",
    values: tuple[float, float, float] = (0.40, 0.10, 0.25),
    baseline_token: str = "1995-2014",
    historical_values: tuple[float, float, float] = (0.20, 0.20, 0.20),
    include_future: bool = True,
    include_historical: bool = True,
) -> pd.DataFrame:
    """Build a synthetic R95p interannual-variability helper frame.

    Post-CHG-0036 the helper rule is a blended absolute+change rule, so the
    helper frame carries both the future ``__{scenario}__{period}__mean``
    columns and a single hyphenated ``__historical__{token}__mean`` baseline
    column. ``include_future`` / ``include_historical`` let tests drop either
    side to exercise the coverage gate and silent-degradation guards.
    """
    out = ids.copy()
    if include_future:
        out[f"r95p_interannual_variability__{target_scenario}__{target_period}__mean"] = list(values)
        # Hydropower bundle iterates all (scenario, period); seed all
        # combinations so the bundle compute does not raise on missing columns.
        for scen in ("ssp245", "ssp585"):
            for per in ("2020-2040", "2040-2060", "2060-2080"):
                col = f"r95p_interannual_variability__{scen}__{per}__mean"
                if col not in out.columns:
                    out[col] = list(values)
    if include_historical:
        out[f"r95p_interannual_variability__historical__{baseline_token}__mean"] = list(historical_values)
    return out


def _seed_hydropower_metric_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
) -> None:
    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"]
    for rule in bundle.rules:
        slug = rule.metric_slug
        if slug == "r95p_interannual_variability":
            continue
        base = _metric_base(slug)
        df = ids.copy()
        for scen in ("ssp245", "ssp585"):
            for per in ("2020-2040", "2040-2060", "2060-2080"):
                df[f"{base}__{scen}__{per}__mean"] = [50.0, 30.0, 40.0]
        df[f"{base}__historical__1995-2014__mean"] = [20.0, 15.0, 18.0]
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


def test_variability_proxy_aligns_to_canonical_key_under_shuffled_helper_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_hydropower_metric_masters(tmp_path, ids=ids, state_name=state_name)

    canonical_helper = _build_hydropower_helper_frame(ids)
    shuffled_helper = canonical_helper.iloc[[2, 0, 1]].reset_index(drop=True)

    out_canonical = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
        helper_frame=canonical_helper,
    )
    out_shuffled = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
        helper_frame=shuffled_helper,
    )

    var_col = proposal_rule_score_column("r95p_interannual_variability_norm", "ssp245", "2020-2040")
    canonical_by_key = dict(zip(out_canonical["district_key"], out_canonical[var_col]))
    shuffled_by_key = dict(zip(out_shuffled["district_key"], out_shuffled[var_col]))
    assert canonical_by_key == shuffled_by_key, (
        "Variability proxy must align by canonical key, not row order. "
        f"canonical: {canonical_by_key}; shuffled: {shuffled_by_key}"
    )


def test_hydropower_rules_persist_expected_active_lens_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHG-0036: Hydropower Rx5day/CDD are three-lens-active (abs+chg+imp);
    the R95p helper rule is absolute+change only (no impact lens)."""
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_hydropower_metric_masters(tmp_path, ids=ids, state_name=state_name)

    helper = _build_hydropower_helper_frame(ids)
    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
        helper_frame=helper,
    )

    for rule_slug in ("rx5day_ge_500", "cdd_ge_60"):
        for lens_col in (
            proposal_rule_abs_score_column(rule_slug, "ssp245", "2020-2040"),
            proposal_rule_chg_score_column(rule_slug, "ssp245", "2020-2040"),
            proposal_rule_imp_score_column(rule_slug, "ssp245", "2020-2040"),
        ):
            assert lens_col in out.columns, f"missing Hydropower lens column {lens_col!r}"

    r95p = "r95p_interannual_variability_norm"
    assert proposal_rule_abs_score_column(r95p, "ssp245", "2020-2040") in out.columns
    assert proposal_rule_chg_score_column(r95p, "ssp245", "2020-2040") in out.columns
    assert proposal_rule_imp_score_column(r95p, "ssp245", "2020-2040") not in out.columns


def test_hydropower_r95p_score_equals_weighted_mean_of_abs_and_chg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The blended R95p helper rule uses both helper future (absolute) and
    helper historical (change) values: score = (abs_w*abs + chg_w*chg) /
    (abs_w + chg_w), since the impact lens is inactive (weight 0)."""
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_hydropower_metric_masters(tmp_path, ids=ids, state_name=state_name)

    # Future and historical helper values chosen with spread so the abs and
    # change reference distributions are non-flat and finite for every row.
    helper = _build_hydropower_helper_frame(
        ids,
        values=(0.40, 0.10, 0.25),
        historical_values=(0.20, 0.30, 0.10),
    )
    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
        helper_frame=helper,
    )

    rule = next(
        rule
        for rule in PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"].rules
        if rule.rule_slug == "r95p_interannual_variability_norm"
    )
    abs_col = proposal_rule_abs_score_column(rule.rule_slug, "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column(rule.rule_slug, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(rule.rule_slug, "ssp245", "2020-2040")

    assert out[chg_col].notna().any()
    denominator = float(rule.absolute_weight) + float(rule.change_weight)
    expected = (
        float(rule.absolute_weight) * out[abs_col].astype(float)
        + float(rule.change_weight) * out[chg_col].astype(float)
    ) / denominator
    pd.testing.assert_series_equal(
        out[score_col].astype(float).reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


def test_hydropower_r95p_change_lens_nan_when_helper_historical_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Silent-degradation guard: with the helper future column present but the
    helper historical column absent, the R95p change lens is all-NaN while the
    rule still scores via the absolute lens and the bundle is retained."""
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_hydropower_metric_masters(tmp_path, ids=ids, state_name=state_name)

    helper = _build_hydropower_helper_frame(ids, include_historical=False)
    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_asset_risk_hydropower"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
        helper_frame=helper,
    )

    r95p = "r95p_interannual_variability_norm"
    chg_col = proposal_rule_chg_score_column(r95p, "ssp245", "2020-2040")
    abs_col = proposal_rule_abs_score_column(r95p, "ssp245", "2020-2040")
    score_col = proposal_rule_score_column(r95p, "ssp245", "2020-2040")
    bundle_col = proposal_bundle_mean_column(
        "composite_asset_risk_hydropower", "ssp245", "2020-2040"
    )

    assert chg_col in out.columns
    assert out[chg_col].isna().all()
    # Rule still scores on the absolute lens; bundle is retained (r95p only
    # contributes 0.20, so available weight stays >= 0.70 regardless).
    assert out[score_col].notna().any()
    pd.testing.assert_series_equal(
        out[score_col].astype(float).reset_index(drop=True),
        out[abs_col].astype(float).reset_index(drop=True),
        check_names=False,
    )
    assert out[bundle_col].notna().any()


# ---------------------------------------------------------------------------
# Test G — Health Risk txx_ge_45 blended decomposition on known inputs
# ---------------------------------------------------------------------------


def test_health_risk_txx_ge_45_blended_score_matches_known_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hand-computed regression for a true three-lens rule.

    Inputs:
      A: current=46 °C, baseline=43 °C  -> delta=+3
      B: current=42 °C, baseline=41 °C  -> delta=+1
      C: current=44 °C, baseline=42 °C  -> delta=+2

    abs lens (p10/p90 scaling over currents [42, 44, 46]):
      lo = 42 + 0.2*(44-42) = 42.4
      hi = 44 + 0.8*(46-44) = 45.6
      A: clip((46-42.4)/3.2)=100  B: clip((42-42.4)/3.2)=0  C: (44-42.4)/3.2=50

    chg lens (p10/p90 scaling over deltas [1, 2, 3]):
      lo = 1.2, hi = 2.8
      A: 100  B: 0  C: 50

    imp lens (band 40-45):
      A: clip((46-40)/5)=100  B: (42-40)/5=40  C: (44-40)/5=80

    Blended (weights 0.40, 0.25, 0.35; sum 1.0):
      A: 0.40*100 + 0.25*100 + 0.35*100 = 100.0
      B: 0.40*0   + 0.25*0   + 0.35*40  = 14.0
      C: 0.40*50  + 0.25*50  + 0.35*80  = 60.5
    """
    state_name = "Telangana"
    ids = _district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_health_risk_masters(tmp_path, ids=ids, state_name=state_name)

    out = _run_health_risk(tmp_path)

    abs_col = proposal_rule_abs_score_column("txx_ge_45", "ssp245", "2020-2040")
    chg_col = proposal_rule_chg_score_column("txx_ge_45", "ssp245", "2020-2040")
    imp_col = proposal_rule_imp_score_column("txx_ge_45", "ssp245", "2020-2040")
    score_col = proposal_rule_score_column("txx_ge_45", "ssp245", "2020-2040")

    by_district = out.set_index("district")
    expected_abs = {"A": 100.0, "B": 0.0, "C": 50.0}
    expected_chg = {"A": 100.0, "B": 0.0, "C": 50.0}
    expected_imp = {"A": 100.0, "B": 40.0, "C": 80.0}
    expected_blend = {"A": 100.0, "B": 14.0, "C": 60.5}

    for district, expected_value in expected_abs.items():
        assert by_district.at[district, abs_col] == pytest.approx(expected_value, abs=1e-6)
    for district, expected_value in expected_chg.items():
        assert by_district.at[district, chg_col] == pytest.approx(expected_value, abs=1e-6)
    for district, expected_value in expected_imp.items():
        assert by_district.at[district, imp_col] == pytest.approx(expected_value, abs=1e-6)
    for district, expected_value in expected_blend.items():
        assert by_district.at[district, score_col] == pytest.approx(expected_value, abs=1e-6)
