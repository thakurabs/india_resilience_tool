"""Tests for proposal bundle builders.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import india_resilience_tool.compute.proposal_bundles as proposal_bundle_module
from india_resilience_tool.compute.proposal_bundles import (
    TargetBuildError,
    compute_proposal_bundle_master_frame,
    compute_r95p_interannual_variability_master_frame,
    parse_args,
    build_proposal_bundles,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES_BY_SLUG,
    ProposalBundleSpec,
    ProposalRuleSpec,
)


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


def _write_master(
    tmp_path: Path,
    *,
    slug: str,
    state_name: str,
    level: str,
    df: pd.DataFrame,
) -> None:
    root = tmp_path / "processed" / slug / state_name
    root.mkdir(parents=True, exist_ok=True)
    filename = "master_metrics_by_block.csv" if level == "block" else "master_metrics_by_district.csv"
    df.to_csv(root / filename, index=False)


def _write_district_yearly(
    tmp_path: Path,
    *,
    slug: str,
    state_name: str,
    district_name: str,
    scenario: str,
    rows: list[dict[str, object]],
) -> None:
    root = tmp_path / "processed" / slug / state_name / district_name
    root.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(root / "district_yearly_ensemble_stats.csv", index=False)


def _seed_investment_metric_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    target_period: str = "2020-2040",
    target_scenario: str = "ssp245",
    baseline_period: str = "1995-2014",
    current_by_slug: dict[str, list[float]] | None = None,
    baseline_by_slug: dict[str, list[float]] | None = None,
) -> None:
    current_defaults = {
        "pr_max_1day_precip": [120.0, 80.0, 100.0],
        "pr_max_5day_precip": [320.0, 180.0, 250.0],
        "r99p_extreme_wet_precip": [400.0, 150.0, 250.0],
        "pr_consecutive_dry_days_lt1mm": [60.0, 30.0, 45.0],
        "hwfi_tmean_90p": [12.0, 6.0, 9.0],
    }
    baseline_defaults = {
        "pr_max_1day_precip": [60.0, 50.0, 55.0],
        "pr_max_5day_precip": [160.0, 140.0, 150.0],
        "r99p_extreme_wet_precip": [200.0, 120.0, 160.0],
        "pr_consecutive_dry_days_lt1mm": [30.0, 25.0, 30.0],
        "hwfi_tmean_90p": [6.0, 5.0, 6.0],
    }
    if current_by_slug:
        current_defaults.update(current_by_slug)
    if baseline_by_slug:
        baseline_defaults.update(baseline_by_slug)

    for slug, values in current_defaults.items():
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__{target_scenario}__{target_period}__mean"] = values
        df[f"{metric_base}__historical__{baseline_period}__mean"] = baseline_defaults[slug]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)


def test_compute_agricultural_risk_uses_weighted_lens_rule_scores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-CHG-0032: every Agricultural Risk rule is lens-decomposed
    (absolute + change + impact). With a uniform baseline of 5.0, the abs and
    chg lenses both produce the same p10-p90 ordering [A=100, B=0, C=50] for
    every rule; impact lens varies by per-metric impact band. The composite
    is the weighted mean of the seven lens-blended rule scores.
    """
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    metric_values = {
        "txx_annual_max": [30.0, 10.0, 20.0],
        "txge35_extreme_heat_days": [30.0, 10.0, 20.0],
        "wsdi_warm_spell_days": [30.0, 10.0, 20.0],
        "spi3_count_events_lt_minus1": [30.0, 10.0, 20.0],
        "spi3_max_spell_lt_minus1": [30.0, 10.0, 20.0],
        "pr_max_5day_precip": [30.0, 10.0, 20.0],
        "tnle10_cold_nights": [30.0, 10.0, 20.0],
    }
    baseline_values = [5.0, 5.0, 5.0]
    for slug, values in metric_values.items():
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2020-2040__mean"] = values
        df[f"{metric_base}__historical__1995-2014__mean"] = baseline_values
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    score_col = "composite_agricultural_risk__ssp245__2020-2040__mean"
    available_count_col = "composite_agricultural_risk__ssp245__2020-2040__available_rule_count"
    available_weight_col = "composite_agricultural_risk__ssp245__2020-2040__available_rule_weight_fraction"
    by_district = dict(zip(out["district"], out[score_col]))

    # Hand-computed expected composite values; full derivation in
    # docs/lens_scoring_methodology.md section 12 and the bundle config:
    #   abs and chg lenses both yield [A=100, B=0, C=50] for every rule
    #   under inputs [30,10,20] vs uniform baseline [5,5,5].
    #   impact lens per rule (band-clipped (value - low) / (high - low) * 100):
    #     TXx 35-45 on [30,10,20]                -> [0, 0, 0]
    #     TXge35 15-60 on [30,10,20]             -> [33.33, 0, 11.11]
    #     WSDI 6-18 on [30,10,20]                -> [100, 33.33, 100]
    #     SPI3 episodes 3-12 on [30,10,20]       -> [100, 77.78, 100]
    #     SPI3 longest 3-12 on [30,10,20]        -> [100, 77.78, 100]
    #     Rx5day 250-500 on [30,10,20]           -> [0, 0, 0]
    #     TNle10 10-30 on [30,10,20]             -> [100, 0, 50]
    #   blended per-rule, then weighted sum with rule weights
    #   (TXx 0.15, TXge35 0.10, WSDI 0.10, SPI3-ep 0.15, SPI3-ls 0.15,
    #    Rx5day 0.20, TNle10 0.15) -> {A: 91.5, B: 4.0, C: 48.6667}.
    assert by_district["A"] == pytest.approx(91.5, abs=1e-2)
    assert by_district["B"] == pytest.approx(4.0, abs=1e-2)
    assert by_district["C"] == pytest.approx(48.6667, abs=1e-2)
    assert out[available_count_col].tolist() == [7, 7, 7]
    assert out[available_weight_col].tolist() == [1.0, 1.0, 1.0]


def test_compute_agricultural_risk_applies_available_weight_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    metric_values = {
        "txx_annual_max": [30.0, 10.0, 20.0],
        "txge35_extreme_heat_days": [30.0, 10.0, 20.0],
        "wsdi_warm_spell_days": [30.0, 10.0, 20.0],
        "spi3_count_events_lt_minus1": [30.0, 10.0, 20.0],
        "spi3_max_spell_lt_minus1": [30.0, 10.0, 20.0],
        "pr_max_5day_precip": [float("nan"), 10.0, float("nan")],
        "tnle10_cold_nights": [30.0, 10.0, float("nan")],
    }
    for slug, values in metric_values.items():
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2020-2040__mean"] = values
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    score_col = "composite_agricultural_risk__ssp245__2020-2040__mean"
    available_count_col = "composite_agricultural_risk__ssp245__2020-2040__available_rule_count"
    available_weight_col = "composite_agricultural_risk__ssp245__2020-2040__available_rule_weight_fraction"
    by_district = {row["district"]: row for _, row in out.iterrows()}
    assert by_district["A"][available_count_col] == 6
    assert by_district["A"][available_weight_col] == pytest.approx(0.80)
    assert pd.notna(by_district["A"][score_col])
    assert by_district["C"][available_count_col] == 5
    # Post-CHG-0032: TNle10 rule weight is 0.15 (was 0.20). District C is
    # missing both Rx5day (0.20) and TNle10 (0.15) -> 1.0 - 0.35 = 0.65,
    # still below the 0.70 coverage gate, so composite remains NaN.
    assert by_district["C"][available_weight_col] == pytest.approx(0.65)
    assert pd.isna(by_district["C"][score_col])


def test_compute_r95p_interannual_variability_master_frame_uses_cv_and_nan_for_insufficient_points(tmp_path: Path) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "B"],
            "district_key": ["telangana|a", "telangana|b"],
        }
    )
    _write_master(tmp_path, slug="r95p_very_wet_precip", state_name=state_name, level="district", df=ids)
    _write_district_yearly(
        tmp_path,
        slug="r95p_very_wet_precip",
        state_name=state_name,
        district_name="A",
        scenario="ssp245",
        rows=[
            {"year": 2020, "mean": 10.0, "scenario": "ssp245"},
            {"year": 2021, "mean": 20.0, "scenario": "ssp245"},
            {"year": 2020, "mean": 10.0, "scenario": "ssp585"},
            {"year": 2021, "mean": 20.0, "scenario": "ssp585"},
        ],
    )
    _write_district_yearly(
        tmp_path,
        slug="r95p_very_wet_precip",
        state_name=state_name,
        district_name="B",
        scenario="ssp245",
        rows=[
            {"year": 2020, "mean": 5.0, "scenario": "ssp245"},
            {"year": 2020, "mean": 5.0, "scenario": "ssp585"},
        ],
    )

    out = compute_r95p_interannual_variability_master_frame(
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    col = "r95p_interannual_variability__ssp245__2020-2040__mean"
    values = dict(zip(out["district"], out[col]))
    assert round(float(values["A"]), 6) == round((5.0 / 15.0), 6)
    assert pd.isna(values["B"])


def test_compute_investment_risk_builds_without_yearly_series(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_investment_metric_masters(tmp_path, ids=ids, state_name=state_name)

    written, warnings, failures = build_proposal_bundles(
        levels=("district",),
        bundle_slugs=("composite_investment_financial_risk",),
        data_dir=tmp_path,
        dry_run=False,
        overwrite=True,
        quiet=True,
    )

    assert failures == []
    assert warnings == []
    assert written == [
        tmp_path / "processed" / "composite_investment_financial_risk" / state_name / "master_metrics_by_district.csv"
    ]


def test_compute_investment_risk_applies_available_weight_gate_pass_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_investment_metric_masters(
        tmp_path,
        ids=ids,
        state_name=state_name,
        current_by_slug={"pr_max_1day_precip": [float("nan"), 80.0, 100.0]},
    )

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_investment_financial_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    score_col = "composite_investment_financial_risk__ssp245__2020-2040__mean"
    available_weight_col = "composite_investment_financial_risk__ssp245__2020-2040__available_rule_weight_fraction"
    row_a = out.loc[out["district"] == "A"].iloc[0]
    assert row_a[available_weight_col] == pytest.approx(0.75)
    assert pd.notna(row_a[score_col])


def test_compute_investment_risk_applies_available_weight_gate_fail_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_investment_metric_masters(
        tmp_path,
        ids=ids,
        state_name=state_name,
        current_by_slug={
            "pr_max_1day_precip": [float("nan"), 80.0, 100.0],
            "pr_consecutive_dry_days_lt1mm": [float("nan"), 30.0, 45.0],
        },
    )

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_investment_financial_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    score_col = "composite_investment_financial_risk__ssp245__2020-2040__mean"
    available_weight_col = "composite_investment_financial_risk__ssp245__2020-2040__available_rule_weight_fraction"
    row_a = out.loc[out["district"] == "A"].iloc[0]
    assert row_a[available_weight_col] == pytest.approx(0.50)
    assert pd.isna(row_a[score_col])


def test_synthetic_trend_rule_still_requires_yearly_series(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["A"],
            "district_key": ["telangana|a"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)

    slug = "pr_max_1day_precip"
    metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
    df = ids.copy()
    df[f"{metric_base}__ssp245__2020-2040__mean"] = [120.0]
    _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    synthetic_bundle = ProposalBundleSpec(
        bundle_label="Synthetic Trend Coverage",
        composite_slug="synthetic_trend_coverage",
        supported_levels=("district",),
        rules=(
            ProposalRuleSpec(
                rule_slug="synthetic_rx1day_trend",
                display_label="Synthetic rainfall trend pressure",
                metric_slug=slug,
                rule_type="trend",
            ),
        ),
    )

    with pytest.raises(TargetBuildError, match="Missing mandatory yearly ensemble series"):
        compute_proposal_bundle_master_frame(
            synthetic_bundle,
            level="district",
            state_name=state_name,
            data_dir=tmp_path,
            warnings=[],
        )


def test_build_proposal_bundles_dry_run_auto_discovers_states_and_returns_target_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["A"],
            "district_key": ["telangana|a"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
    for slug in (
        "txx_annual_max",
        "wsdi_warm_spell_days",
        "tnx_annual_max",
        "pr_max_1day_precip",
        "cwd_consecutive_wet_days",
    ):
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2020-2040__mean"] = [1.0]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    written, _, failures = build_proposal_bundles(
        levels=("district",),
        bundle_slugs=("composite_health_risk",),
        data_dir=tmp_path,
        dry_run=True,
        overwrite=False,
        quiet=True,
    )

    assert failures == []
    assert written == [
        tmp_path / "processed" / "composite_health_risk" / state_name / "master_metrics_by_district.csv"
    ]


def test_load_canonical_unit_frame_normalizes_stale_block_keys_from_adm3_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    stale_blocks = pd.DataFrame(
        {
            "state_name": [state_name, state_name],
            "district_name": ["Adilabad", "Adilabad"],
            "block_name": ["Adilabad Rural", "Bazar Hatnur"],
            "block_key": [
                "TELANGANA::Adilabad::Adilabad Rural",
                "TELANGANA::Adilabad::Bazar Hatnur",
            ],
        }
    )

    def _fake_load_local_adm3(*args, **kwargs) -> pd.DataFrame:
        return stale_blocks.copy()

    monkeypatch.setattr(proposal_bundle_module, "load_local_adm3", _fake_load_local_adm3)

    out = proposal_bundle_module._load_canonical_unit_frame(
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert out["block_key"].tolist() == [
        "telangana|adilabad|adilabad rural",
        "telangana|adilabad|bazar hatnur",
    ]


def test_compute_proposal_bundle_master_frame_merges_block_metrics_by_canonical_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    canonical_blocks = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["Adilabad"],
            "block": ["Adilabad Rural"],
            "block_key": ["telangana|adilabad|adilabad rural"],
        }
    )
    _patch_canonical_units(monkeypatch, block_df=canonical_blocks)

    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_life_livelihood_loss_risk"]
    rows_by_slug = {
        "pr_max_1day_precip": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "pr_max_5day_precip": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
        "pr_consecutive_dry_days_lt1mm": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "wsdi_warm_spell_days": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
    }
    values_by_slug = {
        "pr_max_1day_precip": 220.0,
        "pr_max_5day_precip": 240.0,
        "pr_consecutive_dry_days_lt1mm": 20.0,
        "wsdi_warm_spell_days": 7.0,
    }

    for slug, row in rows_by_slug.items():
        df = pd.DataFrame([row])
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2040-2060__mean"] = [values_by_slug[slug]]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="block", df=df)

    out = compute_proposal_bundle_master_frame(
        bundle,
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    assert out.shape[0] == 1
    row = out.iloc[0]
    assert row["state"] == "Telangana"
    assert row["district"] == "Adilabad"
    assert row["block"] == "Adilabad Rural"
    assert row["block_key"] == "telangana|adilabad|adilabad rural"
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__available_rule_count"] == 4
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__mean"] == 50.0


def test_compute_proposal_bundle_master_frame_normalizes_stale_canonical_block_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    stale_blocks = pd.DataFrame(
        {
            "state_name": [state_name],
            "district_name": ["Adilabad"],
            "block_name": ["Adilabad Rural"],
            "block_key": ["TELANGANA::Adilabad::Adilabad Rural"],
        }
    )

    def _fake_load_local_adm3(*args, **kwargs) -> pd.DataFrame:
        return stale_blocks.copy()

    monkeypatch.setattr(proposal_bundle_module, "load_local_adm3", _fake_load_local_adm3)

    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_life_livelihood_loss_risk"]
    rows_by_slug = {
        "pr_max_1day_precip": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "pr_max_5day_precip": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
        "pr_consecutive_dry_days_lt1mm": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "wsdi_warm_spell_days": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
    }
    values_by_slug = {
        "pr_max_1day_precip": 220.0,
        "pr_max_5day_precip": 240.0,
        "pr_consecutive_dry_days_lt1mm": 20.0,
        "wsdi_warm_spell_days": 7.0,
    }

    for slug, row in rows_by_slug.items():
        df = pd.DataFrame([row])
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2040-2060__mean"] = [values_by_slug[slug]]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="block", df=df)

    out = compute_proposal_bundle_master_frame(
        bundle,
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    assert out.shape[0] == 1
    row = out.iloc[0]
    assert row["block_key"] == "telangana|adilabad|adilabad rural"
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__available_rule_count"] == 4
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__mean"] == 50.0


def test_compute_proposal_bundle_master_frame_fails_on_duplicate_source_block_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    canonical_blocks = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["Adilabad"],
            "block": ["Adilabad Rural"],
            "block_key": ["telangana|adilabad|adilabad rural"],
        }
    )
    _patch_canonical_units(monkeypatch, block_df=canonical_blocks)

    duplicate_metric = pd.DataFrame(
        [
            {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
            {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
        ]
    )
    metric_base = METRICS_BY_SLUG["pr_max_1day_precip"].periods_metric_col or METRICS_BY_SLUG["pr_max_1day_precip"].value_col
    duplicate_metric[f"{metric_base}__ssp245__2040-2060__mean"] = [220.0, 150.0]
    _write_master(tmp_path, slug="pr_max_1day_precip", state_name=state_name, level="block", df=duplicate_metric)

    for slug, value in (
        ("pr_max_5day_precip", 240.0),
        ("pr_consecutive_dry_days_lt1mm", 20.0),
        ("wsdi_warm_spell_days", 7.0),
    ):
        df = canonical_blocks[["state", "district", "block"]].copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2040-2060__mean"] = [value]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="block", df=df)

    with pytest.raises(TargetBuildError, match="pr_max_1day_precip"):
        compute_proposal_bundle_master_frame(
            PROPOSAL_BUNDLES_BY_SLUG["composite_life_livelihood_loss_risk"],
            level="block",
            state_name=state_name,
            data_dir=tmp_path,
            warnings=[],
        )


def test_compute_proposal_bundle_master_frame_preserves_full_canonical_block_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    canonical_blocks = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["Adilabad", "Adilabad"],
            "block": ["North", "South"],
            "block_key": ["telangana|adilabad|north", "telangana|adilabad|south"],
        }
    )
    _patch_canonical_units(monkeypatch, block_df=canonical_blocks)

    first_only = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["ADILABAD"],
            "block": ["NORTH"],
        }
    )
    for slug, value in (
        ("pr_max_1day_precip", 220.0),
        ("pr_max_5day_precip", 240.0),
        ("pr_consecutive_dry_days_lt1mm", 45.0),
        ("wsdi_warm_spell_days", 7.0),
    ):
        df = first_only.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2040-2060__mean"] = [value]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="block", df=df)

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_life_livelihood_loss_risk"],
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    assert out["block_key"].tolist() == ["telangana|adilabad|north", "telangana|adilabad|south"]
    south = out.loc[out["block_key"] == "telangana|adilabad|south"].iloc[0]
    assert south["composite_life_livelihood_loss_risk__ssp245__2040-2060__available_rule_count"] == 0
    assert pd.isna(south["composite_life_livelihood_loss_risk__ssp245__2040-2060__mean"])


def test_score_by_reference_distribution_handles_direction_and_flat_values() -> None:
    values = pd.Series([10.0, 20.0, 30.0])

    higher = proposal_bundle_module._score_by_reference_distribution(values, direction="higher_worse")
    lower = proposal_bundle_module._score_by_reference_distribution(values, direction="lower_worse")
    flat = proposal_bundle_module._score_by_reference_distribution(pd.Series([5.0, 5.0, 5.0]))

    assert higher.iloc[0] < higher.iloc[1] < higher.iloc[2]
    assert lower.iloc[0] > lower.iloc[1] > lower.iloc[2]
    assert flat.tolist() == [50.0, 50.0, 50.0]


def test_score_impact_threshold_is_continuous() -> None:
    values = pd.Series([39.0, 40.0, 42.5, 45.0, 46.0])

    score = proposal_bundle_module._score_impact_threshold(
        values,
        impact_low=40.0,
        impact_high=45.0,
        direction="higher_worse",
    )

    assert score.tolist() == [0.0, 0.0, 50.0, 100.0, 100.0]


def test_change_values_returns_nan_for_zero_relative_baseline() -> None:
    current = pd.Series([5.0, 20.0])
    baseline = pd.Series([0.0, 10.0])

    change = proposal_bundle_module._change_values(
        current,
        baseline,
        metric_slug="pr_max_1day_precip",
        change_mode="relative_pct",
    )

    assert pd.isna(change.iloc[0])
    assert change.iloc[1] == 100.0


def test_append_score_quality_warnings_flags_flat_score_columns() -> None:
    warnings: list[proposal_bundle_module.BuildWarning] = []

    proposal_bundle_module._append_score_quality_warnings(
        pd.Series([50.0, 50.0, 50.0]),
        warnings=warnings,
        bundle_slug="composite_test",
        level="district",
        state_name="Telangana",
        column_name="flat_score",
        label="bundle",
    )

    assert warnings
    assert "Flat proposal bundle score" in warnings[0].message


def test_parse_args_rejects_bundle_and_metric_together() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--bundle", "composite_health_risk", "--metric", "composite_health_risk"])


def test_parse_args_accepts_deprecated_metric_alias() -> None:
    args = parse_args(["--metric", "composite_health_risk"])
    assert args.bundle == ["composite_health_risk"]
