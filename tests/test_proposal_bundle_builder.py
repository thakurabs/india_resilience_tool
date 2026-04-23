from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.compute.proposal_bundles import (
    compute_proposal_bundle_master_frame,
    compute_r95p_interannual_variability_master_frame,
    parse_args,
    build_proposal_bundles,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.proposal_bundles import PROPOSAL_BUNDLES_BY_SLUG


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


def test_compute_proposal_bundle_master_frame_scores_thresholds_and_baseline_change(tmp_path: Path) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "B"],
            "district_key": ["telangana|a", "telangana|b"],
        }
    )
    metric_values = {
        "pr_max_1day_precip": [220.0, 180.0],
        "pr_max_5day_precip": [320.0, 280.0],
        "pr_consecutive_dry_days_lt1mm": [25.0, 15.0],
        "txx_annual_max": [41.0, 39.0],
        "r95p_very_wet_precip": [150.0, 110.0],
    }
    for slug, values in metric_values.items():
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2020-2040__mean"] = values
        if slug == "r95p_very_wet_precip":
            df[f"{metric_base}__historical__1995-2014__mean"] = [100.0, 100.0]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    score_col = "composite_agricultural_risk__ssp245__2020-2040__mean"
    by_district = dict(zip(out["district"], out[score_col]))
    assert by_district["A"] == 100.0
    assert by_district["B"] == 0.0


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


def test_build_proposal_bundles_fails_target_when_trend_series_missing(tmp_path: Path) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["A"],
            "district_key": ["telangana|a"],
        }
    )
    for slug in (
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "r99p_extreme_wet_precip",
        "pr_consecutive_dry_days_lt1mm",
        "hwfi_tmean_90p",
    ):
        df = ids.copy()
        metric_base = METRICS_BY_SLUG[slug].periods_metric_col or METRICS_BY_SLUG[slug].value_col or slug
        df[f"{metric_base}__ssp245__2020-2040__mean"] = [1.0]
        if slug == "pr_consecutive_dry_days_lt1mm":
            df[f"{metric_base}__historical__1995-2014__mean"] = [1.0]
        _write_master(tmp_path, slug=slug, state_name=state_name, level="district", df=df)

    written, warnings, failures = build_proposal_bundles(
        levels=("district",),
        bundle_slugs=("composite_investment_financial_risk",),
        data_dir=tmp_path,
        dry_run=False,
        overwrite=True,
        quiet=True,
    )

    assert warnings == []
    assert written == []
    assert failures
    assert "Missing mandatory yearly ensemble series" in failures[0]


def test_build_proposal_bundles_dry_run_auto_discovers_states_and_returns_target_paths(tmp_path: Path) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["A"],
            "district_key": ["telangana|a"],
        }
    )
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


def test_parse_args_rejects_bundle_and_metric_together() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--bundle", "composite_health_risk", "--metric", "composite_health_risk"])


def test_parse_args_accepts_deprecated_metric_alias() -> None:
    args = parse_args(["--metric", "composite_health_risk"])
    assert args.bundle == ["composite_health_risk"]
