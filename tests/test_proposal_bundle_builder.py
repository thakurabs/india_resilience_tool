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
from india_resilience_tool.config.proposal_bundles import PROPOSAL_BUNDLES_BY_SLUG


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


def test_compute_proposal_bundle_master_frame_scores_thresholds_and_baseline_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    ids = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "B"],
            "district_key": ["telangana|a", "telangana|b"],
        }
    )
    _patch_canonical_units(monkeypatch, district_df=ids)
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


def test_build_proposal_bundles_fails_target_when_trend_series_missing(
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
        "pr_2day_heavy_rainfall_events_ge150mm": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
        "pr_consecutive_dry_days_lt1mm": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "wsdi_warm_spell_days": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
    }
    values_by_slug = {
        "pr_max_1day_precip": 220.0,
        "pr_2day_heavy_rainfall_events_ge150mm": 2.0,
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
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__mean"] == 75.0


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
        "pr_2day_heavy_rainfall_events_ge150mm": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
        "pr_consecutive_dry_days_lt1mm": {"state": state_name, "district": "ADILABAD", "block": "ADILABAD RURAL"},
        "wsdi_warm_spell_days": {"state": state_name, "district": "Adilabad", "block": "Adilabad Rural"},
    }
    values_by_slug = {
        "pr_max_1day_precip": 220.0,
        "pr_2day_heavy_rainfall_events_ge150mm": 2.0,
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
    assert row["composite_life_livelihood_loss_risk__ssp245__2040-2060__mean"] == 75.0


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
        ("pr_2day_heavy_rainfall_events_ge150mm", 2.0),
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
        ("pr_2day_heavy_rainfall_events_ge150mm", 2.0),
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


def test_parse_args_rejects_bundle_and_metric_together() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--bundle", "composite_health_risk", "--metric", "composite_health_risk"])


def test_parse_args_accepts_deprecated_metric_alias() -> None:
    args = parse_args(["--metric", "composite_health_risk"])
    assert args.bundle == ["composite_health_risk"]
