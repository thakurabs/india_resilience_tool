"""Grid-first provenance and strict-schema contract tests for proposal bundles.

These tests lock the invariants that keep the proposal-bundle compute layer
faithful to its upstream grid-first masters:

1. Master I/O resolves through ``resolve_processed_root(slug, mode="portfolio")``
   and uses the canonical ``master_metrics_by_<level>.csv`` candidate path
   (the loader is free to read a parquet companion).
2. The compute module does not import or call raster / xarray / gridfirst
   modules directly; bundles consume materialized masters only.
3. Wrong canonical period, scenario, or stat produces ``NaN`` rule scores
   rather than a silent substitution. (The historical baseline-token fallback
   in ``find_baseline_column_for_metric`` is a separate silent-drift surface
   owned by CHG-0025.)
4. The yearly-series I/O path used by trend rules also routes through
   ``resolve_processed_root(..., mode="portfolio")``.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import india_resilience_tool.compute.proposal_bundles as proposal_bundle_module
from india_resilience_tool.compute.proposal_bundles import (
    _load_legacy_yearly_series,
    _load_metric_master,
    compute_proposal_bundle_master_frame,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import resolve_processed_root
from india_resilience_tool.config.proposal_bundles import PROPOSAL_BUNDLES_BY_SLUG


# ---------------------------------------------------------------------------
# Local synthetic helpers (mirror the patterns in test_proposal_bundle_builder.py
# without depending on cross-file imports, which are fragile without conftest).
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


def _write_district_master(
    tmp_path: Path,
    *,
    slug: str,
    state_name: str,
    df: pd.DataFrame,
) -> Path:
    root = tmp_path / "processed" / slug / state_name
    root.mkdir(parents=True, exist_ok=True)
    target = root / "master_metrics_by_district.csv"
    df.to_csv(target, index=False)
    return target


def _metric_base(slug: str) -> str:
    spec = METRICS_BY_SLUG[slug]
    return spec.periods_metric_col or spec.value_col or slug


def _agricultural_risk_district_ids(state_name: str = "Telangana") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["telangana|a", "telangana|b", "telangana|c"],
        }
    )


def _seed_agricultural_risk_masters(
    tmp_path: Path,
    *,
    ids: pd.DataFrame,
    state_name: str,
    column_suffix: str,
) -> None:
    """Seed every Agricultural Risk metric master with one column ``<base>{suffix}``."""
    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"]
    for rule in bundle.rules:
        slug = rule.metric_slug
        df = ids.copy()
        base = _metric_base(slug)
        df[f"{base}{column_suffix}"] = [30.0, 10.0, 20.0]
        _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)


# ---------------------------------------------------------------------------
# Test 1 — master I/O resolves the portfolio processed root and CSV candidate
# ---------------------------------------------------------------------------


def test_load_metric_master_resolves_portfolio_processed_root_and_canonical_csv_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_name = "Telangana"
    slug = "txx_annual_max"
    base = _metric_base(slug)

    ids = _agricultural_risk_district_ids(state_name)
    df = ids.copy()
    df[f"{base}__ssp245__2020-2040__mean"] = [30.0, 10.0, 20.0]
    _write_district_master(tmp_path, slug=slug, state_name=state_name, df=df)

    seen_resolve_calls: list[tuple[str, str]] = []
    real_resolve_processed_root = proposal_bundle_module.resolve_processed_root

    def _capture_resolve(s, *, data_dir=None, mode="single"):
        seen_resolve_calls.append((s, mode))
        return real_resolve_processed_root(s, data_dir=data_dir, mode=mode)

    seen_candidate_paths: list[Path] = []
    real_preferred = proposal_bundle_module.resolve_preferred_master_path

    def _capture_preferred(path):
        seen_candidate_paths.append(Path(path))
        return real_preferred(path)

    monkeypatch.setattr(proposal_bundle_module, "resolve_processed_root", _capture_resolve)
    monkeypatch.setattr(proposal_bundle_module, "resolve_preferred_master_path", _capture_preferred)

    frame = _load_metric_master(slug, level="district", state_name=state_name, data_dir=tmp_path)
    assert not frame.empty

    portfolio_slugs = [s for s, m in seen_resolve_calls if m == "portfolio"]
    assert slug in portfolio_slugs, (
        "_load_metric_master must call resolve_processed_root with mode='portfolio'; "
        f"observed calls: {seen_resolve_calls}"
    )

    expected_candidate = (
        tmp_path / "processed" / slug / state_name / "master_metrics_by_district.csv"
    ).resolve()
    resolved_candidates = {p.resolve() for p in seen_candidate_paths}
    assert expected_candidate in resolved_candidates, (
        "_load_metric_master must hand the canonical "
        "<root>/<slug>/<state>/master_metrics_by_district.csv path to "
        f"resolve_preferred_master_path; observed candidates: {resolved_candidates}"
    )


# ---------------------------------------------------------------------------
# Test 2 — AST guard: no raster / xarray / gridfirst dependency
# ---------------------------------------------------------------------------


FORBIDDEN_IMPORT_ROOTS = ("rasterio", "xarray")
FORBIDDEN_MODULE_SUBSTRINGS = ("gridfirst",)
FORBIDDEN_CALL_NAMES = {"open_dataset", "open_dataarray", "aggregate_cell_values"}


def _walk_for_dependency_violations(tree: ast.AST) -> list[str]:
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias_node in node.names:
                root = alias_node.name.split(".")[0]
                if root in FORBIDDEN_IMPORT_ROOTS:
                    violations.append(f"import {alias_node.name}")
                elif any(tok in alias_node.name for tok in FORBIDDEN_MODULE_SUBSTRINGS):
                    violations.append(f"import {alias_node.name}")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            root = module.split(".")[0]
            if root in FORBIDDEN_IMPORT_ROOTS:
                violations.append(f"from {module} import ...")
            elif any(tok in module for tok in FORBIDDEN_MODULE_SUBSTRINGS):
                violations.append(f"from {module} import ...")
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in FORBIDDEN_CALL_NAMES:
                violations.append(f"call {name}()")
    return violations


def test_proposal_bundles_module_has_no_raster_or_gridfirst_dependency() -> None:
    source = Path(proposal_bundle_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    violations = _walk_for_dependency_violations(tree)
    assert not violations, (
        "compute/proposal_bundles.py must not depend on raster/xarray/gridfirst "
        "layers — bundles read materialized masters only. Violations: "
        f"{violations}"
    )


# ---------------------------------------------------------------------------
# Test 3 — strict schema contract: wrong period/scenario/stat -> NaN scores
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "wrong_column_suffix",
    [
        "__ssp245__2030-2050__mean",   # wrong period (target is 2020-2040)
        "__ssp370__2020-2040__mean",   # wrong scenario (target is ssp245)
        "__ssp245__2020-2040__median", # wrong stat   (target is mean)
    ],
)
def test_strict_metric_column_returns_nan_for_wrong_period_scenario_or_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    wrong_column_suffix: str,
) -> None:
    state_name = "Telangana"
    ids = _agricultural_risk_district_ids(state_name)
    _patch_canonical_units(monkeypatch, district_df=ids)
    _seed_agricultural_risk_masters(
        tmp_path,
        ids=ids,
        state_name=state_name,
        column_suffix=wrong_column_suffix,
    )

    out = compute_proposal_bundle_master_frame(
        PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"],
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
        warnings=[],
    )

    bundle = PROPOSAL_BUNDLES_BY_SLUG["composite_agricultural_risk"]
    target_scenario = "ssp245"
    target_period = "2020-2040"
    for rule in bundle.rules:
        score_col = f"{rule.rule_slug}__{target_scenario}__{target_period}__score"
        assert score_col in out.columns, f"missing score column {score_col!r}"
        assert out[score_col].isna().all(), (
            f"rule {rule.rule_slug!r} must yield NaN for the requested "
            f"({target_scenario}, {target_period}) when only "
            f"'<base>{wrong_column_suffix}' columns are present; "
            f"observed values: {out[score_col].tolist()}"
        )

    bundle_score_col = f"{bundle.composite_slug}__{target_scenario}__{target_period}__mean"
    assert out[bundle_score_col].isna().all()


# ---------------------------------------------------------------------------
# Test 4 — yearly-series path (trend rules) also uses portfolio processed root
# ---------------------------------------------------------------------------


def test_load_legacy_yearly_series_uses_portfolio_processed_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    seen_resolve_calls: list[tuple[str, str]] = []
    real_resolve_processed_root = proposal_bundle_module.resolve_processed_root

    def _capture_resolve(s, *, data_dir=None, mode="single"):
        seen_resolve_calls.append((s, mode))
        return real_resolve_processed_root(s, data_dir=data_dir, mode=mode)

    captured_ts_roots: list[Path] = []

    def _fake_load_district_yearly(*, ts_root, state_dir, district_display, scenario_name, varcfg, normalize_fn):
        _ = state_dir, district_display, scenario_name, varcfg, normalize_fn
        captured_ts_roots.append(Path(ts_root))
        return pd.DataFrame({"year": [], "mean": []})

    monkeypatch.setattr(proposal_bundle_module, "resolve_processed_root", _capture_resolve)
    monkeypatch.setattr(proposal_bundle_module, "load_district_yearly", _fake_load_district_yearly)

    slug = "pr_max_1day_precip"
    state_name = "Telangana"
    _load_legacy_yearly_series(
        metric_slug=slug,
        level="district",
        state_name=state_name,
        district_name="A",
        block_name=None,
        scenario="ssp245",
        data_dir=tmp_path,
    )

    portfolio_slugs = [s for s, m in seen_resolve_calls if m == "portfolio"]
    assert slug in portfolio_slugs, (
        "_load_legacy_yearly_series must call resolve_processed_root with "
        f"mode='portfolio'; observed calls: {seen_resolve_calls}"
    )

    expected_ts_root = resolve_processed_root(slug, data_dir=tmp_path, mode="portfolio").resolve()
    assert captured_ts_roots and captured_ts_roots[0].resolve() == expected_ts_root, (
        "_load_legacy_yearly_series must pass the portfolio-mode processed root "
        f"to load_district_yearly; observed: {captured_ts_roots}"
    )
