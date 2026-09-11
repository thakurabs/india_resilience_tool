"""Focused contracts for the bundle-generic frozen-ruler fitter (CHG-0419)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from india_resilience_tool.analysis.frozen_rulers import (
    build_cdf_ruler,
    frozen_ruler_dir,
    load_ruler_set,
    sha256_file,
)
from india_resilience_tool.config.bundle_weights import get_bundle_headline_weight_total
from india_resilience_tool.config.composite_metrics import COMPOSITES_BY_SLUG
from tools.pipeline import fit_frozen_ruler as fitter


def _riverine_master(values: list[float] | None = None) -> pd.DataFrame:
    scores = values or [1.0, 3.0]
    return pd.DataFrame(
        {
            "state": ["Example"] * len(scores),
            "district": [f"District {index}" for index in range(len(scores))],
            "district_key": [
                f"Example::District {index}" for index in range(len(scores))
            ],
            "jrc_flood_depth_index_rp100__snapshot__Current__mean": scores,
        }
    )


def test_snapshot_only_slice_discovery_uses_component_master_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fitter, "_load_component_master", lambda *args, **kwargs: _riverine_master())

    observed = fitter.discover_fitted_slices(
        ["jrc_flood_depth_index_rp100"],
        states=["Example"],
        data_dir=Path("unused"),
    )

    assert observed == (("snapshot", "Current"),)


def test_slice_discovery_rejects_present_master_with_missing_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    complete = _riverine_master()
    incomplete = complete.drop(
        columns=["jrc_flood_depth_index_rp100__snapshot__Current__mean"]
    )
    frames = iter([complete, incomplete])
    monkeypatch.setattr(fitter, "_load_component_master", lambda *args, **kwargs: next(frames))

    with pytest.raises(RuntimeError, match="slice contract is inconsistent"):
        fitter.discover_fitted_slices(
            ["jrc_flood_depth_index_rp100"],
            states=["Complete", "Incomplete"],
            data_dir=Path("unused"),
        )


def test_riverine_fit_uses_snapshot_contract_and_full_coverage_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    master = _riverine_master([1.0, np.nan])
    roster = pd.DataFrame(
        {
            "district_key": ["example|district 0", "example|district 1"],
            "state": ["Example", "Example"],
            "district": ["District 0", "District 1"],
            "area_m2": [1.0, 1.0],
        }
    )
    long_frame = master.rename(
        columns={
            "jrc_flood_depth_index_rp100__snapshot__Current__mean": (
                "jrc_flood_depth_index_rp100"
            )
        }
    )
    long_frame["scenario"] = "snapshot"
    long_frame["period"] = "Current"
    monkeypatch.setattr(fitter, "discover_states", lambda *args, **kwargs: ["Example"])
    monkeypatch.setattr(fitter, "_load_component_master", lambda *args, **kwargs: master)
    monkeypatch.setattr(fitter, "load_district_roster", lambda *args, **kwargs: roster)
    monkeypatch.setattr(fitter, "load_national_long_frame", lambda *args, **kwargs: long_frame)

    ruler_set, _, reconciliation = fitter.fit_ruler_set(
        "Riverine Flood", version="v1", data_dir=Path("unused"), verbose=False
    )

    assert ruler_set.slices == (("snapshot", "Current"),)
    assert ruler_set.coverage_gate == 1.0
    assert ruler_set.meta["n_fit_rows"] == 2
    assert ruler_set.meta["n_below_coverage_gate"] == 1
    assert ruler_set.meta["coverage_failure_rate"] == pytest.approx(0.5)
    assert reconciliation["status"].tolist() == [
        "roster_and_master",
        "roster_master_no_finite_value",
    ]


def test_single_metric_cdf_is_strict_for_distinct_values_and_stable_for_ties() -> None:
    ruler = build_cdf_ruler(
        "jrc_flood_depth_index_rp100",
        np.array([1.0, 1.0, 2.0, 3.0, 3.0]),
        higher_is_worse=True,
    )
    assert ruler is not None

    scored = ruler.apply(pd.Series([1.0, 1.0, 2.0, 3.0, 3.0]))

    assert scored.iloc[0] == scored.iloc[1]
    assert scored.iloc[3] == scored.iloc[4]
    assert scored.iloc[0] < scored.iloc[2] < scored.iloc[3]


def test_unknown_bundle_requires_an_evidence_backed_coverage_gate() -> None:
    with pytest.raises(ValueError, match="No coverage gate is configured"):
        fitter.coverage_gate_for_bundle("Unmeasured Bundle")


def test_committed_riverine_ruler_and_canaries_match_config() -> None:
    spec = COMPOSITES_BY_SLUG["composite_flood_jrc_depth"]
    ruler_dir = frozen_ruler_dir(spec.composite_slug, spec.frozen_ruler_version)
    ruler_set = load_ruler_set(ruler_dir)
    canaries = pd.read_csv(ruler_dir / "golden_canaries.csv")

    assert spec.normalization == "frozen_national_cdf"
    assert spec.frozen_ruler_version == "cdf_v1"
    assert spec.headline_metric_slugs == ("jrc_flood_depth_index_rp100",)
    assert ruler_set.ruler_id == "composite_flood_jrc_depth_cdf_v1"
    assert ruler_set.slices == (("snapshot", "Current"),)
    assert ruler_set.coverage_gate == 1.0
    assert ruler_set.configured_weight == get_bundle_headline_weight_total(
        "Riverine Flood"
    )
    assert ruler_set.ruler_sha256 == sha256_file(ruler_dir / "cdf_support.parquet")
    assert set(canaries["level"]) == {"district", "block"}
    assert len(canaries) == 24
