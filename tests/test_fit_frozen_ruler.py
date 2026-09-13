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
from india_resilience_tool.config.bundle_weights import (
    get_bundle_headline_weight_total,
    get_bundle_headline_weights,
)
from india_resilience_tool.config.composite_metrics import COMPOSITES_BY_SLUG
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from tools.pipeline import fit_frozen_ruler as fitter


HEAT_RISK_SLICES = (
    ("historical", "1990-2010"),
    ("ssp245", "2020-2040"),
    ("ssp245", "2040-2060"),
    ("ssp245", "2060-2080"),
    ("ssp585", "2020-2040"),
    ("ssp585", "2040-2060"),
    ("ssp585", "2060-2080"),
)


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


def test_heat_risk_slice_discovery_uses_registered_component_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metric_slugs = [
        entry.metric_slug for entry in get_bundle_headline_weights("Heat Risk")
    ]

    def load_master(metric_slug: str, **_kwargs: object) -> pd.DataFrame:
        metric_spec = METRICS_BY_SLUG[metric_slug]
        column_base = metric_spec.periods_metric_col or metric_spec.value_col
        assert column_base
        return pd.DataFrame(
            {
                "state": ["Example"],
                "district": ["District 0"],
                "district_key": ["example|district 0"],
                **{
                    f"{column_base}__{scenario}__{period}__mean": [float(index)]
                    for index, (scenario, period) in enumerate(HEAT_RISK_SLICES)
                },
            }
        )

    monkeypatch.setattr(fitter, "_load_component_master", load_master)

    observed = fitter.discover_fitted_slices(
        metric_slugs,
        states=["Example"],
        data_dir=Path("unused"),
    )

    assert observed == HEAT_RISK_SLICES


def test_heat_stress_slice_discovery_uses_registered_component_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metric_slugs = [
        entry.metric_slug for entry in get_bundle_headline_weights("Heat Stress")
    ]

    def load_master(metric_slug: str, **_kwargs: object) -> pd.DataFrame:
        metric_spec = METRICS_BY_SLUG[metric_slug]
        column_base = metric_spec.periods_metric_col or metric_spec.value_col
        assert column_base
        return pd.DataFrame(
            {
                "state": ["Example"],
                "district": ["District 0"],
                "district_key": ["example|district 0"],
                **{
                    f"{column_base}__{scenario}__{period}__mean": [float(index)]
                    for index, (scenario, period) in enumerate(HEAT_RISK_SLICES)
                },
            }
        )

    monkeypatch.setattr(fitter, "_load_component_master", load_master)

    observed = fitter.discover_fitted_slices(
        metric_slugs,
        states=["Example"],
        data_dir=Path("unused"),
    )

    assert observed == HEAT_RISK_SLICES
    assert fitter.coverage_gate_for_bundle("Heat Stress") == 1.0


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


def test_committed_heat_stress_ruler_and_canaries_match_config() -> None:
    spec = COMPOSITES_BY_SLUG["composite_heat_stress"]
    ruler_dir = frozen_ruler_dir(spec.composite_slug, spec.frozen_ruler_version)
    ruler_set = load_ruler_set(ruler_dir)
    canaries = pd.read_csv(ruler_dir / "golden_canaries.csv")

    assert spec.normalization == "frozen_national_cdf"
    assert spec.frozen_ruler_version == "cdf_v1"
    assert spec.headline_metric_slugs == (
        "twb_annual_mean",
        "twb_summer_mean",
        "twb_annual_max",
        "twb_days_ge_28",
        "twb_days_ge_30",
        "tasmin_tropical_nights_gt28",
    )
    assert ruler_set.ruler_id == "composite_heat_stress_cdf_v1"
    assert ruler_set.slices == HEAT_RISK_SLICES
    assert ruler_set.coverage_gate == 1.0
    assert ruler_set.configured_weight == get_bundle_headline_weight_total(
        "Heat Stress"
    )
    assert ruler_set.ruler_sha256 == sha256_file(ruler_dir / "cdf_support.parquet")
    assert set(canaries["level"]) == {"district", "block"}
    assert len(canaries) == 168


def _roster(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["district_key", "state", "district"])


def _long(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["district_key", "state", "district"])
    frame["scenario"] = "snapshot"
    frame["period"] = "Current"
    frame["value"] = 1.0
    return frame


def test_roster_alignment_rewrites_source_keys_to_canonical_keys() -> None:
    """The happy path must still realign by name, not by source key."""
    roster = _roster([("TS-01", "Telangana", "Hyderabad")])
    aligned = fitter._align_long_frame_to_roster(
        _long([("OLD-99", "TELANGANA", "hyderabad")]), roster
    )
    assert aligned["district_key"].tolist() == ["TS-01"]


def test_roster_alignment_refuses_a_key_belonging_to_another_district() -> None:
    """A name miss whose source key shadows a different roster district is the
    silent-misattribution case: the row would be scored as a district it is not."""
    roster = _roster(
        [("TS-01", "Telangana", "Hyderabad"), ("TS-02", "Telangana", "Warangal")]
    )
    with pytest.raises(RuntimeError, match="misattribute"):
        fitter._align_long_frame_to_roster(
            _long([("TS-02", "Telangana", "Renamed District")]), roster
        )


def test_roster_alignment_warns_but_keeps_an_unplaceable_row() -> None:
    """A name miss with a key the roster does not know stays an orphan for
    ``expand_to_roster`` to report, but must not pass silently."""
    roster = _roster([("TS-01", "Telangana", "Hyderabad")])
    with pytest.warns(RuntimeWarning, match="orphans"):
        aligned = fitter._align_long_frame_to_roster(
            _long([("UNKNOWN-1", "Telangana", "Renamed District")]), roster
        )
    assert aligned["district_key"].tolist() == ["UNKNOWN-1"]


def test_roster_alignment_refuses_two_source_keys_collapsing_onto_one() -> None:
    """Collapsing two source districts onto one canonical key double-weights it
    in the pooled CDF."""
    roster = _roster([("TS-01", "Telangana", "Hyderabad")])
    with pytest.raises(RuntimeError, match="double-weight"):
        fitter._align_long_frame_to_roster(
            _long([("OLD-A", "Telangana", "Hyderabad"), ("OLD-B", "Telangana", "HYDERABAD")]),
            roster,
        )


def test_roster_alignment_refuses_duplicate_district_slice_rows() -> None:
    """One district must contribute one value per slice to the pool."""
    roster = _roster([("TS-01", "Telangana", "Hyderabad")])
    with pytest.raises(RuntimeError, match="duplicate"):
        fitter._align_long_frame_to_roster(
            _long([("TS-01", "Telangana", "Hyderabad"), ("TS-01", "Telangana", "Hyderabad")]),
            roster,
        )
