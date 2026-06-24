"""
Tests for the pure Method-B bundle-score resolver in app.map_pipeline.

`_resolve_bundle_score_column` is Streamlit-free: the master loader and the
composite-source resolver are injected, so it is exercised here with fakes
(no disk, no ribbon, no Streamlit).

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.app.map_pipeline import _resolve_bundle_score_column


COMPOSITE_COL = "composite_heat_risk__ssp245__2020-2040__mean"
METRIC_COL = "txge30_hot_days__ssp245__2020-2040__mean"


class _FakeLoader:
    """Records invocations and returns a fixed (df, items, metrics, by_metric)."""

    def __init__(self, comp_df: pd.DataFrame) -> None:
        self.comp_df = comp_df
        self.calls = 0

    def __call__(self, source, slug):  # noqa: ANN001 - test double
        self.calls += 1
        return self.comp_df, [], [], {}


def _fake_source_fn(composite_slug, *, level, selected_state, spatial_family, data_dir):
    return Path(f"/fake/{composite_slug}/{level}")


def _ranking_frame(districts: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state_name": ["Telangana"] * len(districts),
            "district_name": districts,
            METRIC_COL: [float(i) for i in range(len(districts))],
        }
    )


def _composite_frame(districts: list[str], col: str = COMPOSITE_COL) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state": ["Telangana"] * len(districts),
            "district": districts,
            col: [10.0 * (i + 1) for i in range(len(districts))],
        }
    )


def _resolve(frame, loader, *, selected_bundle="Heat Risk", variable_slug="txge30_hot_days",
            metric_col=METRIC_COL, level="district", spatial_family="admin"):
    return _resolve_bundle_score_column(
        ranking_source=frame,
        selected_bundle=selected_bundle,
        variable_slug=variable_slug,
        metric_col=metric_col,
        level=level,
        selected_state="Telangana",
        spatial_family=spatial_family,
        data_dir=Path("/fake"),
        load_master_and_schema_fn=loader,
        resolve_composite_source_fn=_fake_source_fn,
    )


def test_happy_path_merges_bundle_score_by_district_name() -> None:
    districts = ["Hyderabad", "Warangal", "Khammam"]
    loader = _FakeLoader(_composite_frame(districts))
    out, col = _resolve(_ranking_frame(districts), loader)

    assert col == "bundle_score"
    assert loader.calls == 1
    assert list(out["bundle_score"]) == [10.0, 20.0, 30.0]


def test_stat_token_resolution_without_mean() -> None:
    # Only a non-mean stat column is present for the prefix; it must still resolve.
    districts = ["Hyderabad", "Warangal"]
    col = "composite_heat_risk__ssp245__2020-2040__median"
    loader = _FakeLoader(_composite_frame(districts, col=col))
    out, ret = _resolve(_ranking_frame(districts), loader)

    assert ret == "bundle_score"
    assert list(out["bundle_score"]) == [10.0, 20.0]


def test_composite_metric_short_circuits_without_loading() -> None:
    # variable_slug is itself a composite -> classify its own value, no load.
    districts = ["Hyderabad", "Warangal"]
    loader = _FakeLoader(_composite_frame(districts))
    frame = _ranking_frame(districts)
    out, ret = _resolve(
        frame,
        loader,
        variable_slug="composite_heat_risk",
        metric_col=COMPOSITE_COL,
    )

    assert ret == COMPOSITE_COL
    assert loader.calls == 0
    assert out is frame  # returned unchanged


def test_non_bundle_selection_returns_none() -> None:
    districts = ["Hyderabad"]
    loader = _FakeLoader(_composite_frame(districts))
    out, ret = _resolve(_ranking_frame(districts), loader, selected_bundle=None)
    assert ret is None
    assert loader.calls == 0


def test_unsupported_level_returns_none() -> None:
    districts = ["Hyderabad"]
    loader = _FakeLoader(_composite_frame(districts))
    # Heat Risk supports district/block, not basin.
    out, ret = _resolve(_ranking_frame(districts), loader, level="basin")
    assert ret is None
    assert loader.calls == 0


def test_unsupported_scenario_returns_none() -> None:
    districts = ["Hyderabad"]
    loader = _FakeLoader(_composite_frame(districts))
    bad_metric = "txge30_hot_days__ssp370__2020-2040__mean"
    out, ret = _resolve(_ranking_frame(districts), loader, metric_col=bad_metric)
    assert ret is None
    assert loader.calls == 0


def test_malformed_metric_col_returns_none() -> None:
    districts = ["Hyderabad"]
    loader = _FakeLoader(_composite_frame(districts))
    out, ret = _resolve(_ranking_frame(districts), loader, metric_col="foo__bar")
    assert ret is None
    assert loader.calls == 0


def test_missing_join_key_returns_none() -> None:
    districts = ["Hyderabad", "Warangal"]
    comp = _composite_frame(districts).drop(columns=["district"])  # no shared key
    loader = _FakeLoader(comp)
    out, ret = _resolve(_ranking_frame(districts), loader)
    assert ret is None


def test_zero_match_name_drift_falls_back() -> None:
    # Composite vs metric masters on different admin-name vintages -> 0 matches.
    loader = _FakeLoader(_composite_frame(["Old Name A", "Old Name B"]))
    out, ret = _resolve(_ranking_frame(["New Name X", "New Name Y"]), loader)
    assert ret is None
    assert out is not None
