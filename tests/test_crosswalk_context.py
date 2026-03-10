from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.crosswalks import (
    build_district_hydro_context,
    build_subbasin_admin_context,
    ensure_district_subbasin_crosswalk,
)


def _crosswalk_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "district_name": ["Nizamabad", "Nizamabad", "Karimnagar"],
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "subbasin_id": ["SB01", "SB02", "SB02"],
            "subbasin_name": ["Godavari Upper", "Godavari Middle", "Godavari Middle"],
            "basin_id": ["B01", "B01", "B01"],
            "basin_name": ["Godavari", "Godavari", "Godavari"],
            "intersection_area_km2": [120.0, 30.0, 80.0],
            "district_area_fraction_in_subbasin": [0.8, 0.2, 0.6],
            "subbasin_area_fraction_in_district": [0.5, 0.25, 0.75],
        }
    )


def test_crosswalk_context_path_defaults_under_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    cfg = get_paths_config()
    assert cfg.district_subbasin_crosswalk_path == (tmp_path / "district_subbasin_crosswalk.csv").resolve()


def test_crosswalk_validation_rejects_duplicate_pairs() -> None:
    df = pd.concat([_crosswalk_df(), _crosswalk_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError):
        ensure_district_subbasin_crosswalk(df)


def test_crosswalk_validation_clips_tiny_fraction_overflow() -> None:
    df = _crosswalk_df()
    df.loc[0, "district_area_fraction_in_subbasin"] = 1.0000001
    validated = ensure_district_subbasin_crosswalk(df)
    assert float(validated.loc[0, "district_area_fraction_in_subbasin"]) == 1.0


def test_build_district_hydro_context_returns_dominant_subbasin() -> None:
    ctx = build_district_hydro_context(
        ensure_district_subbasin_crosswalk(_crosswalk_df()),
        district_name="Nizamabad",
        state_name="Telangana",
        alias_fn=lambda s: str(s).strip().lower(),
    )
    assert ctx is not None
    assert ctx.section_title == "Hydrology context"
    assert ctx.primary_basin_name == "Godavari"
    assert ctx.dominant_counterpart_name == "Godavari Upper"
    assert ctx.overlap_count == 2
    assert ctx.classification == "dominant_subbasin"
    assert len(ctx.overlaps) == 2
    assert ctx.all_counterpart_ids == ("SB01", "SB02")


def test_build_subbasin_admin_context_returns_ordered_districts() -> None:
    ctx = build_subbasin_admin_context(
        ensure_district_subbasin_crosswalk(_crosswalk_df()),
        subbasin_id="SB02",
        subbasin_name="Godavari Middle",
        alias_fn=lambda s: str(s).strip().lower(),
    )
    assert ctx is not None
    assert ctx.section_title == "Administrative context"
    assert ctx.primary_basin_name == "Godavari"
    assert ctx.dominant_counterpart_name == "Karimnagar"
    assert ctx.overlap_count == 2
    assert ctx.classification == "concentrated_in_one_district"
    assert [ov.counterpart_name for ov in ctx.overlaps] == ["Karimnagar", "Nizamabad"]
    assert [ov.counterpart_state_name for ov in ctx.overlaps] == ["Telangana", "Telangana"]
    assert ctx.all_counterpart_ids == ("Telangana::Karimnagar", "Telangana::Nizamabad")
