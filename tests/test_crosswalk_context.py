from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.crosswalks import (
    build_basin_admin_context,
    build_block_hydro_context,
    build_district_hydro_context,
    build_subbasin_admin_context,
    ensure_block_basin_crosswalk,
    ensure_block_subbasin_crosswalk,
    ensure_district_basin_crosswalk,
    ensure_district_subbasin_crosswalk,
)


def _district_subbasin_df() -> pd.DataFrame:
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


def _block_subbasin_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "block_name": ["Armur", "Armur", "Bheemgal"],
            "district_name": ["Nizamabad", "Nizamabad", "Nizamabad"],
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "subbasin_id": ["SB01", "SB02", "SB02"],
            "subbasin_name": ["Godavari Upper", "Godavari Middle", "Godavari Middle"],
            "basin_id": ["B01", "B01", "B01"],
            "basin_name": ["Godavari", "Godavari", "Godavari"],
            "intersection_area_km2": [40.0, 10.0, 15.0],
            "block_area_fraction_in_subbasin": [0.8, 0.2, 0.6],
            "subbasin_area_fraction_in_block": [0.3, 0.1, 0.2],
        }
    )


def _district_basin_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "district_name": ["Nizamabad", "Nizamabad", "Adilabad"],
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "basin_id": ["B01", "B02", "B02"],
            "basin_name": ["Godavari", "Krishna", "Krishna"],
            "intersection_area_km2": [110.0, 40.0, 70.0],
            "district_area_fraction_in_basin": [0.73, 0.27, 0.8],
            "basin_area_fraction_in_district": [0.2, 0.1, 0.5],
        }
    )


def _block_basin_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "block_name": ["Armur", "Armur", "Bheemgal"],
            "district_name": ["Nizamabad", "Nizamabad", "Nizamabad"],
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "basin_id": ["B01", "B02", "B02"],
            "basin_name": ["Godavari", "Krishna", "Krishna"],
            "intersection_area_km2": [45.0, 5.0, 20.0],
            "block_area_fraction_in_basin": [0.9, 0.1, 0.8],
            "basin_area_fraction_in_block": [0.08, 0.02, 0.1],
        }
    )


def test_crosswalk_context_path_defaults_under_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    cfg = get_paths_config()
    assert cfg.district_subbasin_crosswalk_path == (tmp_path / "district_subbasin_crosswalk.csv").resolve()
    assert cfg.block_subbasin_crosswalk_path == (tmp_path / "block_subbasin_crosswalk.csv").resolve()
    assert cfg.district_basin_crosswalk_path == (tmp_path / "district_basin_crosswalk.csv").resolve()
    assert cfg.block_basin_crosswalk_path == (tmp_path / "block_basin_crosswalk.csv").resolve()


def test_crosswalk_validation_rejects_duplicate_pairs() -> None:
    df = pd.concat([_district_subbasin_df(), _district_subbasin_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError):
        ensure_district_subbasin_crosswalk(df)


def test_crosswalk_validation_clips_tiny_fraction_overflow() -> None:
    df = _district_subbasin_df()
    df.loc[0, "district_area_fraction_in_subbasin"] = 1.0000001
    validated = ensure_district_subbasin_crosswalk(df)
    assert float(validated.loc[0, "district_area_fraction_in_subbasin"]) == 1.0


def test_other_crosswalk_validators_accept_expected_contracts() -> None:
    assert not ensure_block_subbasin_crosswalk(_block_subbasin_df()).empty
    assert not ensure_district_basin_crosswalk(_district_basin_df()).empty
    assert not ensure_block_basin_crosswalk(_block_basin_df()).empty


def test_build_district_subbasin_context_returns_dominant_subbasin() -> None:
    ctx = build_district_hydro_context(
        ensure_district_subbasin_crosswalk(_district_subbasin_df()),
        district_name="Nizamabad",
        state_name="Telangana",
        alias_fn=lambda s: str(s).strip().lower(),
        hydro_level="sub_basin",
    )
    assert ctx is not None
    assert ctx.section_title == "Hydrology context"
    assert ctx.primary_basin_name == "Godavari"
    assert ctx.dominant_counterpart_name == "Godavari Upper"
    assert ctx.overlap_count == 2
    assert ctx.classification == "dominant_subbasin"
    assert ctx.all_counterpart_ids == ("SB01", "SB02")


def test_build_district_basin_context_returns_dominant_basin() -> None:
    ctx = build_district_hydro_context(
        ensure_district_basin_crosswalk(_district_basin_df()),
        district_name="Nizamabad",
        state_name="Telangana",
        alias_fn=lambda s: str(s).strip().lower(),
        hydro_level="basin",
    )
    assert ctx is not None
    assert ctx.section_title == "Basin context"
    assert ctx.dominant_counterpart_name == "Godavari"
    assert ctx.classification == "mostly_one_basin"
    assert ctx.open_action_label == "Open basin"


def test_build_block_subbasin_context_returns_dominant_subbasin() -> None:
    ctx = build_block_hydro_context(
        ensure_block_subbasin_crosswalk(_block_subbasin_df()),
        block_name="Armur",
        district_name="Nizamabad",
        state_name="Telangana",
        alias_fn=lambda s: str(s).strip().lower(),
        hydro_level="sub_basin",
    )
    assert ctx is not None
    assert ctx.selected_level == "block"
    assert ctx.dominant_counterpart_name == "Godavari Upper"
    assert ctx.selected_fraction_label == "Block share"


def test_build_subbasin_admin_context_returns_ordered_districts() -> None:
    ctx = build_subbasin_admin_context(
        ensure_district_subbasin_crosswalk(_district_subbasin_df()),
        subbasin_id="SB02",
        subbasin_name="Godavari Middle",
        alias_fn=lambda s: str(s).strip().lower(),
        admin_level="district",
    )
    assert ctx is not None
    assert ctx.section_title == "Administrative context"
    assert ctx.primary_basin_name == "Godavari"
    assert ctx.dominant_counterpart_name == "Karimnagar"
    assert ctx.dominant_label == "District covering the largest share of this sub-basin"
    assert ctx.selected_fraction_label == "Share of sub-basin"
    assert ctx.counterpart_fraction_label == "Share of district in sub-basin"
    assert ctx.overlap_count == 2
    assert ctx.classification == "concentrated_in_one_district"
    assert [ov.counterpart_name for ov in ctx.overlaps] == ["Karimnagar", "Nizamabad"]
    assert ctx.all_counterpart_ids == ("Telangana::Karimnagar", "Telangana::Nizamabad")


def test_build_basin_admin_context_can_drill_to_blocks() -> None:
    ctx = build_basin_admin_context(
        ensure_block_basin_crosswalk(_block_basin_df()),
        basin_id="B02",
        basin_name="Krishna",
        alias_fn=lambda s: str(s).strip().lower(),
        admin_level="block",
    )
    assert ctx is not None
    assert ctx.selected_level == "basin"
    assert ctx.counterpart_level == "block"
    assert ctx.dominant_counterpart_name == "Bheemgal"
    assert ctx.dominant_label == "Block covering the largest share of this basin"
    assert ctx.selected_fraction_label == "Share of basin"
    assert ctx.counterpart_fraction_label == "Share of block in basin"
    assert ctx.highlight_action_label == "Highlight related blocks"
