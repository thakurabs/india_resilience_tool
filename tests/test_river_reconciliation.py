from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon

from india_resilience_tool.data.river_loader import (
    ensure_river_basin_reconciliation,
    ensure_river_subbasin_diagnostics,
    load_river_basin_reconciliation,
    load_river_subbasin_diagnostics,
    resolve_river_basin_reconciliation,
    resolve_river_subbasin_diagnostics,
)
from tools.geodata.build_river_basin_reconciliation import (
    build_river_basin_reconciliation_df,
)


def _basin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01", "B02"],
            "basin_name": ["Godavari Basin", "East flowing rivers between Krishna and Pennar Basin"],
            "hydro_level": ["basin", "basin"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ],
        crs="EPSG:4326",
    )


def _river_display_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_001", "riv_002"],
            "source_uid_river": ["101", "102"],
            "river_name_clean": ["Pranhita", "Wardha"],
            "basin_name_clean": ["Godavari", "Godavari"],
            "subbasin_name_clean": ["Pranhita and others", "Wardha"],
            "state_names_clean": ["Telangana", "Maharashtra"],
            "length_km_source": [100.0, 50.0],
        },
        geometry=[
            LineString([(0, 0), (1, 1)]),
            LineString([(1, 1), (2, 2)]),
        ],
        crs="EPSG:4326",
    )


def test_build_river_basin_reconciliation_df_marks_exact_matches_and_reviews() -> None:
    df = build_river_basin_reconciliation_df(_basin_gdf(), _river_display_gdf())
    assert df["hydro_basin_name"].tolist() == [
        "East flowing rivers between Krishna and Pennar Basin",
        "Godavari Basin",
    ]
    godavari = df.loc[df["hydro_basin_name"] == "Godavari Basin"].iloc[0]
    unresolved = df.loc[
        df["hydro_basin_name"] == "East flowing rivers between Krishna and Pennar Basin"
    ].iloc[0]
    assert godavari["match_status"] == "matched"
    assert godavari["river_basin_name"] == "Godavari"
    assert unresolved["match_status"] == "review_required"
    assert unresolved["river_basin_name"] == ""


def test_ensure_river_basin_reconciliation_rejects_bad_status() -> None:
    df = pd.DataFrame(
        {
            "hydro_basin_name": ["Godavari Basin"],
            "hydro_basin_id": ["B01"],
            "river_basin_name": ["Godavari"],
            "match_status": ["unknown"],
            "notes": [""],
        }
    )
    with pytest.raises(ValueError, match="invalid match_status"):
        ensure_river_basin_reconciliation(df)


def test_resolve_river_basin_reconciliation_returns_messages_by_status() -> None:
    df = ensure_river_basin_reconciliation(
        pd.DataFrame(
            {
                "hydro_basin_name": ["Godavari Basin", "Minor rivers draining into Myanmar Basin"],
                "hydro_basin_id": ["B01", "B99"],
                "river_basin_name": ["Godavari", ""],
                "match_status": ["matched", "no_source_rivers"],
                "notes": ["", ""],
            }
        )
    )
    matched = resolve_river_basin_reconciliation(
        hydro_basin_name="Godavari Basin",
        reconciliation_df=df,
        alias_fn=lambda s: str(s).lower(),
    )
    no_source = resolve_river_basin_reconciliation(
        hydro_basin_name="Minor rivers draining into Myanmar Basin",
        reconciliation_df=df,
        alias_fn=lambda s: str(s).lower(),
    )
    review = resolve_river_basin_reconciliation(
        hydro_basin_name="East flowing rivers between Krishna and Pennar Basin",
        reconciliation_df=df,
        alias_fn=lambda s: str(s).lower(),
    )
    assert matched["status"] == "matched"
    assert matched["river_basin_name"] == "Godavari"
    assert matched["message"] is None
    assert no_source["status"] == "no_source_rivers"
    assert "No river features" in str(no_source["message"])
    assert review["status"] == "review_required"
    assert "pending basin-name reconciliation" in str(review["message"])


def test_resolve_river_subbasin_diagnostics_returns_expected_message() -> None:
    df = ensure_river_subbasin_diagnostics(
        pd.DataFrame(
            {
                "basin_id": ["B01", "B01"],
                "basin_name": ["Godavari Basin", "Godavari Basin"],
                "subbasin_id": ["SB01", "SB02"],
                "subbasin_name": ["Pranhita and others", "Wardha"],
                "matched_river_feature_count": [2, 0],
                "placeholder_river_feature_count": [0, 1],
                "match_status": ["matched", "review_required"],
                "notes": ["", ""],
            }
        )
    )
    matched = resolve_river_subbasin_diagnostics(
        hydro_subbasin_name="Pranhita and others",
        diagnostics_df=df,
        alias_fn=lambda s: str(s).lower(),
    )
    unresolved = resolve_river_subbasin_diagnostics(
        hydro_subbasin_name="Wardha",
        diagnostics_df=df,
        alias_fn=lambda s: str(s).lower(),
    )
    assert matched["status"] == "matched"
    assert matched["message"] is None
    assert unresolved["status"] == "review_required"
    assert "No river features" in str(unresolved["message"])


def test_load_river_basin_reconciliation_accepts_parquet(tmp_path: Path) -> None:
    path = tmp_path / "river_basin_name_reconciliation.parquet"
    pd.DataFrame(
        {
            "hydro_basin_name": ["Godavari Basin"],
            "hydro_basin_id": ["B01"],
            "river_basin_name": ["Godavari"],
            "match_status": ["matched"],
            "notes": [""],
        }
    ).to_parquet(path, index=False)

    out = load_river_basin_reconciliation(path)

    assert out["river_basin_name"].tolist() == ["Godavari"]


def test_load_river_subbasin_diagnostics_accepts_parquet(tmp_path: Path) -> None:
    path = tmp_path / "river_subbasin_diagnostics.parquet"
    pd.DataFrame(
        {
            "basin_id": ["B01"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["SB01"],
            "subbasin_name": ["Pranhita and others"],
            "matched_river_feature_count": [2],
            "placeholder_river_feature_count": [0],
            "match_status": ["matched"],
            "notes": [""],
        }
    ).to_parquet(path, index=False)

    out = load_river_subbasin_diagnostics(path)

    assert out["subbasin_id"].tolist() == ["SB01"]
