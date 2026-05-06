from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.compute.glance_view_model import build_glance_view_models
from india_resilience_tool.data.optimized_bundle import optimized_glance_root


def _write_optimized_district_master(tmp_path: Path, slug: str, rows: list[dict[str, object]]) -> None:
    path = tmp_path / "processed_optimised" / "metrics" / slug / "masters" / "admin" / "district" / "state=Telangana.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_glance_builder_persists_ranks_deltas_drivers_and_distributions(tmp_path: Path) -> None:
    _write_optimized_district_master(
        tmp_path,
        "composite_heat_risk",
        [
            {"state": "Telangana", "district": "A", "composite_heat_risk__ssp585__2040-2060__mean": 90.0},
            {"state": "Telangana", "district": "B", "composite_heat_risk__ssp585__2040-2060__mean": 10.0},
        ],
    )
    _write_optimized_district_master(
        tmp_path,
        "tasmin_tropical_nights_gt25",
        [
            {"state": "Telangana", "district": "A", "tasmin_tropical_nights_gt25__ssp585__2040-2060__mean": 5.0},
            {"state": "Telangana", "district": "B", "tasmin_tropical_nights_gt25__ssp585__2040-2060__mean": 1.0},
        ],
    )

    results = build_glance_view_models(
        data_dir=tmp_path,
        composite_slugs=["composite_heat_risk"],
        overwrite=True,
    )

    assert len(results) == 1
    root = optimized_glance_root(
        "composite_heat_risk",
        scenario="ssp585",
        period="2040-2060",
        data_dir=tmp_path,
    )
    district = pd.read_parquet(root / "district.parquet")
    state = pd.read_parquet(root / "state.parquet")
    drivers = pd.read_parquet(root / "drivers.parquet")
    distributions = pd.read_parquet(root / "distributions.parquet")

    assert district["district_rank"].tolist() == [1.0, 2.0]
    assert district["district_count"].tolist() == [2, 2]
    assert district["state_mean_score"].tolist() == [50.0, 50.0]
    assert district["delta_vs_state_mean_display"].tolist() == ["+40.0", "-40.0"]
    assert state["bundle_score"].tolist() == [50.0]
    assert drivers.loc[drivers["scope_level"] == "district", "driver_source"].eq("thematic_component_norm").all()
    assert {"national", "state"} <= set(distributions["scope_level"])
    assert set(distributions["band"]) == {"Low", "Moderate", "High", "Very High"}


def test_glance_builder_uses_sector_rule_score_drivers(tmp_path: Path) -> None:
    _write_optimized_district_master(
        tmp_path,
        "composite_health_risk",
        [
            {
                "state": "Telangana",
                "district": "A",
                "composite_health_risk__ssp585__2040-2060__mean": 75.0,
                "txx_ge_45__ssp585__2040-2060__score": 100.0,
            }
        ],
    )

    build_glance_view_models(
        data_dir=tmp_path,
        composite_slugs=["composite_health_risk"],
        overwrite=True,
    )

    root = optimized_glance_root(
        "composite_health_risk",
        scenario="ssp585",
        period="2040-2060",
        data_dir=tmp_path,
    )
    drivers = pd.read_parquet(root / "drivers.parquet")

    assert drivers["driver_slug"].tolist() == ["txx_ge_45", "txx_ge_45"]
    assert drivers["driver_source"].eq("proposal_rule_score").all()


def test_glance_builder_persists_raw_primary_class_metric_for_riverine_flood(tmp_path: Path) -> None:
    _write_optimized_district_master(
        tmp_path,
        "composite_flood_jrc_depth",
        [
            {
                "state": "Telangana",
                "district": "A",
                "composite_flood_jrc_depth__snapshot__Current__mean": 72.0,
            }
        ],
    )
    _write_optimized_district_master(
        tmp_path,
        "jrc_flood_depth_index_rp100",
        [
            {
                "state": "Telangana",
                "district": "A",
                "jrc_flood_depth_index_rp100__snapshot__Current__mean": 3.0,
            }
        ],
    )

    build_glance_view_models(
        data_dir=tmp_path,
        composite_slugs=["composite_flood_jrc_depth"],
        overwrite=True,
    )

    root = optimized_glance_root(
        "composite_flood_jrc_depth",
        scenario="snapshot",
        period="Current",
        data_dir=tmp_path,
    )
    district = pd.read_parquet(root / "district.parquet")

    assert district["jrc_flood_depth_index_rp100"].tolist() == [3]
