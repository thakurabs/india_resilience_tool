from pathlib import Path

import pandas as pd

from india_resilience_tool.data.exposure_summary import (
    load_admin_exposure_summary,
    slice_exposure_for_admin_key,
)
from tools.pipeline.build_admin_exposure_summary import build


def test_load_admin_exposure_summary_returns_empty_on_missing_file(tmp_path):
    df = load_admin_exposure_summary(tmp_path / "missing.parquet")
    assert df.empty


def test_slice_exposure_for_admin_key_pipe_separator():
    df = pd.DataFrame([
        {
            "admin_key": "telangana|nizamabad",
            "admin_level": "district",
            "state_name": "Telangana",
            "district_name": "Nizamabad",
            "block_name": "",
            "pop_2020": 100,
            "parent_pop_2020": 1000,
            "parent_level": "state",
            "parent_name": "Telangana",
            "population_share_parent_pct": 10,
        }
    ])
    row = slice_exposure_for_admin_key(df, admin_key="telangana|nizamabad", admin_level="district")
    assert row is not None
    assert row["pop_2020"] == 100


def test_slice_exposure_returns_none_on_miss():
    df = pd.DataFrame(columns=["admin_key", "admin_level"])
    assert slice_exposure_for_admin_key(df, admin_key="x|y", admin_level="district") is None


def test_admin_exposure_summary_merges_rural_facilities(tmp_path):
    population_dir = tmp_path / "population"
    population_dir.mkdir()
    pd.DataFrame(
        [
            {
                "state": "TELANGANA",
                "district": "Hanumakonda",
                "district_key": "TELANGANA::Hanumakonda",
                "population_total__snapshot__2025__mean": 1000,
            }
        ]
    ).to_csv(population_dir / "population_district_master_qa.csv", index=False)
    pd.DataFrame(
        [
            {
                "state": "TELANGANA",
                "district": "Hanumakonda",
                "block": "Bheemadevarapalle",
                "block_key": "TELANGANA::Hanumakonda::Bheemadevarapalle",
                "population_total__snapshot__2025__mean": 500,
            }
        ]
    ).to_csv(population_dir / "population_block_master_qa.csv", index=False)

    for slug, col, value in [
        ("rural_facilities_total_count", "rural_facilities_total_count__snapshot__2019-2021__mean", 74),
        ("rural_facilities_agro_count", "rural_facilities_agro_count__snapshot__2019-2021__mean", 10),
        ("rural_facilities_education_count", "rural_facilities_education_count__snapshot__2019-2021__mean", 20),
        ("rural_facilities_health_count", "rural_facilities_health_count__snapshot__2019-2021__mean", 30),
        ("rural_facilities_service_count", "rural_facilities_service_count__snapshot__2019-2021__mean", 14),
        ("rural_facilities_total_count_per_100k", "rural_facilities_total_count_per_100k__snapshot__2019-2021__mean", 14800),
    ]:
        root = tmp_path / "processed" / slug / "TELANGANA"
        root.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "state": "TELANGANA",
                    "district": "Hanumakonda",
                    "block": "Bheemadevarapalle",
                    "block_key": "TELANGANA::Hanumakonda::Bheemadevarapalle",
                    col: value,
                }
            ]
        ).to_parquet(root / "master_metrics_by_block.parquet", index=False)

    out_path = build(tmp_path)
    df = pd.read_parquet(out_path)
    row = slice_exposure_for_admin_key(
        df,
        admin_key="telangana|hanumakonda|bheemadevarapalle",
        admin_level="block",
    )

    assert row is not None
    assert row["rural_facilities_total_count"] == 74
    assert row["rural_facilities_education_count"] == 20
    assert row["rural_facilities_total_count_per_100k"] == 14800


def test_admin_exposure_summary_merges_built_up_without_population(tmp_path):
    for slug, col, value in [
        ("built_up_area_km2", "built_up_area_km2__snapshot__Current__mean", 12.5),
        ("built_up_area_share_pct", "built_up_area_share_pct__snapshot__Current__mean", 8.25),
    ]:
        root = tmp_path / "processed" / slug / "TELANGANA"
        root.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "state": "TELANGANA",
                    "district": "Hanumakonda",
                    "block": "Bheemadevarapalle",
                    "block_key": "TELANGANA::Hanumakonda::Bheemadevarapalle",
                    col: value,
                }
            ]
        ).to_parquet(root / "master_metrics_by_block.parquet", index=False)

    out_path = build(tmp_path)
    df = pd.read_parquet(out_path)
    row = slice_exposure_for_admin_key(
        df,
        admin_key="telangana|hanumakonda|bheemadevarapalle",
        admin_level="block",
    )

    assert row is not None
    assert pd.isna(row["pop_2020"])
    assert row["built_up_area_km2"] == 12.5
    assert row["built_up_area_share_pct"] == 8.25


def test_admin_exposure_summary_merges_lulc_without_population(tmp_path):
    for slug, col, value in [
        ("lulc_agri_area_km2", "lulc_agri_area_km2__snapshot__Current__mean", 42.0),
        ("lulc_agri_share_pct", "lulc_agri_share_pct__snapshot__Current__mean", 61.5),
    ]:
        root = tmp_path / "processed" / slug / "TELANGANA"
        root.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "state": "TELANGANA",
                    "district": "Hanumakonda",
                    "block": "Bheemadevarapalle",
                    "block_key": "TELANGANA::Hanumakonda::Bheemadevarapalle",
                    col: value,
                }
            ]
        ).to_parquet(root / "master_metrics_by_block.parquet", index=False)

    out_path = build(tmp_path)
    df = pd.read_parquet(out_path)
    row = slice_exposure_for_admin_key(
        df,
        admin_key="telangana|hanumakonda|bheemadevarapalle",
        admin_level="block",
    )

    assert row is not None
    assert pd.isna(row["pop_2020"])
    assert row["lulc_agri_area_km2"] == 42.0
    assert row["lulc_agri_share_pct"] == 61.5


def test_load_admin_exposure_summary_coerces_lulc_numeric_columns(tmp_path):
    path = tmp_path / "admin_exposure_summary.parquet"
    pd.DataFrame(
        [
            {
                "admin_key": "telangana|hanumakonda",
                "admin_level": "district",
                "state_name": "TELANGANA",
                "district_name": "Hanumakonda",
                "block_name": "",
                "pop_2020": "1000",
                "parent_pop_2020": "2000",
                "parent_level": "state",
                "parent_name": "TELANGANA",
                "population_share_parent_pct": "50",
                "lulc_agri_area_km2": "42.5",
                "lulc_agri_share_pct": "61.5",
            }
        ]
    ).to_parquet(path, index=False)

    df = load_admin_exposure_summary(path)
    assert df["lulc_agri_area_km2"].iloc[0] == 42.5
    assert df["lulc_agri_share_pct"].iloc[0] == 61.5
