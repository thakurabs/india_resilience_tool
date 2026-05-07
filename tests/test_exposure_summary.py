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
