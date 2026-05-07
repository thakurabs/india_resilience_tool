import pandas as pd

from india_resilience_tool.data.hydro_summary import (
    ADMIN_HYDRO_REQUIRED_COLUMNS,
    load_admin_hydro_summary,
    parse_hydro_intersections,
    slice_hydro_for_admin_key,
)


def test_load_admin_hydro_summary_returns_empty_on_missing_file(tmp_path):
    df = load_admin_hydro_summary(tmp_path / "missing.parquet")
    assert df.empty


def test_hydro_summary_uses_basin_id_not_hydro_id():
    assert "basin_id" in ADMIN_HYDRO_REQUIRED_COLUMNS
    assert "subbasin_id" in ADMIN_HYDRO_REQUIRED_COLUMNS
    assert "hydro_id" not in ADMIN_HYDRO_REQUIRED_COLUMNS


def test_parse_hydro_intersections_handles_bad_values():
    assert parse_hydro_intersections(None) == []
    assert parse_hydro_intersections("") == []
    assert parse_hydro_intersections("not json") == []
    assert parse_hydro_intersections('{"x": 1}') == []
    assert parse_hydro_intersections('[{"basin_id": "b1"}]') == [{"basin_id": "b1"}]


def test_slice_hydro_for_admin_key_returns_none_on_miss():
    df = pd.DataFrame(columns=["admin_key", "admin_level"])
    assert slice_hydro_for_admin_key(df, admin_key="x|y", admin_level="district") is None
