"""
Unit tests for master loader normalization and schema parsing.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.data.master_loader import (
    load_master_csv,
    load_master_csvs,
    master_source_signature,
    normalize_master_columns,
    parse_master_schema,
    parse_master_schema_obj,
    resolve_preferred_master_path,
)


def test_normalize_master_columns_basic() -> None:
    df = pd.DataFrame(
        {
            "district": ["A"],
            "days_gt_32C_ssp585_2020_2040__mean": [1.0],
            "days_gt_32C_ssp585_2020_2040__p95": [2.0],
        }
    )
    out = normalize_master_columns(df)

    assert "days_gt_32C__ssp585__2020-2040__mean" in out.columns
    assert "days_gt_32C__ssp585__2020-2040__p95" in out.columns
    assert "days_gt_32C_ssp585_2020_2040__mean" not in out.columns


def test_parse_master_schema_ignores_non_stat_cols() -> None:
    df = pd.DataFrame(
        {
            "district": ["A"],
            "x_ssp585_2020_2040__mean": [1.0],
            "x_ssp585_2020_2040__n_models": [10],  # normalized by normalize_master_columns but not parsed
        }
    )
    out = normalize_master_columns(df)

    schema_items, metrics, by_metric = parse_master_schema(out.columns)

    # Only the __mean column should be included by parser
    cols_in_items = [i["column"] for i in schema_items]
    assert "x__ssp585__2020-2040__mean" in cols_in_items
    assert "x__ssp585__2020-2040__n_models" not in cols_in_items

    assert metrics == ["x"]
    assert "x" in by_metric
    assert len(by_metric["x"]) == 1


def test_parse_master_schema_obj_typed_fields() -> None:
    cols = [
        "m__ssp245__2020-2040__mean",
        "m__ssp245__2020-2040__median",
        "n__historical__1985-2014__p95",
    ]
    schema = parse_master_schema_obj(cols)

    assert sorted(schema.metrics) == ["m"]
    assert sorted(schema.scenarios) == ["ssp245"]
    assert sorted(schema.stats) == ["mean", "median"]


def test_load_master_csv_encoding_fallback(tmp_path: Path) -> None:
    # Write a CSV using cp1252 encoding with a non-utf8 character
    p = tmp_path / "master.csv"
    raw = "name,value\ncafé,1\n".encode("cp1252")
    p.write_bytes(raw)

    df = load_master_csv(p)
    assert df.loc[0, "name"] == "café"
    assert int(df.loc[0, "value"]) == 1


def test_load_master_csvs_concatenates_multiple_files_and_reports_signature(tmp_path: Path) -> None:
    p1 = tmp_path / "state_a.csv"
    p2 = tmp_path / "state_b.csv"
    p1.write_text("state,district,value\nTelangana,A,1\n", encoding="utf-8")
    p2.write_text("state,district,value\nOdisha,B,2\n", encoding="utf-8")

    df = load_master_csvs((p1, p2))
    signature = master_source_signature((p1, p2))

    assert df["state"].tolist() == ["Telangana", "Odisha"]
    assert len(signature) == 2
    assert signature[0][0].endswith("state_a.csv")
    assert signature[1][0].endswith("state_b.csv")


def test_load_master_csv_prefers_parquet_companion(tmp_path: Path) -> None:
    csv_path = tmp_path / "master.csv"
    parquet_path = tmp_path / "master.parquet"
    csv_path.write_text("state,district,value\nTelangana,A,1\n", encoding="utf-8")
    pd.DataFrame({"state": ["Telangana"], "district": ["A"], "value": [7]}).to_parquet(parquet_path, index=False)

    df = load_master_csv(csv_path)

    assert int(df.loc[0, "value"]) == 7
    assert resolve_preferred_master_path(csv_path) == parquet_path


def test_master_source_signature_tracks_preferred_parquet_companion(tmp_path: Path) -> None:
    csv_path = tmp_path / "master.csv"
    parquet_path = tmp_path / "master.parquet"
    csv_path.write_text("state,district,value\nTelangana,A,1\n", encoding="utf-8")
    pd.DataFrame({"state": ["Telangana"], "district": ["A"], "value": [7]}).to_parquet(parquet_path, index=False)

    signature = master_source_signature(csv_path)

    assert len(signature) == 1
    assert signature[0][0].endswith("master.parquet")
