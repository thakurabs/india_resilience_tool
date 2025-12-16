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
    normalize_master_columns,
    parse_master_schema,
    parse_master_schema_obj,
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
        "m__ssp245__2020-2040__p95",
        "n__historical__1985-2014__std",
    ]
    schema = parse_master_schema_obj(cols)

    assert sorted(schema.metrics) == ["m", "n"]
    assert sorted(schema.scenarios) == ["historical", "ssp245"]
    assert sorted(schema.stats) == ["mean", "p95", "std"]


def test_load_master_csv_encoding_fallback(tmp_path: Path) -> None:
    # Write a CSV using cp1252 encoding with a non-utf8 character
    p = tmp_path / "master.csv"
    raw = "name,value\ncafé,1\n".encode("cp1252")
    p.write_bytes(raw)

    df = load_master_csv(p)
    assert df.loc[0, "name"] == "café"
    assert int(df.loc[0, "value"]) == 1
