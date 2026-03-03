"""
Regression test: pyarrow.dataset Dataset.to_table(filter=...) expects an Expression.

Our processed_io.read_table API accepts simple filter triples and must convert
them into a pyarrow Expression (supporting only '==' for now).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.utils.processed_io import read_table


def test_read_table_parquet_dataset_filters_eq(tmp_path: Path) -> None:
    root = tmp_path / "yearly"
    (root / "scenario=ssp585").mkdir(parents=True, exist_ok=True)
    (root / "scenario=ssp245").mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"district": ["A"], "year": [2020], "value": [1.0]}).to_parquet(
        root / "scenario=ssp585" / "data.parquet",
        index=False,
    )
    pd.DataFrame({"district": ["B"], "year": [2020], "value": [2.0]}).to_parquet(
        root / "scenario=ssp245" / "data.parquet",
        index=False,
    )

    df = read_table(
        root,
        columns=["district", "scenario", "year", "value"],
        filters=[("scenario", "==", "ssp585")],
    )
    assert df.shape[0] == 1
    assert df.loc[0, "district"] == "A"
    assert str(df.loc[0, "scenario"]) == "ssp585"

