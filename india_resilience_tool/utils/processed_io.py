"""
Lightweight IO helpers for processed outputs.

Goals:
- Prefer compact Parquet stores where available
- Preserve backward compatibility with legacy CSV outputs
- Support both Parquet *files* and Parquet *datasets* (directories)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

import pandas as pd


def _filters_to_pyarrow_expression(
    filters: Optional[Union[Iterable[tuple[str, str, Any]], Any]]
):
    """
    Convert a list of filter triples into a pyarrow.compute.Expression.

    We intentionally support only the minimal operator set needed by the pipeline
    today (choice A): '==' combined with AND semantics.
    """
    if filters is None:
        return None

    # If caller already provided an Expression-like object, return as-is.
    # (We avoid importing pyarrow.compute types just for isinstance checks.)
    if not isinstance(filters, (list, tuple)):
        return filters

    import pyarrow.dataset as ds

    expr = None
    for item in filters:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            raise ValueError(f"Invalid filter triple: {item!r}")
        col, op, val = item
        op_norm = str(op).strip()
        if op_norm != "==":
            raise ValueError(f"Unsupported filter op {op!r}; only '==' is supported")
        term = ds.field(str(col)) == val
        expr = term if expr is None else (expr & term)

    return expr


def _read_parquet_dataset(
    path: Path,
    *,
    columns: Optional[Sequence[str]] = None,
    filters: Optional[Iterable[tuple[str, str, Any]]] = None,
) -> pd.DataFrame:
    # Local import keeps pandas-only paths light for callers.
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(path), format="parquet", partitioning="hive")
    expr = _filters_to_pyarrow_expression(filters)
    table = dataset.to_table(columns=list(columns) if columns else None, filter=expr)
    return table.to_pandas()


def read_table(
    path: Path,
    *,
    columns: Optional[Sequence[str]] = None,
    filters: Optional[Iterable[tuple[str, str, Any]]] = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """
    Read a table from either Parquet (file or dataset dir) or CSV.

    - Parquet dataset dirs can be filtered via `filters` (pyarrow.dataset filter triples).
    - CSV ignores `filters` and is read with pandas.read_csv.
    """
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()

    if p.is_dir():
        return _read_parquet_dataset(p, columns=columns, filters=filters)

    suf = p.suffix.lower()
    if suf == ".parquet":
        if filters:
            return _read_parquet_dataset(p, columns=columns, filters=filters)
        return pd.read_parquet(p, columns=list(columns) if columns else None)

    # CSV / CSV.GZ
    return pd.read_csv(p, **read_csv_kwargs)
