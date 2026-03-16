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


def _read_csv_robust(
    path: Path,
    *,
    encoding_priority: Optional[Sequence[str]] = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """Read a CSV with small encoding fallbacks."""
    if encoding_priority is None:
        encoding_priority = ("utf-8", "utf-8-sig", "cp1252", "latin-1")

    try:
        return pd.read_csv(path, **read_csv_kwargs)
    except UnicodeDecodeError:
        last_err: Optional[UnicodeDecodeError] = None
        for enc in encoding_priority:
            try:
                return pd.read_csv(path, encoding=enc, **read_csv_kwargs)
            except UnicodeDecodeError as err:
                last_err = err
                continue
        if last_err is not None:
            raise last_err
        raise


def resolve_preferred_table_path(path: Path) -> Path:
    """
    Prefer a Parquet sibling when callers pass a legacy CSV path.

    Examples:
      - `/x/master.csv` -> `/x/master.parquet` when present, otherwise `/x/master.csv`
      - `/x/master.parquet` -> `/x/master.parquet` when present, otherwise `/x/master.csv`
      - `/x/yearly` -> `/x/yearly` when it is a dataset directory
    """
    p = Path(path)
    if p.is_dir():
        return p

    suffixes = tuple(s.lower() for s in p.suffixes)
    candidates: list[Path] = []

    if suffixes[-2:] == (".csv", ".gz"):
        stem_path = p.with_suffix("").with_suffix("")
        candidates.extend([stem_path.with_suffix(".parquet"), p])
    elif suffixes[-1:] == (".csv",):
        candidates.extend([p.with_suffix(".parquet"), p])
    elif suffixes[-1:] == (".parquet",):
        candidates.extend([p, p.with_suffix(".csv")])
    else:
        candidates.extend([p, p.with_suffix(".parquet"), p.with_suffix(".csv")])

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return p


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
    p = resolve_preferred_table_path(Path(path))
    if not p.exists():
        return pd.DataFrame()

    if p.is_dir():
        return _read_parquet_dataset(p, columns=columns, filters=filters)

    suf = p.suffix.lower()
    if suf == ".parquet":
        return pd.read_parquet(p, columns=list(columns) if columns else None)

    # CSV / CSV.GZ
    return _read_csv_robust(p, **read_csv_kwargs)


def write_table(df: pd.DataFrame, path: Path, *, index: bool = False) -> None:
    """Write a DataFrame to either Parquet or CSV based on the target suffix."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.suffix.lower() == ".parquet":
        try:
            df.to_parquet(target, index=index)
        except ImportError as err:
            raise RuntimeError(
                "Writing Parquet requires pyarrow or fastparquet in the active environment."
            ) from err
        return

    df.to_csv(target, index=index)


def write_partitioned_dataset(
    df: pd.DataFrame,
    root: Path,
    *,
    partition_cols: Sequence[str],
) -> None:
    """Write a partitioned Parquet dataset rooted at ``root``."""
    out_root = Path(root)
    out_root.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_root, partition_cols=list(partition_cols), index=False)
    except ImportError as err:
        raise RuntimeError(
            "Writing Parquet datasets requires pyarrow or fastparquet in the active environment."
        ) from err
