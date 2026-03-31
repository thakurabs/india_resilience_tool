"""
Lightweight IO helpers for processed outputs.

Goals:
- Prefer compact Parquet stores where available
- Preserve backward compatibility with legacy CSV outputs
- Support both Parquet *files* and Parquet *datasets* (directories)
"""

from __future__ import annotations

import glob
import os
import shutil
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


def _windows_extended_length_path(path: Path) -> str:
    """
    Return an extended-length absolute Windows path when running on Windows.

    On non-Windows platforms, this returns the normal absolute path string.
    """
    resolved = path.resolve(strict=False)
    raw = str(resolved)
    if os.name != "nt":
        return raw
    if raw.startswith("\\\\?\\"):
        return raw
    if raw.startswith("\\\\"):
        return "\\\\?\\UNC\\" + raw.lstrip("\\")
    return "\\\\?\\" + raw


def _strip_windows_extended_prefix(path_str: str) -> str:
    """Convert an extended-length Windows path back to a regular display path."""
    if os.name != "nt":
        return path_str
    if path_str.startswith("\\\\?\\UNC\\"):
        return "\\\\" + path_str[len("\\\\?\\UNC\\") :]
    if path_str.startswith("\\\\?\\"):
        return path_str[len("\\\\?\\") :]
    return path_str


def path_exists(path: Path) -> bool:
    """Check path existence with Windows long-path support."""
    return os.path.exists(_windows_extended_length_path(Path(path)))


def is_dir(path: Path) -> bool:
    """Check whether a path is a directory with Windows long-path support."""
    return os.path.isdir(_windows_extended_length_path(Path(path)))


def ensure_directory(path: Path) -> None:
    """Create a directory tree with Windows long-path support."""
    os.makedirs(_windows_extended_length_path(Path(path)), exist_ok=True)


def read_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Read a CSV with Windows long-path support."""
    return pd.read_csv(_windows_extended_length_path(Path(path)), **kwargs)


def write_csv(df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
    """Write a CSV with Windows long-path support."""
    target = Path(path)
    ensure_directory(target.parent)
    df.to_csv(_windows_extended_length_path(target), **kwargs)


def unlink_file(path: Path) -> None:
    """Delete one file with Windows long-path support if it exists."""
    try:
        os.unlink(_windows_extended_length_path(Path(path)))
    except FileNotFoundError:
        return


def remove_tree(path: Path) -> None:
    """Delete one directory tree with Windows long-path support if it exists."""
    target = Path(path)
    if not path_exists(target):
        return
    shutil.rmtree(_windows_extended_length_path(target))


def glob_paths(directory: Path, pattern: str) -> list[Path]:
    """Glob inside a directory with Windows long-path support."""
    base = Path(directory)
    matches = glob.glob(os.path.join(_windows_extended_length_path(base), pattern))
    return sorted(Path(_strip_windows_extended_prefix(match)) for match in matches)


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
    if not path_exists(p):
        return pd.DataFrame()

    if is_dir(p):
        return _read_parquet_dataset(p, columns=columns, filters=filters)

    suf = p.suffix.lower()
    if suf == ".parquet":
        if filters:
            return _read_parquet_dataset(p, columns=columns, filters=filters)
        return pd.read_parquet(p, columns=list(columns) if columns else None)

    # CSV / CSV.GZ
    return read_csv(p, **read_csv_kwargs)
