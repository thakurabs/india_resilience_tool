# india_resilience_tool/data/master_loader.py
"""
Master CSV loading, normalization, and schema parsing for IRT.

This module preserves the dashboard's contractual normalization behavior:
    days_gt_32C_ssp585_2020_2040__mean
->  days_gt_32C__ssp585__2020-2040__mean

Schema parsing recognizes normalized columns of the form:
    <metric>__<scenario>__<period>__<stat>
where stat is one of: mean, median, std, p05, p95.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

import pandas as pd

from india_resilience_tool.utils.processed_io import read_table

PathLike = Union[str, Path]
MasterSourceLike = Union[PathLike, Sequence[PathLike]]


def resolve_preferred_master_path(path: PathLike) -> Path:
    """
    Resolve the preferred on-disk serving artifact for a master table.

    Runtime prefers a Parquet companion when one exists next to the CSV, while
    preserving CSV fallback for compatibility with older builds.
    """
    resolved = Path(path)
    if resolved.suffix.lower() == ".csv":
        parquet_path = resolved.with_suffix(".parquet")
        if parquet_path.exists():
            return parquet_path
    return resolved


def normalize_master_source_paths(master_source: MasterSourceLike) -> tuple[Path, ...]:
    """
    Normalize one or many master CSV paths into an ordered tuple of Paths.

    Raises:
        ValueError: If no paths are supplied.
    """
    if isinstance(master_source, (str, Path)):
        paths = (Path(master_source),)
    else:
        paths = tuple(Path(p) for p in master_source)

    if not paths:
        raise ValueError("No master CSV paths were provided.")
    return paths


def master_source_signature(master_source: MasterSourceLike) -> tuple[tuple[str, Optional[float]], ...]:
    """Return a stable `(resolved_path, mtime)` signature for one or many master CSVs."""
    signature: list[tuple[str, Optional[float]]] = []
    for path in normalize_master_source_paths(master_source):
        preferred_path = resolve_preferred_master_path(path)
        try:
            resolved = str(preferred_path.resolve())
        except Exception:
            resolved = str(preferred_path)
        try:
            mtime = preferred_path.stat().st_mtime
        except Exception:
            mtime = None
        signature.append((resolved, mtime))
    return tuple(signature)


def read_csv_robust(
    path: PathLike,
    *,
    encoding_priority: Optional[list[str]] = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """
    Read a CSV with encoding fallbacks.

    Contract:
      - First attempt uses pandas defaults (no explicit encoding) to preserve behavior.
      - If a UnicodeDecodeError occurs, try a small set of common encodings.

    Args:
        path: CSV path.
        encoding_priority: List of encodings to attempt after default fails.
        **read_csv_kwargs: Passed to pandas.read_csv

    Returns:
        DataFrame loaded from CSV.

    Raises:
        UnicodeDecodeError: If all fallback encodings fail with UnicodeDecodeError.
        Exception: Propagates other pandas read_csv errors.
    """
    if encoding_priority is None:
        encoding_priority = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]

    path_str = str(path)

    try:
        return pd.read_csv(path_str, **read_csv_kwargs)
    except UnicodeDecodeError:
        last_err: Optional[UnicodeDecodeError] = None
        for enc in encoding_priority:
            try:
                return pd.read_csv(path_str, encoding=enc, **read_csv_kwargs)
            except UnicodeDecodeError as e:
                last_err = e
                continue
        if last_err is not None:
            raise last_err
        raise


def load_master_csv(path: PathLike) -> pd.DataFrame:
    """
    Load the master metrics CSV.

    Intentionally thin wrapper around read_csv_robust().
    """
    preferred_path = resolve_preferred_master_path(path)
    if preferred_path.suffix.lower() == ".parquet" or preferred_path.is_dir():
        return read_table(preferred_path)
    return read_csv_robust(preferred_path)


def load_master_csvs(master_source: MasterSourceLike) -> pd.DataFrame:
    """
    Load one or many master CSVs and return a single concatenated DataFrame.
    """
    paths = normalize_master_source_paths(master_source)
    frames = [load_master_csv(path) for path in paths]
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True, sort=False)


def normalize_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns like:
        days_gt_32C_ssp585_2020_2040__mean
    to:
        days_gt_32C__ssp585__2020-2040__mean

    and likewise for:
        __median, __std, __p05, __p95, __n_models, __models, __values_per_model.

    Returns:
        A DataFrame with renamed columns if matches exist; otherwise returns df unchanged.
    """
    mapping: dict[Any, str] = {}
    pat = re.compile(
        r"^(.+?)_"
        r"(historical|ssp119|ssp126|ssp245|ssp370|ssp434|ssp460|ssp585)_"
        r"(\d{4})_(\d{4})__"
        r"(mean|median|std|p05|p95|n_models|models|values_per_model)$",
        re.I,
    )

    for c in df.columns:
        s = str(c).strip()
        m = pat.match(s)
        if not m:
            continue
        metric, scen, y0, y1, suffix = m.groups()
        new = f"{metric.strip()}__{scen.lower().strip()}__{y0}-{y1}__{suffix.lower().strip()}"
        mapping[c] = new

    if mapping:
        return df.rename(columns=mapping)
    return df


@dataclass(frozen=True)
class MasterSchema:
    """
    Parsed schema of normalized master columns.

    Attributes:
        items: List of dicts with keys: metric, scenario, period, stat, column
        metrics: Sorted unique metrics found
        by_metric: metric -> list of items
        scenarios: Sorted unique scenarios found
        periods: Sorted unique periods found
        stats: Sorted unique stats found
    """

    items: list[dict[str, Any]]
    metrics: list[str]
    by_metric: dict[str, list[dict[str, Any]]]
    scenarios: list[str]
    periods: list[str]
    stats: list[str]


def parse_master_schema_obj(cols: Iterable[Any]) -> MasterSchema:
    """
    Parse normalized master schema columns into a typed MasterSchema.

    Recognizes columns in the canonical normalized form:
        <metric>__<scenario>__<period>__<stat>
    where stat is one of: mean, median, std, p05, p95

    Notes:
      - This intentionally does NOT include columns such as __n_models, __models,
        __values_per_model, even though normalize_master_columns() can normalize them.
        This matches current dashboard parsing behavior.
    """
    pat = re.compile(
        r"^(?P<metric>[^_][^:]*)__(?P<scenario>[^_]+)__(?P<period>[^_]+)__(?P<stat>mean|median|std|p05|p95)$"
    )
    items: list[dict[str, Any]] = []
    for c in cols:
        m = pat.match(str(c))
        if m:
            items.append({**m.groupdict(), "column": c})

    metrics = sorted(set(i["metric"] for i in items))
    by_metric = {m: [i for i in items if i["metric"] == m] for m in metrics}
    scenarios = sorted(set(i["scenario"] for i in items))
    periods = sorted(set(i["period"] for i in items))
    stats = sorted(set(i["stat"] for i in items))

    return MasterSchema(
        items=items,
        metrics=metrics,
        by_metric=by_metric,
        scenarios=scenarios,
        periods=periods,
        stats=stats,
    )


def parse_master_schema(cols: Iterable[Any]):
    """
    Legacy-compatible wrapper used by the current dashboard.

    Returns:
        (schema_items, metrics, by_metric)

    This preserves the dashboard's existing unpacking behavior:
        schema_items, metrics, by_metric = parse_master_schema(df.columns)
    """
    schema = parse_master_schema_obj(cols)
    return schema.items, schema.metrics, schema.by_metric
