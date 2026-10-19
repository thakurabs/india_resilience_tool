"""Compare legacy and Heat Stress v2 grid-first metric extracts.

The tool is intentionally non-destructive. Operators provide legacy and
grid-first CSV files for each metric; the script reports value deltas, rank
shifts, and top movers, optionally writing a Markdown report.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd


DEFAULT_METRICS = (
    "twb_annual_mean",
    "twb_summer_mean",
    "twb_annual_max",
    "twb_days_ge_28",
    "twb_days_ge_30",
    "tasmin_tropical_nights_gt28",
    # CHG-0012: WBGT/SWBGT diagnostics promoted to grid-first under v2.1.
    "wbgt_shade_stull_annual_mean",
    "wbgt_shade_stull_days_ge_28",
    "wbgt_shade_stull_days_ge_30",
    "wbgt_shade_stull_days_ge_32",
    "swbgt_empirical_annual_mean",
    "swbgt_empirical_days_ge_28",
    "swbgt_empirical_days_ge_30",
    "swbgt_empirical_days_ge_32",
)


def _parse_metric_csv(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected METRIC=CSV_PATH")
    metric, path = value.split("=", 1)
    metric = metric.strip()
    if not metric:
        raise argparse.ArgumentTypeError("Metric slug cannot be empty")
    return metric, Path(path)


def _parse_metric_column(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Expected METRIC=COLUMN")
    metric, column = value.split("=", 1)
    metric = metric.strip()
    column = column.strip()
    if not metric or not column:
        raise argparse.ArgumentTypeError("Metric slug and column cannot be empty")
    return metric, column


def _to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = list(df.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in columns) + " |")
    return "\n".join(lines)


def _value_column(df: pd.DataFrame, metric: str, explicit: str | None) -> str:
    if explicit:
        if explicit not in df.columns:
            raise ValueError(f"Explicit value column {explicit!r} not found for {metric}")
        return explicit
    if metric in df.columns:
        return metric
    candidates = [col for col in df.columns if col not in {"state", "district", "block", "year", "period", "model", "scenario"}]
    if "value" in df.columns:
        return "value"
    metric_like = [col for col in candidates if col.startswith(metric) or metric in col]
    if len(metric_like) == 1:
        return metric_like[0]
    raise ValueError(f"Could not infer value column for {metric}; pass --value-col {metric}=COLUMN")


def _unit_columns(df: pd.DataFrame) -> list[str]:
    cols = [col for col in ("state", "district", "block") if col in df.columns]
    if "district" not in cols:
        raise ValueError("Input CSVs must include at least a district column")
    return cols


def _load_metric_pair(
    metric: str,
    legacy_path: Path,
    gridfirst_path: Path,
    value_cols: dict[str, str],
) -> pd.DataFrame:
    legacy = pd.read_csv(legacy_path)
    gridfirst = pd.read_csv(gridfirst_path)
    unit_cols = _unit_columns(legacy)
    for col in unit_cols:
        if col not in gridfirst.columns:
            raise ValueError(f"Grid-first CSV for {metric} is missing unit column {col!r}")
    legacy_value = _value_column(legacy, metric, value_cols.get(metric))
    grid_value = _value_column(gridfirst, metric, value_cols.get(metric))
    keep = unit_cols + [legacy_value]
    if "period" in legacy.columns and "period" in gridfirst.columns:
        keep.insert(len(unit_cols), "period")
    if "year" in legacy.columns and "year" in gridfirst.columns:
        keep.insert(len(unit_cols), "year")
    left = legacy[keep].rename(columns={legacy_value: "legacy_value"})
    right_keep = [col for col in keep if col != legacy_value] + [grid_value]
    right = gridfirst[right_keep].rename(columns={grid_value: "gridfirst_value"})
    join_cols = [col for col in keep if col != legacy_value]
    out = left.merge(right, on=join_cols, how="inner")
    out["metric"] = metric
    out["delta"] = out["gridfirst_value"] - out["legacy_value"]
    out["abs_delta"] = out["delta"].abs()
    rank_group = [col for col in ("period", "year") if col in out.columns]
    out["legacy_rank"] = out.groupby(rank_group)["legacy_value"].rank(ascending=False, method="min") if rank_group else out["legacy_value"].rank(ascending=False, method="min")
    out["gridfirst_rank"] = out.groupby(rank_group)["gridfirst_value"].rank(ascending=False, method="min") if rank_group else out["gridfirst_value"].rank(ascending=False, method="min")
    out["rank_shift"] = out["gridfirst_rank"] - out["legacy_rank"]
    return out


def _summaries(combined: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary = (
        combined.groupby("metric", sort=False)
        .agg(
            rows=("delta", "size"),
            mean_delta=("delta", "mean"),
            median_delta=("delta", "median"),
            max_abs_delta=("abs_delta", "max"),
            mean_abs_rank_shift=("rank_shift", lambda s: s.abs().mean()),
            max_abs_rank_shift=("rank_shift", lambda s: s.abs().max()),
        )
        .reset_index()
    )
    rank_summary = (
        combined.groupby("metric", sort=False)["rank_shift"]
        .apply(lambda s: (s.abs() >= 5).sum())
        .rename("units_with_abs_rank_shift_ge_5")
        .reset_index()
    )
    movers = combined.sort_values(["metric", "abs_delta"], ascending=[True, False]).groupby("metric", sort=False).head(top_n)
    return summary, rank_summary, movers


def _markdown_report(summary: pd.DataFrame, rank_summary: pd.DataFrame, movers: pd.DataFrame, *, state: str) -> str:
    return "\n\n".join(
        [
            f"# Heat Stress v2 Grid-First Parity Diagnostic: {state}",
            "## Summary Deltas\n\n" + _to_markdown(summary),
            "## Rank-Shift Summary\n\n" + _to_markdown(rank_summary),
            "## Top Movers\n\n" + _to_markdown(movers),
        ]
    ) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="Telangana", help="State label for the report title.")
    parser.add_argument("--legacy", action="append", type=_parse_metric_csv, required=True, metavar="METRIC=CSV")
    parser.add_argument("--gridfirst", action="append", type=_parse_metric_csv, required=True, metavar="METRIC=CSV")
    parser.add_argument("--value-col", action="append", type=str, default=[], metavar="METRIC=COLUMN")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--report-out", type=Path, help="Optional Markdown report path to write.")
    args = parser.parse_args(argv)

    legacy = dict(args.legacy)
    gridfirst = dict(args.gridfirst)
    value_cols = dict(_parse_metric_column(item) for item in args.value_col)
    missing = [metric for metric in DEFAULT_METRICS if metric not in legacy or metric not in gridfirst]
    if missing:
        raise SystemExit(f"Missing legacy/gridfirst CSV arguments for: {', '.join(missing)}")

    combined = pd.concat(
        [_load_metric_pair(metric, legacy[metric], gridfirst[metric], value_cols) for metric in DEFAULT_METRICS],
        ignore_index=True,
    )
    summary, rank_summary, movers = _summaries(combined, args.top_n)
    report = _markdown_report(summary, rank_summary, movers, state=args.state)
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(report, encoding="utf-8")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
