"""Read-only audit: check whether proposal-bundle metric masters carry
`__historical__1990-2010__mean` columns.

Run from repo root:
    python audit_proposal_baseline_columns.py
    python audit_proposal_baseline_columns.py --csv audit.csv
    python audit_proposal_baseline_columns.py --states Telangana,Karnataka
    python audit_proposal_baseline_columns.py --levels district

Outputs:
- Per (metric, level, state): periods present, strict-1990-2010 hit, old-BASELINE_TOKENS hit
- Per (bundle, rule, level): coverage rate under strict 1990-2010 vs current BASELINE_TOKENS
- Optional CSV dump for offline inspection
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

# Ensure repo root is importable when invoked from cwd
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd  # noqa: E402

from india_resilience_tool.app.geography import list_available_states_from_processed_root  # noqa: E402
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG  # noqa: E402
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root  # noqa: E402
from india_resilience_tool.config.proposal_bundles import PROPOSAL_BUNDLES  # noqa: E402
from india_resilience_tool.data.master_loader import resolve_preferred_master_path  # noqa: E402

# Mirror the constants used by compute/proposal_bundles.py so the audit reflects
# real loader behavior — DO NOT diverge from those defaults.
LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
}
OLD_BASELINE_TOKENS = ("1995-2014", "1995_2014", "1985-2014")
STRICT_BASELINE_PERIOD = "1990-2010"
HELPER_METRIC_SLUG = "r95p_interannual_variability"
HELPER_SOURCE_METRIC_SLUG = "r95p_very_wet_precip"


def _metric_base(metric_slug: str) -> str:
    spec = METRICS_BY_SLUG[metric_slug]
    return spec.periods_metric_col or spec.value_col or metric_slug


def _read_columns_only(path: Path) -> list[str]:
    """Read column names without loading data; supports CSV and Parquet."""
    if path.suffix.lower() == ".parquet":
        # Use pyarrow schema if available, else pandas fallback
        try:
            import pyarrow.parquet as pq  # local import to avoid hard dep
            return list(pq.read_schema(str(path)).names)
        except Exception:
            return list(pd.read_parquet(path).columns)
    # CSV path
    return list(pd.read_csv(path, nrows=0).columns)


_HIST_MEAN_RE_CACHE: dict[str, re.Pattern] = {}


def _hist_mean_pattern(metric_base: str) -> re.Pattern:
    pat = _HIST_MEAN_RE_CACHE.get(metric_base)
    if pat is None:
        pat = re.compile(rf"^{re.escape(metric_base)}__historical__(?P<period>[^_]+(?:[_-][^_]+)?)__mean$")
        _HIST_MEAN_RE_CACHE[metric_base] = pat
    return pat


def _historical_mean_periods(columns: list[str], metric_base: str) -> dict[str, str]:
    """Return {normalized_period: original_column_name} for matching cols."""
    pat = _hist_mean_pattern(metric_base)
    found: dict[str, str] = {}
    for col in columns:
        m = pat.match(str(col))
        if not m:
            continue
        # Schema is double-underscore-separated; `__historical__<period>__mean`
        # already gives one token between scenario and stat. Normalize variants.
        period_raw = m.group("period").strip()
        normalized = period_raw.replace("_", "-")
        found.setdefault(normalized, str(col))
        # also keep underscore form keyed under same value for downstream check
    return found


def _strict_hit(periods: dict[str, str]) -> Optional[str]:
    return periods.get(STRICT_BASELINE_PERIOD)


def _old_token_hit(periods: dict[str, str]) -> Optional[tuple[str, str]]:
    """First match under legacy BASELINE_TOKENS order."""
    for token in OLD_BASELINE_TOKENS:
        norm = token.replace("_", "-")
        if norm in periods:
            return token, periods[norm]
    return None


def _iter_unique_metric_slugs() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for bundle in PROPOSAL_BUNDLES:
        for rule in bundle.rules:
            ms = rule.metric_slug
            if ms == HELPER_METRIC_SLUG:
                ms = HELPER_SOURCE_METRIC_SLUG
            if ms in seen:
                continue
            seen.add(ms)
            out.append(ms)
    return out


def _states_for_metric(metric_slug: str, *, data_dir: Path, override: Optional[list[str]]) -> list[str]:
    root = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
    discovered = list_available_states_from_processed_root(str(root))
    if override:
        wanted = {s.strip() for s in override if s.strip()}
        return [s for s in discovered if s in wanted]
    return discovered


def audit(
    *,
    data_dir: Path,
    levels: tuple[str, ...],
    state_filter: Optional[list[str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict] = []

    metric_slugs = _iter_unique_metric_slugs()
    print(f"Auditing {len(metric_slugs)} unique source metric(s) across "
        f"{len(PROPOSAL_BUNDLES)} bundles, levels={levels}.")
    print(f"Data dir: {data_dir}")
    print()

    for metric_slug in metric_slugs:
        if metric_slug not in METRICS_BY_SLUG:
            rows.append({
                "metric_slug": metric_slug,
                "level": "-",
                "state": "-",
                "master_path": "",
                "exists": False,
                "periods_found": "",
                "n_periods": 0,
                "strict_1990_2010": False,
                "strict_column": "",
                "old_tokens_match": "",
                "old_tokens_column": "",
                "note": "metric_slug not in METRICS_BY_SLUG",
            })
            continue

        metric_base = _metric_base(metric_slug)
        states = _states_for_metric(metric_slug, data_dir=data_dir, override=state_filter)
        if not states:
            rows.append({
                "metric_slug": metric_slug,
                "level": "-",
                "state": "-",
                "master_path": str(resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")),
                "exists": False,
                "periods_found": "",
                "n_periods": 0,
                "strict_1990_2010": False,
                "strict_column": "",
                "old_tokens_match": "",
                "old_tokens_column": "",
                "note": "no states discovered under processed root",
            })
            continue

        for level in levels:
            filename = LEGACY_MASTER_FILENAMES[level]
            for state in states:
                source_path = (
                    resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
                    / state
                    / filename
                )
                preferred = resolve_preferred_master_path(source_path)
                exists = preferred.exists()
                row = {
                    "metric_slug": metric_slug,
                    "metric_base": metric_base,
                    "level": level,
                    "state": state,
                    "master_path": str(preferred),
                    "exists": exists,
                    "periods_found": "",
                    "n_periods": 0,
                    "strict_1990_2010": False,
                    "strict_column": "",
                    "old_tokens_match": "",
                    "old_tokens_column": "",
                    "note": "",
                }
                if not exists:
                    row["note"] = "master file not found"
                    rows.append(row)
                    continue
                try:
                    cols = _read_columns_only(preferred)
                except Exception as exc:
                    row["note"] = f"read_failed: {type(exc).__name__}: {exc}"
                    rows.append(row)
                    continue

                periods = _historical_mean_periods(cols, metric_base)
                row["periods_found"] = ",".join(sorted(periods.keys()))
                row["n_periods"] = len(periods)

                strict_col = _strict_hit(periods)
                if strict_col:
                    row["strict_1990_2010"] = True
                    row["strict_column"] = strict_col

                old_hit = _old_token_hit(periods)
                if old_hit:
                    row["old_tokens_match"] = old_hit[0]
                    row["old_tokens_column"] = old_hit[1]

                rows.append(row)

    detail_df = pd.DataFrame(rows)

    # Rule-level rollup (one row per bundle x rule x level)
    rollup_rows: list[dict] = []
    detail_indexed = detail_df.set_index(["metric_slug", "level", "state"], drop=False) if not detail_df.empty else None
    for bundle in PROPOSAL_BUNDLES:
        for rule in bundle.rules:
            actual_metric = HELPER_SOURCE_METRIC_SLUG if rule.metric_slug == HELPER_METRIC_SLUG else rule.metric_slug
            for level in levels:
                if detail_indexed is None:
                    continue
                sub = detail_df[(detail_df["metric_slug"] == actual_metric) & (detail_df["level"] == level)]
                states_total = (sub["exists"] == True).sum()  # noqa: E712
                states_strict = ((sub["exists"] == True) & (sub["strict_1990_2010"] == True)).sum()  # noqa: E712
                states_old = ((sub["exists"] == True) & (sub["old_tokens_match"] != "")).sum()
                losers = sub[(sub["exists"] == True) & (sub["strict_1990_2010"] == False)]["state"].tolist()  # noqa: E712
                rollup_rows.append({
                    "bundle_slug": bundle.composite_slug,
                    "rule_slug": rule.rule_slug,
                    "metric_slug": rule.metric_slug,
                    "resolved_source_slug": actual_metric,
                    "level": level,
                    "states_with_master": int(states_total),
                    "states_with_strict_1990_2010": int(states_strict),
                    "states_with_old_BASELINE_TOKENS": int(states_old),
                    "coverage_strict_pct": (100.0 * states_strict / states_total) if states_total else 0.0,
                    "states_missing_strict": ",".join(losers) if losers else "",
                    "change_weight": rule.change_weight,
                    "impact_weight": rule.impact_weight,
                })
    rollup_df = pd.DataFrame(rollup_rows)
    return detail_df, rollup_df


def _print_period_histogram(detail_df: pd.DataFrame) -> None:
    counts: dict[str, int] = defaultdict(int)
    for periods_str in detail_df.loc[detail_df["exists"] == True, "periods_found"]:  # noqa: E712
        for p in (periods_str or "").split(","):
            p = p.strip()
            if p:
                counts[p] += 1
    print("Historical-period frequency across all (metric, level, state) cells that have a master:")
    for period, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        marker = "  <-- STRICT TARGET" if period == STRICT_BASELINE_PERIOD else ""
        marker = "  <-- legacy BASELINE_TOKENS" if period in {t.replace("_", "-") for t in OLD_BASELINE_TOKENS} else marker
        print(f"  {period:>16s}  {n:>6d}{marker}")
    print()


def _print_rollup(rollup_df: pd.DataFrame) -> None:
    print("Rule-level coverage (one row per bundle x rule x level):")
    cols = [
        "bundle_slug", "rule_slug", "resolved_source_slug", "level",
        "states_with_master", "states_with_strict_1990_2010",
        "states_with_old_BASELINE_TOKENS", "coverage_strict_pct",
        "change_weight", "impact_weight",
    ]
    print(rollup_df[cols].to_string(index=False))
    print()

    losers = rollup_df[(rollup_df["states_with_master"] > 0)
                        & (rollup_df["states_with_strict_1990_2010"] < rollup_df["states_with_master"])
                        & (rollup_df["change_weight"] > 0)]
    if losers.empty:
        print("All rules with change-lens weight have strict-1990-2010 baseline available in every state with a master.")
    else:
        print("Rules that would lose change-lens (NaN) in some states under strict 1990-2010 pin:")
        print(losers[["bundle_slug", "rule_slug", "level", "states_missing_strict",
                    "change_weight", "impact_weight"]].to_string(index=False))
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=None, help="Override IRT data dir (else from get_paths_config()).")
    parser.add_argument("--levels", default="district,block", help="Comma-separated levels (default: district,block).")
    parser.add_argument("--states", default=None, help="Optional comma-separated state filter.")
    parser.add_argument("--csv", default=None, help="Optional CSV path for the detail table.")
    parser.add_argument("--rollup-csv", default=None, help="Optional CSV path for the rollup table.")
    args = parser.parse_args()

    paths = get_paths_config()
    data_dir = Path(args.data_dir) if args.data_dir else paths.data_dir
    levels = tuple(s.strip() for s in args.levels.split(",") if s.strip())
    state_filter = [s.strip() for s in args.states.split(",")] if args.states else None

    detail_df, rollup_df = audit(data_dir=data_dir, levels=levels, state_filter=state_filter)

    if detail_df.empty:
        print("No rows produced — check that PROPOSAL_BUNDLES is non-empty and data_dir is correct.")
        return 1

    _print_period_histogram(detail_df)
    _print_rollup(rollup_df)

    if args.csv:
        detail_df.to_csv(args.csv, index=False)
        print(f"Wrote detail CSV: {args.csv}")
    if args.rollup_csv:
        rollup_df.to_csv(args.rollup_csv, index=False)
        print(f"Wrote rollup CSV: {args.rollup_csv}")

    # Exit non-zero if any rule with change-lens weight would lose strict baseline
    risky = rollup_df[(rollup_df["states_with_master"] > 0)
                    & (rollup_df["states_with_strict_1990_2010"] < rollup_df["states_with_master"])
                    & (rollup_df["change_weight"] > 0)]
    return 0 if risky.empty else 2


if __name__ == "__main__":
    sys.exit(main())