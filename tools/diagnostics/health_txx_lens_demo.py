"""Diagnostic: build the Health Risk `txx_ge_45` rule end-to-end for one state.

This is a read-only inspection. It does not write any masters. It exercises the
same code paths the Health Risk bundle builder uses (so what we see here is what
the dashboard would see), but decomposes the blended rule into its three lens
components so you can sanity-check absolute vs change vs impact contributions
against the cited 40-45 deg C IMD plains heatwave band.

It also reads `method_version` and `aggregation_method` off the source TXx
master so we know whether the underlying values came from the grid-first
(`heat-risk-v2-gridfirst-*`) path or the legacy polygon-average-first path.

CLI:
    python -m tools.diagnostics.health_txx_lens_demo \\
        --state Telangana --level district \\
        --scenario ssp585 --period 2060-2080

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.compute.proposal_bundles import (
    BASELINE_TOKENS,
    BuildWarning,
    _baseline_values_for_rule,
    _change_values,
    _ensure_required_id_columns,
    _load_canonical_unit_frame,
    _load_metric_master,
    _required_id_columns,
    _score_by_reference_distribution,
    _score_impact_threshold,
    _series_for_rule,
    _weighted_component_score,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES_BY_SLUG,
    ProposalRuleSpec,
    proposal_rule_score_column,
)
from india_resilience_tool.data.master_columns import (
    find_baseline_column_for_metric,
    resolve_metric_column,
)


BUNDLE_SLUG = "composite_health_risk"
RULE_SLUG = "txx_ge_45"
GRID_FIRST_VERSION_PREFIXES = ("heat-risk-v2-gridfirst",)


def _resolve_rule(rule_slug: str) -> ProposalRuleSpec:
    spec = PROPOSAL_BUNDLES_BY_SLUG[BUNDLE_SLUG]
    for rule in spec.rules:
        if rule.rule_slug == rule_slug:
            return rule
    raise SystemExit(f"Rule {rule_slug!r} not found in bundle {BUNDLE_SLUG!r}.")


def _metric_base(metric_slug: str) -> str:
    entry = METRICS_BY_SLUG[metric_slug]
    return entry.periods_metric_col or entry.value_col or metric_slug


def _read_source_metadata(source_frame: pd.DataFrame) -> dict[str, str]:
    meta: dict[str, str] = {}
    for col in ("method_version", "aggregation_method"):
        if col in source_frame.columns:
            values = (
                source_frame[col]
                .astype("string")
                .dropna()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
            meta[col] = ", ".join(values) if values else "<empty>"
        else:
            meta[col] = "<column absent>"
    return meta


def _impact_position(value: float, low: float, high: float) -> str:
    if not np.isfinite(value):
        return "missing"
    if value < low:
        return "below"
    if value > high:
        return "above"
    frac = (value - low) / (high - low) if high > low else 0.0
    return f"in-band ({frac * 100.0:.0f}%)"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--state", default="Telangana", help="Admin state name (default: Telangana).")
    parser.add_argument(
        "--level",
        choices=("district", "block"),
        default="district",
        help="Aggregation level (default: district).",
    )
    parser.add_argument("--scenario", default="ssp585", help="SSP scenario (default: ssp585).")
    parser.add_argument("--period", default="2060-2080", help="Future period (default: 2060-2080).")
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of districts/blocks to show at the top and bottom of the ranked table (default: 10).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Optional override for IRT data_dir (defaults to paths.py resolution).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    data_dir = args.data_dir if args.data_dir is not None else get_paths_config().data_dir
    rule = _resolve_rule(RULE_SLUG)

    id_cols = list(_required_id_columns(args.level))
    label_col = "block" if args.level == "block" else "district"

    # Load canonical unit frame and TXx source master (same paths the builder uses).
    key_frame = _load_canonical_unit_frame(level=args.level, state_name=args.state, data_dir=data_dir)
    if key_frame.empty:
        raise SystemExit(f"No canonical units for state={args.state!r}, level={args.level!r}.")
    source_frame = _load_metric_master(
        rule.metric_slug, level=args.level, state_name=args.state, data_dir=data_dir
    )

    # Identify the actual columns being read so the diagnostic is reproducible.
    base = _metric_base(rule.metric_slug)
    current_col = resolve_metric_column(source_frame, base, args.scenario, args.period, "mean")
    baseline_col = find_baseline_column_for_metric(
        list(source_frame.columns),
        base_metric=base,
        preferred_period_tokens=BASELINE_TOKENS,
    )

    source_meta = _read_source_metadata(source_frame)
    method_version = source_meta.get("method_version", "<missing>")
    grid_first = any(method_version.startswith(prefix) for prefix in GRID_FIRST_VERSION_PREFIXES)

    print("=" * 80)
    print(f"Health Risk TXx lens demo  ({BUNDLE_SLUG} / rule {RULE_SLUG})")
    print("=" * 80)
    print(f"  data_dir         : {data_dir}")
    print(f"  state / level    : {args.state} / {args.level}")
    print(f"  scenario / period: {args.scenario} / {args.period}")
    print(f"  source slug      : {rule.metric_slug} (base column {base!r})")
    print(f"  current column   : {current_col!r}")
    print(f"  baseline column  : {baseline_col!r}  (BASELINE_TOKENS={BASELINE_TOKENS})")
    print(f"  rule lens weights: abs={rule.absolute_weight:.2f} chg={rule.change_weight:.2f} imp={rule.impact_weight:.2f}")
    print(
        f"  impact band      : {rule.impact_low:g}-{rule.impact_high:g} "
        f"(change_mode={rule.change_mode!r}, direction={rule.direction!r})"
    )
    print(f"  rule weight      : {rule.rule_weight:.3f}")
    print()
    print("Source master metadata:")
    for col, value in source_meta.items():
        marker = ""
        if col == "method_version":
            marker = "  [GRID-FIRST OK]" if grid_first else "  [NOT GRID-FIRST: verify aggregation]"
        print(f"  {col:>20}: {value}{marker}")
    print()

    # Recompute lens components using the same helpers the bundle builder uses.
    current_values = _series_for_rule(
        key_frame,
        source_frame,
        level=args.level,
        metric_slug=rule.metric_slug,
        scenario=args.scenario,
        period=args.period,
    )
    warnings: list[BuildWarning] = []
    baseline_values = _baseline_values_for_rule(
        key_frame,
        source_frame,
        level=args.level,
        rule=rule,
        warnings=warnings,
        bundle_slug=BUNDLE_SLUG,
        state_name=args.state,
    )
    change_values = _change_values(
        current_values,
        baseline_values,
        metric_slug=rule.metric_slug,
        change_mode=rule.change_mode,
    )
    absolute_lens = _score_by_reference_distribution(current_values, direction=rule.direction)
    change_lens = _score_by_reference_distribution(change_values, direction=rule.direction)
    impact_lens = _score_impact_threshold(
        current_values,
        impact_low=rule.impact_low,
        impact_high=rule.impact_high,
        direction=rule.direction,
    )

    component_scores: list[pd.Series] = []
    component_weights: list[float] = []
    if rule.absolute_weight > 0:
        component_scores.append(absolute_lens)
        component_weights.append(float(rule.absolute_weight))
    if rule.change_weight > 0:
        component_scores.append(change_lens)
        component_weights.append(float(rule.change_weight))
    if rule.impact_weight > 0:
        component_scores.append(impact_lens)
        component_weights.append(float(rule.impact_weight))
    blended = _weighted_component_score(component_scores, component_weights)

    # Build display table.
    out = key_frame.loc[:, id_cols].copy()
    out["txx_C"] = current_values.to_numpy(dtype=float)
    out["txx_baseline_C"] = baseline_values.to_numpy(dtype=float)
    out["delta_C"] = out["txx_C"] - out["txx_baseline_C"]
    out["band_position"] = [
        _impact_position(v, rule.impact_low, rule.impact_high) for v in out["txx_C"].tolist()
    ]
    out["abs_lens"] = absolute_lens.to_numpy(dtype=float)
    out["chg_lens"] = change_lens.to_numpy(dtype=float)
    out["imp_lens"] = impact_lens.to_numpy(dtype=float)
    out["rule_score"] = blended.to_numpy(dtype=float)

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.float_format", lambda v: f"{v:8.2f}")

    n_total = len(out)
    n_below = int((out["txx_C"] < rule.impact_low).sum())
    n_in = int(((out["txx_C"] >= rule.impact_low) & (out["txx_C"] <= rule.impact_high)).sum())
    n_above = int((out["txx_C"] > rule.impact_high).sum())
    print(
        f"Band coverage (TXx vs {rule.impact_low:g}-{rule.impact_high:g} degC): "
        f"below={n_below}  in-band={n_in}  above={n_above}  (total {n_total})"
    )
    print()

    sorted_out = out.sort_values("rule_score", ascending=False, kind="stable", na_position="last")
    display_cols = id_cols + [
        "txx_C",
        "txx_baseline_C",
        "delta_C",
        "band_position",
        "abs_lens",
        "chg_lens",
        "imp_lens",
        "rule_score",
    ]
    print(f"Top {args.top} by rule score:")
    print(sorted_out[display_cols].head(args.top).to_string(index=False))
    print()
    print(f"Bottom {args.top} by rule score:")
    print(sorted_out[display_cols].tail(args.top).to_string(index=False))
    print()

    # Lens-component summary.
    desc = out[["abs_lens", "chg_lens", "imp_lens", "rule_score"]].describe(
        percentiles=[0.1, 0.5, 0.9]
    )
    print("Lens-component summary (0-100 scale):")
    print(desc.T.to_string())
    print()

    if warnings:
        print(f"Warnings emitted by lens builder ({len(warnings)}):")
        for w in warnings[:10]:
            print(f"  - {w.message}")
        if len(warnings) > 10:
            print(f"  ... ({len(warnings) - 10} more suppressed)")
        print()

    # Cross-check against the persisted bundle column (if the bundle has been built).
    persisted_col = proposal_rule_score_column(rule.rule_slug, args.scenario, args.period)
    print(
        f"Persisted bundle rule column to cross-check (if built): {persisted_col!r}"
    )

    if not grid_first:
        print()
        print(
            "WARNING: source method_version does not start with "
            f"{GRID_FIRST_VERSION_PREFIXES!r}. The values above came from the "
            "legacy polygon-average path. Rebuild the TXx master via the "
            "grid-first pipeline before treating these scores as final."
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
