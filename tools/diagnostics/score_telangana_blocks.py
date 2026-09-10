"""Score Telangana's blocks on the frozen Heat Risk ruler (CHG-0391).

The national pilot is district-level by construction (``LEVEL = "district"``),
because neither its geometry roster nor its State aggregation was defined for
blocks. Neither is needed here: this tool scores one State's blocks against the
*already fitted* ruler so the Overview flow prototype can paint a real block
layer instead of standing districts in for it.

Nothing is refitted. The exact pooled mid-rank ``cdf`` support is read back from
the pilot's own ``cdf_support.csv``, and the orientation of each metric from
``metric_ruler_spec.csv``, so the rulers reconstructed here are the rulers the
district scores were produced with. That is what the workflow requires: district
and block scores are directly comparable because both are read off one frozen
national ruler, and neither level is derived from the other --

    "each is scored from its own physical values, and because the ruler is
     non-linear the area-weighted mean of a district's block scores does not
     equal that district's own score"

-- so a block and a district holding the same physical value receive the same
score and the same colour, while a district's own score is *not* recoverable
from its blocks. Both consequences are expected, not defects.

All fourteen bundle metrics are scored, exactly as the district pipeline does,
and the split into the frozen headline happens inside ``score_national_frame``:
``composite_absolute_threshold`` carries the nine absolute metrics with their
weights renormalized 0.633 -> 1.000, while the five baseline-referenced metrics
are retained as a separate lens and excluded from the headline by decision.

Usage
-----
    python -m tools.diagnostics.score_telangana_blocks \
        --state Telangana \
        --support docs/diagnostics/heat_risk_pilot/cdf_support.csv \
        --spec docs/diagnostics/heat_risk_pilot/metric_ruler_spec.csv \
        --out docs/diagnostics/heat_risk_pilot/telangana_block_scores.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.config.paths import get_paths_config
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    BUNDLE_DOMAIN,
    MetricRuler,
    SLICES,
    load_national_long_frame,
    score_national_frame,
)
from india_resilience_tool.compute.composite_metrics import _bundle_metric_specs
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle

FROZEN_RULER = "cdf"
LEVEL = "block"
DEFAULT_STATE = "Telangana"
DEFAULT_SUPPORT = Path("docs/diagnostics/heat_risk_pilot/cdf_support.csv")
DEFAULT_SPEC = Path("docs/diagnostics/heat_risk_pilot/metric_ruler_spec.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/telangana_block_scores.csv")

#: The pilot's own coverage gate for a composite row.
COVERAGE_GATE = 0.60


def load_frozen_rulers(support_path: Path, spec_path: Path) -> dict[str, MetricRuler]:
    """Rebuild the frozen ``cdf`` rulers from the pilot's persisted support.

    The support table is the ruler: one row per distinct pooled value, carrying
    that value's mid-rank score. Reconstructing rather than refitting is the
    point -- a refit over a different sample would silently produce a different
    ruler, and a block map painted against it would not be comparable to the
    district map beside it.
    """
    support = pd.read_csv(support_path)
    for column in ("ruler", "metric_slug", "knot_value", "midrank_score"):
        if column not in support.columns:
            raise ValueError(f"{support_path} has no '{column}' column; not a CDF support table")
    support = support.loc[support["ruler"] == FROZEN_RULER]
    if support.empty:
        raise ValueError(f"{support_path} carries no rows for ruler '{FROZEN_RULER}'")

    spec = pd.read_csv(spec_path)
    spec = spec.loc[spec["ruler"] == FROZEN_RULER].set_index("metric_slug")
    if spec.empty:
        raise ValueError(f"{spec_path} carries no rows for ruler '{FROZEN_RULER}'")

    rulers: dict[str, MetricRuler] = {}
    for slug, group in support.groupby("metric_slug", sort=True):
        group = group.sort_values("knot_value")
        values = group["knot_value"].to_numpy(dtype=float)
        scores = group["midrank_score"].to_numpy(dtype=float)
        if values.size < 2:
            raise ValueError(f"ruler '{slug}' has fewer than two knots; refusing to score")
        if not np.all(np.diff(values) > 0):
            raise ValueError(f"ruler '{slug}' knots are not strictly increasing")
        if slug not in spec.index:
            raise ValueError(f"{spec_path} has no orientation row for metric '{slug}'")
        row = spec.loc[slug]
        rulers[str(slug)] = MetricRuler(
            metric_slug=str(slug),
            kind=FROZEN_RULER,
            higher_is_worse=bool(row["higher_is_worse"]),
            knot_values=values,
            knot_scores=scores,
            pooled_min=float(row.get("pooled_min", values[0])),
            pooled_max=float(row.get("pooled_max", values[-1])),
            pooled_n=int(row.get("pooled_n", values.size)),
            duplicate_knots=0,
            modal_value=float(row.get("modal_value", float("nan"))),
            modal_mass=float(row.get("modal_mass_fraction", float("nan"))),
            knot_counts=(
                group["tie_count"].to_numpy(dtype=float)
                if "tie_count" in group.columns
                else None
            ),
        )
    return rulers


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Score one State's blocks on the frozen Heat Risk cdf ruler (CHG-0391).",
    )
    parser.add_argument("--state", default=DEFAULT_STATE, help="State/UT to score (default: %(default)s).")
    parser.add_argument("--support", type=Path, default=DEFAULT_SUPPORT)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be scored, then stop without writing.")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else Path(get_paths_config().data_dir)

    rulers = load_frozen_rulers(args.support, args.spec)
    print(f"rulers    : {len(rulers)} frozen '{FROZEN_RULER}' metric rulers reconstructed")

    composite = get_composite_metric_for_bundle(BUNDLE_DOMAIN)
    if composite is None:
        print(f"No composite spec for bundle {BUNDLE_DOMAIN!r}", file=sys.stderr)
        return 2
    specs = _bundle_metric_specs(composite)
    scored_specs = [s for s in specs if s.slug in rulers]
    skipped = [s.slug for s in specs if s.slug not in rulers]
    print(f"metrics   : {len(scored_specs)} of {len(specs)} bundle metrics have a frozen ruler")
    if skipped:
        print(f"            baseline-referenced, excluded from the headline: {', '.join(skipped)}")

    if args.dry_run:
        print(f"dry-run   : would score {args.state} blocks over {len(SLICES)} slices; nothing written")
        return 0

    long_frame = load_national_long_frame(
        [s.slug for s in scored_specs],
        level=LEVEL,
        states=[args.state],
        data_dir=data_dir,
        verbose=True,
    )
    if long_frame.empty:
        print(f"ERROR     : no block masters found for {args.state}", file=sys.stderr)
        return 1

    scored = score_national_frame(
        long_frame,
        metric_specs=scored_specs,
        rulers=rulers,
        id_columns=("state", "district", "block", "block_key"),
        coverage_gate=COVERAGE_GATE,
    )
    scored.insert(0, "ruler", FROZEN_RULER)

    n_blocks = scored["block_key"].nunique()
    n_slices = scored.loc[:, ["scenario", "period"]].drop_duplicates().shape[0]
    valid = scored["composite_absolute_threshold"].notna().sum()
    print(f"scored    : {len(scored):,} rows · {n_blocks} blocks × {n_slices} slices · "
          f"{valid:,} valid headline values")
    if valid != len(scored):
        print(f"WARNING   : {len(scored) - valid} rows have no headline value", file=sys.stderr)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(args.out, index=False)
    print(f"wrote     : {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
