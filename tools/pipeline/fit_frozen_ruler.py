"""Fit and freeze a national CDF ruler for one composite bundle (CHG-0367c).

The ruler is fitted **once**, over the national district pool across every
declared scenario/period slice, and then committed to the repository under
``india_resilience_tool/config/frozen_rulers/<composite_slug>/<version>/``. From
that point it is read, never refitted: the published composite for every unit at
every level is scored through the same transfer function, which is what makes
scores comparable across states and across time.

Only the **headline** half of the bundle is fitted -- the metrics scored against
absolute physical thresholds or levels (``is_baseline_referenced=False`` in
``config/bundle_weights``). Their configured weights sum to less than 1.0 and are
renormalized at scoring time.

The national assembly path is the pilot's, imported rather than reimplemented:
``load_district_roster``, ``load_national_long_frame`` and ``expand_to_roster``
already read the canonical district roster from
``processed_optimised/geometry/admin/district/state=*.geojson`` and reconcile the
component masters against it.

Usage
-----
    python -m tools.pipeline.fit_frozen_ruler --bundle "Heat Risk" --out-version v1
    python -m tools.pipeline.fit_frozen_ruler --bundle "Heat Risk" --out-version v1 --dry-run

The tool refuses to overwrite an existing version directory. Refitting a
published version in place is how a frozen ruler silently changes; fit ``v2``
into its own directory instead.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.frozen_rulers import (
    CDF_KIND,
    FrozenRulerSet,
    build_cdf_ruler,
    frozen_ruler_dir,
    save_ruler_set,
    snapshot_hash,
)
from india_resilience_tool.compute.composite_metrics import (
    _build_wide_component_frame,
    _bundle_metric_specs,
    _compute_frozen_ruler_score_frame,
    _load_component_master,
    _required_id_columns,
    _resolve_component_metric_column,
    _resolve_state_paths,
)
from india_resilience_tool.config.bundle_weights import (
    get_bundle_headline_weight_total,
    get_bundle_headline_weights,
)
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.master_loader import resolve_preferred_master_path
from india_resilience_tool.utils.naming import alias
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    discover_states,
    expand_to_roster,
    load_district_roster,
    load_national_long_frame,
)

#: Evidence-backed publication gates by bundle. Heat Risk's nine-metric pilot
#: measured 0.00% of rows below 0.70. Riverine Flood has one effective metric,
#: so coverage is binary; all 784 fitted districts measured 1.0 coverage.
BUNDLE_COVERAGE_GATES: dict[str, float] = {
    "Heat Risk": 0.70,
    "Riverine Flood": 1.0,
}

LEVEL = "district"

#: Colour ramp shipped beside the scores so the vendor never has a "pick a
#: colour scale" step -- that step is where per-state rescaling gets reinvented.
COLOUR_SCALE_ID = "whbgyr-101-floor045-v1"
COLOUR_SCALE_FILENAME = "colour_scale.json"
GOLDEN_CANARIES_FILENAME = "golden_canaries.csv"


def coverage_gate_for_bundle(bundle_domain: str) -> float:
    """Return the evidence-backed coverage gate configured for one bundle."""
    try:
        return BUNDLE_COVERAGE_GATES[str(bundle_domain)]
    except KeyError as exc:
        raise ValueError(
            f"No coverage gate is configured for bundle {bundle_domain!r}; measure "
            "headline completeness and add an evidence-backed gate before fitting."
        ) from exc


def _ordered_metric_slices(
    frame: pd.DataFrame, metric_slug: str
) -> tuple[tuple[str, str], ...]:
    """Discover resolvable four-token mean slices from one component master."""
    pairs: list[tuple[str, str]] = []
    for column in frame.columns:
        parts = str(column).split("__")
        if (
            len(parts) == 4
            and parts[1]
            and parts[2]
            and parts[3] == "mean"
        ):
            pair = (parts[1], parts[2])
            if pair not in pairs and _resolve_component_metric_column(
                frame,
                metric_slug=metric_slug,
                scenario=pair[0],
                period=pair[1],
            ):
                pairs.append(pair)
    return tuple(pairs)


def discover_fitted_slices(
    metric_slugs: Sequence[str], *, states: Sequence[str], data_dir: Path
) -> tuple[tuple[str, str], ...]:
    """Derive and validate the ruler grid from every present component master.

    All present state/metric masters must declare the same exact slice schema.
    A missing master remains a coverage/reconciliation concern, but a present
    master with a divergent schema is refused before fitting.
    """
    expected: Optional[tuple[tuple[str, str], ...]] = None
    mismatches: list[str] = []
    for state_name in states:
        for metric_slug in metric_slugs:
            frame = _load_component_master(
                metric_slug,
                level=LEVEL,
                state_name=state_name,
                data_dir=data_dir,
            )
            if frame is None or frame.empty:
                continue
            observed = _ordered_metric_slices(frame, metric_slug)
            if not observed:
                mismatches.append(f"{state_name}/{metric_slug}: no exact mean slices")
                continue
            if expected is None:
                expected = observed
            elif set(observed) != set(expected):
                mismatches.append(
                    f"{state_name}/{metric_slug}: {list(observed)!r} != {list(expected)!r}"
                )
    if expected is None:
        raise RuntimeError("No component master exposes an exact scenario/period mean slice.")
    if mismatches:
        raise RuntimeError(
            "Component-master slice contract is inconsistent: " + "; ".join(mismatches[:12])
        )
    return expected


def _align_long_frame_to_roster(
    long_frame: pd.DataFrame, roster: pd.DataFrame
) -> pd.DataFrame:
    """Replace source-specific district keys with canonical roster keys by name."""
    roster_keys = roster.loc[:, ["district_key", "state", "district"]].copy()
    roster_keys["_state_name_key"] = roster_keys["state"].map(alias)
    roster_keys["_district_name_key"] = roster_keys["district"].map(alias)
    if roster_keys.duplicated(["_state_name_key", "_district_name_key"]).any():
        raise RuntimeError("Canonical district roster contains ambiguous state/district names.")
    lookup = roster_keys.loc[
        :, ["_state_name_key", "_district_name_key", "district_key"]
    ].rename(columns={"district_key": "_canonical_district_key"})
    aligned = long_frame.copy()
    aligned["_state_name_key"] = aligned["state"].map(alias)
    aligned["_district_name_key"] = aligned["district"].map(alias)
    aligned = aligned.merge(lookup, on=["_state_name_key", "_district_name_key"], how="left")
    aligned["district_key"] = aligned["_canonical_district_key"].fillna(
        aligned["district_key"]
    )
    return aligned.drop(
        columns=["_state_name_key", "_district_name_key", "_canonical_district_key"]
    )


def _master_source_paths(
    metric_slugs: Sequence[str], *, states: Sequence[str], data_dir: Path
) -> list[Path]:
    """Component master files the fit actually reads, for the data snapshot hash."""
    paths: list[Path] = []
    for slug in metric_slugs:
        for state_name in states:
            source, _ = _resolve_state_paths(
                slug, level=LEVEL, state_name=state_name, data_dir=data_dir
            )
            preferred = resolve_preferred_master_path(source)
            if preferred.exists():
                paths.append(preferred)
    return paths


def colour_scale_payload() -> dict[str, object]:
    """The 101-stop ramp as data.

    Sourced from ``build_heat_risk_frozen_map.ramp_hex`` so the frozen map, the
    prototype and the vendor bundle cannot paint three different maps of one
    score.
    """
    from tools.diagnostics.build_heat_risk_frozen_map import (
        FROZEN_VMAX,
        FROZEN_VMIN,
        MISSING_COLOR,
        ramp_hex,
    )

    return {
        "colour_scale_id": COLOUR_SCALE_ID,
        "stops": list(ramp_hex(101)),
        "domain_min": float(FROZEN_VMIN),
        "domain_max": float(FROZEN_VMAX),
        "missing": MISSING_COLOR,
        # The score is already on one national ruler. Rescaling it to a subset's
        # own min/max is exactly the failure the frozen ruler removes.
        "rescale": "forbidden",
    }


def build_golden_canaries(
    ruler_set: FrozenRulerSet,
    *,
    data_dir: Path,
    states: Sequence[str],
    per_slice_level: int = 12,
) -> pd.DataFrame:
    """Build deterministic representative unit/slice score and colour canaries."""
    composite = get_composite_metric_for_bundle(ruler_set.bundle_domain)
    if composite is None:
        raise ValueError(f"No composite is configured for {ruler_set.bundle_domain!r}")
    metric_slugs = tuple(ruler_set.rulers)
    metric_specs = _bundle_metric_specs(composite, slugs=metric_slugs)
    rows: list[pd.DataFrame] = []
    for level in ("district", "block"):
        id_columns = list(_required_id_columns(level))
        for state_name in states:
            component_frames: dict[str, pd.DataFrame] = {}
            for metric_slug in metric_slugs:
                frame = _load_component_master(
                    metric_slug,
                    level=level,
                    state_name=state_name,
                    data_dir=data_dir,
                )
                if frame is not None and not frame.empty:
                    component_frames[metric_slug] = frame
            if set(component_frames) != set(metric_slugs):
                continue
            for scenario, period in ruler_set.slices:
                wide = _build_wide_component_frame(
                    component_frames,
                    level=level,
                    scenario=scenario,
                    period=period,
                )
                if wide is None or wide.empty:
                    continue
                scored = _compute_frozen_ruler_score_frame(
                    wide,
                    ruler_set=ruler_set,
                    metric_specs=metric_specs,
                    id_columns=id_columns,
                    coverage_gate=ruler_set.coverage_gate,
                )
                scored = scored.loc[scored["bundle_score"].notna(), id_columns + ["bundle_score"]]
                scored = scored.rename(columns={"bundle_score": "expected_score"})
                scored["level"] = level
                scored["scenario"] = scenario
                scored["period"] = period
                rows.append(scored)
    columns = (
        "level",
        "state",
        "district",
        "block",
        "district_key",
        "block_key",
        "scenario",
        "period",
        "expected_score",
        "expected_hex",
    )
    if not rows:
        return pd.DataFrame(columns=columns)
    candidates = pd.concat(rows, ignore_index=True)
    selected: list[pd.DataFrame] = []
    identity = [column for column in ("state", "district", "block") if column in candidates]
    for _, group in candidates.groupby(["level", "scenario", "period"], sort=True):
        ordered = group.sort_values(["expected_score", *identity], kind="mergesort").reset_index(drop=True)
        count = min(int(per_slice_level), len(ordered))
        positions = np.unique(np.linspace(0, len(ordered) - 1, count).round().astype(int))
        selected.append(ordered.iloc[positions])
    out = pd.concat(selected, ignore_index=True)
    ramp = colour_scale_payload()["stops"]
    indices = np.floor(out["expected_score"].to_numpy(dtype=float) + 0.5).astype(int)
    indices = np.clip(indices, 0, len(ramp) - 1)
    out["expected_hex"] = [ramp[index] for index in indices]
    out["expected_score"] = out["expected_score"].round(6)
    for column in columns:
        if column not in out.columns:
            out[column] = ""
    return out.loc[:, columns]


def fit_ruler_set(
    bundle_domain: str,
    *,
    version: str,
    data_dir: Path,
    states: Optional[Sequence[str]] = None,
    coverage_gate: Optional[float] = None,
    verbose: bool = True,
) -> tuple[FrozenRulerSet, pd.DataFrame, pd.DataFrame]:
    """Fit one frozen ruler set. Returns ``(ruler_set, fit_report, reconciliation)``."""
    composite = get_composite_metric_for_bundle(bundle_domain)
    if composite is None:
        raise ValueError(f"No composite metric is configured for bundle {bundle_domain!r}")

    headline = get_bundle_headline_weights(bundle_domain)
    if not headline:
        raise ValueError(f"Bundle {bundle_domain!r} declares no headline (absolute) metrics")
    metric_slugs = [entry.metric_slug for entry in headline]
    weights = {entry.metric_slug: float(entry.weight) for entry in headline}
    configured_weight = get_bundle_headline_weight_total(bundle_domain)

    if verbose:
        print(
            f"Fitting {len(metric_slugs)} headline metrics for {bundle_domain!r} "
            f"(configured weight {configured_weight:.6f} of 1.0)",
            file=sys.stderr,
        )

    state_names = list(states) if states else discover_states(metric_slugs, data_dir=data_dir)
    slices = discover_fitted_slices(metric_slugs, states=state_names, data_dir=data_dir)
    gate = (
        coverage_gate_for_bundle(bundle_domain)
        if coverage_gate is None
        else float(coverage_gate)
    )
    if not 0.0 < gate <= 1.0:
        raise ValueError(f"coverage_gate must be in (0, 1], got {gate!r}")
    roster = load_district_roster(data_dir, verbose=verbose)
    long_frame = load_national_long_frame(
        metric_slugs,
        level=LEVEL,
        states=state_names,
        data_dir=data_dir,
        slices=slices,
        verbose=verbose,
    )
    if long_frame.empty:
        raise RuntimeError("No component master rows were assembled; check processed data.")
    long_frame = _align_long_frame_to_roster(long_frame, roster)
    expanded, reconciliation = expand_to_roster(
        long_frame, roster, metric_slugs, slices=slices
    )

    numeric = expanded.loc[:, metric_slugs].apply(pd.to_numeric, errors="coerce")
    finite = pd.DataFrame(
        np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan)),
        index=expanded.index,
        columns=metric_slugs,
    )
    available_weight = finite.mul(pd.Series(weights), axis=1).sum(axis=1)
    coverage = available_weight / configured_weight
    n_below_gate = int((coverage < gate).sum())

    rulers = {}
    report_rows: list[dict[str, object]] = []
    for slug in metric_slugs:
        pool = pd.to_numeric(expanded[slug], errors="coerce").to_numpy(dtype=float)
        higher_is_worse = bool(METRICS_BY_SLUG[slug].rank_higher_is_worse)
        ruler = build_cdf_ruler(slug, pool, higher_is_worse=higher_is_worse)
        if ruler is not None:
            rulers[slug] = ruler
        report_rows.append(
            {
                "metric_slug": slug,
                "weight": weights[slug],
                "weight_fraction": weights[slug] / configured_weight,
                "higher_is_worse": higher_is_worse,
                "n_finite": int(np.isfinite(pool).sum()),
                "ruler_fitted": ruler is not None,
                "n_knots": int(ruler.knot_values.size) if ruler else 0,
                "pooled_min": float(ruler.pooled_min) if ruler else float("nan"),
                "pooled_max": float(ruler.pooled_max) if ruler else float("nan"),
                "modal_mass_fraction": float(ruler.modal_mass) if ruler else float("nan"),
            }
        )
    fit_report = pd.DataFrame(report_rows)

    if len(rulers) != len(metric_slugs):
        missing = sorted(set(metric_slugs) - set(rulers))
        raise RuntimeError(
            "Refusing to freeze a partial ruler set; no finite value anywhere in the pool "
            f"for: {', '.join(missing)}. A metric that cannot be scored still carries weight "
            "in the coverage denominator, so every published score would be wrong."
        )

    sources = _master_source_paths(metric_slugs, states=state_names, data_dir=data_dir)
    ruler_set = FrozenRulerSet(
        ruler_id=f"{composite.composite_slug}_{CDF_KIND}_{version}",
        composite_slug=composite.composite_slug,
        bundle_domain=bundle_domain,
        level_fitted=LEVEL,
        rulers=rulers,
        slices=slices,
        weights=weights,
        configured_weight=configured_weight,
        coverage_gate=gate,
        data_snapshot_hash=snapshot_hash(sources, root=Path(data_dir)),
        fitted_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        meta={
            "n_districts": int(roster["district_key"].nunique()),
            "n_states": len(state_names),
            "n_source_masters": len(sources),
            "colour_scale_id": COLOUR_SCALE_ID,
            "n_fit_rows": int(len(expanded)),
            "n_below_coverage_gate": n_below_gate,
            "coverage_failure_rate": (
                float(n_below_gate / len(expanded)) if len(expanded) else 0.0
            ),
        },
    )
    return ruler_set, fit_report, reconciliation


def _assert_roster_clean(reconciliation: pd.DataFrame, *, strict: bool) -> list[str]:
    """Report roster gaps. A ruler fitted over a broken roster is quietly wrong."""
    counts = reconciliation["status"].value_counts().to_dict()
    problems = [
        f"{status}: {count}"
        for status, count in sorted(counts.items())
        if status != "roster_and_master"
    ]
    if problems and strict:
        raise RuntimeError(
            "Roster reconciliation is not clean: "
            + "; ".join(problems)
            + ". Fit with --allow-roster-gaps only if the gaps are understood."
        )
    return problems


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bundle", default="Heat Risk", help="Bundle domain to fit.")
    parser.add_argument(
        "--out-version", default="v1", help="Version directory suffix, e.g. 'v1'."
    )
    parser.add_argument("--state", action="append", dest="states", default=None)
    parser.add_argument(
        "--coverage-gate",
        type=float,
        default=None,
        help=(
            "Override the bundle-specific evidence-backed coverage gate. "
            "Omit for the configured bundle gate."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fit and report, write nothing.",
    )
    parser.add_argument(
        "--allow-roster-gaps",
        action="store_true",
        help="Fit even when districts are missing from the masters (default: refuse).",
    )
    parser.add_argument(
        "--colour-scale-out",
        type=Path,
        default=None,
        help=(
            "Where to write colour_scale.json "
            "(default: <IRT_DATA_DIR>/processed_optimised/colour_scale.json)."
        ),
    )
    args = parser.parse_args(argv)

    data_dir = Path(get_paths_config().data_dir)
    ruler_set, fit_report, reconciliation = fit_ruler_set(
        args.bundle,
        version=args.out_version,
        data_dir=data_dir,
        states=args.states,
        coverage_gate=args.coverage_gate,
    )

    problems = _assert_roster_clean(reconciliation, strict=not args.allow_roster_gaps)

    out_dir = frozen_ruler_dir(
        ruler_set.composite_slug, f"{CDF_KIND}_{args.out_version}"
    )
    colour_out = args.colour_scale_out or (
        data_dir / "processed_optimised" / COLOUR_SCALE_FILENAME
    )

    print(f"\nRuler id        : {ruler_set.ruler_id}")
    print(f"Bundle          : {ruler_set.bundle_domain}")
    print(f"Metrics fitted  : {len(ruler_set.rulers)} (headline half)")
    print(f"Configured wt   : {ruler_set.configured_weight:.6f} -> renormalized to 1.0")
    print(f"Coverage gate   : {ruler_set.coverage_gate}")
    print(
        "Coverage failures: "
        f"{ruler_set.meta.get('n_below_coverage_gate')}/"
        f"{ruler_set.meta.get('n_fit_rows')} "
        f"({100.0 * float(ruler_set.meta.get('coverage_failure_rate', 0.0)):.2f}%)"
    )
    print(f"Slices          : {len(ruler_set.slices)}")
    print(f"Districts       : {ruler_set.meta.get('n_districts')}")
    print(f"Snapshot hash   : {ruler_set.data_snapshot_hash[:16]}...")
    print(f"Roster status   : {'clean' if not problems else '; '.join(problems)}")
    print("\n" + fit_report.to_string(index=False))

    if args.dry_run:
        print(f"\n[dry-run] would write {out_dir}")
        print(f"[dry-run] would write {colour_out}")
        return 0

    canaries = build_golden_canaries(
        ruler_set,
        data_dir=data_dir,
        states=list(args.states) if args.states else discover_states(
            tuple(ruler_set.rulers), data_dir=data_dir
        ),
    )
    if canaries.empty:
        raise RuntimeError("No finite district or block rows were available for golden canaries.")
    written = save_ruler_set(ruler_set, out_dir)
    canary_path = out_dir / GOLDEN_CANARIES_FILENAME
    canaries.to_csv(canary_path, index=False)
    colour_out.parent.mkdir(parents=True, exist_ok=True)
    colour_out.write_text(
        json.dumps(colour_scale_payload(), indent=2) + "\n", encoding="utf-8"
    )
    for label, path in written.items():
        print(f"  wrote {label:8s} {path}")
    print(f"  wrote canaries {canary_path}")
    print(f"  wrote colour   {colour_out}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
