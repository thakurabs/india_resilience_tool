"""Build a self-contained weight-sensitivity proof-of-concept dashboard.

The dashboard answers one question for one thematic bundle at one
``(scenario, period)`` slice: *what does each component metric contribute to the
published composite, and what happens to the composite when its weight moves?*

Left panel is the composite, recomputed live in the browser. Right panel is one
selected component metric, painted either as the 0-100 frozen-ruler score that
the weight actually multiplies, or as its physical values.

Everything the page needs is precomputed here and embedded:

* per-district frozen-ruler scores (0-100) for every headline component,
* per-district physical values for the same components,
* the published composite, read back from the optimised bundle as a check that
  the recomputation reproduces it,
* simplified district geometry.

The page performs only a weighted average, so it needs no server, no network and
no plotting library.

Scoring follows ``india_resilience_tool.compute.composite_metrics``: each metric
is mapped through its frozen national CDF ruler, then combined as
``sum(w_i * s_i) / sum(w_i)``. The denominator tracks the live weight sum, so a
weight edit moves the headline total away from the configured value; the page
reports that drift rather than hiding it, because the coverage gate is expressed
against the configured total.

Usage::

    python -m tools.diagnostics.build_weight_sensitivity_poc --dry-run
    python -m tools.diagnostics.build_weight_sensitivity_poc
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.frozen_rulers import (
    FrozenRulerSet,
    frozen_ruler_dir,
    load_ruler_set,
)
from india_resilience_tool.config.bundle_weights import (
    get_bundle_weights,
    get_bundle_headline_weight_total,
)
from india_resilience_tool.config.composite_metrics import (
    COMPOSITES_BY_SLUG,
    VISIBLE_GLANCE_COMPOSITES,
    CompositeMetricSpec,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.master_columns import resolve_metric_column

SUPPORTED_STAT = "mean"
DEFAULT_BUNDLE = "Heat Risk"
DEFAULT_SCENARIO = "ssp245"
DEFAULT_PERIOD = "2040-2060"
DEFAULT_LEVEL = "district"
DEFAULT_OUTPUT = Path("docs/diagnostics/heat_risk_pilot/weight_sensitivity_poc.html")

#: Degrees. ~1.1 km at the equator; a national choropleth of 784 districts does
#: not resolve finer than this, and the raw geometry is 11 MB.
DEFAULT_SIMPLIFY_TOLERANCE = 0.01
#: Decimal places kept on simplified coordinates. 3 dp is ~110 m.
COORD_DECIMALS = 3

#: Sequential ramp for the physical-value view. Deliberately NOT the published
#: composite colourbar: ``colour_scale.json`` declares ``rescale: forbidden``,
#: so reusing those stops over a physical domain would misrepresent the ruler.
PHYSICAL_RAMP = (
    "#f7f4ea", "#eae3cf", "#ddd1b4", "#d0bf9a", "#c3ad81",
    "#b69b69", "#a88952", "#99773d", "#8a642a", "#7a511a",
    "#693e0d",
)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


def _optimised_root(data_dir: Path) -> Path:
    """Root of the optimised runtime bundle this build reads from."""
    return Path(data_dir) / "processed_optimised"


def _spec_for_bundle(bundle_domain: str) -> CompositeMetricSpec:
    """Resolve the composite spec for one thematic bundle domain."""
    for spec in VISIBLE_GLANCE_COMPOSITES:
        if spec.bundle_domain == bundle_domain:
            return spec
    known = sorted({s.bundle_domain for s in VISIBLE_GLANCE_COMPOSITES})
    raise SystemExit(f"Unknown bundle {bundle_domain!r}. Known bundles: {known}")


def _master_dir(optimised_root: Path, metric_slug: str, *, level: str) -> Path:
    return optimised_root / "metrics" / metric_slug / "masters" / "admin" / level


def _read_metric_master(
    optimised_root: Path,
    metric_slug: str,
    *,
    level: str,
) -> Optional[pd.DataFrame]:
    """Concatenate every state partition of one metric's optimised master."""
    master_dir = _master_dir(optimised_root, metric_slug, level=level)
    if not master_dir.is_dir():
        return None
    frames: list[pd.DataFrame] = []
    for path in sorted(master_dir.glob("state=*.parquet")):
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:  # pragma: no cover - corrupt partition
            raise SystemExit(f"Failed to read {path}: {exc}") from exc
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _resolve_value_column(
    frame: pd.DataFrame,
    *,
    metric_slug: str,
    scenario: str,
    period: str,
) -> Optional[str]:
    """Resolve one metric's value column for a slice, with legacy fallbacks.

    Mirrors ``compute.composite_metrics._resolve_component_metric_column`` so the
    proof-of-concept reads exactly the column the pipeline scored.
    """
    registry_spec = METRICS_BY_SLUG[metric_slug]
    candidates: list[str] = []
    for candidate in (registry_spec.periods_metric_col, registry_spec.value_col, metric_slug):
        value = str(candidate or "").strip()
        if value and value not in candidates:
            candidates.append(value)
    for candidate in candidates:
        resolved = resolve_metric_column(frame, candidate, scenario, period, SUPPORTED_STAT)
        if resolved:
            return resolved
    return None


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def build_component_frame(
    optimised_root: Path,
    *,
    ruler_set: FrozenRulerSet,
    scenario: str,
    period: str,
    level: str,
    key_column: str,
) -> tuple[pd.DataFrame, list[str]]:
    """Return one wide frame of physical values and ruler scores per unit.

    Columns are ``<slug>__value`` (physical) and ``<slug>__score`` (0-100), plus
    the identifier columns. Returns the metric slugs that were actually loaded.
    """
    ruler_set.validate_slice(scenario, period)
    merged: Optional[pd.DataFrame] = None
    loaded: list[str] = []
    id_columns = ["state", level, key_column] if level != "state" else ["state"]

    for metric_slug in sorted(ruler_set.rulers):
        frame = _read_metric_master(optimised_root, metric_slug, level=level)
        if frame is None:
            print(f"   [skip] {metric_slug}: no optimised master under {level}")
            continue
        column = _resolve_value_column(
            frame, metric_slug=metric_slug, scenario=scenario, period=period
        )
        if column is None:
            print(f"   [skip] {metric_slug}: no column for {scenario}/{period}")
            continue
        missing_ids = [c for c in id_columns if c not in frame.columns]
        if missing_ids:
            print(f"   [skip] {metric_slug}: master is missing {missing_ids}")
            continue

        part = frame.loc[:, id_columns].copy()
        part[f"{metric_slug}__value"] = pd.to_numeric(frame[column], errors="coerce")
        merged = part if merged is None else merged.merge(part, on=id_columns, how="outer")
        loaded.append(metric_slug)

    if merged is None:
        raise SystemExit("No component metric could be loaded; nothing to build.")

    for metric_slug in loaded:
        ruler = ruler_set.rulers[metric_slug]
        merged[f"{metric_slug}__score"] = ruler.apply(merged[f"{metric_slug}__value"])

    merged = merged.drop_duplicates(subset=[key_column]).reset_index(drop=True)
    return merged, loaded


def recompute_composite(
    frame: pd.DataFrame,
    *,
    weights: dict[str, float],
    slugs: Sequence[str],
) -> pd.Series:
    """Weighted mean of ruler scores, denominator tracking the live weight sum.

    Weights of metrics that are NaN for a row drop out of both numerator and
    denominator, exactly as ``weighted_row_score`` does.
    """
    score_frame = frame.loc[:, [f"{s}__score" for s in slugs]]
    weight_vector = pd.Series(
        [float(weights.get(s, 0.0)) for s in slugs],
        index=score_frame.columns,
        dtype=float,
    )
    available = score_frame.notna().mul(weight_vector, axis=1).sum(axis=1)
    weighted = score_frame.mul(weight_vector, axis=1).sum(axis=1, skipna=True)
    return weighted.div(available.where(available > 0.0))


def read_published_composite(
    optimised_root: Path,
    *,
    composite_slug: str,
    scenario: str,
    period: str,
    level: str,
    key_column: str,
) -> Optional[pd.DataFrame]:
    """Read the published composite for one slice, for parity checking."""
    frame = _read_metric_master(optimised_root, composite_slug, level=level)
    if frame is None:
        return None
    column = f"{composite_slug}__{scenario}__{period}__mean"
    if column not in frame.columns:
        column = _resolve_value_column(
            frame, metric_slug=composite_slug, scenario=scenario, period=period
        ) if composite_slug in METRICS_BY_SLUG else None
    if not column or column not in frame.columns:
        return None
    out = frame.loc[:, [key_column]].copy()
    out["published"] = pd.to_numeric(frame[column], errors="coerce")
    return out.drop_duplicates(subset=[key_column]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------


def _round_coords(geom: Any, decimals: int) -> Any:
    """Recursively round a GeoJSON coordinate structure."""
    if isinstance(geom, (int, float)):
        return round(float(geom), decimals)
    return [_round_coords(part, decimals) for part in geom]


def load_simplified_geometry(
    optimised_root: Path,
    *,
    level: str,
    key_column: str,
    tolerance: float,
    keys: Iterable[str],
) -> dict[str, Any]:
    """Load district geometry, simplify it, and key it by admin key.

    Simplification is topology-preserving per feature. Shared borders can pick up
    hairline gaps at this tolerance; that is acceptable for a diagnostic
    choropleth and is why this artifact never feeds a published map.
    """
    from shapely.geometry import mapping, shape

    wanted = set(keys)
    geom_dir = optimised_root / "geometry" / "admin" / level
    if not geom_dir.is_dir():
        raise SystemExit(f"No geometry directory at {geom_dir}")

    shapes: dict[str, Any] = {}
    for path in sorted(geom_dir.glob("state=*.geojson")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for feature in payload.get("features", []):
            props = feature.get("properties") or {}
            key = str(props.get(key_column) or "").strip()
            if not key or key not in wanted or key in shapes:
                continue
            geometry = feature.get("geometry")
            if not geometry:
                continue
            simplified = shape(geometry).simplify(tolerance, preserve_topology=True)
            if simplified.is_empty:
                simplified = shape(geometry)
            shapes[key] = _round_coords(
                mapping(simplified)["coordinates"], COORD_DECIMALS
            )
            shapes[key] = {"t": mapping(simplified)["type"], "c": shapes[key]}
    return shapes


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


def build_payload(
    *,
    bundle_domain: str,
    spec: CompositeMetricSpec,
    ruler_set: FrozenRulerSet,
    frame: pd.DataFrame,
    slugs: Sequence[str],
    published: Optional[pd.DataFrame],
    geometry: dict[str, Any],
    colour_stops: list[str],
    missing_colour: str,
    scenario: str,
    period: str,
    level: str,
    key_column: str,
) -> dict[str, Any]:
    """Assemble everything the page needs into one JSON-serialisable dict."""
    approved = {s: float(ruler_set.weights.get(s, 0.0)) for s in slugs}
    recomputed = recompute_composite(frame, weights=approved, slugs=slugs)

    parity: dict[str, Any] = {"checked": False}
    if published is not None:
        merged = frame.loc[:, [key_column]].copy()
        merged["recomputed"] = recomputed.to_numpy()
        merged = merged.merge(published, on=key_column, how="inner")
        both = merged.dropna(subset=["recomputed", "published"])
        if not both.empty:
            diff = (both["recomputed"] - both["published"]).abs()
            parity = {
                "checked": True,
                "n": int(len(both)),
                "max_abs_diff": float(diff.max()),
                "mean_abs_diff": float(diff.mean()),
            }

    units_by_slug = {
        s: str(METRICS_BY_SLUG[s].units or "").strip() for s in slugs
    }
    metrics = [
        {
            "slug": s,
            "label": METRICS_BY_SLUG[s].label,
            "units": units_by_slug[s],
            "weight": approved[s],
            "group": _workbook_group(bundle_domain, s),
            "locked": False,
        }
        for s in slugs
    ]
    held_out = [
        {
            "slug": e.metric_slug,
            "label": METRICS_BY_SLUG[e.metric_slug].label
            if e.metric_slug in METRICS_BY_SLUG
            else e.metric_slug,
            "units": "",
            "weight": float(e.weight),
            "group": e.workbook_group or "",
            "locked": True,
        }
        for e in get_bundle_weights(bundle_domain)
        if not e.is_attribute and e.is_baseline_referenced
    ]

    units: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        key = str(row[key_column])
        if key not in geometry:
            continue
        units.append(
            {
                "k": key,
                "n": str(row.get(level, "")),
                "s": str(row.get("state", "")),
                "v": [_round_or_none(row.get(f"{s}__value"), 4) for s in slugs],
                "r": [_round_or_none(row.get(f"{s}__score"), 2) for s in slugs],
            }
        )

    return {
        "bundle": bundle_domain,
        "composite_slug": spec.composite_slug,
        "composite_label": spec.composite_label,
        "ruler_id": ruler_set.ruler_id,
        "ruler_version": spec.frozen_ruler_version,
        "scenario": scenario,
        "period": period,
        "level": level,
        "configured_weight": float(get_bundle_headline_weight_total(bundle_domain)),
        "coverage_gate": float(ruler_set.coverage_gate),
        "slugs": list(slugs),
        "metrics": metrics,
        "held_out": held_out,
        "units": units,
        "geometry": geometry,
        "colour_stops": colour_stops,
        "missing_colour": missing_colour,
        "physical_ramp": list(PHYSICAL_RAMP),
        "parity": parity,
    }


def _workbook_group(bundle_domain: str, metric_slug: str) -> str:
    for entry in get_bundle_weights(bundle_domain):
        if entry.metric_slug == metric_slug:
            return entry.workbook_group or ""
    return ""


def _round_or_none(value: Any, decimals: int) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return round(numeric, decimals)


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


TEMPLATE_PATH = Path(__file__).resolve().parent / "templates" / "weight_sensitivity_poc.template.html"


def render_html(payload: dict[str, Any]) -> str:
    """Inject the payload into the standalone template.

    ``</`` is escaped so a string in the data can never close the script block.
    """
    if not TEMPLATE_PATH.exists():
        raise SystemExit(f"Template not found: {TEMPLATE_PATH}")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    if "__PAYLOAD__" not in template:
        raise SystemExit(f"Template {TEMPLATE_PATH} has no __PAYLOAD__ placeholder")
    blob = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    blob = blob.replace("</", "<\\/")
    return template.replace("__PAYLOAD__", blob)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the composite weight-sensitivity proof-of-concept dashboard."
    )
    parser.add_argument("--bundle", default=DEFAULT_BUNDLE, help="Thematic bundle domain.")
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument("--period", default=DEFAULT_PERIOD)
    parser.add_argument("--level", default=DEFAULT_LEVEL, choices=("district", "block"))
    parser.add_argument("--data-dir", default=None, help="Override IRT_DATA_DIR.")
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT), type=str)
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=DEFAULT_SIMPLIFY_TOLERANCE,
        help="Geometry simplification tolerance in degrees.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load, score and report; write nothing.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else Path(get_paths_config().data_dir)
    optimised_root = _optimised_root(data_dir)
    if not optimised_root.is_dir():
        raise SystemExit(f"No optimised bundle at {optimised_root}")

    spec = _spec_for_bundle(args.bundle)
    if spec.normalization != "frozen_national_cdf":
        raise SystemExit(
            f"Bundle {args.bundle!r} uses {spec.normalization!r} normalization. "
            "This tool only explains frozen-national-ruler composites."
        )
    key_column = f"{args.level}_key"

    print(f"[1/5] bundle {args.bundle} -> {spec.composite_slug} ({spec.frozen_ruler_version})")
    ruler_set = load_ruler_set(frozen_ruler_dir(spec.composite_slug, spec.frozen_ruler_version))
    print(f"      {len(ruler_set.rulers)} headline rulers, "
          f"configured weight {ruler_set.configured_weight:.4f}, "
          f"coverage gate {ruler_set.coverage_gate}")

    print(f"[2/5] reading component masters for {args.scenario}/{args.period}")
    frame, slugs = build_component_frame(
        optimised_root,
        ruler_set=ruler_set,
        scenario=args.scenario,
        period=args.period,
        level=args.level,
        key_column=key_column,
    )
    print(f"      {len(frame)} {args.level}s x {len(slugs)} metrics")

    print("[3/5] parity check against the published composite")
    published = read_published_composite(
        optimised_root,
        composite_slug=spec.composite_slug,
        scenario=args.scenario,
        period=args.period,
        level=args.level,
        key_column=key_column,
    )

    print(f"[4/5] geometry (simplify tolerance {args.simplify_tolerance} deg)")
    geometry = load_simplified_geometry(
        optimised_root,
        level=args.level,
        key_column=key_column,
        tolerance=float(args.simplify_tolerance),
        keys=frame[key_column].astype(str).tolist(),
    )
    missing_geom = int(len(frame) - len(geometry))
    if missing_geom:
        print(f"      [warn] {missing_geom} scored {args.level}s have no geometry and are dropped")

    colour_path = optimised_root / "colour_scale.json"
    if not colour_path.exists():
        raise SystemExit(f"No colour scale at {colour_path}")
    colour = json.loads(colour_path.read_text(encoding="utf-8"))

    payload = build_payload(
        bundle_domain=args.bundle,
        spec=spec,
        ruler_set=ruler_set,
        frame=frame,
        slugs=slugs,
        published=published,
        geometry=geometry,
        colour_stops=list(colour["stops"]),
        missing_colour=str(colour.get("missing", "#d5d8dc")),
        scenario=args.scenario,
        period=args.period,
        level=args.level,
        key_column=key_column,
    )

    parity = payload["parity"]
    if parity["checked"]:
        print(f"      recomputed vs published: max |diff| {parity['max_abs_diff']:.6f} pts "
              f"over n={parity['n']}")
        if parity["max_abs_diff"] > 0.5:
            print("      [warn] recomputation does not reproduce the published composite; "
                  "the page will say so")
    else:
        print("      [warn] published composite unavailable; no parity check")

    html = render_html(payload)
    out_path = Path(args.out)
    print(f"[5/5] {len(html) / 1e6:.2f} MB -> {out_path}")
    if args.dry_run:
        print("      --dry-run: nothing written")
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print("      written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
