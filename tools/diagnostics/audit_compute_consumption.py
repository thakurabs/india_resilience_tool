#!/usr/bin/env python3
"""Compute-set consumption audit (Front 0): what consumes each computed slug?

Read-only registry introspection -- no data, no IO. For every slug the climate
pipeline computes (``PIPELINE_SLUGS``), report which *scored* bundles consume it
(thematic composites via ``LANDING_BUNDLE_WEIGHTS`` + sectoral proposal bundles
via ``PROPOSAL_BUNDLES``) and which *active* dashboard domains list it
(``DOMAINS`` gated by ``PILLAR_DOMAINS``). The point is to find compute we can
DROP before we spend effort optimizing it.

Classification per computed slug:
    scored          -- consumed by >=1 thematic and/or sectoral scored bundle.
                       Optimize (don't drop): it feeds a dashboard score.
    browsable_only  -- in an active domain but NO scored bundle. Served only as a
                       standalone browsable metric (e.g. dtr/etr). DROP CANDIDATE:
                       a product call -- keep the domain or delete the compute.
    orphan          -- in NO scored bundle AND NO active domain. Computed but
                       nothing serves it. STRONGEST DROP CANDIDATE (free win).

Example
-------
    python -m tools.diagnostics.audit_compute_consumption
    python -m tools.diagnostics.audit_compute_consumption --csv D:\\tmp\\audit.csv
    python -m tools.diagnostics.audit_compute_consumption --only-drop-candidates
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from india_resilience_tool.config import bundle_weights as bw
from india_resilience_tool.config import metrics_registry as mr
from india_resilience_tool.config import proposal_bundles as pb


def _thematic_consumers() -> dict[str, list[str]]:
    """slug -> [thematic bundle domains] from LANDING_BUNDLE_WEIGHTS."""
    out: dict[str, list[str]] = {}
    for domain in bw.LANDING_BUNDLE_WEIGHTS:
        for slug in bw.get_bundle_attribute_slugs(domain):
            out.setdefault(str(slug), []).append(str(domain))
    return out


def _sectoral_consumers() -> dict[str, list[str]]:
    """slug -> [sectoral proposal bundles] from PROPOSAL_BUNDLES rules."""
    out: dict[str, list[str]] = {}
    for spec in pb.get_proposal_bundle_specs():
        bundle = str(getattr(spec, "bundle_domain", getattr(spec, "composite_slug", "?")))
        for rule in spec.rules:
            slug = str(rule.metric_slug).strip()
            if slug:
                out.setdefault(slug, []).append(bundle)
    return out


def _active_domain_members() -> dict[str, list[str]]:
    """slug -> [active dashboard domains].

    A domain is *active* iff it appears under some pillar in PILLAR_DOMAINS
    (empty pillars like Vulnerability / Adaptive Capacity contribute nothing).
    """
    active_domains = {d for domains in mr.PILLAR_DOMAINS.values() for d in domains}
    out: dict[str, list[str]] = {}
    for domain, slugs in mr.DOMAINS.items():
        if domain not in active_domains:
            continue
        for slug in slugs:
            out.setdefault(str(slug), []).append(str(domain))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", type=Path, default=None, help="Write the full per-slug table as CSV.")
    ap.add_argument("--only-drop-candidates", action="store_true",
                    help="Print only browsable_only + orphan slugs.")
    args = ap.parse_args()

    thematic = _thematic_consumers()
    sectoral = _sectoral_consumers()
    domains = _active_domain_members()

    rows: list[dict[str, object]] = []
    for slug in sorted(mr.PIPELINE_SLUGS):
        th = thematic.get(slug, [])
        se = sectoral.get(slug, [])
        dm = domains.get(slug, [])
        scored = bool(th or se)
        if scored:
            cls = "scored"
        elif dm:
            cls = "browsable_only"
        else:
            cls = "orphan"
        rows.append({
            "slug": slug,
            "classification": cls,
            "thematic_bundles": ";".join(th),
            "sectoral_bundles": ";".join(se),
            "active_domains": ";".join(dm),
        })

    by_cls: dict[str, int] = {}
    for r in rows:
        by_cls[str(r["classification"])] = by_cls.get(str(r["classification"]), 0) + 1

    print("==== compute-set consumption audit ====")
    print(f"  computed slugs (PIPELINE_SLUGS): {len(rows)}")
    for cls in ("scored", "browsable_only", "orphan"):
        print(f"    {cls:16s}: {by_cls.get(cls, 0)}")

    print("\n==== DROP CANDIDATES (browsable_only + orphan) ====")
    drop = [r for r in rows if r["classification"] in {"browsable_only", "orphan"}]
    if not drop:
        print("  (none -- every computed slug feeds a scored bundle)")
    for r in sorted(drop, key=lambda r: (r["classification"], r["slug"])):
        dm = r["active_domains"] or "<no active domain>"
        print(f"  [{r['classification']:14s}] {r['slug']:34s} domain: {dm}")

    if not args.only_drop_candidates:
        print("\n==== SCORED (keep; optimize only) ====")
        for r in sorted([r for r in rows if r["classification"] == "scored"], key=lambda r: r["slug"]):
            tags = []
            if r["thematic_bundles"]:
                tags.append(f"thematic={r['thematic_bundles']}")
            if r["sectoral_bundles"]:
                tags.append(f"sectoral={r['sectoral_bundles']}")
            print(f"  {r['slug']:34s} {' | '.join(tags)}")

    if args.csv is not None:
        with args.csv.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["slug", "classification", "thematic_bundles",
                                               "sectoral_bundles", "active_domains"])
            w.writeheader()
            w.writerows(rows)
        print(f"\n  wrote {args.csv}")


if __name__ == "__main__":
    main()
