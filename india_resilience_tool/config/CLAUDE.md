# CLAUDE.md — Config Layer (registries, constants, declarative settings)

Applies to: `india_resilience_tool/config/`

Modules: `metrics_registry.py`, `composite_metrics.py`, `proposal_bundles.py`, `bundle_weights.py`, `constants.py`, `variables.py`, `paths.py`

Goal: keep configuration declarative, stable, and safe to import everywhere.

---

## Declarative-only rule

Config modules must remain declarative: constants, registries, and dataclasses only.

- **No side effects at import time** — no filesystem reads, no network calls, no heavy computation.
- **No dependency on `tools/`** or app runtime — config must remain lightweight and importable from any layer.

---

## Stable identifiers

Metric slugs are **effectively public API**. They appear in:
- Processed output paths (`processed/{metric_slug}/...`)
- Saved artifacts and exports
- Dashboard selectors and persisted composites

If a slug or label must change:
- Keep backward compatibility where feasible
- Document the migration explicitly
- Check that all downstream paths and masters are rebuilt

---

## Adding or changing metrics

Every new metric must define:
- Units (explicit)
- Parameters, especially baselines and thresholds
- Which pillar(s) and domain(s) it belongs to in `metrics_registry.py`

`metrics_registry.py` is the **canonical source of truth** for metric definitions. All other config files derive from it.

Any new metric or bundle change must also update registry validation tests if present.

---

## Bundle and composite changes

- `composite_metrics.py` declares the persisted visible-Glance bundle → composite metric mapping.
- `bundle_weights.py` declares declarative landing bundle weights for all visible Glance bundles.
- Changes to either require rebuilding the composite masters:

```bash
python -m tools.pipeline.build_composite_metrics --help
```

---

## Validation

Run: `python -m pytest -q`

Pay particular attention to `tests/test_metrics_registry.py` and `tests/test_config.py` after any config change.
