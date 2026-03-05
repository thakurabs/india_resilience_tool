# AGENTS.md — Config Layer (registries, constants, declarative settings)

Applies to: `india_resilience_tool/config/`

Goal: keep configuration **declarative**, stable, and safe to import everywhere.

## Rules
- Keep config modules declarative: constants/registries/dataclasses only.
- Avoid side effects at import time (no filesystem reads, network, heavy computation).
- Avoid depending on `tools/` or app runtime; config must remain lightweight.

## Stable identifiers
- Metric slugs are effectively public API (used in processed paths, exports, and saved artifacts).
- If a slug/label changes, keep backward compatibility where feasible and document the migration.

## Adding / changing metrics
- Every metric must define units and parameters clearly (especially baselines/thresholds).
- Any new metric or bundle change should update registry validation tests if present.

## Validation
- Fast command: `python -m pytest -q`

