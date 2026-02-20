# AGENTS.md — Visualization Layer (plots, exports, styling)

Applies to: `india_resilience_tool/viz/`

Goal: deterministic figures and safe export behavior.

## Rules
- Keep plotting deterministic and reproducible.
- Avoid global style changes unless explicitly requested.
- If changing autoscaling/limits/normalization: validate across multiple metrics and edge cases.
- Prefer testing data transforms feeding plots rather than pixel-perfect image tests.

## Exports (PDF / images)
Any export change must include a manual check:
1) generate one sample export
2) verify the file opens and pages render
3) verify key labels/legends are present

## Performance
Coordinate with app caching; avoid redoing expensive prep on every rerun.
