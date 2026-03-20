# AGENTS.md — Compute Layer (pipeline adapters / derived indices)

Applies to: `india_resilience_tool/compute/`

Goal: protect scientific correctness and keep compute functions callable from the pipeline/registry.

## Non-negotiables
- **No Streamlit imports** in this directory.
- No silent methodology changes: if baselines, thresholds, calendar handling, or aggregation changes, call it out explicitly and add/adjust tests.

## Registry / pipeline compatibility
- Pipeline compute functions are often invoked by passing `metric["params"]` as kwargs.
- Compute entrypoints used by the pipeline MUST be **kwarg-compatible**:
  - accept declared params even if unused, or
  - map aliases to the canonical parameter, or
  - ignore unknown params safely via `**kwargs` (preferred when methodologically safe).
- Document what is accepted/ignored in the entrypoint docstring.

## Missing data behavior
- Be explicit about NaN handling and missing periods (skip vs coerce vs partial+warn).
- Avoid surprising type changes (e.g., returning `None` vs `DataFrame`) without tests.

## Validation
- Add/adjust tests for any parameter-mapping or calendar/aggregation change.
- Fast command: `python -m pytest -q`

