# CLAUDE.md — Compute Layer (pipeline adapters / derived indices)

Applies to: `india_resilience_tool/compute/`

Modules: `composite_metrics.py`, `proposal_bundles.py`, `master_builder.py`, `spi_adapter.py`

Goal: protect scientific correctness and keep compute functions callable from the pipeline and registry.

---

## Non-negotiables

- **No Streamlit imports** in this directory — compute is Streamlit-free.
- **No silent methodology changes**: if baselines, thresholds, calendar handling, or aggregation changes, call it out explicitly and add or adjust tests.

---

## Registry and pipeline compatibility

Pipeline compute functions are invoked by passing `metric["params"]` as kwargs.

Compute entrypoints used by the pipeline **must be kwarg-compatible**:
- Accept declared params even if unused, OR
- Map aliases to the canonical parameter, OR
- Ignore unknown params safely via `**kwargs` (preferred when methodologically safe)

Document what is accepted and what is ignored in the entrypoint docstring.

---

## SPI adapter specifics (`spi_adapter.py`)

- Wraps the `climate-indices` package; do not replace with custom implementations without explicit discussion.
- Method of Moments is the validated parameter estimation approach for 30-year baselines — do not switch to MLE without explicit approval and test validation.
- Historical calibration parameters must be applied to future scenario transformations so SPI values remain comparable across periods.
- Any change to gamma fitting, calibration, or baseline handling must include a reference validation test.

---

## Missing data behavior

- Be explicit about NaN handling and missing periods (skip vs coerce vs partial+warn).
- Avoid surprising return-type changes (e.g., `None` vs `DataFrame`) without tests.

---

## Validation

For any parameter-mapping or calendar/aggregation change:
- Add or adjust tests in `india_resilience_tool/compute/tests/` or `tests/`
- Run: `python -m pytest -q`
