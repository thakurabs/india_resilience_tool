# Functionality Contract — India Resilience Tool (IRT)

This document defines the **must-not-break** contract for cleanup, dead-code removal, and modularization work.

If a change violates this contract, it is considered a regression even if tests pass.

## 1) Primary dashboard entrypoint (single supported way)

The dashboard must launch via:

```bash
streamlit run india_resilience_tool/app/main.py
```

Contract:
- `india_resilience_tool/app/main.py` must *execute* the app when run by Streamlit (not just define functions).
- This is the only supported launch path; legacy/root shims are explicitly out of scope to preserve.

## 2) Dashboard flows that must work

### 2.1 Single-unit exploration (District / ADM2)

Must work end-to-end:
1. **Map view**
   - Choose a risk domain (bundle) → metric → scenario → period → statistic
   - Choose map mode (absolute vs change-from-baseline)
   - Choropleth renders for the selected state/district scope
2. **Rankings**
   - Rankings table renders for the same selection
   - Sorting modes function (top-N, all, delta views when baseline exists)
3. **Details panel (Climate Profile)**
   - Selecting a district shows a right-side details panel with:
     - baseline/current values and delta
     - position (rank/percentile) and risk class
     - trend over time
     - scenario comparison panel
4. **Exports**
   - Case-study PDF generation succeeds (downloads/bytes build)
   - Case-study ZIP generation succeeds (downloads/bytes build)

### 2.2 Single-unit exploration (Block / ADM3)

Must work end-to-end:
1. Switch administrative level to **Block**
2. Map view renders blocks and supports selection
3. Rankings table renders blocks
4. Details panel renders for a selected block
5. Exports must not crash; if some exports are district-only, the UI must gate/disable appropriately.

### 2.3 Portfolio mode (multi-unit) — District and Block

For both levels (District and Block), portfolio mode must support:
- Adding units to the portfolio via:
  - map click (“Add to portfolio” inline control)
  - rankings table add/remove controls
  - “Add by Location” panel (single + batch coordinates)
- Portfolio compare:
  - bundle multi-select → metric expansion
  - optional manual metric refinement
  - comparison table builds and downloads as CSV
  - scenario mode supports “single scenario” and “compare scenarios”
  - charts/visualizations render when the tab is opened

## 3) Ops / diagnostic / data-prep scripts (must be retained)

These scripts count as “functionality” even if they are not part of the Streamlit runtime UI.

After cleanup, they must remain runnable (relocated under `tools/`):
- Index computation pipeline:
  - `python -m tools.pipeline.compute_indices_multiprocess ...`
  - `python -m tools.pipeline.compute_indices ...` (single-process / debug)
- Master CSV builder:
  - `python -m tools.pipeline.build_master_metrics ...`
- Diagnostics / sanity checks:
  - `python -m tools.diagnostics.spi_diagnostic ...`
- Data acquisition / prep helpers (ERA5/NEX/etc.) under `tools/` (documented individually)

Contract:
- Runtime package modules under `india_resilience_tool/` must **not import `tools/`**.

## 4) Environment variables and data roots (must remain supported)

Must continue to work:
- `IRT_DATA_DIR` — base data directory
- `IRT_PROCESSED_ROOT` — processed-root override semantics
- `IRT_PILOT_STATE` — default state selection
- `IRT_DEBUG` — debug/perf toggles

## 5) Methodology guardrail (non-negotiable)

Cleanup work must not silently change:
- ranking logic / percentile computation semantics
- thresholds for risk classes
- baselines, aggregation windows, or unit conversions

If a behavior change is required, it must be:
- explicitly called out in the change notes
- covered by a targeted pytest on a small synthetic dataset

