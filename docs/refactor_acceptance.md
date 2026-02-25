# Refactor Acceptance Criteria — IRT Prototype (No Regression Contract)

Author: Abu Bakar Siddiqui Thakur  
Email: absthakur@resilience.org.in

## Purpose

This document is the **no-regression contract** for the modular refactor of the IRT prototype repo.  
Every refactor PR must be verified against these acceptance criteria before merge.

If anything in this document is violated, it is considered a **breaking change** unless explicitly approved.

---

## A. Streamlit session-state and widget-key contract (API stability)

### A1) Core router keys (must preserve name, meaning, defaults)
- `analysis_mode` default **"Single district focus"**
- `portfolio_districts` (list)
- `portfolio_build_route` (portfolio UX router)
- Jump-once flags (must be honored **before** creating the left-panel radio, then reset):
  - `jump_to_rankings`
  - `jump_to_map`
- Left-panel view sync:
  - `active_view`
  - `main_view_selector` with **exact** string options:
    - `"Map view"`
    - `"Rankings table"`

### A2) Other stable widget/session keys (must not rename)
- Unified index selection keys:
  - `selected_var`, `selected_index_group`, `registry_metric`
- Portfolio-only hover toggle:
  - `hover_enabled` (shown only in portfolio mode)
- Portfolio multi-index selection:
  - `portfolio_multiindex_selection` (portfolio Step 2 multiselect)
- Performance timing instrumentation:
  - `perf_enabled`, `_perf_records`
- mtime caches in session state:
  - `_master_cache`, `_merged_cache`, `_portfolio_master_cache`

### A3) Rules
- **Never** rename these keys.
- **Never** change defaults silently.
- **Never** change widget key strings.
- Any refactor that touches session logic must include a manual check of:
  - jump-once behavior
  - view routing
  - portfolio UX flow routing

---

## B. Master outputs + filenames are contractual

`build_master_metrics` must continue producing these exact filenames (same names, same schemas):
- `master_metrics_by_district.csv`
- `state_model_averages.csv`
- `state_ensemble_stats.csv`
- `state_yearly_model_averages.csv`
- `state_yearly_ensemble_stats.csv`

Dashboard must continue reading these filenames exactly.

---

## C. Processed-root semantics + environment variables (contractual)

### C1) Processed root resolution rules (must remain identical)
Dashboard resolves processed root per index slug via:

- If `IRT_PROCESSED_ROOT` is set:
  - treat it as either:
    - already pointing to `.../<slug>`, OR
    - a base dir to which `<slug>` must be appended
- If not set:
  - default to `DATA_DIR / "processed" / <VARIABLE_SLUG>`

This same behavior must remain true in portfolio multi-index mode.

### C2) Other env vars (must preserve)
- `IRT_PILOT_STATE` default `"Telangana"`
- `IRT_DEBUG` affects debug/perf defaults (keep existing meaning)

### C3) Allowed additions
- You may introduce `IRT_DATA_DIR`, but **must not break** behavior when it is not set.

---

## D. Master column normalization + schema parsing must not break

### D1) Normalization contract
Input example:
- `days_gt_32C_ssp585_2020_2040__mean`

Normalized example:
- `days_gt_32C__ssp585__2020-2040__mean`

Normalization must remain compatible with existing master CSV columns, and downstream computations must still work.

### D2) Schema parsing contract
Downstream uses the canonical form:
- `<metric>__<scenario>__<period>__<stat>`

Refactor must keep parsing compatible with existing master CSV schemas.

---

## E. Deterministic merge + caching behavior (perf-critical)

Must preserve:

1) Deterministic ADM2 ↔ master join using normalized keys (aliasing must remain compatible)
2) Restrict ADM2 to only states present in master (avoid showing polygons with no metrics)
3) Cache merged GeoDataFrame by **master mtime** in:
   - `st.session_state["_merged_cache"]`

### E2) Master freshness logic must remain identical
Master rebuild needed if **any** `*_periods.csv` under processed root is newer than the master CSV.

---

## F. Registry contract (must preserve + unify cleanly)

The dashboard currently depends on a variables/registry mapping:
- slug → spec including:
  - `label`, `group`, and crucially `periods_metric_col`
  - yearly discovery templates/candidates

The dashboard rebuilds master using:
- `metric_col_in_periods = VARCFG["periods_metric_col"]`

The refactor must unify registry between:
- dashboard “VARIABLES” reality
- pipeline “METRICS” reality (slug, var, value_col, compute, params, etc.)

**Acceptance requirement:**
- `periods_metric_col` must map consistently to the pipeline’s per-period output column used by `build_master_metrics`.
- Registry validation must catch mismatches (slug, column names, missing specs).

---

## G. Root-level scripts must continue to work (backward compatibility)

These entrypoints must remain runnable from repo root:
- `dashboard_unfactored.py` (may become shim)
- `compute_indices.py` (may become shim)
- `build_master_metrics.py` (may become shim)
- `nex_india_subset_download_s3_v1.py` (may become shim)

README drift must be resolved either by:
- updating README, OR
- adding shims for referenced legacy filenames if they exist historically.

---

## H. Performance acceptance (qualitative)

Refactor must not introduce obvious new bottlenecks. In particular:
- merged caching must still prevent repeated expensive joins
- master rebuild checks must not re-scan excessively in tight UI loops
- portfolio multi-index should not re-load all masters unnecessarily

(Exact timings may differ by machine, but behavior/caching semantics must remain.)

---

## I. Manual end-to-end acceptance run (required per major refactor step)

For **one pilot state** (default Telangana), confirm:

### I1) Map view
- ADM2 polygons render
- Hover behavior works (only in portfolio mode if required)
- Map click selects district and updates right panel appropriately
- No unintended full-detail dump in portfolio flow

### I2) Rankings view
- Rankings table renders
- Selection sync with map works
- “Add to portfolio” works from rankings

### I3) Single district focus
- Details cards populate
- Trend chart renders
- Scenario comparison renders
- Exports (if enabled) produce outputs

### I4) Portfolio mode
- Portfolio build route UX works
- Selected portfolio list persists in session
- Multi-index selection produces a comparison table without “None/Unknown” regressions for valid metrics

### I5) Master rebuild trigger
- If any `*_periods.csv` is newer than master, dashboard triggers rebuild (or warns/does rebuild per current behavior)
- If not newer, it should not rebuild

---

## J. PR sign-off checklist (paste into PR description)

- [ ] Session-state keys unchanged (A1/A2)
- [ ] View routing strings unchanged (A1)
- [ ] Master output filenames unchanged (B)
- [ ] Processed-root semantics preserved (C)
- [ ] Master normalization + parsing preserved (D)
- [ ] Deterministic merge + cache semantics preserved (E)
- [ ] Registry mapping preserved and validated (F)
- [ ] Root scripts still runnable (G)
- [ ] Manual acceptance run completed (I)

---

## Baseline notes (fill once, then keep updated)

- Baseline commit:
- Test state used:
- Notes/screenshots stored at:
- Known quirks (existing behavior, not regressions):
  - (add here)