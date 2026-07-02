# IRT Backlog

## Purpose

This file is the durable backlog for deferred or shelved work in the India Resilience Tool.

Use it for:
- work we know we want to do later
- follow-up tasks that should not be lost between sessions
- larger initiatives that are not the current execution priority

Do not use it for:
- session handoff details
- change-by-change implementation history
- generated-data observations with no reusable action item

Session handoffs stay in chat and, when explicitly confirmed by the user, in `docs/HANDOFF.md`.

## How to Use This File

- Keep entries short and action-oriented.
- Prefer one durable backlog item over many tiny notes.
- Move items between `Now`, `Next`, `Later`, and `Icebox` instead of duplicating them.
- Update the `Done when` line when scope becomes clearer.

Entry fields:
- `ID`
- `Title`
- `Area`
- `Why deferred`
- `Dependency / trigger`
- `Done when`

## Now

### BL-0001 — Close remaining river topology QA issues
- `Area`: river, topology
- `Why deferred`: the river foundation is in place, but a small set of unresolved assignment and self-loop cases still needs closure before the river layer can be treated as fully stable.
- `Dependency / trigger`: continue after the latest `build_river_topology` outputs are regenerated and the debug artifacts are available.
- `Done when`: unresolved river hydro assignments are explained or fixed, remaining self-loops are inspected, and `river_topology_qa.csv` contains only accepted residual issues.

### BL-0002 — Complete structured visual validation of river overlays and topology artifacts
- `Area`: river, QA
- `Why deferred`: the dashboard now shows hydro river overlays and river summaries, but representative basin/sub-basin validation still needs to be completed systematically.
- `Dependency / trigger`: requires current `river_basin_name_reconciliation.csv`, `river_subbasin_diagnostics.csv`, `river_reaches.parquet`, and the missing-assignment debug artifacts.
- `Done when`: a representative set of major basins, sub-basins, unresolved cases, and debug layers has been manually reviewed and accepted.

### BL-0003 — Decide whether sub-basin river matching needs a permanent reconciliation artifact
- `Area`: river, data-contract
- `Why deferred`: basin-level reconciliation is permanent, but sub-basin matching is still diagnostics-driven and may or may not need to graduate to a canonical mapping file.
- `Dependency / trigger`: review the current `river_subbasin_diagnostics.csv` results after visual validation.
- `Done when`: the team explicitly decides either to keep diagnostics-only matching or to introduce `river_subbasin_name_reconciliation.csv`.

## Next

### BL-0004 — Build the weighted admin ↔ hydro translation engine
- `Area`: crosswalk, analytics
- `Why deferred`: current crosswalks are intentionally read-optimized and explanatory, not analytical transfer engines.
- `Dependency / trigger`: start after current river QA closure and once the desired weighting/aggregation semantics are agreed.
- `Done when`: the platform can translate values across admin and hydro geographies with explicit weighting rules and provenance.

### BL-0005 — Add hydro portfolio workflows
- `Area`: hydro, UI
- `Why deferred`: portfolio support currently exists only for district and block flows.
- `Dependency / trigger`: start after hydro single-unit flows and the polygon crosswalk bridge are considered stable enough to widen the interaction model.
- `Done when`: basin and sub-basin portfolio selection, comparison, and portfolio-side summaries work reliably in the dashboard.

### BL-0006 — Build the river-network/reach translation layer
- `Area`: river, crosswalk
- `Why deferred`: the current river work is hydro-facing and topology-ready, but not yet connected to admin/hydro crosswalk semantics.
- `Dependency / trigger`: start after river topology QA closure and once the desired river/admin translation semantics are defined.
- `Done when`: the platform can relate reaches to admin and hydro polygons in a reusable, audited way.

## Proposal Bundles Backlog

### BL-0013 — Recalibrate threshold-heavy proposal bundles to avoid spatial saturation
- `Area`: proposal bundles, methodology
- `Why deferred`: several sector-wise proposal bundles currently collapse to one or two composite values across Telangana district and block units, especially in later future periods, because too many rules are binary `0/100` thresholds that saturate statewide.
- `Dependency / trigger`: start after the team confirms whether sector-wise bundles should remain threshold-led, move to continuous severity scoring, or adopt mixed threshold-plus-continuous rules.
- `Done when`: `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Infrastructure Risk`, and `Life & Livelihood Loss Risk` all retain meaningful spatial differentiation for representative district and block slices without silently changing intended semantics.

### BL-0014 — Expose proposal bundle constituent metrics and rule diagnostics in Deep Dive
- `Area`: proposal bundles, UI, transparency
- `Why deferred`: Deep Dive currently exposes only the persisted composite slug for sector-wise domains, so users cannot inspect the constituent metrics, rule logic, or rule-level scores that produced the bundle outcome.
- `Dependency / trigger`: start after the preferred UX is chosen for sector-wise drill-down, such as exposing constituent metrics in the ribbon, adding a dedicated `Bundle drivers` panel in Deep Dive, or supporting both patterns together.
- `Done when`: Deep Dive allows users to inspect the constituent metrics and rule diagnostics for each sector-wise bundle, including rule labels, source metric names, raw values, thresholds or baseline logic, and per-rule scores for the selected geography.

### BL-0015 — Add proposal-bundle saturation diagnostics in runtime and QA workflows
- `Area`: proposal bundles, QA, diagnostics, UI
- `Why deferred`: the current runtime can render flat proposal-bundle maps without clearly telling operators or users that the bundle has collapsed to very low variance because of saturated rule outcomes.
- `Dependency / trigger`: start after the team agrees on what counts as unacceptable bundle flatness, such as fully constant slices or slices with two-or-fewer distinct composite values.
- `Done when`: the dashboard and supporting QA workflows can flag low-variance proposal-bundle outputs, distinguish methodological saturation from data or rendering bugs, and surface an operator-friendly warning or report.

### BL-0016 — Make proposal-bundle baseline semantics explicit and auditable
- `Area`: proposal bundles, methodology, data-contract
- `Why deferred`: change-vs-baseline proposal rules currently rely on fallback baseline-column discovery, which works operationally but makes the intended historical comparison period less explicit than it should be.
- `Dependency / trigger`: start after the desired historical baseline window is agreed for proposal-bundle change rules and reconciled against the currently persisted source-master periods.
- `Done when`: proposal-bundle config explicitly declares the intended baseline semantics, source masters are aligned or transformed to that contract, and change-rule behavior is documented and test-covered.

### BL-0017 — Add regression tests for proposal-bundle rule diversity and Deep Dive driver exposure
- `Area`: proposal bundles, tests
- `Why deferred`: the current test suite validates builder mechanics and output presence, but it does not yet protect against bundles becoming uniformly flat across all units or against Deep Dive hiding constituent metrics and driver context unintentionally.
- `Dependency / trigger`: start after the methodology and UI direction are settled for threshold-heavy bundles and sector-wise driver inspection.
- `Done when`: tests cover representative proposal-bundle outputs for district and block levels, detect unintended saturation patterns, validate baseline-rule behavior, and verify the intended Deep Dive exposure of constituent metrics and bundle-driver diagnostics.

### BL-0018 — Define partial-coverage policy for sector-wise proposal bundles
- `Area`: proposal bundles, UI, data availability
- `Why deferred`: sector-wise proposal bundles can appear valid in the dashboard even when only a subset of states currently has persisted proposal-bundle masters, which can make nationwide views look broken or misleading.
- `Dependency / trigger`: start after the team decides whether partial state coverage should remain visible, be hidden, or be shown only with explicit coverage messaging.
- `Done when`: the dashboard has a clear and tested policy for sector-wise proposal bundles with incomplete state coverage, including visibility rules and user-facing messaging for `State=All` views.

### BL-0020 — Geography-zone-specific impact bands for lens scoring
- `Area`: proposal bundles, methodology, geospatial
- `Why deferred`: the lens impact bands (danger thresholds) are currently plains/national defaults, but institutional danger standards are physiography-specific (e.g. IMD heatwave trigger 40 / 37 / 30 deg C for plains / coastal / hilly). Refining bands per zone needs a per-geography zone label, and no single ready-made all-India district -> {plains/coastal/hilly} classification matching IMD's taxonomy exists off-the-shelf. Deferred to avoid stitching one mid-task; plains default is correct for the Telangana pilot anyway.
- `Dependency / trigger`: start after the sectoral lens dossiers are complete and the team picks a zone source.
- `Candidate zone sources` (evaluate later):
  - ICAR / Planning-Commission **agro-climatic regions** (15) — promising, official.
  - NBSS&LUP **agro-ecological zones** (20) — promising, finer physiographic basis.
  - **DEM-derived** classification (elevation cutoff for hilly) + standard **coastal-district list** (Census / MoES-NCCR) + plains residual.
- `Done when`: each district/block carries a defensible physiographic-zone label, the impact-band scorer looks up per-zone bands, and the per-metric dossiers record zone-specific bands (external where published, self-derived via the protocol otherwise) with the plains default retained as fallback.

### BL-0021 — CHG-0032-RT: Agricultural Risk artifact rebuild on data-prod
- `Area`: proposal bundles, ops, data pipeline
- `Why deferred`: CHG-0032 (Agricultural Risk lens migration, code + config + docs + tests) was applied on the dev workstation and verified by `pytest -q` (37/37 on the three targeted test files). The artifact rebuild was attempted but cannot complete on this box: only `txx_annual_max` has the upstream per-state per-level intermediate CSVs in `processed/`, and three of the seven rule metrics (`spi3_count_events_lt_minus1`, `spi3_max_spell_lt_minus1`, `tnle10_cold_nights`) are absent from `processed_optimised/metrics/` as well. Rebuilding them locally would require running `tools.pipeline.compute_indices_multiprocess` from raw CMIP6 model outputs — multi-hour and outside CHG-0032 scope. The dashboard on the data-prod machine will continue to serve old single-lens Agricultural scores until this runs there.
- `Dependency / trigger`: run on the environment that already has all seven rule metrics' intermediate per-model CSVs under `IRT_DATA_DIR/processed/<metric>/<state>/{districts,blocks}/`. Required metric slugs: `txx_annual_max`, `txge35_extreme_heat_days`, `wsdi_warm_spell_days`, `spi3_count_events_lt_minus1`, `spi3_max_spell_lt_minus1`, `pr_max_5day_precip`, `tnle10_cold_nights`. Confirm via `python -m tools.pipeline.build_master_metrics --list-metrics` before kicking off the build.
- `Commands` (run from repo root, in order; all flags load-bearing):
  ```bash
  # Step 1 — build the seven upstream rule master CSVs (overwrite is default; no flag needed).
  # Repeat per state if rebuilding more than Telangana; --level both covers district + block.
  python -m tools.pipeline.build_master_metrics --level both --state Telangana --metrics txx_annual_max txge35_extreme_heat_days wsdi_warm_spell_days spi3_count_events_lt_minus1 spi3_max_spell_lt_minus1 pr_max_5day_precip tnle10_cold_nights

  # Step 2 — aggregate the seven rule masters into the composite bundle master.
  python -m tools.pipeline.build_proposal_bundles --bundle composite_agricultural_risk --level admin --overwrite

  # Step 3 — convert legacy bundle masters into the optimized runtime parquet shards.
  # --prune-scope deletes stale optimized files inside the agricultural-risk metric/level scope (destructive; requires --overwrite).
  python -m tools.optimized.build_processed_optimised --metric composite_agricultural_risk --level admin --overwrite --prune-scope

  # Verification — confirm lens-decomposed columns landed and parity is clean.
  python -m tools.optimized.audit_processed_optimised_parity --metric composite_agricultural_risk
  python -c "import pandas as pd; from pathlib import Path; import os; root = Path(os.environ.get('IRT_DATA_DIR','/data/irt'))/'processed'/'composite_agricultural_risk'/'Telangana'; csv = next(root.rglob('master_metrics_by_district.csv')); df = pd.read_csv(csv); print(sorted(c for c in df.columns if 'txx_peak_crop_heat' in c and any(s in c for s in ('abs_score','chg_score','imp_score'))))"
  ```
- `Done when`:
  - All seven rule masters present in `processed/<metric>/<state>/master_metrics_by_{district,block}.csv` for every state in scope.
  - `processed/composite_agricultural_risk/<state>/master_metrics_by_{district,block}.csv` present and includes `__abs_score`, `__chg_score`, `__imp_score` columns for every (rule, scenario, period) combination.
  - `processed_optimised/metrics/composite_agricultural_risk/masters/admin/{district,block}/state=<state>.parquet` present.
  - `audit_processed_optimised_parity --metric composite_agricultural_risk` reports `issues=0`.
  - Manual dashboard check (Streamlit, Telangana, Sector-wise → Agricultural Risk): bundle ranks shift relative to pre-rebuild baseline (new weights and a live change lens guarantee numeric movement).

### BL-0022 — CHG-0035-RT: Thermal Power lens migration after SPI-3 baseline preflight
- `Area`: proposal bundles, methodology, compute, docs, tests
- `Why deferred`: CHG-0035 is currently `SUGGESTED` but blocked by its hard precondition. In the active data environment, `IRT_DATA_DIR` resolves to `/mnt/d/projects/irt_data`, but `processed/spi3_count_months_lt_minus1/` has no state-level legacy master CSVs, so the required read-only preflight could not verify that `_resolve_baseline_column()` finds a historical baseline for real SPI-3 district masters. Landing the Thermal Power change without that proof would make the planned SPI-3 change lens cosmetic in production.
- `CHG Ledger`: `CHG-0035` — `india_resilience_tool/config/proposal_bundles.py`, `india_resilience_tool/compute/proposal_bundles.py`, `docs/lens_scoring_methodology.md`, `docs/bundle_calculation_audit.md`, `docs/proposal_bundle_methodology.md`, `docs/climate_risk_indicator_inventory.md`, `README.md`, `MANIFEST.md`, and the proposal-bundle test suites. Status: `SUGGESTED`, blocked pending SPI-3 baseline-column verification.
- `Dependency / trigger`: resume only on an environment that has a real legacy SPI-3 master at `IRT_DATA_DIR/processed/spi3_count_months_lt_minus1/<state>/master_metrics_by_district.csv`. First verify `_resolve_baseline_column(frame, "spi3_count_months_lt_minus1")` returns a non-`None` historical column on that real frame; if it does not, stop and fix baseline availability or baseline-token support before applying CHG-0035.
- `Plan`:
  1. Run the blocking SPI-3 baseline preflight against a real district master CSV in the live data environment.
  2. If the preflight passes, migrate `composite_asset_risk_thermal_power` to dossier §10 explicit weights in `india_resilience_tool/config/proposal_bundles.py` while preserving the current rule order, slugs, and user-visible labels.
  3. Remove the Thermal SPI-3 absolute-only special case in `india_resilience_tool/compute/proposal_bundles.py` so `spi3_low_flow_proxy_norm` becomes a normal blended rule with absolute + change lenses and no impact lens; keep the Hydropower `r95p_interannual_variability_norm` helper special case unchanged.
  4. Update the landed docs and inventory text to reflect the applied Thermal Power methodology and the migrated explicit-weight bundle count.
  5. Extend the proposal-bundle config, builder, per-lens persistence, grid-first contract, and optimized-bundle tests, then run the targeted pytest suite or the documented fallback validation if `pytest` is unavailable.
- `Done when`:
  - The SPI-3 preflight proves a real `spi3_count_months_lt_minus1` district master exposes a resolvable historical baseline column.
  - `composite_asset_risk_thermal_power` uses `weight_mode="explicit_normalized"` with a `0.70` minimum available rule-weight fraction and dossier-§10 rule weights/lens splits.
  - `spi3_low_flow_proxy_norm` persists `__chg_score` columns, still omits `__imp_score`, and no longer uses the Thermal absolute-only dispatch path.
  - Thermal Power docs and inventory text describe the landed explicit-weight methodology without changing current slugs or labels.
  - The targeted proposal-bundle and `processed_optimised` compatibility tests pass in the real project environment.

## Later

### BL-0007 — Migrate processed-data storage to build/published/archive Parquet serving
- `Area`: storage, architecture
- `Why deferred`: this is a large repo-wide migration and the immediate focus remains river QA/topology closure and hydro-facing runtime hardening.
- `Dependency / trigger`: begin after current river v1 closure, when runtime loader changes and publish/prune workflow changes can be tackled systematically.
- `Done when`: processed serving data uses the planned `build / published / archive` structure, runtime prefers Parquet with CSV fallback during transition, GeoParquet reference geometry is in place, and legacy hot-path CSV forests are pruned only after parity validation.

### BL-0011 — Reframe exposure rankings so they do not present hazard-style risk classes
- `Area`: exposure, UI, semantics
- `Why deferred`: exposure layers currently inherit the generic ranking and `risk_class` presentation used for hazard metrics, which is mechanically correct but semantically awkward for non-hazard layers such as population.
- `Dependency / trigger`: revisit after the first exposure-layer tranche is stable and the desired exposure-side summary language is agreed across details, map tooltips, and rankings.
- `Done when`: exposure pillars no longer present hazard-style `risk_class` labels by default, and the UI uses clearly named relative-exposure language or suppresses those labels entirely where appropriate.

### BL-0008 — Add upstream/downstream routing behavior to the river experience
- `Area`: river, topology, UI
- `Why deferred`: topology artifacts exist offline, but no routed or direction-aware product behavior has been added yet.
- `Dependency / trigger`: requires stable reach/node/adjacency artifacts and a clear contract for directionality and routed queries.
- `Done when`: the product can surface upstream/downstream relationships in a user-facing way without ambiguous or misleading routing behavior.

### BL-0009 — Add admin-side river overlays
- `Area`: river, admin-ui
- `Why deferred`: the current river overlay is intentionally hydro-only.
- `Dependency / trigger`: start after the hydro-side river experience is accepted and the desired admin-side narrative is clear.
- `Done when`: district/block views can optionally show river context without confusing the current admin analysis workflow.

### BL-0010 — Add river-based metric computation
- `Area`: river, analytics
- `Why deferred`: the current river work is limited to cleaning, topology-ready artifacts, overlays, and hydro-side summary context.
- `Dependency / trigger`: requires a settled reach-level analytical contract and clear methodology for river-native metrics.
- `Done when`: river reaches can participate in metric computation and serving contracts in a scientifically explicit way.

### BL-0012 — Converge from `processed` plus `processed_optimised` to one final runtime-serving contract
- `Area`: storage, architecture, deployment
- `Why deferred`: the current two-step flow is the safest migration path while the compact runtime contract is still being validated, but it adds duplication, rebuild drift risk, and operational complexity if kept forever.
- `Dependency / trigger`: revisit after `processed_optimised` is validated across climate, Aqueduct, population, groundwater, hydro, time-series, and case-study flows, and once the retained field/artifact contract is considered stable.
- `Done when`: the team explicitly decides whether to keep a permanent build-vs-runtime split or move to one canonical dashboard-serving processed directory, with a documented migration plan and clear separation for raw/build/QA artifacts.
- `Audit context (2026-05-04)`: a full data-feed audit was performed across `runtime.py`, `ribbon.py`, `timeseries.py`, `geo_cache.py`, `geography_controls.py`, `details_runtime.py`, and `master_freshness.py`. The following is the current state:

  **Already routed through `processed_optimised` (optimised-first, legacy fallback):**
  - Master CSVs for all metric types: climate, Aqueduct, population, groundwater, JRC flood depth, and dashboard bundle composites — routed via `resolve_processed_optimised_root` with `prefer_optimized_runtime=True` in `ribbon.py`; falls back to `processed/{slug}/{state}/master_metrics_by_{level}.{csv,parquet}`
  - Yearly ensemble timeseries for district, block, and hydro — `timeseries.py` checks `is_optimized_metric_root()` first, falls back to legacy CSV discovery
  - Per-model yearly timeseries (spaghetti charts) — `metrics/{slug}/yearly_models/admin/{level}/state={STATE}.parquet`; falls back to `discover_district_model_yearly_files()` / `discover_block_model_yearly_files()`
  - State-level yearly trend (trend chart) — aggregated from optimised state parquet; falls back to `state_yearly_ensemble_stats_{level}.csv`
  - District/block geometry when a state is selected — `geometry/admin/{level}/state={STATE}.geojson`; falls back to nationwide `districts_4326.geojson` / `blocks_4326.geojson`
  - Basin geometry — `geometry/hydro/basin.geojson`; falls back to `basins.geojson`
  - Sub-basin geometry when a basin is selected — `geometry/hydro/sub_basin/basin_id={id}.geojson`; falls back to `subbasins.geojson`
  - River display, reconciliation, diagnostics, reaches — `context/river_*.{geojson,parquet}`; falls back to legacy `IRT_DATA_DIR` flat files
  - Crosswalk context (details panel) — `context/{district,block}_{subbasin,basin}.parquet`; falls back to legacy crosswalk CSVs
  - Block dropdown index — `context/admin_block_index.parquet` (no graceful fallback — see BL-0019)
  - Sub-basin dropdown index — `context/hydro_subbasin_index.parquet`; falls back to loading `subbasins.geojson` directly

  **Not yet routed through `processed_optimised` — gaps:**
  1. Landing page ADM2 geometry: `runtime.py` always reads the raw nationwide `districts_4326.geojson` on the landing page (`ADM2_GEOJSON = DISTRICTS_PATH`), even when a state-sharded optimised GeoJSON exists. See BL-0019.
  2. ADM1 (state boundary dissolve): built by dissolving the raw ADM2; follows from gap 1.
  3. Block selector fallback: if `admin_block_index.parquet` is missing, `geography_controls.py` has no graceful fallback. See BL-0019.
  4. Legacy master rebuild output: when a climate metric falls back to legacy, `build_master_metrics()` writes the rebuilt master into `processed/{slug}/{state}/` rather than into `processed_optimised/`. The rebuild path still targets the legacy tree.

  **Long-term action (when ready to remove legacy fallback branches):** remove the legacy fallback arms from `_resolve_admin_master_source`, `_resolve_hydro_master_source`, and the timeseries loaders. This makes the dashboard fail fast and clearly when the bundle is incomplete, rather than silently reading stale legacy data. Block this on confirming `processed_optimised` is complete across all metric slugs and levels.

### BL-0019 — Fix two remaining data-feed gaps: landing page geometry and block selector fallback
- `Area`: app, data-loading, processed_optimised
- `Why deferred`: both are non-blocking for the current dashboard state (landing page works via raw GeoJSON; block selector works when `admin_block_index.parquet` exists), but they leave the dashboard partially dependent on legacy paths in ways that will matter at deployment time.
- `Dependency / trigger`: fix before the first deployment where `IRT_DATA_DIR` boundary flat files are not co-deployed alongside `processed_optimised`.
- `Done when`:
  1. `runtime.py` landing page: in state-focused landing mode (when `selected_state != "All"`), tries `optimized_geometry_path(level="district", state=selected_state)` before falling back to `DISTRICTS_PATH`. For the India-level overview (`state=All`) the raw nationwide GeoJSON remains correct.
  2. `geography_controls.py` block selector: if `admin_block_index.parquet` is missing, falls back to loading block names from `blocks_4326.geojson` directly (matching the pattern already used by the sub-basin selector when `hydro_subbasin_index.parquet` is absent).
- `Files to change`: `india_resilience_tool/app/runtime.py`, `india_resilience_tool/app/geography_controls.py`
- `Test to add`: extend `tests/test_app_geography_controls.py` to assert that block selector gracefully returns an empty-but-valid index when the optimised context artifact is absent.

### BL-0023 — Retire the Aqueduct hydro scripts (extract shared helpers, then delete)
- `Area`: aqueduct, tools, lean-down
- `Why deferred`: the final piece of the hydro lean-down's G11 file-deletes. `tools/geodata/build_aqueduct_hydro_crosswalk.py` (225 lines) and `build_aqueduct_hydro_masters.py` (486 lines) are named "hydro" but are ~70% **live shared Aqueduct-general** code that the retained admin builders import — `load_aqueduct_boundaries`, `load_soi_hydro_boundaries`, `AqueductMetricSpec`, `AQUEDUCT_METRIC_SPECS`, `get_aqueduct_metric_spec`, `get_aqueduct_source_column_map`, `get_supported_aqueduct_metric_slugs`, `load_metric_source_table`, `load_crosswalk`, `aggregate_crosswalk_to_targets`. Only ~240 lines (the hydro crosswalk/master `main`/`build_cli` + write helpers, which write the no-longer-surfaced SOI basin/sub-basin Aqueduct masters) is genuinely dead. Deleting the files is therefore a 6-file extract-then-delete refactor of working retained code for a small dead-code payoff, so it was consciously deferred (user decision, 2026-07-02) rather than done as part of the docs + river-pair lean-down (CHG-0174/CHG-0175).
- `Dependency / trigger`: pick up when the Aqueduct tooling is being reorganized anyway, or when the dead SOI basin/sub-basin master-build path is confirmed permanently unwanted. Retained importers to rewire: `build_aqueduct_admin_crosswalk.py`, `build_aqueduct_block_crosswalk.py`, `build_aqueduct_admin_masters.py`, `validate_aqueduct_workflow.py`, and `tests/test_validate_aqueduct_workflow.py`. `tests/test_aqueduct_hydro_transfer.py` still exists (Phase 3 planned its deletion but was G11-blocked) and would be deleted with the scripts.
- `Plan`:
  1. Create `tools/geodata/aqueduct_common.py` and move the shared Aqueduct-general helpers/constants/`AqueductMetricSpec` there (with their internal helpers: `_default_aqueduct_dir`, `_assert_areal_geometries`, `_normalize_pfaf_id_series`, `_numeric_metric_series`, `HydroLevel` alias).
  2. Rewire the four retained importers + `test_validate_aqueduct_workflow.py` to import from `aqueduct_common`.
  3. Delete `build_aqueduct_hydro_crosswalk.py`, `build_aqueduct_hydro_masters.py`, and `tests/test_aqueduct_hydro_transfer.py`.
  4. Update `README.md` / `MANIFEST.md`: drop the two Aqueduct hydro builder commands and the SOI basin/sub-basin master references; keep the admin Aqueduct district/block workflow.
- `Done when`: the two Aqueduct hydro scripts are gone, no `processed/{aqueduct_slug}/hydro/` master-build path remains, the retained admin Aqueduct district/block builders + `validate_aqueduct_workflow` import their shared helpers from `aqueduct_common` and pass their tests, and no dangling imports remain repo-wide.

## Icebox

- No items recorded yet.
