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

## Next

### BL-0004 — Build the weighted admin ↔ hydro translation engine
- `Area`: crosswalk, analytics
- `Why deferred`: current crosswalks are intentionally read-optimized and explanatory, not analytical transfer engines.
- `Dependency / trigger`: start after current river QA closure and once the desired weighting/aggregation semantics are agreed.
- `Done when`: the platform can translate values across admin and hydro geographies with explicit weighting rules and provenance.

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
  - Block dropdown index — `context/admin_block_index.parquet`; falls back to loading block names from `blocks_4326.geojson` directly when missing
  - Sub-basin dropdown index — `context/hydro_subbasin_index.parquet`; falls back to loading `subbasins.geojson` directly

  **Not yet routed through `processed_optimised` — gaps:**
  1. Landing page ADM2 geometry: `runtime.py` always reads the raw nationwide `districts_4326.geojson` on the landing page (`ADM2_GEOJSON = DISTRICTS_PATH`), even when a state-sharded optimised GeoJSON exists. See BL-0019.
  2. ADM1 (state boundary dissolve): built by dissolving the raw ADM2; follows from gap 1.
  3. Legacy master rebuild output: when a climate metric falls back to legacy, `build_master_metrics()` writes the rebuilt master into `processed/{slug}/{state}/` rather than into `processed_optimised/`. The rebuild path still targets the legacy tree.

  **Long-term action (when ready to remove legacy fallback branches):** remove the legacy fallback arms from `_resolve_admin_master_source`, `_resolve_hydro_master_source`, and the timeseries loaders. This makes the dashboard fail fast and clearly when the bundle is incomplete, rather than silently reading stale legacy data. Block this on confirming `processed_optimised` is complete across all metric slugs and levels.

### BL-0019 — Fix remaining data-feed gap: landing page geometry not routed through processed_optimised
- `Area`: app, data-loading, processed_optimised
- `Why deferred`: non-blocking for the current dashboard state (landing page works via raw GeoJSON), but leaves the dashboard partially dependent on legacy paths in ways that will matter at deployment time. The companion block-selector-fallback gap is already resolved — `geography_controls.py` now falls back to `blocks_4326.geojson` when `admin_block_index.parquet` is missing.
- `Dependency / trigger`: fix before the first deployment where `IRT_DATA_DIR` boundary flat files are not co-deployed alongside `processed_optimised`.
- `Done when`: `runtime.py` landing page, in state-focused landing mode (when `selected_state != "All"`), tries `optimized_geometry_path(level="district", state=selected_state)` before falling back to `DISTRICTS_PATH`. For the India-level overview (`state=All`) the raw nationwide GeoJSON remains correct.
- `Files to change`: `india_resilience_tool/app/runtime.py`

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

### BL-0024 — Purge inert basin/sub_basin residue left by the hydro lean-down
- `Area`: hydro lean-down, code hygiene, tech-debt
- `Why deferred`: after the hydro lean-down (Phases 1–5 + G11 river-pair), the navigable Hydro family and offline basin/sub-basin compute are gone, but the completeness-grep gate is still **red** on genuinely dead `basin`/`sub_basin` branches. The phases removed every runtime **entry point** that could set `level`/`family` to a hydro value (`SPATIAL_FAMILY_HYDRO`, `ADMIN_LEVEL_BASIN/SUB_BASIN`, the sidebar Hydro family selector, hydro `LEVEL_GROUPS`/compute dispatch), so `level` can now only be `district`/`block` and these interior `if level == "basin":` arms are unreachable. The branch bodies were deliberately left because they live **inside functions the retained admin district/block paths still call**, so excising just the hydro arms is fiddlier and carries regression risk to the admin paths — unlike deleting a whole navigable entry point. The gate is a lean-down hygiene check (dead text), not a correctness check; the suite is green and there is no reachable hydro, so this is non-blocking cleanup, not a bug.
- `Dependency / trigger`: pick up as a focused hygiene pass when the portfolio/rankings surface is being touched anyway. Do it with the completeness-grep gate as the acceptance check and full-suite parity against the current 14-failure baseline.
- `Scope` (8 files with truly-dead residue — confirmed outside the retained-context allowlist):
  - `india_resilience_tool/analysis/portfolio.py` — `get_portfolio_storage_key`, `portfolio_key_basin/_subbasin`, `_basin/_subbasin_matches`, `KEY_BASINS/SUBBASINS`, and the `level_norm == "basin"/"sub_basin"` arms throughout (G7 trim only partially applied).
  - `india_resilience_tool/app/portfolio_ui.py` — `is_basin`/`is_subbasin`/`is_hydro` and `Basin`/`Sub-basin` column handling (~15 sites; highest excision risk, ~2000 lines with admin logic interwoven).
  - `india_resilience_tool/app/portfolio_multistate.py`, `india_resilience_tool/app/portfolio_state_runtime.py` — shared portfolio storage-key/multistate hydro arms.
  - `india_resilience_tool/app/views/rankings_view.py` — file-local `AdminLevel = Literal["district","block","basin","sub_basin"]` (never narrowed, G4) + basin/subbasin table-rendering branches.
  - `india_resilience_tool/compute/spi_adapter.py` — `level in {"block","sub_basin"}` / `level == "basin"` unit-key/name row-shaping (compute is admin-only now).
  - `india_resilience_tool/data/discovery.py` — the `Literal["basin","sub_basin"]` discover-yearly helpers (siblings of the deleted `discover_hydro_yearly_file`, now uncalled).
  - `india_resilience_tool/app/geography_controls.py` — dead `st.session_state["selected_basin"/"selected_subbasin"] = "All"` reset writes (session keys no longer drive any widget/render).
  - Bonus leftover: `india_resilience_tool/app/geo_cache.py` `load_river_*_cached` (G3 intended Phase-1 removal; still present, dead-harmless).
- `Do NOT touch` (retained hydrology context — correctly excluded from the gate): `map_layer_runtime.py` reference-overlay path, `geo_cache` basin/sub-basin context-geometry builders, `details_runtime` crosswalk_contexts, `details_panel` crosswalk rendering, `crosswalks.py`/`crosswalk_runtime.py`, `folium_featurecollection.py`, `map_view.py`, `hydro_loader`/`hydro_summary`/`hydro_boundary_overlay`/`river_loader`/`river_topology`/`context_cards`/`summary_cache`.
- `Done when`: the completeness-grep gate (`grep '"sub_basin"|== "basin"|selected_basin|selected_subbasin'` minus the retained-context allowlist) returns nothing across the 8 files, narrowed type aliases no longer list hydro levels, and the full suite shows no new failures vs the recorded baseline.

### BL-0025 — Add an absolute (cross-state, cross-period comparable) composite score
- `Area`: methodology, thematic bundles, sectoral bundles
- `Why deferred`: today every normalization step derives its scaling constants from the spatial spread of the current cohort and rebuilds them per `(state, scenario, period)` — thematic min–max (§6.2), the sectoral absolute lens's p10–p90 rescale, and even the change lens's final spatial rank (§7.2). Because the denominator moves with the frame, a "70" in one state/period is not on the same ruler as a "70" in another, so scores cannot be compared across states or across periods. The only already-absolute quantity in the system is the sectoral **impact lens** (fixed physical band `[a,b]`). The team wants to design this deliberately later rather than swap methodology mid-stream.
- `Dependency / trigger`: pick up when cross-state / cross-period comparability becomes a required product capability, and once the team decides which *sense* of "absolute" is wanted (see options). Note that "absolute" is ambiguous — pick the comparison question first.
- `Design options` (evaluate later; each answers a different question):
  - **Route A — fixed physical reference bands** (absolute in the *danger* sense): generalize the impact-lens approach to every metric, replacing cohort min/max with a fixed per-metric `[a,b]`. Most meaningful, but blocked for metrics with no defensible threshold (R99p, SPI-3 low-flow proxy, R95p variability — currently impact weight 0); must not invent phantom thresholds (§7.4 provenance discipline).
  - **Route B — frozen global reference envelope** (absolute in the *ranking-ruler* sense): keep min–max/decile shape but compute the endpoints once over the pooled all-state × all-period population and freeze them. Least invasive; cost = loses within-state contrast, and the envelope must include the hottest future slice or clip.
  - **Route C — baseline-anchored anomaly** (absolute in the *change-from-history* sense): normalize departures from the fixed 1990–2010 baseline against fixed anchor magnitudes. Note: a **dormant baseline-anchored mode already exists in §6.2 code** (the "≥4 anchored components" floor, currently inactive), and the sectoral change lens is already change-vs-baseline — swap only its final spatial scaler for a fixed one.
  - Partial lever for sectoral bundles without new machinery: raise the impact-lens weight (score → absolute as `ω_imp → 1`), or publish `S_imp` itself as a separate "absolute danger" field.
- `Done when`: an absolute, cross-state/cross-period-comparable composite is published **alongside** (not replacing) the current per-period relative scores, with the chosen route documented in the technical note, a scientific-compute pytest test added (methodology-impacting change per CLAUDE.md §4), and the relative-vs-absolute distinction surfaced in the UI so the two are not conflated.

## Icebox

- No items recorded yet.
