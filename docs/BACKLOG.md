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

### BL-0028 — Reconcile Read the Docs coverage (review Finding 2)
- `Area`: documentation, thematic bundles
- `Why deferred`: resolve Finding 1 (thematic scoring methodology) first.
- `Dependency / trigger`: resume after the Finding 1 guidance text and figures are approved and reconciled; include standalone HTML in the review.
- `Done when`: Section 8.1 matches the supported historical/future/static outputs, includes static Water Risk alongside Riverine Flood, and states Water Risk's district-only scope; claims that all bundles support both administrative levels are corrected. Verify against current configuration and published slice contracts.

### BL-0029 — Verify and correct narrow-screen documentation navigation (review Finding 3)
- `Area`: Read the Docs HTML, responsive navigation
- `Why deferred`: resolve Finding 1 first; the scrolling-container mismatch remains a suspected browser behavior, not a confirmed reproduction.
- `Dependency / trigger`: resume after Finding 1; test standalone HTML below and above the 760px breakpoint.
- `Done when`: browser checks confirm subsection links, citation jumps, search-result navigation, and Back to top use the actual scrolling container at each width. Correct the mismatch if reproduced: narrow-screen CSS scrolls `.doc-shell` while JavaScript currently targets `.content-scroll`.

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

### BL-0030 — Migrate the eight sector bundles to frozen national CDF scoring under a physical-hazard definition
- `Area`: proposal bundles, methodology, composite scoring, data-contract
- `Why deferred`: the governing scientific definition was settled on 2026-09-19 and its consequences were computed, but two consequential decisions remain open (the change lens, and three bundles becoming near-duplicates). Freezing rulers before those are settled would bake the unresolved choices into committed artifacts. Paused deliberately at the decision point, not blocked.
- `Dependency / trigger`: resume when sector work is the active priority. Depends on `BL-0016` (baseline semantics) **only if** the change lens is retained; if the change lens is dropped from the headline, `BL-0016` ceases to block this item. Coordinate with the Drought thematic work: `pr_consecutive_dry_days_lt1mm` is scored by `composite_drought_risk/cdf_v1`, and a `cdf_v2` refit there changes five sector bundles.

#### Decision taken (2026-09-19) — do not re-open without new reasons

**A sector bundle score states physical hazard conditions relevant to that sector**, not exceedance of what that sector is locally built for. The alternative framing (locally-referenced exceedance) was considered and rejected; it remains defensible for a separate supporting presentation but not for the published headline number.

This mirrors the thematic precedent: `is_baseline_referenced` in `config/bundle_weights.py` (CHG-0367b, 18 entries) already excludes locally-referenced metrics from thematic headlines — Heat Risk went from 14 metrics to 9 for exactly this reason. The sector engine had never made the equivalent call.

#### The defect being corrected

Sector bundles run on a different engine from thematic bundles: rule specs in `config/proposal_bundles.py` scored by `compute/proposal_bundles.py`, with **no entries in `LANDING_BUNDLE_WEIGHTS`**. The absolute and change lenses normalize between the p10 and p90 of a cohort rebuilt per `state x level x scenario x period` (`_score_by_reference_distribution`, `compute/proposal_bundles.py:324`). The builder loads masters one state at a time, so the cohort is the state. Consequence: published sector scores are comparable neither across states nor across periods; only the fixed-band impact lens carries absolute meaning. All eight sector composites are already published in `processed_optimised/metrics/`, so this is live vendor-facing data, not a greenfield build.

Blocks are additionally scored against a state-wide *block* cohort today, rather than against their own district's ruler. The thematic precedent (decisions doc A5) scores blocks against the district-fitted ruler and never refits; adopting it also removes this level inconsistency.

#### Metric classification (the basis for everything below)

The 15 sector metrics split into two physical kinds.

**Kind A — absolute physical quantities.** Units-bearing; the same value means the same thing anywhere. All carry fixed, cited impact bands.

| metric | quantity |
|---|---|
| `txx_annual_max`, `tnx_annual_max` | hottest day / hottest night, deg C |
| `txge35_extreme_heat_days` | days at or above 35 deg C |
| `tnle10_cold_nights` | nights at or below 10 deg C |
| `pr_max_1day_precip`, `pr_max_5day_precip` | heaviest 1-day / 5-day rainfall, mm |
| `pr_consecutive_dry_days_lt1mm` | longest run of days under 1 mm |
| `cwd_consecutive_wet_days` | longest run of wet days |

**Kind B — locally-referenced quantities.** The threshold is the location's own historical distribution, so the same value denotes different physical conditions in different places. These leave the headline under the decision above.

| metric | reference |
|---|---|
| `wsdi_warm_spell_days` | days in a spell above that location's own 90th-percentile Tmax |
| `hwfi_tmean_90p` | same, on daily mean temperature |
| `spi3_count_events_lt_minus1`, `spi3_max_spell_lt_minus1`, `spi3_count_months_lt_minus1` | 3-month rainfall more than 1 SD below that location's own history |
| `r99p_extreme_wet_precip` | rain above that location's own 99th percentile |
| `r95p_interannual_variability` | year-to-year spread of above-95th-percentile rain |

Kind B carried a material share of the pre-cut rule weight: Agricultural 40%, Investment/Financial 35%, Asset Thermal 30%, Life & Livelihood 25%, Asset Hydropower 20%, Health 12%, Industrial 0%, Infrastructure 0%.

Note: the seven Kind-B metrics are exactly the seven with no committed national level ruler. Not a coincidence — the thematic fitter deliberately fits headline components only.

#### Roster after the cut: 34 rules -> 25, 15 distinct metrics -> 8

| bundle | rules | headline weight retained | kept metrics (renormalized rule weights) |
|---|---|---|---|
| Industrial Risk | 4 -> 4 | 100% | Rx1day .250, Rx5day .150, CDD .200, TXx .400 |
| Infrastructure Risk | 3 -> 3 | 100% | Rx1day .450, Rx5day .300, TXx .250 |
| Health Risk | 5 -> 4 | 88% | TXx .341, TNx .205, Rx1day .284, CWD .170 |
| Life & Livelihood Loss | 4 -> 3 | 75% | Rx1day .400, Rx5day .333, CDD .267 |
| Asset Risk (Hydropower) | 3 -> 2 | 80% | Rx5day .562, CDD .437 |
| Asset Risk (Thermal Power) | 3 -> 2 | 70% | CDD .500, TXx .500 |
| Investment / Financial | 5 -> 3 | 65% | Rx1day .385, Rx5day .231, CDD .385 |
| Agricultural Risk | 7 -> 4 | 60% | TXx .250, TX>=35 .167, Rx5day .333, TN<=10 .250 |

Dropping `wsdi_warm_spell_days` moots the previously flagged `change_mode` divergence (`auto` in Health Risk vs `relative_pct` in Agricultural and Life & Livelihood).

#### Key finding: zero new level rulers are required

Every one of the eight surviving metrics already has a committed frozen national CDF under `config/frozen_rulers/`. Verified on disk 2026-09-19 — all at `pooled_n` 5488 (784 districts x 7 slices), all `higher_is_worse=True`, matching the sector config's uniform `higher_worse` direction.

| metric | committed ruler artifact |
|---|---|
| `txx_annual_max`, `tnx_annual_max`, `txge35_extreme_heat_days` | `composite_heat_risk/cdf_v1` |
| `pr_max_1day_precip`, `pr_max_5day_precip`, `cwd_consecutive_wet_days` | `composite_flood_extreme_rainfall_risk/cdf_v1` |
| `tnle10_cold_nights` | `composite_cold_risk/cdf_v1` |
| `pr_consecutive_dry_days_lt1mm` | `composite_drought_risk/cdf_v1` |

This is structural rather than lucky: the thematic headlines are themselves the absolute-physical half, so a physically-defined sector headline draws from the same metric pool. The absolute-lens migration becomes an artifact-referencing and publication job, not a fitting job. Earlier ruler-count estimates (68, then ~31, then 30) are superseded — **the correct count under this definition is 0 new level rulers**, plus 8 change rulers only if the change lens is retained.

#### Open decision 1 — does the change lens belong in the headline? (blocking)

All 25 surviving rules still carry a change lens. A change lens measures **trajectory**, not **conditions**: a district at 47 deg C warming slowly scores below one at 38 deg C warming fast. Under the decision above that is the wrong quantity for a headline, and the thematic engine has no change lens at all.

**Recommendation (not yet approved): drop the change lens from the sector headline**, renormalize each rule across absolute + impact, and retain `__chg_score` as a published decomposition column so trajectory stays visible without entering the ranked number. Per-lens persistence (`__abs_score`, `__chg_score`, `__imp_score` alongside the blended `__score`) is already implemented and tested, so this costs nothing to keep.

Consequences of each branch:
- **Drop**: zero new ruler fits of any kind; `BL-0016` stops blocking; migration reduces to reference-renormalize-republish.
- **Retain**: 8 national delta rulers must be fitted, and `BL-0016` becomes a hard blocker — `BASELINE_TOKENS` (`compute/proposal_bundles.py:65`) still accepts `1995-2014` / `1985-2014` fallbacks against the required `1990-2010`, and a frozen delta ruler bakes the resolved baseline in permanently. A mixed-baseline national pool cannot be unwound after publication.

#### Open decision 2 — three bundles become near-duplicates (blocking)

Kind-B metrics were carrying most of what distinguished the sectors from one another: SPI-3 months distinguished thermal power, rainfall interannual variability distinguished hydropower, R99p and the heatwave-frequency index distinguished the financial view. After the cut:

- **Investment / Financial Risk and Life & Livelihood Loss Risk use the identical three metrics** — Rx1day, Rx5day, CDD — differing only in weights (.385/.231/.385 vs .400/.333/.267) and in one impact band.
- **Asset Risk (Hydropower)** is a two-metric subset of both.

Three published layers over one physical signal produce near-identical maps, which is misleading regardless of labelling. Two honest responses:

1. **Consolidate** — merge or retire the redundant layers. Cheap, honest, available immediately.
2. **Re-specify with discriminating absolute metrics** — a thermal plant's exposure is cooling-water availability and intake temperature; hydropower's is seasonal inflow volume and sediment-bearing extreme rain. Physically better, but these are new metrics requiring a compute wave, not a reweighting.

Do not resolve this by renormalizing and shipping.

#### Secondary items to settle in the same pass

- **Coverage gate is tight at the new roster sizes.** With 2-4 rules per bundle, a single missing metric drops available rule weight below the 0.70 gate and NaNs the bundle for that unit. Pre-existing for Industrial and Infrastructure (rosters unchanged), worsened for the two-rule bundles. Measure against real coverage before publishing rather than assuming it is benign. Overlaps `BL-0018`.
- **Cross-bundle inconsistencies that survive the cut**, both cases of the same physical value scoring differently by sector with no stated reason: `pr_consecutive_dry_days_lt1mm` impact band is 30-90 days in four bundles but 60-120 in Life & Livelihood; `pr_max_5day_precip` lens weights are .45/.40/.15 in five bundles but .40/.30/.30 in Life & Livelihood. Declare deliberate or reconcile. (`txx_annual_max` at 35-45 in Agricultural vs 40-45 elsewhere looks deliberate — crops fail below human-health thresholds — and is the model for how the others should read.)
- **Hazard-pressure, not risk.** Sector scores model the hazard determinant only: no exposure, no vulnerability, no adaptive capacity. Labels such as "Health Risk" must be read as climate hazard pressure relevant to that sector. Unchanged by this work, but the renaming question rides along with it.
- **Ensemble central estimate stays out of scope.** The builder reads the mean (`SUPPORTED_STAT = "mean"`); the methodology doc recommends the median. That is a tool-wide change that would move thematic scores too, and bundling it here would make moved scores un-attributable. Separate wave.
- **Expect test-fixture breakage.** Migrating Drought broke 12 tests, only 2 of which were about Drought; fixtures borrow whichever bundle was convenient.

#### Reference material

`docs/lens_scoring_methodology.md` (3,055 lines) is the governing document: section 2 the three lenses, 2.5 baseline reconciliation, 4 impact-band provenance policy, 5.1 the decomposition schema, 6-13 per-bundle dossiers (section 6 Health Risk is the worked template), 14 the deferred reverse extension of lenses to thematic bundles, 15 the 34 cited sources.

Related: `BL-0013` (threshold-heavy saturation), `BL-0016` (baseline semantics), `BL-0018` (partial coverage), `BL-0020` (zone-specific impact bands), `BL-0025` (absolute comparable composite score).

- `Done when`: the change-lens and redundancy decisions are recorded; `config/proposal_bundles.py` declares the Kind-A/Kind-B split explicitly rather than implying it; sector scoring reads the committed frozen rulers instead of `_score_by_reference_distribution`; blocks score against the district-fitted ruler without refit; per-lens decomposition columns are published; the Jensen gap (`|district_direct - area_weighted_block_rollup|`, threshold < 10) is reported before and after per bundle; one bundle is piloted end to end and physically read before the other seven are touched; masters and `state_values` are republished with `ruler_id` stamped in `bundle_manifest.json`; and `docs/lens_scoring_methodology.md`, the vendor data contract, `README.md` and `MANIFEST.md` record the changed meaning of the sector score columns.

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

### BL-0026 — Execute the coordinated dead and redundant code purge
- `Area`: code hygiene, hydro lean-down, Aqueduct tooling, repository maintenance
- `Why deferred`: the purge is fully planned but deliberately held for an isolated worktree and staged validation because it combines untracked-file disposition, caller-sensitive removal of inert hydro branches, and extraction of live Aqueduct helpers before deleting obsolete CLIs. Treating these as one coordinated initiative prevents partial cleanup from losing pending work or leaving dangling imports.
- `Dependency / trigger`: begin only after re-baselining the live SHA and classifying every untracked file. Preserve the pending exposure/hydro patch, map/design/performance specifications, block-placeholder test, and active boundary-audit diagnostics unless they are explicitly abandoned.
- `Plan`: follow `docs/dead_code_redundancy_purge_plan.md` (`CHG-0222` through `CHG-0227`). This umbrella item coordinates `BL-0023` and `BL-0024`; `build_blocks_geojson.py` remains methodology-deferred and is not a purge candidate.
- `Done when`: approved untracked removals have persistent per-file quarantine records, all hydro-residue hits are classified, downstream callers retain compatible interfaces, the Aqueduct hydro CLIs are removed only after the shared-helper import gate passes, focused and full-suite results show no new failures, documentation has no stale operational references, and graphify has been refreshed.

### BL-0027 — Standardize Context and Evidence source data: fetch, attribute, and publish national context rasters
- `Area`: context and evidence, exposure, hydrology, data provenance, geodata pipeline
- `Why deferred`: the Context and Evidence card (§5, `CHG-0399..0403`, committed `9f43211`) renders numbers the repository cannot currently attribute. Two of its four exposure sources are filename contracts with **no recoverable upstream provenance** — `Cleaned_India_Built_Surface_WGS84.tif` (built-up area and share) and `LULC_2_Agri.tif` (agricultural area and share). `README.md` and `MANIFEST.md` pin only where an operator should place each file, never who produced it, from what, under what licence, or for which epoch. `basins.geojson`, `subbasins.geojson`, and the river network share the same defect. The user confirmed on 2026-09-10 that the origin of both rasters is not recoverable, so they cannot be cited and must be replaced rather than documented. Acquiring national rasters, landing them on disk, and processing them into `processed_optimised`-shaped parquets is a substantial pipeline initiative in its own right and was explicitly deferred rather than started.
- `Dependency / trigger`: this is the next objective for the Context and Evidence module specifically; start when context-data work is the active priority. No hard blocker — the card already degrades correctly when an artifact is missing, so the tool ships without this. Do a live verification pass on release IDs, DOIs, and licence terms before writing any registry record; the assistant knowledge cutoff makes version claims unreliable from memory.
- `Approved layer set` (decided 2026-09-10; free-and-open download is a binding filter):

  | Slot | Layer | Source | Disposition |
  |------|-------|--------|-------------|
  | A1 | Population | WorldPop constrained UN-adjusted | keep, standardize the record |
  | A2 | Age structure (65+, under-5) | WorldPop age-sex structures | new, nice-to-have |
  | B4 | Roads and rail | OpenStreetMap | new — see the ODbL flag below |
  | B5 | Facilities | Mission Antyodaya | keep; pin the survey round and year |
  | C1 | Land cover **including built-up** | ESA WorldCover v200, 10 m | fetch fresh; replaces both unattributable rasters |
  | D1 | Basins / sub-basins | India-WRIS (verify this is the existing GeoJSON) | keep; attribution required |
  | D2 | River network | HydroRIVERS v1.0 | keep or refetch; attribution required |
  | D3 | Surface water | JRC Global Surface Water, 30 m | new, recommended |
  | D5 | Flood hazard | JRC CEMS-GloFAS v2.1 | keep — the one layer already cited properly |
  | D6 | Groundwater | CGWB | keep as-is; no scenario layer |

- `Deliberately rejected` (do not re-propose without new reasons):
  - **GHS-BUILT-S / GHS-BUILT-H (B1/B2)** — superseded by C1. WorldCover's 10 m built-up class covers the card's area and share fields; GHSL's advantages (continuous sub-pixel surface fraction, 1975–2030 change epochs) are unused because the card shows neither density nor change. Revisit only if built-up change over time becomes a requirement.
  - **Building footprints (B3)**, **cropland-specific products (C2)**, **irrigation (C3)**, **crop area/yield (C4)** — C1 answers the mapped question; C3/C4 are tabular and out of scope for a map card.
  - **HydroLAKES / GRanD (D4)** — a static inventory of *large* dams. Decisive against it: Telangana is tank country, with tens of thousands of small irrigation tanks absent from GRanD entirely but clearly visible in D3. On the pilot geography D4 would render near-empty.
  - **Terrain / Copernicus DEM GLO-30 (E)** — not decision-relevant today. Hillshade is cartography; slope needs a landslide bundle; low-elevation coastal zone needs a coastal bundle; elevation would explain rather than measure heat, which NEX-GDDP already carries. A 30 m national DEM is significant storage and processing that no current bundle consumes. Revisit if a landslide, coastal, or terrain-driven flood bundle is added.
  - **Economic activity (F)** — nighttime lights and gridded GDP, dropped as overkill.
  - **Degree of urbanisation (A3)** and **socio-economic vulnerability (A4)** — dropped for scope. Note that A2 and A4 are *vulnerability* rather than hazard; §5 forbids blending context into the bundle score, and these will be the first candidates someone tries to fold in.
  - **Scenario x period groundwater** — investigated and rejected on resolution grounds. ISIMIP (WaterGAP2, PCR-GLOBWB, CWatM, H08 under CMIP6/SSPs) is the credible option but resolves at 0.5 degrees, roughly 55 km — coarser than most Indian districts and far coarser than a block. Painting one recharge value across many blocks would repeat the indefensible-number failure mode that the national absolute scale work removed. GRACE/GRACE-FO offers an *observed* storage trend (~300 km) as a possible complement to CGWB's well points. **WRI Aqueduct 4.0 Future is the conventional answer and was deliberately retired** from this project (`CHG-0228`, and the clean-regen pivot) — reopening it is a user decision, not a side effect of this work.

- `Latent defect this closes`: `build_built_up_area_admin_masters.py` and `build_lulc_admin_masters.py` both use full canonical polygon area in EPSG:6933 as the share denominator but read **different rasters**, and no test anywhere constrains their sum. Nothing prevents `built_up_area_share_pct + lulc_agri_share_pct > 100%` for some unit. Deriving both classes from one classified raster (C1) makes the shares mutually exclusive by construction. Observed values are currently plausible (Jangaon: 1.1% + 93.8% = 94.9%) but unguarded.
- `Licence flag`: every approved layer is free to download, but OpenStreetMap (B4) is **ODbL**, not permissive, and share-alike obligations can attach to a derived database. That is a different question from free-to-download once the output ships to a vendor and onward to clients. If ODbL is unwelcome, B4 drops and roads come from Bhuvan or go unmapped. Everything else on the list is CC BY 4.0 or Government of India open data. Separately, the Survey of India village boundary layer is recorded in the sibling `visualize rivers` project as licence *"Unknown/restricted; internal planning only pending publication review"* — treat as a blocker, not a caveat, if it is ever proposed.
- `Reusable asset`: the sibling project `D:\projects\visualize rivers` already implements the source-registry schema IRT lacks — `sites/<key>/reports/source_registry.json`, `schema_version: 2`, recording per source `source_id`, `organization`, `dataset`, `version_or_reference_year`, `acquisition_date`, `resolution_or_scale`, `crs`, `tile_ids`, `checksum_sha256`, `url`, `official_page`, `licence`, `processing`, `attribution`, `limitation`, `status`, and an `audit` pointer. `status` uses the closed vocabulary `PRESENT` / `NOT_APPLICABLE` / `EXCLUDED_WITH_REASON`, with the rule that an empty query is never evidence of real-world absence. That project also carries working acquisition code for Copernicus DEM GLO-30 (`iwm_map_series/products/terrain_dem.py`) and GHSL tiles (`community_settlement.py`), including checksum and tile-ID handling, should either be revisited.
- `Suggested change set` (proposed 2026-09-10, none applied):
  - `CHG-0404` — `docs/source_registry_contract.md` (new): port the registry schema and status vocabulary as IRT's source contract.
  - `CHG-0405` — `india_resilience_tool/config/sources.py` (new): one machine-readable record per context source; single origin for the card footer, the technical note, and audits.
  - `CHG-0406` — `tools/diagnostics/build_overview_flow_prototype.py`: replace the five hand-typed `CONTEXT_PROVENANCE` strings (line ~517) with rendered registry records.
  - `CHG-0407` — `tools/geodata/build_*_admin_masters.py`: emit organization / dataset / version / licence / `checksum_sha256` alongside each master.
  - `CHG-0408` — `docs/technical_guidance_note.md`: give the exposure datasets the citation treatment JRC already has (JRC is cited as CEMS-GloFAS v2.1, DOI `10.2905/JRC.VD32YWG`; the exposure half has no equivalent).
- `Also note`: §5 requires State/UT basin shares to come from State-to-basin geometry intersection rather than counting districts' dominant basins. The prototype states this limitation honestly instead of computing it. Acquiring D1 with proper geometry is what would let that subsection exist at State scope.
- `Done when`: every layer in the approved set has a registry record with a verified version, licence, URL, and checksum; the two unattributable rasters are retired and replaced by C1-derived products; national rasters are on disk and processed into `processed_optimised`-shaped context parquets; `admin_exposure_summary.parquet` and `admin_hydro_summary.parquet` carry source, unit, source date/version, validity, and coverage per §5; the Context and Evidence provenance footer renders from the registry rather than hard-coded strings; a scientific-compute pytest covers the new aggregation and the built-up/agricultural share exclusivity; and `README.md` / `MANIFEST.md` describe each source by provenance rather than by expected filename.

## Icebox

- No items recorded yet.
