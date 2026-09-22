# India Resilience Tool (IRT) - Codebase Manifest

## Overview

IRT is a Streamlit-based climate-risk and resilience dashboard organized around admin geographies:

- **Admin**: district, block

The current working tree supports:
- a default climate-hazard landing / discovery surface that opens on an India state-level bundle map and drills down India -> state -> district before handing off to the detailed workflow
- a grouped dashboard bundle scope covering exact selector labels like `Thematic - Heat Risk` and `Sector-wise - Health Risk`
- thematic bundles for `Heat Risk`, `Heat Stress`, `Drought Risk`, `Flood & Extreme Rainfall Risk`, and `Cold Risk`
- sector-wise bundles for `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Investment / Financial Risk`, `Infrastructure Risk`, `Asset Risk (Thermal Power Plants)`, `Asset Risk (Hydropower Plants)`, and `Life & Livelihood Loss Risk`
- `Agricultural Risk` is the survivor agriculture bundle; the former `Agriculture & Growing Conditions` thematic bundle is retired and retained only as a legacy alias
- `Life & Livelihood Loss Risk` is available at district and block level when the persisted block proposal bundle master has been built
- declarative landing bundle weights in `india_resilience_tool/config/bundle_weights.py`, now used for all visible Glance bundles
- persisted visible-Glance composite metrics declared in `india_resilience_tool/config/composite_metrics.py` and optimized Glance view-model artifacts built offline from admin master files
- unified Glance `drivers.parquet` artifacts carrying state, district, and optional block-scoped metric/rule drivers for block drawer drill-downs
- Glance Rankings answer/export helpers that emit copyable prose, visible-row CSVs, and Excel answer packs from the same filtered ranking frame shown in the UI
- explicit state-click handling on the India overview map and validated district-click handling within state focus
- type-to-filter geography suggestions in the landing top bar that mirror the map drill-down flow
- a top-right deep-dive `Back to Glance` action that returns to landing mode using a reverse handoff, with Glance -> Deep Dive now opening the matching persisted composite metric
- map, rankings, and details flows for district and block
- drill-down-only nationwide behavior for the finest-grain views:
  - `Admin -> Block` requires a selected state
- portfolio workflows for district and block
- domain-based metric navigation under a single `Climate Hazards` pillar (exposure/groundwater domains retained for pipelines but hidden from UI nav)
- static exposure-layer support for admin district/block views
- static groundwater snapshot support for admin district views
- retained hydro boundaries (`basins.geojson`/`subbasins.geojson`) for admin-side hydrology context (basin-outline overlay, crosswalk context); the navigable Hydro spatial family and offline basin/sub-basin climate compute pipeline have been removed
- Aqueduct direct district/block masters for water stress, interannual variability, seasonal variability, and water depletion (the offline Aqueduct SOI basin/sub-basin masters are retained by their standalone builder but are no longer surfaced in the dashboard)
- population exposure masters for total population and population density on district/block units
- `population_exposure_2025_raster` display-only overlay support across admin map levels, backed by exported PNG/metadata artifacts rather than the raw TIFF
- rural facilities exposure masters for total/category counts and per-100k people rates on district/block units
- `rural_facilities_density` display-only overlay support across admin map levels, backed by exported category PNG/metadata artifacts
- groundwater district masters for extraction stage, future availability, extractable resource, and total extraction
- actionable polygon crosswalk context and related-unit highlighting from district/block to basin/sub-basin context
- shared reference overlay framework for the river network in admin district/block views sliced by selected district when the river artifact carries a `district_names_clean` column, plus the admin RP-100 flood-depth raster

The crosswalk layer is currently **read-optimized and explanatory**. It is not yet a full weighted transfer engine across spatial families.

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in  
**Primary stack:** Python 3.10+, Streamlit, Pandas, GeoPandas, Folium, Matplotlib, Plotly, Xarray

## Quick reference

### Main entry points

| Command | Purpose |
|---------|---------|
| `streamlit run main.py` | Launch dashboard from root entrypoint |
| `streamlit run india_resilience_tool/app/main.py` | Launch dashboard from package entrypoint |
| `python -m tools.docs.build_technical_note_html --help` | Build the committed self-contained `Read the Docs` dashboard HTML asset from `docs/technical_guidance_note.md`, approved figure callouts, and vendored KaTeX |
| `python -m tools.runs.prepare_dashboard --help` | Show the canonical dashboard-ready prep command for climate, persisted visible-Glance composites, Aqueduct, population, groundwater, state-scoped JRC flood depth, validation, and full package workflows, including level-aware climate readiness, optimized refresh, and final readiness verification |
| `powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all` | Refresh active admin dashboard climate bundles for district and block by computing only thematic and sector-wise source metrics, then rebuilding masters, composites, proposal bundles, optimized artifacts, precomputed area-weighted state headline values, and strict parity reports. Optional `-Bundle "<name>"` scopes the run to named dashboard bundle(s); compute is incremental (`--skip-existing`) by default with opt-in `-Overwrite`/`-OverwriteMetrics`; masters rebuild freshness-aware via compute completion markers; `-Workers` is opt-in; parity is audited once. Compute→master→composite run per-bundle with fail isolation (C2), then one union optimized+audit pass over the succeeded bundles; on any failure optimized+audit is skipped by default (non-zero exit) unless `-AllowPartialPublish` publishes the succeeded subset with a `*_partial_run.json` manifest. Per-task compute failures (CHG-0059; the compute CLI exits 0 on task — vs ensemble — failures) are surfaced as warnings, a RUN SUMMARY block, and a `*_compute_failures.json` sidecar without blocking publish; pass `-FailOnComputeError` to escalate any bundle with compute failures to a bundle failure |
| `powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra` | Refresh the full admin Riverine Flood bundle for one state from the default strict RP-100 source manifest (`D:/projects/irt_data/jrc_raw_new/source_manifest.json`): state-scoped JRC flood-depth masters, district + block `composite_flood_jrc_depth` masters, state-scoped optimized artifacts for the Riverine Flood dashboard metrics, and a strict scoped parity audit. `-PlanOnly` prints the exact commands without running them |
| `powershell -ExecutionPolicy Bypass -File tools/runs/rebuild_jrc_rp100_national.ps1` | Run the Flag C national RP-100 rebuild across every state not yet built against the strict source manifest (derived from each state's QA `run_summary.csv`; `-States` overrides, `-IncludeStrict` forces all 36). Logs per-state status/elapsed to CSV and continues past failures. In-place `-Overwrite` with no rollback — stop the Streamlit app first. `-PlanOnly` prints the exact per-state commands without running them |
| `python -m tools.pipeline.build_composite_metrics --help` | Build persisted district/block composite masters for active thematic dashboard bundles under the legacy `processed/` metric layout; use `--prune-retired --dry-run` to inspect retired composite cleanup |
| `python -m tools.pipeline.build_proposal_bundles --help` | Build persisted admin district/block proposal climate-risk bundle masters under `processed/<proposal_composite_slug>/<state>/` and the helper `r95p_interannual_variability` masters; all 8 sector-wise proposal bundles use explicit rule weights, lens-decomposed scoring, and an `available_rule_weight_fraction` coverage gate (0.70): Health Risk (lens dossier §6), Industrial Risk (CHG-0028, lens dossier §7), Investment / Financial Risk (CHG-0033, lens dossier §8), Infrastructure Risk (CHG-0034, lens dossier §9), Asset Risk (Thermal Power Plants) (CHG-0057/0058, lens dossier §10), Asset Risk (Hydropower Plants) (CHG-0036, lens dossier §11), Agricultural Risk (CHG-0032, lens dossier §12), and Life & Livelihood Loss Risk (CHG-0037, lens dossier §13). Each rule persists `{rule_slug}__{scenario}__{period}__{score,abs_score,chg_score,imp_score}` columns under the four-token master schema, but only active lenses are written (for example Thermal `spi3_low_flow_proxy_norm` and Investment `r99p_positive_trend` omit `__imp_score`; see `docs/lens_scoring_methodology.md` §5.1). Per-lens rule columns are part of the `processed_optimised/` contract from artifact version 3 onward |
| `python -m tools.pipeline.build_glance_view_model --help` | Build persisted optimized Glance view-model artifacts under `processed_optimised/context/glance/v1/{composite_slug}/{scenario}/{period}/`; normal dashboard prep gets these through `build_processed_optimised` |
| `python -m tools.optimized.build_processed_optimised --help` | Build the compact `processed_optimised` runtime bundle from the legacy `processed/` tree, with scoped `--overwrite`, optional exact-target `--prune-scope`, destructive `--full-rebuild`, optional admin `--state` scoping, opt-in shared-admin artifact rebuilds, optional scoped `--report-path`, optional admin `--level` filtering (`district`/`block`), `--workers` overrides, and nested terminal progress bars |
| python -m tools.optimized.audit_processed_optimised_parity --help | Audit processed_optimised with optional level/admin-state filtering, state-scoped report paths, and strict block yearly-model checks via require-block-yearly-models plus strict. Warning-severity issues (e.g. an absent precomputed `state_values` table) are non-fatal unless `--strict` |
| `python -m tools.optimized.build_state_values --help` | Precompute area-weighted state headline values per `(metric, scenario, period, stat)` into `processed_optimised/metrics/<slug>/state_values/admin/<level>/all_states.parquet`; parity with the live KPI by construction (shared canonical merge + `analysis.area_weighting`), enabling a real Position-in-India rank. Run automatically by `prepare_dashboard` between the optimized build and the parity audit |
| `python -m tools.diagnostics.wbgt_method_validation --self-test` | Stage A validation of the Tier-2 outdoor-WBGT method against hourly Liljegren physics on ARCO-ERA5, before any NEX download (CHG-0538) |
| `python -m tools.diagnostics.wbgt_deployed_vs_reference --self-test` | Compare IRT's deployed WBGT formulations against the Liljegren (outdoor) and Bernard (indoor) references recommended by Lemke & Kjellstrom (2012), separating formula error from daily-mean aggregation error (CHG-0546) |
| `python -m tools.diagnostics.wbgt_carbonplan_crosscheck --dry-run` | Cross-check IRT's published WBGT >= 32 C day counts against CarbonPlan extreme-heat v1.0 by distribution shape (CHG-0544) |
| `python -m tools.diagnostics.wbgt_era5_grid_download --dry-run` | Fetch one hourly ERA5 day over a lat/lon box from Copernicus CDS, supplying the gridded driver for the WBGT method comparison (CHG-0550) |
| `python -m tools.diagnostics.wbgt_era5_grid_compare --dry-run` | Score IRT's deployed WBGT formulas against Liljegren and Bernard per 0.25 deg cell, separating formula error from daily-mean aggregation error (CHG-0550) |
| `python -m tools.diagnostics.wbgt_era5_grid_maps --dry-run` | Render the gridded WBGT comparison as spatial maps: each IRT field beside its reference, the biases and their drivers (CHG-0551, CHG-0552) |
| `python -m tools.diagnostics.heat_stress_gridfirst_parity --help` | Compare legacy polygon-mean-first vs Heat Stress v2 grid-first CSV extracts for pilot diagnostics, including deltas, rank shifts, and top movers |
| python -m tools.diagnostics.list_optimized_yearly_metrics --help | List optimized metrics with yearly artifacts for a selected level/state; format args emits repeated metric flags for recovery runs |
| `python -m tools.pipeline.build_master_metrics` | Rebuild admin (district/block) master CSVs |
| python -m tools.pipeline.compute_indices_multiprocess --help | Show compute-pipeline options, including yearly-cleanup-policy {default,preserve,delete_after_ensemble} for per-model yearly CSV retention |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics <slug>` | Build district outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level block --metrics <slug>` | Build block outputs |
| `python -m tools.pipeline.build_spatial_weights --help` | Build private Heat Risk v2 grid-first spatial-weight caches under `processed/_internal/spatial_weights/`; the builder resolves defaults from the effective `--data-dir`, skips valid existing caches, and requires `--overwrite` to replace stale ones |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics spi3_max_spell_lt_minus1` | Build a Drought Risk v2 grid-first admin metric; the active bundle's six event/spell slugs and `spi3_count_months_lt_minus1` now use admin district/block grid-first paths |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics pr_max_1day_precip` | Build an Extreme Rainfall v2 admin grid-first metric; private annual grids plus `p95` / `p99` percentile thresholds are persisted under `processed/_internal/extreme_rainfall/` |

CHG-0038 scope note: `jrc_flood_depth_index_rp100` and `r95p_interannual_variability` remain unchanged and out of scope.
| `python -m tools.subbasin_shp_explore --help` | Inspect/repair/export hydro boundaries |
| `python -m tools.geodata.build_district_subbasin_crosswalk --overwrite` | Build district ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_block_subbasin_crosswalk --overwrite` | Build block ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_district_basin_crosswalk --overwrite` | Build district ↔ basin crosswalk CSV |
| `python -m tools.geodata.build_block_basin_crosswalk --overwrite` | Build block ↔ basin crosswalk CSV |
| `python -m tools.geodata.build_admin_boundaries_from_lgd --overwrite` | **Single-source admin builder**: derive `blocks_4326.geojson`, `districts_4326.geojson`, and `states_4326.geojson` from one bharatlas `LGD_Blocks` shapefile so all three nest exactly (`--dry-run` for roster/QA only) |
| `python -m tools.geodata.build_blocks_geojson --overwrite` | _(Superseded)_ Rebuild only the canonical block GeoJSON and block-label QA outputs from the legacy `Block_GH_WUP` source |
| `python -m tools.geodata.build_adm1_geojson --overwrite` | Build the compact optimized ADM1 state-polygons artifact for fast dashboard boot |
| `python -m tools.geodata.build_states_geojson --overwrite` | Build full-fidelity `states_4326.geojson` by dissolving district boundaries (shareable, not used at runtime) |
| `python -m tools.geodata.prepare_aqueduct_baseline --help` | Build the canonical clean Aqueduct baseline artifact and India-only future geometry subset from future geometry + baseline CSV |
| `python -m tools.geodata.build_aqueduct_admin_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ district overlap CSVs |
| `python -m tools.geodata.build_aqueduct_block_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ block overlap CSVs |
| `python -m tools.geodata.build_aqueduct_admin_masters --help` | Build Aqueduct district/block master CSVs on canonical admin units |
| `python -m tools.geodata.build_aqueduct_hydro_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ SOI basin/sub-basin overlap CSVs |
| `python -m tools.geodata.build_aqueduct_hydro_masters --help` | Build Aqueduct hydro master CSVs on SOI basin/sub-basin units |
| `python -m tools.geodata.build_population_admin_masters --help` | Build district/block population exposure master CSVs plus the display-only population exposure PNG/metadata overlay from the 2025 raster |
| `python -m tools.geodata.build_rural_facilities_admin_masters --help` | Build district/block rural facilities exposure master CSVs plus display-only density PNG/metadata overlays for total/agro/education/health/service categories |
| `python -m tools.geodata.build_built_up_area_admin_masters --help` | Build district/block built-up area exposure master CSVs plus the display-only built-up area PNG/metadata overlay from the cleaned built-surface raster |
| `python -m tools.geodata.build_lulc_admin_masters --help` | Build district/block agricultural LULC exposure master CSVs plus the display-only binary agricultural LULC PNG/metadata overlay |
| `python -m tools.geodata.build_worldpop_agesex_admin_masters --help` | Build district/block age-structure master CSVs (65+ and under-5 counts and shares) from the WorldPop 2025 age-sex bands |
| `python -m tools.geodata.build_lgrip_admin_masters --help` | Build district/block cropland master CSVs from LGRIP30 V001 via a generated no-resample VRT |
| `python -m tools.geodata.build_gsw_admin_masters --help` | Build district/block inland surface-water master CSVs from JRC Global Surface Water v1.4 occurrence, with marine water removed by a connectivity mask |
| `python -m tools.geodata.build_groundwater_district_masters --help` | Build district groundwater assessment master CSVs from the 2024-2025 GEC workbook |
| `python -m tools.runs.prepare_dashboard water-availability --overwrite` | Build the district-only Water Risk bundle end to end: NITI per-capita water-scarcity source masters, the `composite_water_risk` composite (absolute pre-scaled ordinal), optimized publish, state values, and parity audit |
| `python -m tools.geodata.clean_river_network --src <path> --overwrite` | Clean Survey of India river network into canonical river artifacts |
| `python -m tools.geodata.build_river_topology --overwrite` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts |
| `python -m tools.pipeline.enrich_river_network_districts [--dry-run]` | Spatial-join cleaned river display with districts and rewrite `river_network_display.geojson` in place with a `district_names_clean` column (drives admin-view river overlay; backs up original to `.bak` on first run) |
| `python -m pytest -q` | Run tests |

### Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in the UI |
| `IRT_DATA_DIR` | resolved in `paths.py` | Base directory for boundaries, crosswalks, and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Optional processed-root override |
| `IRT_PROCESSED_OPTIMISED_ROOT` | `IRT_DATA_DIR/processed_optimised` | Optional optimized runtime-bundle override |
| `IRT_DEBUG` | `0` | Enable debug/perf output |
| `IRT_GRIDFIRST_BBOX` | `1` (on) | Grid-first compute reads only the requested state's bounding-box slice of the climate grid before loading it into RAM (per-state memory fix). Set `0`/`false` to revert to the exact full-grid behavior. Wired for **all** grid-first families: Drought Risk, Heat Risk, Cold Risk, Heat Stress, and Extreme Rainfall \| Flash Flood Risk. Compute is admin-only (district/block). |
| `IRT_GRIDFIRST_BBOX_STRICT` | `0` (off) | When on, a failure to derive the bbox subset (e.g. missing boundary CRS) raises instead of silently falling back to the full grid. Use for acceptance runs so a green run cannot have quietly skipped the memory fix. No effect when `IRT_GRIDFIRST_BBOX=0`. |

## Top-level repo map

### Root files

| Path | Purpose |
|------|---------|
| `AGENTS.md` | Repo-wide agent instructions and workflow guardrails |
| `README.md` | Human-facing setup and usage guide |
| `MANIFEST.md` | AI/engineer-facing repo map and contracts |
| `main.py` | Root Streamlit entrypoint |
| `paths.py` | Canonical path and data-contract configuration |
| `environment.yml` | Canonical Conda environment |
| `environment.freeze.yml` | Reference environment snapshot |
| `requirements.txt` | Pointer/reference requirements file |
| `requirements.freeze.txt` | Freeze/export reference |
| `LICENSE` | License text |
| `resilience_actions_logo_transparent.png` | Branding asset used in the sidebar |

### Primary directories

| Path | Purpose |
|------|---------|
| `india_resilience_tool/` | Main application package |
| `assistant/` | Proposed architecture for the governed conversational data-analysis assistant plus isolated evaluation harnesses; evaluation code is not assistant runtime code |
| `tools/` | Operational, data-prep, pipeline, diagnostics, and geodata utilities |
| `tests/` | Main pytest suite |
| `docs/` | Handoffs, smoke tests, and repo/process notes |
| `notebooks/` | Exploratory notebooks and notebook-specific instructions |

Notes:
- `__pycache__/` directories are intentionally omitted below.
- Local logs, zips, and untracked working files are not treated as canonical repo modules.

### `assistant/`

| Path | Purpose |
|------|---------|
| `ARCHITECTURE.md` | Proposed governed IRT data-analysis assistant architecture and its explicit separation from external-product evaluations |
| `evaluations/cravis/` | CHG-0321 human-gated, `n=1` CRAVIS interaction driver and evaluation framework (Node 18 + existing Playwright dependency; independent of `qa/`) |
| `evaluations/cravis/cli.mjs` | Inert-on-import CLI exposing simulation, authentication capture, zero-send recon, one-prompt campaign execution, human review locking, and locked report generation |
| `evaluations/cravis/config/` | Fixed P01-P08 prompts and non-sent oracles, fourteen-dimension rubric, selector policy, and origin policy |
| `evaluations/cravis/lib/` | Pure hashing, validation, ledger, quota, redaction, download, CSV, scoring, review, reporting, reference-data, simulator, and browser workflow modules |
| `evaluations/cravis/fixtures/user-districts.csv` | Ten-row synthetic upload fixture for exact, alias, unmatched, duplicate, and blank district behavior |
| `evaluations/cravis/tests/` | Node tests for crash safety, DOM observation, quota proof, policy/security, scoring, locking, deterministic reports, and inert imports |

CRAVIS raw evidence, authentication state, campaign runs, reference downloads,
videos, screenshots, downloads, temporary state, and draft reports are local and
gitignored. Final report generation requires a complete, hash-valid P01-P08 human
review lock. The evaluator does not alter IRT methodology; CRAVIS RCP and IRT SSP
contracts remain distinct unless documented source support establishes a mapping.

Reference overlay contracts:
- `rp100_flood_depth_raster`: display-only RP-100 flood-depth raster overlay backed by `jrc_flood_depth/overlay/rp100_depth_overlay.png` and metadata, with optimized copies under `processed_optimised/context/`.
- The RP-100 overlay render layer may carry `OverlayRenderLayer.legend_html`, derived from static `RP100_FLOOD_DEPTH_BINS`, so the active reference overlay can share the existing map legend column.
- `build_rp100_flood_depth_legend_html()` builds the compact RP-100 legend from those bins; its swatches mirror the exporter `RP100_OVERLAY_COLORS` ramp: transparent `<=0`, then `#d6f0ff`, `#9dd9ff`, `#5bb7f0`, `#2f7fc1`, `#1d4f91`, and `#0f2f5f`.
- `population_exposure_2025_raster`: display-only India-wide population exposure overlay backed by `population/overlay/population_exposure_2025_overlay.png` and `population/overlay/population_exposure_2025_overlay_meta.json`, with optimized copies under `processed_optimised/context/population/overlay/`.
- Population overlay display semantics are binned people per source cell from `ind_pop_2025_CN_1km_R2025A_UA_v1.tif`; the canonical ramp is transparent for `<=0`, then `#fff7bc`, `#fee391`, `#fec44f`, `#fe9929`, `#ec7014`, `#cc4c02`, `#993404`, `#7f1d1d`, and `#4c0519` for values above 10000.
- `built_up_area_current_raster`: display-only India-wide built-up area overlay backed by `built_up_area/overlay/built_up_area_current_overlay.png` and `built_up_area/overlay/built_up_area_current_overlay_meta.json`, with optimized copies under `processed_optimised/context/built_up_area/overlay/`.
- Built-up source contract: operators should place `Cleaned_India_Built_Surface_WGS84.tif` at `IRT_DATA_DIR/built_up_area/`; timestamped download paths may be passed to the builder with `--raster` but are not hard-coded defaults.
- Built-up metric masters are `processed/built_up_area_km2/{state}/master_metrics_by_{district,block}.{csv,parquet}` and `processed/built_up_area_share_pct/{state}/master_metrics_by_{district,block}.{csv,parquet}` with columns `built_up_area_km2__snapshot__Current__mean` and `built_up_area_share_pct__snapshot__Current__mean`.
- Built-up raster values are interpreted as `m2/source cell`; `0` is valid no built-up and `65535` is invalid/background. The builder scans the raster before writes and fails national totals outside `43,000-220,000 km2` unless `--allow-total-outlier` is supplied.
- Built-up tabulation happens in the source raster CRS by reprojecting admin vectors to the raster CRS; the raster is not reprojected for metrics. Edge cells use `all_touched=False`.
- Built-up area share uses full polygon area in `EPSG:6933`: `built_up_m2 / polygon_area_m2 * 100`. QA also records raster-supported area denominators, but those do not replace the canonical geometry-based share.
- Built-up v1 is excluded from composites, proposal bundles, and bundle weights.
- Built-up overlay display semantics are binned `m2/source cell` in an EPSG:3857 PNG; bins are transparent `0`, then `0-100`, `100-500`, `500-1000`, `1000-2500`, `2500-5000`, and `>5000 m2/cell` with colors `#edf8fb`, `#b2e2e2`, `#66c2a4`, `#2ca25f`, `#006d2c`, and `#00441b`.
- `lulc_agri_current_raster`: display-only India-wide agricultural LULC overlay backed by `lulc/overlay/lulc_agri_current_overlay.png` and `lulc/overlay/lulc_agri_current_overlay_meta.json`, with optimized copies under `processed_optimised/context/lulc/overlay/`.
- LULC agriculture source contract: operators should place `LULC_2_Agri.tif` at `IRT_DATA_DIR/lulc/`; alternate paths may be passed to the builder with `--raster` / `--lulc-raster`.
- LULC agriculture metric masters are `processed/lulc_agri_area_km2/{state}/master_metrics_by_{district,block}.{csv,parquet}` and `processed/lulc_agri_share_pct/{state}/master_metrics_by_{district,block}.{csv,parquet}` with columns `lulc_agri_area_km2__snapshot__Current__mean` and `lulc_agri_share_pct__snapshot__Current__mean`.
- LULC raster values are interpreted as binary classes: only `1` is agricultural LULC; `0` is nodata/background and is never treated as an explicit non-agriculture class. The builder scans the raster before writes and rejects values outside `{0, 1}` unless `--allow-unexpected-values` is supplied.
- LULC tabulation reads the source through a nearest-neighbor `EPSG:6933` equal-area `WarpedVRT`; area is `agri_cell_count * abs(transform.a * transform.e)`. Edge cells use `all_touched=False`.
- LULC agriculture share uses full polygon area in `EPSG:6933`: `agri_area_m2 / polygon_area_m2 * 100`. QA records raster extent support area, support coverage, low-coverage flags, and `share_out_of_range`.
- LULC guardrails fail national agriculture totals outside `1,200,000-2,300,000 km2` unless `--allow-total-outlier` is supplied, and fail district/block shares above `100.01%` unless `--allow-share-outlier` is supplied.
- LULC v1 is excluded from composites, proposal bundles, and bundle weights.
- Water Risk (`group="water"`) is a district-only thematic snapshot bundle from the NITI Aayog ICED per-capita water-availability dataset. Masters are `processed/{water_scarcity_percapita,water_scarcity_percapita_2050,water_scarcity_deterioration_2050}/{state}/master_metrics_by_district.{csv,parquet}` with columns `<slug>__snapshot__Current__mean`. Values are integer ordinal codes: scarcity `1..4` (1=No Stress … 4=Absolute scarcity, higher worse); deterioration is a `0..3` delta of class steps (2050 code − 2025 code), **not** a scarcity class — it uses its own delta labels and renders label-only (bare label, no numeric suffix).
- The `composite_water_risk` composite scores the 2025 scarcity class with an **absolute pre-scaled ordinal** normalization (fixed classes `1..4` → `0/33.3/66.7/100`), not the default per-period relative min-max — so the same class scores identically in every state and is cross-state comparable. This is methodology-impacting and intentional; the mode raises if any scored component lacks contiguous integer `class_labels`. The 2050 projection and deterioration ride as inline glance attributes (weight 0).
- Water Risk surfaces under the **Climate Hazards** pillar for display, but the water metrics are externally sourced snapshots and are excluded from climate compute by the positive contract `is_climate_compute_metric(spec)` (`= source_type=="pipeline" and selection_mode=="scenario_period"`); the climate pipeline resolves its metric scope through that contract, and composites/derived snapshots are built by their own steps.
- LULC overlay display semantics are binary class colors in an EPSG:3857 PNG: transparent for `0`, `#2ca25f` for `1`.
- `rural_facilities_density`: display-only category-selectable rural facilities density overlay backed by `rural_facilities/overlay/rural_facilities_density_<category>_overlay.png` and metadata, with optimized copies under `processed_optimised/context/rural_facilities/overlay/`. Artifacts are written per real category (`agro`, `education`, `health`, `service`); the `total` UI selection is virtual and is rendered at runtime by stacking the four per-category layers.
- Each real-category PNG uses a distinct single-hue ramp (agro=greens, education=blues, health=reds, service=oranges) so the four layers stay distinguishable when stacked under `total`.
- Rural facilities overlay display semantics are facilities per 1,000 km2 on an EPSG:6933 10 km grid reprojected to EPSG:3857 for Leaflet.
- The Folium map registers per-category panes (`irt-rural-facilities-density-{agro,education,health,service}`); the active overlay layer carries a `legend_html` colorbar that is stacked into the map legend column.
- Dashboard runtime reads exported overlay artifacts only; it does not read source TIFFs.

Aqueduct methodology note:
- `docs/aqueduct_onboarding_methodology.md` is the canonical narrative for Aqueduct cleanup, HydroSHEDS `pfaf_id` normalization, direct `pfaf_id -> district/block` transfer, and HydroSHEDS → SOI hydro transfer.
- that same doc now includes a short reader guide for interpreting the Aqueduct validation bundles under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/`
- `docs/aqueduct_field_contract.md` records the currently used Aqueduct source-field mappings and interpretation notes for the onboarded Aqueduct district, block, and hydro metrics.
- `docs/command_catalog.md` is the canonical operator-facing command catalog for dashboard prep, Aqueduct, climate hazards, population exposure, groundwater, and validation workflows.

## Package inventory

### `india_resilience_tool/analysis/`

| File | Purpose |
|------|---------|
| `area_weighting.py` | Streamlit-free area-weighted state aggregation: `with_area_weights` (attach `__area_m2`, prefer `area_m2`, lazy geodesic fallback) and `weighted_state_mean`→`(value, n_units)` from one mask. Single definition shared by `state_summary_view` and `build_state_values` |
| `bundle_scores.py` | Streamlit-free landing bundle-score normalization, aggregation, and driver helpers |
| `frozen_rulers.py` | Frozen national CDF rulers: `MetricRuler` (apply/clamp), exact pooled mid-rank fit, `FrozenRulerSet` with slice-grid validation, and `save_ruler_set`/`load_ruler_set` for the committed artifact under `config/frozen_rulers/` |
| `__init__.py` | Package marker |
| `map_enrichment.py` | Streamlit-free map enrichment helpers: baseline/delta, ranking, tooltip prep |
| `metrics.py` | Risk-class and percentile/ranking helpers |
| `portfolio.py` | Portfolio comparison logic and portfolio-level data prep |
| `timeseries.py` | Yearly series loading for admin district/block flows |

### `india_resilience_tool/app/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `_ui_text.py` | Shared UI label/caption text constants |
| `adm2_cache.py` | Streamlit-cached ADM2 loading and FeatureCollection helpers |
| `case_study_runtime.py` | Runtime helpers for district-focused case-study export |
| `color_range_controls.py` | Robust color-range default calculation for maps |
| `crosswalk_runtime.py` | App-layer crosswalk navigation and overlay-state helpers |
| `dashboard_bundle_runtime.py` | Runtime helpers for dashboard bundle visibility and composite-source lookup |
| `details_runtime.py` | Right-panel orchestration and data prep for details views |
| `geo_cache.py` | Streamlit-cached admin geometry loading/builders plus retained basin/sub-basin context geometry for the crosswalk reference overlay |
| `geography.py` | Filesystem-backed admin geography discovery helpers |
| `geography_controls.py` | Sidebar geography + analysis-focus controls for admin district/block |
| `glance_exports.py` | Streamlit-free Glance Rankings answer, CSV, and Excel answer-pack helpers |
| `help_text.py` | Tooltip/help-text helpers for ribbon widgets |
| `hydro_boundary_overlay.py` | Admin-side dominant-basin outline overlay: reads `basins.geojson`/`subbasins.geojson` directly to draw the retained hydrology-context outline on district/block maps |
| `landing_runtime.py` | Climate-hazard landing/discovery orchestrator that loads persisted optimized Glance view models only, plus state transitions and Deep Dive handoff |
| `left_panel_runtime.py` | Left-panel orchestration for map vs rankings |
| `main.py` | Package Streamlit entrypoint |
| `assets/` | Packaged committed HTML assets, including self-contained `read_the_docs.html`, which opens directly in a browser offline and also backs the Read the Docs dashboard view |
| `map_layer_runtime.py` | Streamlit-free Folium layer construction using cached FeatureCollections |
| `map_pipeline.py` | Merge -> enrich -> colors -> map/rankings pipeline, including fine-grain drill-down guards and rankings-only fast paths |
| `master_cache.py` | Streamlit session-state cache for master CSV + schema loading |
| `master_freshness.py` | Master CSV freshness/rebuild gating helpers |
| `overlays.py` | Shared reference-overlay framework: session keys, visibility/availability, RP-100 artifact validation/discovery, and river/flood render-layer contracts |
| `perf.py` | Lightweight timing/performance instrumentation |
| `point_selection_ui.py` | Coordinate input, preview, and saved-point support |
| `portfolio_multistate.py` | Multi-state portfolio helper functions |
| `portfolio_state_runtime.py` | Session-state wrappers around portfolio operations |
| `portfolio_ui.py` | Portfolio right-panel UI and comparison workflows |
| `ribbon.py` | Metric selection ribbon, master loading, and admin-master readiness checks |
| `runtime.py` | Canonical app orchestrator (`run_app`) |
| `sidebar.py` | Family/level/view selector widgets and jump-once helpers |
| `sidebar_branding.py` | Sidebar logo/branding render block |
| `state.py` | Session-state defaults, level constants, and level-aware helpers |
| `summary_cache.py` | Streamlit session-state cache for admin district/block context summaries (exposure/hydrology context cards) |

#### `india_resilience_tool/app/views/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `context_cards.py` | Render admin district/block context cards, including the retained Hydrological Context card and exposure context |
| `details_panel.py` | Render the single-unit details panel and crosswalk context/actions |
| `map_view.py` | Render Folium map and extract level-aware click payloads, including landing state clicks |
| `rankings_view.py` | Rankings table rendering and portfolio add flows |
| `read_the_docs_view.py` | Render the committed Technical Guidance Note HTML asset via `components.html`, including packaged-asset resolution and theme stamping helpers |
| `state_summary_view.py` | State summary view for admin-focused overview flows |

### `india_resilience_tool/compute/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `composite_metrics.py` | Streamlit-free builders for persisted district/block composite Glance metric masters; frozen-ruler composites publish the ruler's complete declared slice grid (Heat Risk: historical baseline plus six future pairs) and fail on an incomplete component grid |
| `glance_view_model.py` | Streamlit-free builder for persisted optimized Glance district/state scores, drivers, attributes, and distributions |
| `proposal_bundles.py` | Streamlit-free builders for persisted proposal climate-risk bundle masters plus the `r95p_interannual_variability` helper masters |
| `master_builder.py` | Build admin district/block master CSVs plus Parquet companions for runtime serving |
| `spi_adapter.py` | SPI adapter around `climate-indices` |
| `gridfirst_spatial.py` | Shared grid-first spatial overlap and NetCDF/sidecar cache helpers used by Heat Risk v2 and Drought Risk v2. Includes `subcell_idw_fill` (CHG-0305): a post-aggregation inverse-distance fill for sub-grid-cell polygons (e.g. Lakshadweep coral atolls) that overlap only ocean-masked NaN cells — fires only when a unit is smaller than half its overlapping cell (in EPSG:6933) **and** has no finite cell; base aggregators stay byte-identical. Filled values are flagged in-data via a per-(unit, metric) `climate_fill_method` master column (`native`/`idw`), threaded through the pipeline period roll-up, `master_builder`, `compute_composite_master_frame` (idw iff any component is idw), and the optimised publish whitelist. See `docs/subcell_climate_fill_methodology.md` |
| `heat_stress_gridfirst.py` | Heat Stress v2 grid-first Twb and tropical-night metrics for admin district/block outputs, with private annual cell caches under `processed/_internal/heat_stress/grid_metrics/`; shared TN90p/WSDI remain in Heat Risk v2 |
| `drought_risk_gridfirst.py` | Drought Risk v2 grid-cell SPI, annual count/spell metrics, period rollups, NaN-aware polygon aggregation, and private cache helpers. Includes `load_or_build_monthly_cube` (CHG-0108): a per-`(model, scenario, grid_id, year-span)` monthly-precip-cube disk cache under `processed/_internal/<drought-root>/monthly_cube/<model>/<grid_id>/<scenario>/<min>-<max>/pr_monthly.nc`, keyed by `DROUGHT_MONTHLY_CUBE_METHOD_VERSION` + a union (baseline∪scenario) input-file hash + `index_range`; it memoizes the shared `concat_years`+`daily_to_monthly_totals` load that all SPI scale slugs of a unit otherwise rebuild redundantly |
| `evapotranspiration.py` | Hargreaves-Samani PET and extraterrestrial-radiation arithmetic, unit conversion and aridity classes. Consumed by the P/PET path in `drought_risk_gridfirst.py`: monthly PET caching, annual precipitation/PET ratios, coverage-gated period means and admin aggregation. PET masks inverted temperature pairs (`tasmax < tasmin`); version-2 caches and completion markers invalidate earlier clipped-range outputs. Registered as `aridity_index_p_over_pet`; not yet incorporated into the published drought composite. |
| `extreme_rainfall_gridfirst.py` | Extreme Rainfall / Flash Flood v2 admin grid-first Rx1day, Rx5day, R20mm, R95p, R95pTOT, CWD, and CDD (CHG-0029) compute with private annual grid and threshold caches |

#### `india_resilience_tool/compute/tests/`

| File | Purpose |
|------|---------|
| `test_spi_adapter.py` | SPI adapter tests |

### `india_resilience_tool/config/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `bundle_weights.py` | Declarative landing bundle weights used for all visible Glance bundle score aggregations |
| `composite_metrics.py` | Declarative visible-Glance bundle -> persisted composite metric mapping and helpers |
| `dashboard_bundles.py` | Declarative dashboard bundle catalog: ordering, grouped selector labels, canonical bundle names, and composite-slug mapping |
| `proposal_bundles.py` | Declarative proposal climate-risk bundle specs, exact rule order, and validation helpers for the offline proposal-bundle builder |
| `constants.py` | UI, styling, scenario, and geometry-render constants, including `ADM2_MIN_AREA`/`ADM3_MIN_AREA` (boundary-load min-area thresholds shared by `app/geo_cache` and the offline state-value precompute for parity) |
| `metrics_registry.py` | Canonical metric, pillar, and domain registry |
| `paths.py` | Library-side path config mirroring root `paths.py` |
| `variables.py` | Dashboard-facing variable registry derived from metrics registry |

### `india_resilience_tool/data/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `adm2_loader.py` | District boundary loading, normalization, and FeatureCollection builders |
| `adm3_loader.py` | Block boundary loading and normalization |
| `admin_coverage.py` | Admin district/block feature-key coverage helpers |
| `crosswalks.py` | Polygon crosswalk validation and context builders for district/block → basin/sub-basin context (basin/sub-basin retained only as read-optimized admin context) |
| `discovery.py` | Processed-artifact discovery helpers for yearly files and outputs |
| `exposure_summary.py` | Streamlit-free exposure-context summary builders for admin district/block cards |
| `hydro_loader.py` | Basin/sub-basin geometry loading, validation, keys, and render simplification for the retained admin basin-outline overlay and crosswalk context |
| `hydro_summary.py` | Streamlit-free hydrology-context summary builders for the admin district/block Hydrological Context card |
| `river_loader.py` | Cleaned river-display loading, validation, reconciliation, diagnostics, and district-slice filtering helpers for the admin river overlay |
| `river_topology.py` | Streamlit-free river reach validation and river summary builders |
| `master_columns.py` | Streamlit-free master column resolution helpers |
| `master_loader.py` | Robust master-table loading, normalization, schema parsing, and Parquet-first runtime preference |
| `optimized_bundle.py` | Path helpers and compact-contract helpers for the `processed_optimised` runtime bundle, including optimized geometry, context, and Glance view-model paths, plus `optimized_state_values_path[_from_metric_root]` for the precomputed area-weighted `state_values/admin/<level>/all_states.parquet` table (kept out of `is_optimized_metric_root`'s child list) |
| `source_inventory.py` | Persistent raw NetCDF inventory shards for source discovery, validation, engine reuse, and marker-invalidating source signatures |
| `merge.py` | Boundary ↔ master merge helpers for district and block |
| `spatial_match.py` | Click/selection matching helpers for admin flows |

### `india_resilience_tool/utils/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `naming.py` | Name normalization, aliasing, and join-key helpers. Folder/file tokens (`safe_fs_component`/`hydro_fs_token`) strip Windows-illegal trailing dots/spaces so processed dir components are Win32-traversable (a trailing `.` makes `pathlib.glob` raise `WinError 3`). |
| `processed_io.py` | Lightweight Parquet/CSV I/O helpers for processed outputs |

### `india_resilience_tool/viz/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `charts.py` | Chart and figure generation for details and portfolio flows |
| `colors.py` | Color scales, legends, and map-color helpers |
| `exports.py` | PDF/ZIP export helpers |
| `folium_featurecollection.py` | Streamlit-free FeatureCollection patching/filtering helpers |
| `formatting.py` | Numeric/text formatting helpers |
| `style.py` | Shared plotting/style helpers |
| `tables.py` | Rankings and comparison table formatting/builders |

## Tools inventory

### `tools/`

| File | Purpose |
|------|---------|
| `AGENTS.md` | Tooling-specific agent instructions |
| `README.md` | Tooling overview and command reference |
| `__init__.py` | Package marker |
| `docs/` | Offline documentation asset generators and vendored dependencies for committed dashboard docs |
| `subbasin_shp_explore.py` | Inspect, repair, and export canonical hydro boundaries |

### `tools/data_acquisition/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `download_era5_daily_stats_structured.py` | Download structured ERA5 daily stats from CDS |
| `prepare_jrc_rp100_source.py` | Prepare and finalize a JRC v2.1.2 RP-100 source inventory/manifest from a local official `tile_extents.geojson` or explicit tile filename list. Default mode selects India-intersecting tiles from the canonical boundary union buffered by one native pixel (`1/1200°`), records boundary SHA-256 and official URL metadata, and writes `source_inventory.json` / planned `source_manifest.json` without downloading rasters. `--finalize` validates already-downloaded official `RP100/*.tif` files against the inventory, checks CRS/resolution/nodata/bounds plus full blockwise readability, writes `RP100_depth.vrt` and explicit `RP100_tile_coverage.vrt`, and replaces the manifest with `acquisition_status: validated` |
| `nex_india_subset_download_s3_v1.py` | Download NEX India subsets from S3 (serial; retained as fallback) |
| `nex_india_subset_download_s3_v2.py` | Parallel pan-India NEX-GDDP-CMIP6 downloader. Four-layer design (CLI/config → scope-cached S3 manifest discovery → local skip/verify/quarantine policy → bounded ThreadPoolExecutor with atomic writes and classified retries). Writes to `${out_dir}/${member_dir}/...` with `member_dir` defaulting to `r1i1p1f1_panIndia` — distinct from the compute-pipeline-consumed `r1i1p1f1/` root. `_v1.py` and `download_pan_india_raw.sh` are unchanged; rewiring downstream pipeline tools to consume the pan-India root is a separate future change. |

### `tools/data_prep/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `derive_hurs_from_era5_tas_tdps.py` | Derive relative humidity from ERA5 tas + dew point |
| `organize_era5_legacy_nc_files.py` | Reorganize legacy ERA5 NetCDF files |
| `prepare_reanalysis_for_pipeline.py` | Prepare ERA5/IMD reanalysis data for compute pipeline ingestion |

### `tools/diagnostics/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `debug_build_master.py` | Diagnose master-building issues |
| `heat_risk_national_ruler_pilot.py` | **Read-only** national-absolute-scale pilot (CHG-0346, hardened CHG-0349..0360): scores the thematic Heat Risk composite for all districts against two candidate frozen national rulers — `linear` (pooled p1..p99, clipped) and `cdf` (exact pooled empirical mid-rank CDF; the 21-knot grid is retained only as a measured approximation) — pooled over 7 scenario/period slices, replacing the production per-state min-max. District level only. Coverage is measured against the canonical district roster read from the geometry property tables with stdlib `json` (state x metric x slice + national totals); the same roster supplies `area_m2` for area-weighted state means, so P-12 does not depend on geopandas, and the run fails unless `--allow-missing-geometry` when the roster is absent, a requested state has no geometry shard, or any retained district lacks a finite `area_m2 > 0`. Emits district/state score tables, per-slice spread + saturation + clamping summaries, ruler-fit specs, long-form exact CDF support, a metric fit report, a coverage report, a roster reconciliation (always written; separates "no master row" from "master rows but no finite value") and national choropleth panels under `--out-dir` only. Evidence for `docs/national_absolute_scale_pitfalls.md` |
| `build_heat_risk_frozen_map.py` | **Read-only** viewer build (CHG-0368): renders the *decided* Heat Risk configuration as one self-contained HTML file with scenario and period selectors and nothing else. Ruler frozen to `cdf`, headline frozen to `composite_absolute_threshold`, domain frozen to `0-100`, ramp frozen to the vendored NCL `WhiteBlueGreenYellowRed` table sampled from fraction 0.045. Re-scores nothing; reads the pilot's `district_scores.csv` and embeds simplified district geometry as SVG path data, so the output has no network dependency. Writes `docs/diagnostics/heat_risk_pilot/heat_risk_frozen_map.html` |
| `score_telangana_blocks.py` | **Read-only** block scoring for one State/UT on the *already fitted* frozen Heat Risk ruler (CHG-0391). Refits nothing: reconstructs the exact pooled mid-rank `cdf` rulers from the pilot's own `cdf_support.csv` plus orientations from `metric_ruler_spec.csv`, then reuses the pilot's `load_national_long_frame(level="block")` and `score_national_frame` unchanged. Scores all 14 bundle metrics; the split into the frozen headline `composite_absolute_threshold` (9 absolute metrics, weights renormalized 0.633 → 1.000) happens inside the shared scorer. Supports `--dry-run`. Writes `docs/diagnostics/heat_risk_pilot/telangana_block_scores.csv` (588 blocks × 7 slices, zero gaps) |
| `build_overview_flow_prototype.py` | **Read-only** workflow prototype build (CHG-0385): renders the Overview flow of `recommended_target_workflow.md` §0-8 as one self-contained HTML page for vendor implementation (the flow itself is §0, added CHG-0489; §1-8 hold the clause detail). This generated page now **lags §0 in two respects**: it does not rank a district's blocks, and it predates the §11 naming. The shell-faithful `irt_target_prototype.html` is the page that demonstrates the finalised flow — India view (districts painted, State/UTs ranked, State/UT means binned) → State view (the live State/UT's blocks painted, its districts ranked and binned) → District view (that district's blocks painted, ranking and distribution inherited from the parent State/UT because blocks are never ranked) → block inspection → Detailed Analysis stub. Its persistent Compare locations tray (CHG-0412..0417) compares up to four mixed-level places at one live slice or up to four independently pinned scenario-period slices for one fixed subject; ranks remain visibly scoped, Blocks are never ranked, and map emphasis yields to the active selection. Reuses the frozen ruler, headline, domain and ramp of `build_heat_risk_frozen_map` and re-scores nothing. Computes area-weighted State means, competition ranks, ten fixed bins and per-level drivers from the pilot's `district_scores.csv` plus canonical `area_m2`. Reads block scores from `score_telangana_blocks.py`. Hover reports the unit a view can *select*, not the unit it paints (CHG-0396): nationally a painted district reports and highlights its State/UT, and the State view inherits that — a painted block reports its parent district and highlights all that district's blocks; only in the District view, where the block is the selectable unit, does a hover report the block. The colourbar carries both §A10 mitigations for a fixed domain (CHG-0393): an always-on bracket marking the range present in the current view, and an opt-in `Local contrast` view that stretches the map fill only to the extent of the painted units — recomputed every render, never stored, cleared on return to India. The breadcrumb sits in the map's top-right corner. A collapsed `Context and Evidence` card (CHG-0399..0402) appears from the State view onward, scoped to the most local selected unit, from `processed_optimised/context/admin_exposure_summary.parquet` and `admin_hydro_summary.parquet`; both are optional and a missing or malformed one omits its subsection rather than raising. State/UT exposure is aggregated from districts under the workflow's rules — counts and areas summed, every share and per-capita rate recomputed from State/UT totals against the same canonical `area_m2` that weights the headline, never averaged over district percentages. Hydrological context is deliberately absent at State/UT scope, because a correct State/UT basin share needs a State-to-basin geometry intersection rather than a count of districts' dominant basins. Three departures are surfaced in the page itself: only Telangana opens below the national view, since no block outside it is scored; Detailed Analysis is a stub (though the state it carries, including the selected driver metric, is real); and Context and Evidence carries no basin or river map overlay, though a population overlay is implemented: proportional circles follow the painted unit on one frozen national scale, sized by population and shaded by population density read from processed_optimised/metrics/population_density/masters/admin/{district,block}/ on five fixed breaks (100 / 400 / 1,000 / 4,000 people per km²) that no data refresh moves; a unit with no density figure is drawn hollow rather than pale, so absence is never readable as low density. A second context layer paints cropland share as a neutral dot stipple superimposed on the unmodified score choropleth — three grain levels on fixed breaks (35% / 75%), the lowest painting nothing so "not agricultural" reads as absence; the pattern tile is divided by the viewBox zoom each render so the grain stays constant in screen space, and the layer is pointer-events: none so hit-testing still reaches the choropleth. Its source (lulc_agri_share_pct) is flagged provisional in the page pending the BL-0027 provenance replacement. Writes `docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html` |
| `wbgt_method_validation.py` | **Stage A validation of the Tier-2 outdoor-WBGT method (CHG-0538)**, run before any NEX-GDDP-CMIP6 download commits to the approach. Reads nothing under `IRT_DATA_DIR` and touches no config, master or bundle. Both sides of the comparison are driven from the **same** hourly ARCO-ERA5 point series (`gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3`, anonymous access, no CDS registration), so climate-model error cancels and what is measured is purely the cost of having only daily inputs: the reference is hourly Liljegren (2008) WBGT via `thermofeel` (verified against 2.3.0, whose documented convention is **pressure in hPa, `ssrd` as instantaneous W/m2, and `fdir` as a dimensionless 0-1 direct-beam fraction** — a wrong convention returns NaN rather than raising, so the harness aborts above a 5 % non-finite rate and `--self-test` pins three reference cases with a known qualitative ordering), reduced to a daily maximum; the candidate collapses the same hours to the daily aggregates a CMIP6 archive actually ships (daily max temperature, daily mean relative humidity, wind and downwelling shortwave) and runs the Tier-2 chain over them. That chain imports `wbgt_shade_stull_cell_c` from `india_resilience_tool/compute/heat_stress_gridfirst.py` rather than re-transcribing it, so the harness grades production code; on top of it sits a CarbonPlan `extreme-heat`-style additive sun adjustment (`-2.1564 - 0.005375*rsds_max + 1.0424*sfcWind`, R2 0.92, refit locally from that project's training points) whose inputs are **clipped to the fit domain** (300-900 W/m2, 0.5-3.0 m/s) because the linear model has no support beyond it. Daily-mean `rsds` is disaggregated to a peak hourly value by top-of-atmosphere solar geometry — the essential assumption of metsim's `solar_geom`+`shortwave`, implemented inline so metsim is not a dependency — then scaled by `--peak-lag-factor` (default 0.75) because WBGT peaks a few hours after solar noon. Six sites span India's distinct heat regimes: Kochi (hot-humid coastal), Kolkata (monsoon delta), Bikaner (hot-dry Thar), Lucknow (Gangetic pre-monsoon), Hyderabad (semi-arid inland), Shimla (elevation). **Acceptance was fixed before any result was inspected: median |bias| < 1.0 C and RMSE < 1.5 C against hourly Liljegren in every regime**, evaluated on warm days (shade WBGT >= `--warm-day-threshold`, default 25 C) because that is the regime the metric is read in; the process exits 1 if any site misses, and the rendered verdict states the agreed consequence — one regime missing triggers a refit against the real Indian joint distribution, a broad miss falls back to publishing Stull shade WBGT alone under an honest name. `--self-test` verifies the Stull (2011) worked example (20 C / 50 % RH -> 13.70 C), WBGT bracketing, solar geometry and adjustment-domain clipping with no network and no optional dependencies. `--dry-run` exercises the entire chain on synthetic forcing with a stand-in reference that is **not Liljegren physics**; every output it produces is stamped as such and must not be read as a result. A real run needs `gcsfs`, `zarr` and `thermofeel`, none of which are geo/PROJ packages. Writes only under `--out-dir` (default `docs/diagnostics/wbgt_method_validation`): `wbgt_method_validation_daily.csv`, `..._summary.csv`, `..._summary.md` |
| `wbgt_deployed_vs_reference.py` | **Comparison of IRT's deployed WBGT formulations against the Lemke & Kjellstrom (2012) reference methods (CHG-0546).** Lemke & Kjellstrom recommend Liljegren et al. (2008) for outdoor WBGT and Bernard et al. (1999) for indoor WBGT at population level; IRT ships neither, so this harness quantifies what that costs. All four quantities per site-day are driven from one hourly ERA5 point series fetched from the Open-Meteo archive API (no key, no CDS registration, ~1 s per site-year, measured 0.0000 NaN rate) so that climate-model error cancels. The reference columns are hourly Liljegren via `thermofeel` reduced to a daily maximum (outdoor) and hourly Bernard reduced to a daily maximum (indoor). The candidate columns are each shipped formula — `0.7*Twb_Stull + 0.3*Ta` and `0.567*Ta + 0.393*e + 3.94` — evaluated **twice**: hourly then daily-max, which isolates *formula* error, and on daily-mean `tas` and `hurs`, which is what `heat_stress_gridfirst` actually receives and therefore carries *formula + aggregation* error. The difference between those two rows is the scientific point of the file. Bernard's psychrometric wet bulb is obtained by solving the Lemke & Kjellstrom energy balance `c4*ed - c5*ed*T - c4*es(T) + c5*es(T)*T + c6*(Ta-T) = 0` (c1..c6 = 6.106, 17.27, 237.3, 1556, 1.484, 1010) with vectorised bisection on [Td, Ta], where the root is provably bracketed because `f(Td) = c6*(Ta-Td) >= 0` and `f(Ta) = (c4-c5*Ta)*(ed-es(Ta)) <= 0`; the reference R implementation (`anacv/HeatStress`, `R/wbgt.Bernard.R`) minimises `|f|` with Brent instead, which is not vectorisable over a decade of hours. Hourly values are grouped into **local** days via a fixed +05:30 offset while solar geometry stays on the UTC timestamps, and partial boundary days (fewer than 24 hours) are dropped so no daily maximum is taken over an incomplete day. Scores are bias, median absolute bias, RMSE, Pearson r and — separately — the bias restricted to the hottest 1 % of reference days, because that tail is where the shipped `*_days_ge_*` slugs are decided and where a mean-agreeing metric can still be wrong. `--self-test` runs eight offline checks (Tpwb = Ta at saturation, Td <= Tpwb <= Ta, residual ~ 0 at the root, Bernard within 1 C of Stull, IRT shade within 1 C of Bernard indoor, sWBGT ordering, Liljegren finite and monotone in solar load) with no network. `--dry-run` prints the request plan and exits. Reads nothing under `IRT_DATA_DIR`; writes only under `--out-dir` (default `docs/diagnostics/wbgt_deployed_vs_reference`): `README.md`, `scores.csv`, `daily_series.parquet`, `run_metadata.json`, plus a per-site-year parquet cache under `--cache-dir` (default `scratch/`, git-ignored) that makes reruns free and interrupted runs resumable |
| `wbgt_carbonplan_crosscheck.py` | **Distribution cross-check of IRT's published WBGT day counts against CarbonPlan `extreme-heat` v1.0 (CHG-0544).** CarbonPlan's dataset (MIT code, CC BY 4.0 data) is built on the same NEX-GDDP-CMIP6 archive IRT uses, which makes it the closest available external reference. Compares days per year with WBGT >= 32 C — the only threshold both products publish, since CarbonPlan offers 29 / 30.5 / 32 / 35 and IRT offers 28 / 30 / 32 — pairing CarbonPlan shade against `wbgt_shade_stull_days_ge_32` and CarbonPlan sun against `swbgt_empirical_days_ge_32`. Indian rows are those whose `hierid` starts with `IND` (2,257 regions); all of them have an empty `ID_HDC_G0`, i.e. they are the climatically-similar *regions* rather than the urban-centre subset, so the comparison is not urban-biased. **The comparison is of distribution shape, never of means alone**: the report gives the full quantile curve from q0 to q1 with an IRT/CarbonPlan ratio at each point, plus zero fraction, standard deviation, skew and a two-sample Kolmogorov-Smirnov test, precisely because two distributions can agree on a mean and disagree everywhere that matters. It deliberately emits **no per-place spatial correlation**: CarbonPlan keys regions by Climate Impact Lab `hierid` (`IND.<state>.<district>.<region>`), neither the CSVs nor the summary Zarrs carry coordinates, and the 592 distinct district-level prefixes do not correspond to IRT's 784 LGD districts, so a name-based join would manufacture its own result; the report states this as a named limitation together with its unblocker (obtain the CIL impact-region geometry, then join point-in-polygon against IRT boundaries). Two further differences are stated rather than silently corrected: the historical window (CarbonPlan `slice("1985","2014")`, pinned by reading `notebooks/09_summarize.ipynb`, against IRT's 1990-2010) and the driver (CarbonPlan evaluates at `tasmax` with RH computed at `tasmax`; IRT evaluates on daily-mean `tas` and `hurs`). Reads IRT district masters from `processed_optimised` read-only and CarbonPlan's public CSVs over HTTPS with no auth; writes only under `--out-dir` (default `docs/diagnostics/wbgt_carbonplan_crosscheck`): `README.md`, `distribution_stats.json`, plus a CSV cache under `--cache-dir` |
| `wbgt_era5_grid_download.py` | **Copernicus CDS fetch of one hourly ERA5 single-level day over a lat/lon box (CHG-0550)**, supplying the gridded driver for `wbgt_era5_grid_compare.py`. Reads nothing under `IRT_DATA_DIR` and touches no config, master or bundle. Requests the seven `reanalysis-era5-single-levels` variables that the four WBGT methods need between them — `2m_temperature` and `2m_dewpoint_temperature` (all four methods; ERA5 publishes no 2 m relative humidity, so RH is derived downstream from Ta and Td with the same Magnus form Bernard uses, keeping the humidity definition identical across methods), `surface_pressure`, `10m_u_component_of_wind`, `10m_v_component_of_wind`, `surface_solar_radiation_downwards` and `total_sky_direct_solar_radiation_at_surface` (Liljegren only). Defaults to **two** consecutive UTC days rather than one: IST is UTC+05:30, so a single UTC day spans 05:30 to 05:30 local and truncates the afternoon peak of one local day or the other, which is precisely the quantity the comparison exists to measure. Refuses a span crossing a month boundary instead of silently over-requesting, because CDS expands `year x month x day` as a cross product and a two-day span over a boundary would return two whole months of that day-of-month. `download_format: unarchived` is honoured only when every requested variable shares a step type; this request mixes instantaneous fields with hourly accumulations, so CDS splits it server-side into `data_stream-oper_stepType-instant.nc` and `data_stream-oper_stepType-accum.nc` and returns a **zip still named `.nc`** — the script detects and unpacks it, rather than leaving an unreadable NetCDF to be diagnosed downstream. Needs `cdsapi` and a `~/.cdsapirc` holding a Copernicus CDS key (not a geo/PROJ package). `--dry-run` prints the variable list, date span, cell count and an uncompressed size estimate and submits nothing. Writes only under `--out-dir` (default `scratch/wbgt_era5_grid`, git-ignored) |
| `wbgt_era5_grid_compare.py` | **Gridded counterpart to `wbgt_deployed_vs_reference.py` (CHG-0550):** the same four WBGT methods scored per 0.25 deg ERA5 cell rather than at six points, which is what makes the *spatial structure* of the error visible. Imports `irt_wbgt_shade_stull_c`, `irt_swbgt_empirical_c`, `bernard_indoor_wbgt_c`, `liljegren_wbgt_c` and the Magnus saturation form from `wbgt_deployed_vs_reference.py` and `cos_solar_zenith` from `wbgt_method_validation.py`, so no formula, no bisection solver and no solar geometry is re-transcribed and all three harnesses grade one implementation. Merges the two step-type NetCDF files CDS returns onto a single hourly grid, then converts the units whose silent misuse is the main failure mode here: `surface_pressure` Pa to hPa; `ssrd` and `fdir` from J/m2 **accumulated over the preceding hour** to W/m2 (a factor of 3600, paired with the instantaneous fields at the same stamp, a <=30 min phase error that is negligible against a daily maximum near solar noon); and `fdir` reduced to the dimensionless 0-1 direct-beam fraction of `ssrd` that `thermofeel` expects. Because a wrong convention returns NaN rather than raising, the run reports the Liljegren NaN rate as its unit check (0.0000 over 13,872 cell-steps on the reference run). Solar geometry is evaluated per cell on the UTC stamps while hours are grouped into **local** days via a fixed +05:30 offset, and local days holding fewer than 24 hours are dropped so that no daily maximum is taken over a truncated day. Each method is reduced three ways — mean of the hourly series, max of the hourly series, and (for the two IRT methods only, since the references have no deployed analogue) the formula evaluated on daily-**mean** Ta and RH, which is what `heat_stress_gridfirst` actually receives. Seven scored comparisons follow from that: `peak vs peak` and `mean vs mean` isolate *formula* error, `AS DEPLOYED vs reference peak` adds *aggregation* error, and the last pairs the shipped shade metric against the outdoor reference. Alongside bias, RMSE and Pearson r the report gives the **min and max per-cell bias**, because that spread is what matters to a frozen national CDF ruler: a uniform offset barely moves ranks, while a bias varying with diurnal temperature range reorders districts. `--dry-run` prints the input and output plan and reads nothing. Writes only under `--out-dir` (default `docs/diagnostics/wbgt_era5_grid`): `per_cell.csv` (one row per cell, every intermediate retained), `README.md` |
| `wbgt_era5_grid_maps.py` | **Spatial renderer for the gridded WBGT comparison (CHG-0551; figure layout revised CHG-0552).** Reads only the `per_cell.csv` written by `wbgt_era5_grid_compare.py` plus, optionally, `DATA_DIR/states_4326.geojson` for orientation; writes nothing under `IRT_DATA_DIR` and recomputes no WBGT — every number it draws comes from the comparison run, so the maps cannot drift from the scores they illustrate. Pivots the long CSV back onto the (lat, lon) grid and raises if more than one complete local day is present, rather than silently averaging days into a map that belongs to neither. Five figures. The three field figures share one 2x2 layout and one colour scale: IRT in the left column, its reference in the right, day mean on the top row and day max below, so method error reads across a row and aggregation error reads down a column — `shade.png` (IRT shade vs Bernard indoor), `outdoor.png` (IRT sWBGT vs Liljegren outdoor) and `deployed.png` (the two AS-DEPLOYED IRT fields, the formulas evaluated on daily-mean inputs, beside the day-max reference each should be graded against). `bias.png` maps four IRT-minus-reference biases on a shared symmetric diverging scale, shade mean and max on the top row and outdoor mean and max on the bottom, each panel annotated with its own mean and per-cell min/max because the shade panels are deliberately near-blank at the scale the outdoor panels demand. `drivers.png` maps Ta day-mean, Ta day-max, the max-minus-mean diurnal swing, day-mean RH, and a scatter of the swing against the as-deployed shade bias. `fields.png` from the superseded eight-panel layout is deleted on each run so a stale figure cannot outlive the layout it belonged to. The state overlay is optional by design: a missing or unreadable boundary layer prints a notice and draws without it, since the maps are data either way. `--dry-run` prints the read, delete and write plan and renders nothing. Writes only under `--out-dir` (default `docs/diagnostics/wbgt_era5_grid`) |
| `build_banding_explorer.py` | **Read-only** banding aesthetics explorer (CHG-0531): renders the national choropleth in five band colours rather than the frozen 101-stop ramp, to settle how the Overview's five interpretive bands should paint. Re-scores nothing and reads no CSV — it parses the JSON payload embedded in `irt_target_prototype.html` and keeps only `districts`, `state_paths`, `district_scores`, `state_stats`, the ramp and the slice labels, so the explorer and the prototype cannot disagree about a score, a State mean or a band cut. Band cuts are the prototype's `BANDS` verbatim (20/40/60/80). Three palettes x two fill rules x a ramp on/off toggle, compared side by side or flickered A/B in one pane. `state-hue` follows the State-averaged value to a band and ramps districts across their own State's min-max; `district-band` takes the hue from the district's own national band and uses intensity for position within it. Both paint the existing 784 district paths; intensity is an OKLab interpolation between a band's derived floor and its full colour, never opacity. Degenerate States (n < 3 scored districts, or spread < 2.0 score points — Chandigarh, Goa, Ladakh, Lakshadweep on most slices) are pinned to flat intensity and named in the caption. The caption also reports band occupancy per slice, measured from the painted field (`composite_absolute_threshold`, the frozen headline) rather than from `composite`. **`state-hue` is a per-State min-max rescale — the operation `colour_scale.json` marks `rescale: forbidden` and `qa/reports/CURRENT_UX_FLOW.md` §7 documents as the deployed product's central defect — and exists here only so the aesthetic can be seen and rejected on its merits; it must not migrate into the published bundle or the prototype without a separate decision.** Template: `tools/diagnostics/templates/banding_explorer.template.html`. Writes `docs/diagnostics/heat_risk_pilot/banding_explorer.html` |
| `spi_diagnostic.py` | SPI output sanity checks and diagnostics |
| `verify_states_geojson.py` | Verify `states_4326.geojson` is consistent with `districts_4326.geojson` |
| `verify_districts_blocks_geojson.py` | Sanity + parity checks for `districts_4326.geojson` and `blocks_4326.geojson` |
| `verify_admin_join_consistency.py` | Cross-level join consistency for all three boundary layers (naming + per-unit dissolve IoU/residual); optional `--figures-dir` renders nesting maps, area-parity scatter, IoU-band chart |
| `roster_audit.py` | Canonical-roster audit + boundary-migration housekeeping (CHG-0089). Audit mode (read-only) reports stale published masters, raw orphan dirs across BOTH the periods and `ensembles/` subtrees, and a completeness gate (`canonical ⊆ published` over master **and** yearly_ensemble keys, exit 1 on any keeper short). `--quarantine-processed` moves old-named raw dirs → `processed/_stale_prelgd_bak/` (district new-name interlock; `--dry-run` default). `--prune-optimised` moves deferred-stale masters → out-of-bundle `_stale_optimised_prelgd_bak/` (keeper-component guard; `--dry-run` default). Apply modes write a JSON move-manifest |
| `profile_prepare_dashboard.py` | End-to-end per-stage wall-clock timer wrapping `prepare_dashboard` (CHG-0097). Monkeypatches `execute_plan` then calls `prepare_dashboard.main` unchanged, classifying each step into canonical stages (load+compute, exposure/context, admin aggregation, bundle assembly, optimized publish, validation). Reports sum-of-stages, true wall, and unattributed overhead; optional `--profile-json`/`--profile-csv`. Forwards all other args to `prepare_dashboard`; **drives the real pipeline and WRITES outputs** (use `--overwrite` on a disposable `IRT_DATA_DIR` for a true cold-path number; `--plan-only`/`--dry-run` are write-free) |

### `tools/geodata/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_district_subbasin_crosswalk.py` | Shared polygon crosswalk builders plus the district ↔ sub-basin CLI |
| `build_admin_boundaries_from_lgd.py` | **Single source of truth** for the admin hierarchy: derive `blocks_4326.geojson`, `districts_4326.geojson`, and `states_4326.geojson` from one bharatlas `LGD_Blocks` shapefile (districts = dissolve of blocks by name; states = dissolve of districts) so all three nest exactly; Title-Case canonical state names, ADM3-loader-identical label repair, `.bak-<timestamp>` backups on `--overwrite` |
| `build_blocks_geojson.py` | _(Superseded by `build_admin_boundaries_from_lgd.py`)_ Rebuild only the canonical `blocks_4326.geojson` from the legacy `Block_GH_WUP` source with canonical block identity columns and label QA |
| `build_adm1_geojson.py` | Build `processed_optimised/geometry/admin/adm1.geojson` from canonical district boundaries for ADM1-first dashboard boot |
| `build_states_geojson.py` | Build full-fidelity `states_4326.geojson` by dissolving `districts_4326.geojson` (unsimplified shareable companion; not used at runtime) |
| `build_block_subbasin_crosswalk.py` | Build canonical block ↔ sub-basin crosswalk CSV |
| `build_district_basin_crosswalk.py` | Build canonical district ↔ basin crosswalk CSV |
| `build_block_basin_crosswalk.py` | Build canonical block ↔ basin crosswalk CSV |
| `prepare_aqueduct_baseline.py` | Build a clean Aqueduct baseline GeoJSON, QA CSV, and India-only `future_annual` GeoJSON with source future attributes preserved |
| `build_aqueduct_admin_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ district overlap CSVs in `EPSG:6933` |
| `build_aqueduct_block_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ block overlap CSVs in `EPSG:6933` |
| `build_aqueduct_admin_masters.py` | Build `processed/{aqueduct_metric_slug}/{state}/master_metrics_by_{district,block}.{csv,parquet}` from direct Aqueduct admin overlaps |
| `build_aqueduct_hydro_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ SOI basin/sub-basin overlap CSVs in `EPSG:6933` |
| `build_aqueduct_hydro_masters.py` | Build `processed/{aqueduct_metric_slug}/hydro/` master `{csv,parquet}` files from Aqueduct overlaps for the onboarded hydro metrics |
| `build_population_admin_masters.py` | Build district/block population total and density master `{csv,parquet}` files from the 2025 population raster plus `population/overlay/population_exposure_2025_overlay.{png,json}` |
| `build_worldpop_agesex_admin_masters.py` | Build district/block `population_age_65plus_{count,share_pct}` and `population_age_under5_{count,share_pct}` master `{csv,parquet}` files, plus the summed rasters under `worldpop_agesex/derived/`. Shares are State/UT proportions and are card text only, never a map fill |
| `build_lgrip_admin_masters.py` | Build district/block `lgrip_{cropland,irrigated,rainfed}_{area_km2,share_pct}` master `{csv,parquet}` files from LGRIP30 V001, plus `irrigation/lgrip30_india.vrt`. Only the `cropland` pair is registered and published; the irrigated/rainfed split is withheld pending Agricultural Census validation |
| `build_gsw_admin_masters.py` | Build district/block `surface_water_{permanent,seasonal}_{area_km2,share_pct}` master `{csv,parquet}` files from JRC GSW v1.4 occurrence, plus `surface_water/gsw_occurrence_india.vrt` and `gsw_sea_mask.tif`. Permanent is occurrence >= 75%, seasonal 25-74% -- repo thresholds, not product definitions. Marine water is removed by a connectivity mask seeded outside the national land union, opened by one coarse cell so coastal lagoons survive |
| `build_groundwater_district_masters.py` | Build district groundwater assessment master `{csv,parquet}` files from the 2024-2025 GEC workbook, reconciled to the canonical district layer via a curated `groundwater_district_aliases.csv` (mapping rows re-point source spellings; `__EXCLUDE__` sentinel drops non-canonical source rows such as `DELHI, NAZUL LAND`) plus declarative same-district aggregation. Refuses to write masters on any residual unmatched source; QA package = `groundwater_unmatched_districts.csv` / `groundwater_excluded_sources.csv` / `groundwater_summary.csv` under `IRT_DATA_DIR/groundwater/` |
| `build_water_availability_district_masters.py` | Build district per-capita water-scarcity master `{csv,parquet}` files from the NITI Aayog ICED *Per Capita Water Availability 2025 & 2050* workbook. Ordinal classes are encoded to integer codes 1..4 (`water_scarcity_percapita`, `water_scarcity_percapita_2050`) with a 0..3 deterioration delta (`water_scarcity_deterioration_2050`), reconciled to the canonical district layer via curated state/district aliases + worst-class collision aggregation and a full-roster left-join (NaN where no source). Fail-fast integrity gates with `source_rows_resolved` vs `canonical_rows_with_source` counters reported separately; QA CSVs default to `water_availability/qa` |
| `build_jrc_flood_depth_admin_masters.py` | Build per-state (`--state`, default Telangana) district/block JRC flood-depth master `{csv,parquet}` files using block flooded-cell `p95` and district flooded-area weighting, plus the derived RP100 Flood Severity Index, RP100 flood-extent masters, RP-100 display overlay PNG/metadata, provenance-aware run summary rows, and stable QA CSVs. Strict RP-100 mode uses `--source-manifest <source_manifest.json> --rp100-only` and requires aligned 3-arc-second depth/coverage rasters so missing source footprint stays no-data while covered non-positive or `-9999` depth is dry support; legacy four-return-period builds require `--source-dir ... --allow-unversioned-source --assume-units m`. Masters write to `processed/{slug}/{state}/`; QA defaults to `jrc_flood_depth/{state}/qa`; the RP-100 overlay is pan-India and shared at `jrc_flood_depth/overlay/` (idempotent across states) |
| `validate_aqueduct_workflow.py` | Validate Aqueduct cleanup plus direct district/block and SOI hydro transfer outputs for the onboarded Aqueduct metrics |
| `clean_river_network.py` | Clean Survey of India river shapefile into canonical GeoParquet + display GeoJSON + QA CSV |
| `build_river_topology.py` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts |
| `convert_blocks_shp_to_geojson.py` | Convert block shapefile to standardized GeoJSON |
| `inspect_block_shapefile.py` | Inspect and optionally convert block shapefiles |

### `tools/legacy/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `DONOTUSE_ArtparkGenerateReport.py` | Legacy script kept for reference only |

### `tools/pipeline/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_all_csv.ps1` | PowerShell helper for CSV build workflows |
| `build_composite_metrics.py` | CLI wrapper that writes persisted district/block composite masters for the thematic dashboard bundles |
| `fit_frozen_ruler.py` | Fits and commits one bundle's frozen national CDF ruler (`config/frozen_rulers/<slug>/cdf_<version>/`) from the component masters' validated slice contract, emits deterministic district/block `golden_canaries.csv`, and writes `processed_optimised/colour_scale.json`. Coverage gates are bundle-specific or explicitly supplied; published versions cannot be overwritten. Committed v1 rulers currently cover Heat Risk, Heat Stress, Extreme Rainfall | Flash Flood Risk, Cold Risk and Drought Risk (7 slices each) plus Riverine Flood (`snapshot/Current`) |
| `build_proposal_bundles.py` | CLI wrapper that writes persisted district/block proposal climate-risk bundle masters and the `r95p_interannual_variability` helper masters |
| `build_master_metrics.py` | CLI wrapper around `compute.master_builder` |
| `compute_indices.py` | Older single-process compute pipeline (district/block oriented) |
| `compute_indices_cli_common.py` | Shared lightweight parser and banner helpers for the climate compute bootstrap/runtime split |
| `compute_indices_bootstrap.py` | Thin bootstrap CLI that prints immediately, then imports the heavy climate compute runtime |
| `compute_indices_multiprocess.py` | Main multi-process climate compute runtime for admin district/block, now using source-inventory prewarm plus validated marker signatures |

### `tools/runs/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `prepare_dashboard.py` | Canonical operator entrypoint that orchestrates bundle prep, optimized runtime refresh, and final readiness verification for climate, Aqueduct, population exposure, groundwater, state-scoped JRC flood depth, validation, and dashboard-package workflows |
| `refresh_dashboard_climate_bundles.ps1` | PowerShell operator script that refreshes active district/block dashboard climate bundle source metrics, thematic composites, sector-wise proposal bundles, optimized artifacts, and parity reports without recomputing retired/unused climate diagnostics. `-Bundle` scopes the run to named dashboard bundles (rejects `Riverine Flood`); compute defaults to incremental `--skip-existing` with opt-in `-Overwrite`/`-OverwriteMetrics`; masters rebuild only when missing/stale/forced based on compute completion markers (with a raw `*_periods.csv` fallback for no-marker metrics and `-SkipCompute` runs, walked with `os.scandir` + per-slug early-exit across a bounded thread pool — `IRT_FRESHNESS_WORKERS` overrides the count, `IRT_FRESHNESS_TIMING=1` prints timings — yielding the identical stale set as the prior serial walk); `-Workers` is opt-in; the compute→master→composite stages run per-bundle with fail isolation while a per-level slug-state cache keeps shared source metrics single-pass, then one union optimized+audit pass runs over the succeeded bundles; a failed bundle's not-yet-built slugs are tainted and their half-written masters deleted so a later bundle force-rebuilds them; if any bundle fails, optimized+audit is skipped by default (non-zero exit) unless `-AllowPartialPublish` publishes the succeeded subset with a `*_partial_run.json` manifest; full-scope all-success runs keep the established report name while any subset/partial run gets a deterministic `*_scope-<token>.json` report so they cannot clobber the full-scope report; `processed_optimised` builds with `--skip-audit`, then `build_state_values` precomputes the area-weighted state headline values over the fresh bundle (same per-level metric scope as the audit, no `--strict`, no `--state` — so the single `all_states.parquet` per metric/level is refreshed across all present states, not clobbered to one) so the audit's `precomputed_state_values_missing` check does not fail under `--strict`; pass `-SkipStateValues` to opt out. Parity is then audited exactly once by the strict audit stage |
| `refresh_dashboard_riverine_flood_bundle.ps1` | PowerShell operator script that refreshes the full dashboard-ready admin Riverine Flood bundle for one state by chaining strict `prepare_dashboard jrc-flood-depth`, district + block `build_composite_metrics` for `composite_flood_jrc_depth`, a state-scoped `build_processed_optimised` pass for the Riverine Flood metrics, and a strict state-scoped parity audit. Defaults `-SourceManifest` to `D:/projects/irt_data/jrc_raw_new/source_manifest.json`; legacy `-JrcDir` / `-SourceDir` remains available for unversioned source-dir runs. Supports `-PlanOnly`, state-specific JRC builder path overrides, and optional `-IncludeSharedAdmin` when the operator intentionally wants shared admin artifacts rebuilt alongside the state-scoped refresh |
| `rebuild_jrc_rp100_national.ps1` | PowerShell operator script for the Flag C national rebuild (`docs/jrc_rp100_flag_c_remediation_plan.md`): loops `refresh_dashboard_riverine_flood_bundle.ps1` over every state not yet built against the strict RP-100 source manifest. State selection is derived from each state's `jrc_flood_depth/<state>/qa/run_summary.csv` (`strict_rp100` metric_kind + non-empty `source_manifest` ⇒ already strict), overridable with `-States` / `-IncludeStrict`. Per-state CSV log written after each state; a failed state is recorded and the loop continues (exit 1 if any failed). Detection is master-level only and cannot detect a state whose composite/optimized stages were interrupted — re-run those via `-States`. Publishes in place with `-Overwrite`; plan §6 checkpointing and §8 staged fail-closed promotion are **not** implemented, so there is no rollback and the Streamlit app must be stopped before running |

## Test inventory

### Test entrypoint

```bash
python -m pytest -q
```

### Test modules under `tests/`

#### App/UI/runtime
- `test_app_adm2_cache.py`
- `test_app_dashboard_entry.py`
- `test_app_details_panel.py`
- `test_app_geo_cache.py`
- `test_app_geography_controls.py`
- `test_app_landing_runtime.py`
- `test_app_left_panel_runtime.py`
- `test_app_map_layer_runtime.py`
- `test_app_map_pipeline.py`
- `test_app_map_view_extract.py`
- `test_app_orchestrator_entry.py`
- `test_app_perf.py`
- `test_app_point_selection_ui.py`
- `test_app_portfolio_ui.py`
- `test_app_rankings_view.py`
- `test_app_ribbon.py`
- `test_app_runtime_view.py`
- `test_app_sidebar_import.py`
- `test_app_state.py`
- `test_app_state_summary_view.py`
- `test_legend_html.py`
- `test_main_app_import.py`
- `test_map_view_layout.py`
- `test_root_main_entrypoint.py`
- `test_scenario_ui_labels.py`
- `test_state_defaults.py`

#### Data, paths, merge, contracts
- `test_available_states.py`
- `test_config.py`
- `test_clean_river_network.py`
- `test_aqueduct_admin_transfer.py`
- `test_aqueduct_hydro_transfer.py`
- `test_build_blocks_geojson.py`
- `test_crosswalk_context.py`
- `test_crosswalk_generator.py`
- `test_crosswalk_runtime.py`
- `test_groundwater_district_masters.py`
- `test_hydro_contracts.py`
- `test_import_boundaries.py`
- `test_imports_smoke.py`
- `test_jrc_flood_depth_admin_masters.py`
- `test_master_loader.py`
- `test_merge.py`
- `test_metrics_registry.py`
- `test_naming.py`
- `test_optimized_bundle.py`
- `test_paths_resolution.py`
- `test_population_admin_masters.py`
- `test_prepare_aqueduct_baseline.py`
- `test_prepare_dashboard_runner.py`
- `test_processed_io_parquet_filters.py`
- `test_prune_legacy_csv.py`
- `test_river_loader.py`
- `test_river_overlay_contract.py`
- `test_river_reconciliation.py`
- `test_river_topology.py`
- `test_timeseries.py`
- `test_timeseries_models.py`
- `test_timeseries_optimized.py`
- `test_validate_aqueduct_workflow.py`

#### Config and bundles
- `test_bundle_scores.py`
- `test_bundle_weights.py`
- `test_composite_metrics.py`
- `test_frozen_national_ruler.py`
- `test_dashboard_bundles.py`
- `test_proposal_bundle_builder.py`
- `test_proposal_bundle_config.py`

#### Analysis, enrichment, portfolio
- `test_analysis_metrics.py`
- `test_map_enrichment.py`
- `test_portfolio.py`
- `test_portfolio_grouping_helpers.py`
- `test_portfolio_tier1_guards.py`
- `test_portfolio_tier2_manage_helpers.py`
- `test_portfolio_tier3_multistate.py`
- `test_state_profile_trend_band_fallback.py`

#### Compute and legacy parity
- `test_build_master_state_summaries.py`
- `test_compute_indices_admin_ensembles.py`
- `test_compute_indices_cold_risk_metrics.py`
- `test_compute_indices_heat_stress_metrics.py`
- `test_compute_indices_marker_validation.py`
- `test_compute_indices_proposal_metrics.py`
- `test_compute_indices_synthetic.py`
- `test_compute_indices_synthetic_comprehensive.py`
- `test_roster_gate.py` — canonical-roster gate at the `build_processed_optimised` master-publish chokepoint (`IRT_ROSTER_GATE` strict/warn/off)
- `test_legacy_dashboard_map_portfolio_wiring.py`
- `test_legacy_dashboard_portfolio_panel_call.py`
- `test_legacy_dashboard_state_profile_files.py`

#### Visualization
- `test_viz_charts.py`
- `test_viz_colors.py`
- `test_viz_exports.py`
- `test_viz_folium_featurecollection.py`
- `test_viz_formatting.py`
- `test_viz_scenario_yaxis_scaling.py`
- `test_viz_tables.py`
- `test_viz_trend_spaghetti.py`

#### Repo/process guards
- `test_no_emojis.py`

## Docs and supporting repo files

### `docs/`

| File | Purpose |
|------|---------|
| `BACKLOG.md` | Long-lived deferred work and shelved initiatives |
| `HANDOFF.md` | Persistent handoff ledger |
| `aqueduct_field_contract.md` | Canonical Aqueduct source-field mappings for onboarded district, block, and hydro metrics |
| `aqueduct_onboarding_methodology.md` | Narrative for Aqueduct cleanup, pfaf_id normalization, direct admin transfer, and HydroSHEDS → SOI hydro transfer |
| `bundle_task_master.md` | Task tracking for bundle-weight methodology migration |
| `climate_risk_indicator_inventory.md` | Working inventory for aligning dashboard taxonomy with proposed climate risk indicators |
| `command_catalog.md` | Canonical operator-facing command catalog for dashboard prep workflows |
| `technical_guidance_note.md` | Methodology reference rendered into the in-dashboard Read the Docs asset |
| `dead_code_candidate_report.md` | Dead-code analysis notes |
| `functionality_contract.md` | Product/functionality contract notes |
| `lens_scoring_methodology.md` | Lens-based scoring framework (absolute/change/impact), scientific basis/references, impact-band provenance policy, score-decomposition schema, and per-metric sector lens dossiers |
| `manual_smoke_test.md` | Manual smoke-test checklist |
| `module_responsibility_map.md` | Historical module responsibility notes |
| `proposal_bundle_methodology.md` | Methodology narrative for sector climate hazard pressure proposal bundles |
| `pytest_baseline_failures.md` | Known/recorded pytest baseline failures |
| `refactor_acceptance.md` | Refactor acceptance criteria/history |

#### `docs/benchmarks/`

| File | Purpose |
|------|---------|
| `bechmark_iith_groundwater_dashboard.md` | Benchmark review comparing IITH groundwater dashboard against IRT |

### Other notable root assets

| File | Purpose |
|------|---------|
| `irt_agents_bundle.zip` | Agent-bundle artifact kept at repo root |
| `irt_agents_data_catalog_patch.zip` | Patch/archive artifact |
| `irt_data_catalog_patch.zip` | Patch/archive artifact |
| `spi3_err.log` / `spi3_out.log` / `spi3_tel.log` | Local diagnostic logs |

## Data contracts

### Boundary inputs expected under `IRT_DATA_DIR`

All three admin-boundary GeoJSONs below are derived together from one bharatlas `LGD_Blocks` shapefile via `tools.geodata.build_admin_boundaries_from_lgd`, so districts nest inside states and blocks nest inside districts by construction (no aliases or crosswalks needed to reconcile them).

| Artifact | Purpose |
|----------|---------|
| `states_4326.geojson` | ADM1 state/UT boundaries (dissolve of districts) |
| `districts_4326.geojson` | ADM2 district boundaries (dissolve of blocks by name) |
| `blocks_4326.geojson` | ADM3 block boundaries (atomic source layer) |
| `basins.geojson` | Canonical basin boundaries |
| `subbasins.geojson` | Canonical sub-basin boundaries |
| `district_subbasin_crosswalk.csv` | District ↔ sub-basin overlap registry |
| `block_subbasin_crosswalk.csv` | Block ↔ sub-basin overlap registry |
| `district_basin_crosswalk.csv` | District ↔ basin overlap registry |
| `block_basin_crosswalk.csv` | Block ↔ basin overlap registry |
| `river_network.parquet` | Canonical cleaned river-network line artifact |
| `river_network_display.geojson` | Simplified river-network display artifact |
| `river_network_qa.csv` | Row-level QA flags for the cleaned river network |
| `river_reaches.parquet` | Topology-ready river reach artifact |
| `river_nodes.parquet` | Topology-ready river node artifact |
| `river_adjacency.parquet` | Reach-to-reach adjacency artifact |
| `river_topology_qa.csv` | QA rows for topology-ready reach artifacts |
| `river_missing_assignments.csv` | Focused diagnostics for reaches still missing basin/sub-basin assignment |
| `river_missing_assignments.geojson` | Visual-debug layer for unresolved river reach assignments |
| `jrc_flood_depth/overlay/rp100_depth_overlay.png` | Canonical RP-100 flood-depth display overlay exported from `RP100_depth.tif` |
| `jrc_flood_depth/overlay/rp100_depth_overlay_meta.json` | RP-100 overlay metadata with Web Mercator image CRS, EPSG:4326 bounds, source maximum, and fixed display scale |

### Canonical identifier expectations

| Level | Required identifiers |
|------|-----------------------|
| District | `state_name`, `district_name` |
| Block | `state_name`, `district_name`, `block_name` |
| Basin | `basin_id`, `basin_name` |
| Sub-basin | `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name` |

### Processed output layout

#### Admin

```text
processed/{metric_slug}/{state}/
├── master_metrics_by_district.csv
├── master_metrics_by_block.csv
├── state_model_averages_district.csv
├── state_ensemble_stats_district.csv
├── state_yearly_model_averages_district.csv
├── state_yearly_ensemble_stats_district.csv
├── state_model_averages_block.csv
├── state_ensemble_stats_block.csv
├── state_yearly_model_averages_block.csv
├── state_yearly_ensemble_stats_block.csv
├── districts/
└── blocks/
```

Identifier columns:
- district master: `state`, `district`
- block master: `state`, `district`, `block`

#### Hydro (retired)

The offline basin/sub-basin **climate** compute pipeline has been removed, so `processed/{metric_slug}/hydro/` is no longer produced or consumed for climate metrics. The only remaining writer of this layout is the retained offline Aqueduct hydro builder, whose SOI basin/sub-basin masters are not surfaced in the admin-only dashboard. The historical layout was:

```text
processed/{metric_slug}/hydro/
├── master_metrics_by_basin.csv
├── master_metrics_by_sub_basin.csv
├── basins/
│   ├── {basin}/{model}/{scenario}/{basin}_yearly.csv
│   └── ensembles/{basin}/{scenario}/{basin}_yearly_ensemble.csv
└── sub_basins/
    ├── {basin}/{sub_basin}/{model}/{scenario}/{sub_basin}_yearly.csv
    └── ensembles/{basin}/{sub_basin}/{scenario}/{sub_basin}_yearly_ensemble.csv
```

Historical identifier columns:
- basin master: `basin_id`, `basin_name`
- sub-basin master: `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name`

### Crosswalk artifact

Current canonical crosswalks:
- `district_subbasin_crosswalk.csv`
- `block_subbasin_crosswalk.csv`
- `district_basin_crosswalk.csv`
- `block_basin_crosswalk.csv`

Required columns:
- `district_name`
- `state_name`
- `subbasin_id`
- `subbasin_name`
- `basin_id`
- `basin_name`
- `intersection_area_km2`
- `district_area_fraction_in_subbasin`
- `subbasin_area_fraction_in_district`

Current behavior:
- district and block details -> basin + sub-basin context
- related-unit reference overlays on the admin map (highlight related basins/sub-basins)

Not yet supported:
- weighted transfer across spatial families
- river-network crosswalk/topology layer
- navigation into basin/sub-basin views (the Hydro family is removed)

### River-network artifact

Current canonical river-network cleaning outputs:
- `river_network.parquet`
- `river_network_display.geojson`
- `river_network_qa.csv`

Current behavior:
- offline cleaning + QA only
- preserves raw Survey of India fields and adds canonical cleaned columns
- admin district/block display overlay available via the district-sliced `river_network_display.geojson`
- topology-ready reach/node/adjacency artifacts are supported offline
- the former basin/sub-basin reconciliation and diagnostics builders were removed with the navigable hydro river overlay
- no upstream/downstream routing UI, river crosswalks, or river-based metric computation yet

### Reference overlays

River and RP-100 flood-depth references share `india_resilience_tool/app/overlays.py`.
The RP-100 overlay is display-only: the builder exports `rp100_depth_overlay.png`
and `rp100_depth_overlay_meta.json` from `RP100_depth.tif`; dashboard runtime reads
only those artifacts. The PNG is exported in EPSG:3857 for Leaflet ImageOverlay
alignment, while metadata stores EPSG:4326 bounds for placement. The display
scale is fixed at `0.0-10.0 m`; `depth <= 0.0` is transparent,
`0.0 < depth <= 0.5` is `#d6f0ff`, `0.5 < depth <= 1.0` is
`#9dd9ff`, `1.0 < depth <= 2.0` is `#5bb7f0`, `2.0 < depth <= 4.0` is
`#2f7fc1`, `4.0 < depth <= 7.0` is `#1d4f91`, and `depth > 7.0` is `#0f2f5f`.

## Current status vs deferred work

### Implemented now
- Admin family: district and block (the only navigable spatial family)
- Retained hydrology context on admin units: Hydrological Context card, dominant-basin outline overlay, and district-sliced river overlay
- Polygon crosswalk context from district/block to related basin/sub-basin units (read-optimized/explanatory)

### Removed / retired
- Navigable Hydro family (basin/sub-basin map, rankings, details, portfolio)
- Offline basin/sub-basin climate compute pipeline and hydro master contracts
- Navigable hydro-only river display overlay

### Deferred
- Weighted admin ↔ hydro translation engine
- River-network/reach translation layer

Long-lived deferred work and shelved initiatives are tracked in `docs/BACKLOG.md`.

## Contact

For questions about the codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** absthakur@resilience.org.in
### `tools/optimized/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_processed_optimised.py` | Build the minimized `processed_optimised` runtime bundle from legacy processed outputs plus current canonical geometry/context artifacts, including admin (district/block) yearly parity outputs, selector-index artifacts, persisted geometry `area_m2`, optional admin level/state filtering, exact-target scoped prune, and a post-build parity audit; artifact version 5 records distinct fitted/published frozen-ruler grids and audits every frozen master and State-value row against the complete grid |
| `audit_processed_optimised_parity.py` | Audit the optimized runtime bundle against the legacy processed contract, with optional level/state filtering and optional scoped report output |
