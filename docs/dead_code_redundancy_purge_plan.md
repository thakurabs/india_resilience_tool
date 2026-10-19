# Dead and Redundant Code Purge Plan

**Plan Status:** Reviewed and ready for execution after explicit approval

**Planning Snapshot:** `GIT:add_flood_depth@37cac63`

**Planning Working Tree:** Dirty; 13 untracked files, no tracked modifications

**Change Range:** `CHG-0222` through `CHG-0227`; extended by `CHG-0230` through `CHG-0233` (amendment snapshot `GIT:add_flood_depth@861f44c`); review-hardening amendments `CHG-0234` and Aqueduct wholesale-retirement definition `CHG-0228` (both at snapshot `GIT:add_flood_depth@9eba23f`). `CHG-0228` supersedes `CHG-0225`. `CHG-0229` (retired-feature test trim) remains a pending in-chat amendment from a prior session, cross-referenced below but not defined in this document yet.

## Summary

Use a clean worktree for tracked purge changes, preserve pending feature and audit work, remove only proven unreachable hydro behavior, and retire the Aqueduct pipeline family wholesale under CHG-0228 (which supersedes the earlier extract-then-delete approach of CHG-0225). Graphify supplies clues only; source references, caller sweeps, documentation, imports, and tests determine deletion safety.

Tracked implementation CHGs receive separate commits. Inventory, quarantine, and decision-gate CHGs are recorded in the in-chat ledger without empty commits.

## Safety and Inventory

### CHG-0222: Baseline and Classification Gate

- Re-capture `git status --short --branch`, HEAD, tracked differences, and all untracked files at execution time.
- Create `chore/dead-code-purge` in a sibling worktree from that exact SHA after explicit approval.
- Classify untracked files:
  - `PROTECTED_PENDING`: exposure/hydro patch README, patch script, and block-placeholder test.
  - `RETAIN_DEFAULT`: design review, figure corrections, and map interaction specification.
  - `REVIEW_RETAIN_DEFAULT`: performance Phase 2 brief, cross-linked to the deferred block-builder decision.
  - `KEEP_IN_PLACE`: all boundary/roster diagnostic scripts until that workstream is explicitly closed.
  - `QUARANTINE_CANDIDATE`: only `tools/_scratch_roster_inventory.py`.
  - `SCRATCH_OUT_OF_SCOPE`: session/scratch working artifacts that fit no other bucket — leave untouched, never quarantine or delete under this plan. At the `9eba23f` re-check this covers `notebooks/scratch/`, `scratch/results/*`, `scratch/run_gridfirst_hwa_hwm_composite.py`, and `scratch/run_hwa_hwm_distinctness.py`. New boundary/roster diagnostics that appear after the planning snapshot (e.g. `tools/diagnostics/blocks_dedupe.py`, `district_jk_dump.py`, `district_merge_audit.py`, `district_roster_audit.py`, `verify_jk_west.py`) join the existing `KEEP_IN_PLACE` bucket.
- The untracked set drifts between sessions (13 entries at `37cac63`, 18 at `9eba23f`); the execution-time re-capture must re-classify every entry into one of the six classes above — no ad-hoc judgment outside the taxonomy.
- **Baseline capture (before any edit):** run `python -m pytest -q` at the execution SHA (or the hermetic focused subset if the full suite is environment-blocked, stating what blocked it) and store the raw output in the CHG-0223 quarantine directory as `pytest_baseline_<sha>.txt`. This record replaces `docs/pytest_baseline_failures.md` (deleted under CHG-0232) as the comparison baseline for the Test Plan's "no new failures" acceptance.
- Tag affected tests as hermetic, data-dependent, or Windows-only before running them.
- Acceptance: every untracked file and affected test has one explicit disposition, and the pytest baseline record exists.

### CHG-0223: Persistent Quarantine

- Never use `git clean`, wildcards, or directory-level moves; skip `tools/patches/` entirely.
- Move approved candidates individually to `/mnt/d/projects/irt_dead_code_quarantine/<date>/<sha>/CHG-0223/`.
- Record source, destination, SHA-256, reason, and exact restore command.
- Keep quarantine until 30 days after the purge branch merges. At merge time, propose (with explicit user approval, per repo rule §7) a dated `docs/BACKLOG.md` line as the disposal reminder — the retention window otherwise has no trigger mechanism.
- This is a filesystem action recorded in-chat; it creates no commit.

## Implementation Changes

### CHG-0224: Navigable Hydro Residue Purge

Apply the purge only to the closed eight-module set covering portfolio, rankings, SPI row shaping, discovery, and geography controls:

- `india_resilience_tool/analysis/portfolio.py`
- `india_resilience_tool/app/portfolio_ui.py`
- `india_resilience_tool/app/portfolio_multistate.py`
- `india_resilience_tool/app/portfolio_state_runtime.py`
- `india_resilience_tool/app/views/rankings_view.py`
- `india_resilience_tool/compute/spi_adapter.py`
- `india_resilience_tool/data/discovery.py`
- `india_resilience_tool/app/geography_controls.py`

Implementation requirements:

- Remove hydro-only helpers such as basin/sub-basin portfolio keys, matching helpers, hydro portfolio conversion, and hydro-yearly discovery functions.
- In shared functions, remove unreachable hydro branches while preserving names, parameters, return shapes, and district/block behavior.
- Preserve runtime-facing interfaces:
  - `render_geography_and_analysis_focus`
  - `build_portfolio_multiindex_df`
  - all seven `_portfolio_*` wrappers imported by `app/runtime.py`: `_portfolio_add`, `_portfolio_clear`, `_portfolio_contains`, `_portfolio_key`, `_portfolio_normalize`, `_portfolio_remove`, and `_portfolio_set_flash`
  - the derived runtime alias `_portfolio_remove_all = _portfolio_clear`; `_portfolio_clear` must survive even though runtime does not call it under its original name
  - `render_portfolio_panel`, `render_rankings_view`, SPI compute functions, and admin discovery functions
- Update hydro-positive cases in `tests/test_portfolio.py`, `tests/test_portfolio_tier3_multistate.py`, and `tests/test_app_geography_controls.py`.
- Explicitly classify `india_resilience_tool/app/runtime.py` as `RETAIN_CALLSITE_VERIFY`; its basin comment documents the retained overlay and is not purge residue.
- Before editing, inventory every importer of the eight modules. After editing, search every removed symbol and parameter repo-wide and verify all retained callsites still match their definitions.

Narrow acceptance pass:

```bash
rg -n 'navigable|hydro_mode|selected_basin|selected_subbasin' <eight-source-files>
```

Context acceptance pass:

```bash
rg -n 'basin' <eight-source-files>
```

Do not add `-w` to either command: underscores are word characters, so word-boundary matching misses compound identifiers such as `basin_id`, `subbasin_name`, `item_basin_id`, `basin_dir`, and `is_basin`. The plain `basin` substring also covers `sub_basin`, `subbasin`, and their compound forms.

Acceptance means zero **unclassified** hits, not zero textual hits. Each broad-pass hit must be labeled `PURGE` or `RETAIN_CONTEXT/COMPUTE`. Budget for a substantial classification pass: planning snapshot `37cac63` contains 513 broad-pass hits across the eight files, while the narrow pass contains only two; refresh both counts at execution time.

Retain the established hydrology-context modules, including `app/runtime.py`, map/runtime overlays, details/context cards, crosswalks, hydro/river loaders, topology, summary cache, geo cache, and Folium feature-collection code.

### CHG-0225: Aqueduct Refactor-Then-Delete — SUPERSEDED by CHG-0228

**Status: SUPERSEDED (2026-07-15).** The decision gate below was resolved by defining `CHG-0228` as wholesale Aqueduct retirement. No `aqueduct_common.py` is created; the 20-symbol closure and its consumers are deleted together under `CHG-0228`. This section is retained verbatim for evidence (the closure inventory and consumer list feed the `CHG-0228` scope) — do not execute it.

**Decision gate (resolved):** define or explicitly reject `CHG-0228` (Aqueduct wholesale amendment) **before** executing this change. The active clean-regen direction contemplates dropping Aqueduct entirely; if `CHG-0228` deletes the whole Aqueduct family, the `aqueduct_common.py` extraction below is wasted work that mints new purge residue. → Resolved: `CHG-0228` defined; this change is superseded.

**Symbol duplication policy (moot under supersession, retained for reference):** `aqueduct_common.py` was to be a deliberate tools-side consolidation, not the single repo-wide home of its symbols. `HydroLevel` is independently defined in `india_resilience_tool/data/hydro_loader.py` (dashboard-live, retained) and in `tools/geodata/build_district_subbasin_crosswalk.py` — both copies survive `CHG-0228` untouched; `_normalize_pfaf_id_series` has its own copy in `tools/geodata/prepare_aqueduct_baseline.py`, which is now deleted under `CHG-0228`.

Original plan (do not execute): create `tools/geodata/aqueduct_common.py` with the verified 20-symbol closure:

```text
HydroLevel, _INVALID_NUMERIC_SENTINELS, BASELINE_PERIOD,
FUTURE_SCENARIO_PERIODS, AqueductMetricSpec, AQUEDUCT_METRIC_SPECS,
AQUEDUCT_METRIC_ORDER, AQ_WATER_STRESS_COLUMN_MAP,
_default_aqueduct_dir, _assert_areal_geometries,
_normalize_pfaf_id_series, _numeric_metric_series,
load_aqueduct_boundaries, load_soi_hydro_boundaries,
get_supported_aqueduct_metric_slugs, get_aqueduct_metric_spec,
get_aqueduct_source_column_map, load_metric_source_table,
load_crosswalk, aggregate_crosswalk_to_targets
```

- Rewire the admin crosswalk builder, block crosswalk builder, admin masters builder, both validator import blocks, and `tests/test_validate_aqueduct_workflow.py`.
- Leave `tests/test_aqueduct_admin_transfer.py` unchanged.
- Keep `tools/geodata/prepare_aqueduct_baseline.py` and its test classified `LIVE`.
- Run the import smoke gate and focused tests before deletion.
- Only after that gate passes, delete both hydro builder modules and `tests/test_aqueduct_hydro_transfer.py`.
- Rerun the same gates after deletion.
- Remove dead CLI invocations and SOI basin/sub-basin master-build references from README, MANIFEST, tool catalog, command catalog, and Aqueduct methodology docs.
- Retain and rekey the live area-weighted transfer methodology prose around `aggregate_crosswalk_to_targets`; do not delete methodology merely because its former CLI was hydro-named.
- Treat `AQUEDUCT_METRIC_ORDER` and `AQ_WATER_STRESS_COLUMN_MAP` as internal closure members, not public rewire targets.
- Acceptance: no old filename, dotted module path, import, runbook, or PowerShell invocation remains.

### CHG-0228: Aqueduct Wholesale Retirement (supersedes CHG-0225)

**Decision (2026-07-15, user-confirmed):** retire the entire Aqueduct pipeline family. Definition snapshot `GIT:add_flood_depth@9eba23f`; re-verify all evidence at the execution SHA.

**Evidence basis:**

- The dashboard package (`india_resilience_tool/`, `main.py`) contains zero `aqueduct` references and zero `aq_*` metric slugs; the config registry does not know the metrics exist.
- Retirement is already a tested negative contract: `tests/test_config.py::test_aqueduct_metrics_are_fully_removed_from_dashboard_variables` and `tests/test_metrics_registry.py::test_aqueduct_metrics_and_domain_are_fully_retired` assert no `aq_*` slug or Aqueduct domain may survive dashboard-side.
- Only the producer side remained: seven `tools/geodata` modules (~3,010 LOC), four dedicated tests (~725 LOC), and the `aqueduct` subcommand + `dashboard-package` wiring in `tools/runs/prepare_dashboard.py` — a pipeline building data the dashboard is test-guarded against ever displaying.
- Consistent with the 2026-06-13 clean-regen direction (drop Aqueduct; keep hydro context, climate, riverine, exposure).

**Scope.** All deletions route through the CHG-0223 quarantine; per-file zero-reference gate with the CHG-0230 exclusions at the execution SHA.

1. **Delete seven `tools/geodata` modules:** `prepare_aqueduct_baseline.py` (542 LOC), `build_aqueduct_admin_crosswalk.py` (151), `build_aqueduct_block_crosswalk.py` (155), `build_aqueduct_admin_masters.py` (409), `build_aqueduct_hydro_crosswalk.py` (225), `build_aqueduct_hydro_masters.py` (486), `validate_aqueduct_workflow.py` (1,042). This **reclassifies `prepare_aqueduct_baseline.py` from LIVE/KEEP** (as recorded under CHG-0225/CHG-0230) **to DELETE** — the earlier classification meant "reachable from the pipeline seed," and the pipeline surface itself is now retired.
2. **Delete four tests:** `tests/test_prepare_aqueduct_baseline.py`, `tests/test_aqueduct_admin_transfer.py`, `tests/test_aqueduct_hydro_transfer.py`, `tests/test_validate_aqueduct_workflow.py`.
3. **Edit `tools/runs/prepare_dashboard.py`:** remove the `aqueduct` subparser, `build_aqueduct_plan`, the aqueduct step builders (`_build_prepare_aqueduct_baseline_step`, `_build_aqueduct_metric_args`), the aqueduct scope resolution inside `dashboard-package`, aqueduct entries in the `validate`/`pytest-validation` test lists, and the module-docstring example. Update `tests/test_prepare_dashboard_runner.py` to match. Sweep `tools/diagnostics/profile_prepare_dashboard.py` for aqueduct references. Note: after this edit the CHG-0230 pipeline seed count (20 unique `"tools.*"` strings) shrinks — re-derive it rather than expecting 20.
4. **KEEP:** the two retirement guard tests (they are the negative contract and must keep passing). **KEEP** the `aq_water_stress` fixture slugs in `tests/test_viz_charts.py` and `tests/test_app_state_summary_view.py` — arbitrary placeholder identifiers exercising generic column-shape logic, not Aqueduct functionality; renaming is optional cosmetic follow-up, not in scope.
5. **Docs:** `docs/aqueduct_onboarding_methodology.md` and `docs/aqueduct_field_contract.md` change from `TRIM` (CHG-0232) to **`DELETE` after skim-confirm**. The CHG-0225 methodology-retention rule dissolves: it protected prose around `aggregate_crosswalk_to_targets`, which is itself deleted here; the dashboard's live area-weighting methodology lives independently in `india_resilience_tool/analysis/area_weighting.py` and its own documentation. Skim-confirm must still verify nothing else unique survives only in these two docs. Sweep remaining rows in `README.md`, `MANIFEST.md`, `tools/README.md`, `docs/command_catalog.md`.
6. **Out of scope:** processed `aq_*` output directories under `IRT_DATA_DIR` (pipeline-written data; do-not-touch per repo rules). Clean regen simply will not rebuild them.

**Acceptance:**

- Import smoke: `python -c "import tools.runs.prepare_dashboard"` plus `python -m tools.runs.prepare_dashboard list`.
- `tests/test_prepare_dashboard_runner.py` and both retirement guard tests pass.
- Repo-wide sweep of `aqueduct` (case-insensitive) and `aq_` slugs: remaining hits only in the guard tests, the fixture slugs noted above, retained SOI hydro-context code (`load_soi_hydro_boundaries` naming heritage, if any), and the CHG-0230 gate-excluded ledger docs.
- No new pytest failures relative to the Step 0 baseline (the four deleted test files are recorded as removed, not failed).

### CHG-0226: Block Builder Deferral

- Keep `tools/geodata/build_blocks_geojson.py` as `METHODOLOGY_DEFERRED`.
- Link its disposition to `docs/perf_phase2_brief.md`.
- Any future LGD migration must cover `tools/runs/prepare_dashboard.py`, `tests/test_prepare_dashboard_runner.py`, `tests/test_build_blocks_geojson.py`, documentation, runbooks, and script references.
- Make no block-boundary methodology change during this purge.

### CHG-0227: Closeout

- Sweep deleted symbols, filenames, dotted paths, command examples, and PowerShell references.
- The sweep list additionally covers every file deleted under `CHG-0228`/`CHG-0231`/`CHG-0232`/`CHG-0233`:
  - the seven Aqueduct modules, four Aqueduct tests, and two Aqueduct docs from `CHG-0228`, plus their rows in `README.md`, `MANIFEST.md`, `tools/README.md`, `docs/command_catalog.md`;
  - `india_resilience_tool/app/adm2_cache.py`, `tests/test_app_adm2_cache.py`, and their mentions in `MANIFEST.md`, `docs/module_responsibility_map.md`, `india_resilience_tool/app/CLAUDE.md`;
  - the four never-referenced diagnostics (`block_orphan_audit`, `health_txx_lens_demo`, `profile_compute_realdata`, `profile_drought_spi`);
  - any doc deleted or trimmed under `CHG-0232`;
  - any tool deleted under `CHG-0233` plus its rows in `tools/README.md`, `docs/command_catalog.md`, and `MANIFEST.md`.
- Remove stale `__pycache__` directories only after deletion and testing.
- Refresh graphify and verify deleted module nodes are absent; use an approved full rebuild if incremental shrink handling leaves stale nodes.
- Update `BL-0023` and `BL-0024` only when explicitly requested.
- Update `docs/HANDOFF.md` only after explicit `Applied CHG-xxxx` confirmation.

## Reachability Inventory and Redundancy Extension (CHG-0230 – CHG-0233)

### CHG-0230: Reachability Inventory (evidence base; no deletions)

**Snapshot:** analysis and re-verification both at `GIT:add_flood_depth@861f44c`. The graphify graph used for initial clues was built at `6e7e222` (2026-07-07); all counts below were re-derived directly from source at `861f44c` and must be re-verified again at the execution SHA of any deletion package.

#### Method

Graphify alone under-reports liveness. Its AST pass misses three dynamic patterns, all present in this repo:

1. **Function-body imports** — `app/runtime.py` imports ~25 modules inside `run_app`, not at module top level.
2. **Subprocess module launches** — `tools/runs/prepare_dashboard.py` invokes pipeline stages via `python -m <tools.* string>`; those modules never appear as import edges.
3. **`importlib` dynamic references** — string-based module loads, e.g. `resources.files(ASSET_PACKAGE)` and `__import__(...)`.

The authoritative analysis is a repo-wide Python-AST **import closure** (walks all `Import`/`ImportFrom` nodes anywhere in each module body, resolving relative imports) seeded from:

- **Dashboard seed:** `main.py`.
- **Pipeline seeds (21):** `tools/runs/prepare_dashboard.py` itself plus the 20 unique `"tools.*"` module strings it launches via `python -m` (this set includes the other documented CLIs: `build_processed_optimised`, `audit_processed_optimised_parity`, `compute_indices_multiprocess`, etc.).

Reproduce commands:

```bash
# pipeline seed list (expect 20 unique strings)
grep -oE '"tools\.[a-z_.]+"' tools/runs/prepare_dashboard.py | sort -u

# per-candidate zero-reference check (expect no hits outside the file itself
# and the ledger/plan exclusions listed below)
git grep -l <name> -- '*.py' '*.md' ':!docs/dead_code_redundancy_purge_plan.md' ':!docs/HANDOFF.md' ':!docs/BACKLOG.md'
```

**Gate exclusions:** this plan document, `docs/HANDOFF.md`, and `docs/BACKLOG.md` are ledger/plan references — they name every candidate by construction, so hits in them never count against the zero-reference gate and they are never sweep targets under it. Without this exclusion the gate is unsatisfiable. Note one cosmetic asymmetry when reading gate output: `tools/diagnostics/block_orphan_audit.py` does not contain its own name in its content, so unlike the other candidates it produces no self-hit.

The closure script itself is short (~90 lines: collect `*.py` under `india_resilience_tool/`, `tools/`, `tests/` plus `main.py`/`paths.py`; parse with `ast`; resolve every import to the longest known module prefix; BFS from each seed set) and is re-written in a scratchpad at execution time rather than kept in the repo.

#### Verified live-feature map (at `861f44c`)

Of **98** `india_resilience_tool` package modules (excluding `compute/tests/`):

- **85 dashboard-live** (reachable from `main.py`), plus `main.py` and `paths.py`. By subpackage: app 32 (runtime, map pipeline/layer-runtime/view, rankings/state-summary/details views + runtimes + context cards, read-the-docs view, portfolio ui/multistate/state-runtime, case-study runtime, glance exports, landing/left-panel runtimes, ribbon, sidebar + branding, geography + controls, geo/master caches + freshness, overlays + hydro boundary overlay, crosswalk/dashboard-bundle runtimes, perf, state, help text, color-range controls); analysis 6 (portfolio, metrics, timeseries, area_weighting, map_enrichment); data 16 (master loader/columns, optimized_bundle, adm2/adm3 loaders, discovery, merge, crosswalks, hydro loader/summary, river loader/topology, exposure_summary, admin_coverage, spatial_match); compute 3 dashboard-side (incl. master_builder, spi_adapter); config 9; viz 8; utils 3.
- **11 pipeline-only live** (reachable only via `prepare_dashboard` planned commands — **all KEEP**): `compute/cold_risk_gridfirst`, `drought_risk_gridfirst`, `extreme_rainfall_gridfirst`, `heat_risk_gridfirst`, `heat_stress_gridfirst`, `gridfirst_spatial`, `composite_metrics`, `proposal_bundles`, `glance_view_model`, `analysis/bundle_scores`, `data/source_inventory`.
- **2 import-unreachable**, of which only one is dead:
  - `app/assets/__init__.py` — **FALSE POSITIVE, KEEP**: loaded dynamically via `ASSET_PACKAGE = "india_resilience_tool.app.assets"` (`app/views/read_the_docs_view.py:12`).
  - `app/adm2_cache.py` — **truly dead** (see CHG-0231).

Additional dynamic-reference pitfalls recorded for future audits:

- `india_resilience_tool/compute/tests/*` are live pytest tests (rootdir collection), not dead code; any redundancy there belongs to the CHG-0229 test-dedup appendix.
- `tools/pipeline/compute_indices_bootstrap.py` is **LIVE**: `compute_indices_multiprocess.py:65` loads it via `__import__("tools.pipeline.compute_indices_bootstrap", ...)`, and it is covered by `tests/test_compute_indices_bootstrap.py`. It appears in no static import edge — a textbook dynamic-import false dead-positive.

### CHG-0231: Orphan Code and Scripts

All deletions route through the CHG-0223 quarantine mechanism and the CHG-0227 closeout sweep. Gate for every row: re-run the zero-reference git-grep at the execution SHA before deleting, using the CHG-0230 gate exclusions (this plan doc, `docs/HANDOFF.md`, `docs/BACKLOG.md` do not count as references).

| File | Evidence (at `861f44c`) | Disposition |
|---|---|---|
| `india_resilience_tool/app/adm2_cache.py` | Unreachable from all entry points; referenced only by its own test and stale doc rows (`MANIFEST.md`, `docs/module_responsibility_map.md`, `app/CLAUDE.md`) | `DELETE` with `tests/test_app_adm2_cache.py`; sweep the three doc mentions |
| `tools/diagnostics/block_orphan_audit.py` | Zero references in `*.py`/`*.md` outside itself | `DELETE` |
| `tools/diagnostics/health_txx_lens_demo.py` | Zero references | `DELETE` |
| `tools/diagnostics/profile_compute_realdata.py` | Zero references | `DELETE` |
| `tools/diagnostics/profile_drought_spi.py` | Zero references | `DELETE` |
| `tools/pipeline/compute_indices.py` | Superseded single-process/debug CLI; no `.py` references outside itself; doc rows in `docs/functionality_contract.md`, `docs/refactor_acceptance.md`, `tools/README.md`, and `MANIFEST.md:410` ("Older single-process compute pipeline") | **ASK-USER** — CLI-surface retirement; on approval, `DELETE` and sweep all four doc rows (the `MANIFEST.md` row is the most damaging if left stale — it is the AI-facing source of truth) |
| `tools/pipeline/compute_indices_bootstrap.py` | LIVE via `__import__` from `compute_indices_multiprocess.py:65` + own test | **KEEP** (corrects an earlier draft that marked it a candidate) |

Untracked files keep their CHG-0222 classifications unchanged (roster diagnostics `KEEP_IN_PLACE`; `tools/_scratch_roster_inventory.py` `QUARANTINE_CANDIDATE`). CHG-0231 touches no untracked files.

### CHG-0232: Stale Documentation Disposition

Vocabulary: `DELETE` / `TRIM` / `REFRESH` / `KEEP`. Rule: a doc that is the sole home of methodology prose is never `DELETE` — at most `TRIM`/`REFRESH`. Every `DELETE` requires a skim-confirm gate (open the file, confirm no open items or unique methodology) at execution time.

| Doc | Signal | Disposition |
|---|---|---|
| `docs/dead_code_candidate_report.md` | Every row marked done; superseded by this plan + git history | `DELETE` after skim-confirm |
| `docs/refactor_acceptance.md` | Completed-initiative artifact | `DELETE` after confirming no open items (note: holds a `compute_indices` doc row swept by CHG-0231) |
| `docs/bundle_task_master.md` | Completed-initiative artifact | `DELETE` after confirming no open items |
| `docs/pytest_baseline_failures.md` | Point-in-time baseline record | `DELETE` only after the CHG-0222 baseline capture (`pytest_baseline_<sha>.txt` in the quarantine directory) exists — that record supersedes this doc |
| `docs/c6_workers_yearly_retention_benchmark.md` | Perf-era brief | `DELETE` only if `docs/perf_phase2_brief.md` is **committed** (it is currently untracked; a working-tree-only supersession record cannot authorize deleting tracked docs from HEAD) and it supersedes this brief; else `KEEP` |
| `docs/c7_bundle_only_runner_design.md` | Perf-era brief | Same gate as above (perf brief must be committed first) |
| `docs/aqueduct_onboarding_methodology.md` (~29 KB) | Retired-CLI material; former TRIM rule dissolved by CHG-0228 (its protected function `aggregate_crosswalk_to_targets` is deleted) | `DELETE` after skim-confirm, per CHG-0228 item 5 |
| `docs/aqueduct_field_contract.md` | Same family | `DELETE` after skim-confirm, per CHG-0228 item 5 |
| `docs/module_responsibility_map.md` | Stale rows (e.g. `adm2_cache`) but live purpose | `REFRESH`, or fold into `MANIFEST.md` if fully redundant with it |
| `docs/functionality_contract.md` | Live contract doc with stale rows | `REFRESH` (sweep `compute_indices` row if CHG-0231 ASK-USER approves) |
| `README_EXPOSURE_HYDRO_CONTEXT_PATCH.md` (untracked) | `PROTECTED_PENDING` per CHG-0222 | Out of scope here |

Aqueduct-doc dispositions defer to CHG-0228 (which superseded the CHG-0225 TRIM rule); do not duplicate them here.

### CHG-0233: Doc-Only-Referenced Tools

Seventeen tools are referenced only from `.md` files. Doc-only reference is **not** dead-code evidence by itself — most are documented offline runbook steps required by the clean-regen direction. Per-file gate: zero-`.py`-reference recheck at execution SHA; every `DELETE` also sweeps its rows in `tools/README.md`, `docs/command_catalog.md`, and `MANIFEST.md`.

**KEEP — documented offline runbook steps (clean-regen + retained hydro-context feature):** `tools/data_acquisition/*` (ERA5/NEX downloads), `tools/data_prep/*` (hurs derivation, reanalysis prep, ERA5 organize), `tools/geodata/build_admin_boundaries_from_lgd.py`, `tools/geodata/build_adm1_geojson.py`, `tools/geodata/build_states_geojson.py`, river/crosswalk builders (`build_river_topology`, `clean_river_network`, `enrich_river_network_districts`, `build_block_*`/`district_*` crosswalk builders, `build_admin_hydro_summary`).

**KEEP — active tooling:** `tools/docs/build_technical_note_html.py` (builds the committed Read-the-Docs asset), `tools/diagnostics/profile_prepare_dashboard.py`, `tools/diagnostics/roster_audit.py`, `tools/diagnostics/verify_admin_join_consistency.py`, `tools/diagnostics/migrate_trailing_dot_dirs.py` (stale-dir remediation), `tools/diagnostics/audit_thematic_bundle_completeness.py`.

**DELETE candidates (superseded / one-shot exploration; each needs its own zero-ref recheck):**

| Tool | Note |
|---|---|
| `tools/subbasin_shp_explore.py` | One-shot exploration of retired hydro-mode inputs |
| `tools/geodata/inspect_block_shapefile.py` | One-shot inspection |
| `tools/geodata/convert_blocks_shp_to_geojson.py` | Verify against the CHG-0226 `build_blocks_geojson` deferral before deleting |
| `tools/diagnostics/heat_stress_gridfirst_parity.py` | Superseded parity check |
| `tools/diagnostics/profile_drought_fullpass.py` | Superseded profiling script |
| `tools/diagnostics/profile_drought_realdata.py` | Superseded profiling script |
| `tools/data_acquisition/nex_india_subset_download_s3_v1.py` | v2 exists and is test-referenced |
| `tools/legacy/DONOTUSE_ArtparkGenerateReport.py` | `tools/legacy/` is do-not-touch — **flag for discussion only, never delete unilaterally** |

Aqueduct tools are disposed wholesale by CHG-0228 (superseding CHG-0225); cross-reference, do not duplicate.

## Public Interfaces

- Remove the entire Aqueduct pipeline CLI surface (seven `tools/geodata` modules and the `prepare_dashboard` `aqueduct` subcommand) per CHG-0228, plus hydro-only internal helpers per CHG-0224.
- No `tools.geodata.aqueduct_common` module is created (CHG-0225 superseded).
- Preserve all dashboard runtime function names, shared call signatures, district/block behavior, and retained hydrological-context behavior.
- No ranking, threshold, aggregation, baseline, unit, or geospatial methodology changes are permitted.

## Test Plan

- **Step 0 — baseline capture (CHG-0222):** before any edit, run `python -m pytest -q` (or the hermetic subset if environment-blocked) at the execution SHA and store the output as `pytest_baseline_<sha>.txt` in the CHG-0223 quarantine directory. All later "no new failures" comparisons are against this record.
- CHG-0224: run portfolio core/UI/tier tests, geography-controls tests, rankings tests, SPI hygiene tests, landing/ribbon/overlay tests, and legacy runtime portfolio wiring tests.
- Add a downstream-caller gate for all eight edited modules:
  - enumerate importers;
  - search removed symbols and keyword parameters;
  - import retained caller modules;
  - verify `app/runtime.py` callsites against preserved signatures.
- CHG-0228 post-delete smoke (replaces the superseded CHG-0225 gate):

```bash
python -c "import tools.runs.prepare_dashboard; print('import OK')"
python -m tools.runs.prepare_dashboard list
python -m pytest -q tests/test_prepare_dashboard_runner.py tests/test_config.py tests/test_metrics_registry.py
```

- The two Aqueduct retirement guard tests (`test_config.py`, `test_metrics_registry.py`) must pass; the four deleted Aqueduct test files are recorded as removed, not failed.
- Run `python -m pytest -q` where the configured Windows/data environment permits.
- If the full suite is environment-blocked, run every hermetic focused test and record the exact Windows/data-dependent checklist and baseline comparison.
- Acceptance requires no new failures relative to the Step 0 baseline record (`pytest_baseline_<sha>.txt`).

## Assumptions and Ledger

- Pending patch, specification, and boundary-audit files are live unless explicitly abandoned.
- Bare basin/sub-basin terminology can remain where it describes retained river-basin context, defensive negative contracts, or legitimate compute concepts.
- `build_blocks_geojson` remains live until a separate methodology decision.

| Change | Status |
|---|---|
| `CHG-0222` baseline and classification | `SUGGESTED` |
| `CHG-0223` persistent quarantine | `SUGGESTED` |
| `CHG-0224` hydro residue and caller verification | `SUGGESTED` |
| `CHG-0225` Aqueduct extraction and deletion | `SUPERSEDED` by `CHG-0228` |
| `CHG-0226` block-builder deferral | `SUGGESTED` |
| `CHG-0227` final validation and graph refresh | `SUGGESTED` |
| `CHG-0228` Aqueduct wholesale retirement (defined 2026-07-15; supersedes `CHG-0225`) | `SUGGESTED` (definition applied; execution pending approval) |
| `CHG-0229` retired-feature test trim (pending in-chat, prior session) | `SUGGESTED` |
| `CHG-0230` reachability inventory (this document section; no deletions) | `SUGGESTED` |
| `CHG-0231` orphan code and scripts | `SUGGESTED` |
| `CHG-0232` stale documentation disposition | `SUGGESTED` |
| `CHG-0233` doc-only-referenced tools | `SUGGESTED` |
| `CHG-0234` review-hardening amendments (gate exclusions, CHG-0228 decision gate, duplication policy, scratch class, committed-brief gate, baseline capture) | `APPLIED (user-confirmed)` |
