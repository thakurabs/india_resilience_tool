# Dead and Redundant Code Purge Plan

**Plan Status:** Reviewed and ready for execution after explicit approval

**Planning Snapshot:** `GIT:add_flood_depth@37cac63`

**Planning Working Tree:** Dirty; 13 untracked files, no tracked modifications

**Change Range:** `CHG-0222` through `CHG-0227`

## Summary

Use a clean worktree for tracked purge changes, preserve pending feature and audit work, remove only proven unreachable hydro behavior, and extract live Aqueduct dependencies before deleting obsolete CLIs. Graphify supplies clues only; source references, caller sweeps, documentation, imports, and tests determine deletion safety.

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
- Tag affected tests as hermetic, data-dependent, or Windows-only before running them.
- Acceptance: every untracked file and affected test has one explicit disposition.

### CHG-0223: Persistent Quarantine

- Never use `git clean`, wildcards, or directory-level moves; skip `tools/patches/` entirely.
- Move approved candidates individually to `/mnt/d/projects/irt_dead_code_quarantine/<date>/<sha>/CHG-0223/`.
- Record source, destination, SHA-256, reason, and exact restore command.
- Keep quarantine until 30 days after the purge branch merges.
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

### CHG-0225: Aqueduct Refactor-Then-Delete

Create `tools/geodata/aqueduct_common.py` with the verified 20-symbol closure:

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

### CHG-0226: Block Builder Deferral

- Keep `tools/geodata/build_blocks_geojson.py` as `METHODOLOGY_DEFERRED`.
- Link its disposition to `docs/perf_phase2_brief.md`.
- Any future LGD migration must cover `tools/runs/prepare_dashboard.py`, `tests/test_prepare_dashboard_runner.py`, `tests/test_build_blocks_geojson.py`, documentation, runbooks, and script references.
- Make no block-boundary methodology change during this purge.

### CHG-0227: Closeout

- Sweep deleted symbols, filenames, dotted paths, command examples, and PowerShell references.
- Remove stale `__pycache__` directories only after deletion and testing.
- Refresh graphify and verify deleted module nodes are absent; use an approved full rebuild if incremental shrink handling leaves stale nodes.
- Update `BL-0023` and `BL-0024` only when explicitly requested.
- Update `docs/HANDOFF.md` only after explicit `Applied CHG-xxxx` confirmation.

## Public Interfaces

- Remove only the two obsolete Aqueduct hydro CLI entry points and hydro-only internal helpers.
- Add the internal module `tools.geodata.aqueduct_common`.
- Preserve all dashboard runtime function names, shared call signatures, district/block behavior, and retained hydrological-context behavior.
- No ranking, threshold, aggregation, baseline, unit, or geospatial methodology changes are permitted.

## Test Plan

- CHG-0224: run portfolio core/UI/tier tests, geography-controls tests, rankings tests, SPI hygiene tests, landing/ribbon/overlay tests, and legacy runtime portfolio wiring tests.
- Add a downstream-caller gate for all eight edited modules:
  - enumerate importers;
  - search removed symbols and keyword parameters;
  - import retained caller modules;
  - verify `app/runtime.py` callsites against preserved signatures.
- CHG-0225 pre-delete and post-delete smoke:

```bash
python -c "import tools.geodata.aqueduct_common; import tools.geodata.build_aqueduct_admin_crosswalk, tools.geodata.build_aqueduct_block_crosswalk, tools.geodata.build_aqueduct_admin_masters, tools.geodata.validate_aqueduct_workflow; print('import OK')"
```

- Run Aqueduct baseline, admin-transfer, validator, metrics-registry, and configuration tests.
- Run `python -m pytest -q` where the configured Windows/data environment permits.
- If the full suite is environment-blocked, run every hermetic focused test and record the exact Windows/data-dependent checklist and baseline comparison.
- Acceptance requires no new failures relative to the execution-time baseline.

## Assumptions and Ledger

- Pending patch, specification, and boundary-audit files are live unless explicitly abandoned.
- Bare basin/sub-basin terminology can remain where it describes retained river-basin context, defensive negative contracts, or legitimate compute concepts.
- `build_blocks_geojson` remains live until a separate methodology decision.

| Change | Status |
|---|---|
| `CHG-0222` baseline and classification | `SUGGESTED` |
| `CHG-0223` persistent quarantine | `SUGGESTED` |
| `CHG-0224` hydro residue and caller verification | `SUGGESTED` |
| `CHG-0225` Aqueduct extraction and deletion | `SUGGESTED` |
| `CHG-0226` block-builder deferral | `SUGGESTED` |
| `CHG-0227` final validation and graph refresh | `SUGGESTED` |
