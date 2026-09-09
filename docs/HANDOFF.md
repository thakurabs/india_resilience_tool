# HANDOFF — India Resilience Tool (IRT)

This file is the **persistent** project handoff + change ledger used by AI agents.

## Update policy (important)

- Agents MUST NOT modify this file unless the user confirms applied work in the format:
  - `Applied CHG-0007`
  - `Applied: CHG-0007, CHG-0008; Rejected: CHG-0006`

Until then, agents should keep the working ledger **in chat** and produce a
**PERFECT HANDOFF POINT** section at the end of any session-ending message.

---

## Current Working Snapshot

- Snapshot Source: git
- Branch: add_flood_depth
- Commit: a0ab120
- Working Tree: dirty (modified: `MANIFEST.md`, `docs/national_absolute_scale_pitfalls.md`, `tests/test_heat_risk_national_ruler_pilot.py`, `tools/README.md`, `tools/diagnostics/heat_risk_national_ruler_pilot.py`; untracked: `docs/diagnostics/heat_risk_pilot_smoke/`, `docs/diagnostics/heat_risk_pilot_smoke_v2/`, `notebooks/irt_place_map.html`)
- Last Updated: 2026-09-08 (UTC)
- Notes:
  - Heat Risk national-ruler pilot hardening (CHG-0349 through CHG-0354) is committed at `a0ab120` and user-confirmed applied.
  - Targeted pilot tests pass (9/9) and the three-state no-map smoke succeeds. The full suite has 15 failures outside the pilot paths; treat these as a recorded baseline, not a green suite.
  - A later, uncommitted working set appeared while this handoff was being prepared. It appears to address all five review findings as CHG-0356 through CHG-0360 (strict shard and area gates, reconciliation split, degraded-mode artifact, concat warning, and CLI help), with tests/docs, but has not been reviewed or run.
  - The original smoke directory appears to have been regenerated with the new schema, so it is no longer a trustworthy pre-change comparison artifact. The `_v2` smoke remains available.

---

## Global Change Ledger

| Change ID | Status | Files | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|---|
| CHG-0028 | APPLIED (user-confirmed) | `india_resilience_tool/config/proposal_bundles.py`, `tests/test_proposal_bundle_config.py`, `docs/lens_scoring_methodology.md`, `MANIFEST.md` | Industrial Risk bundle rewritten to lens dossier §7 (4 rules: rx1day, rx5day, cdd, txge35) with absolute+change+impact weights, `weight_mode="explicit_normalized"`, `min_available_rule_weight_fraction=0.70`. | `pytest -q` on `tests/test_proposal_bundle_config.py` passes (incl. new `test_industrial_risk_matches_lens_dossier_section_7`). Yearly-ensemble/yearly-model artifacts (4790+2063 tasks) wrote without failure on `--level admin` rebuild. | ea98c3e | Source-masters provenance verified before commit per user constraint. |
| CHG-0029 | APPLIED (user-confirmed) | `india_resilience_tool/compute/extreme_rainfall_gridfirst.py`, `tests/test_extreme_rainfall_gridfirst.py`, `tests/test_metrics_registry.py`, `docs/extreme_rainfall_flash_flood_methodology_v2.md`, `docs/climate_risk_indicator_inventory.md`, `MANIFEST.md` | Migrated CDD (`pr_consecutive_dry_days_lt1mm`) admin path to grid-first compute via new `_cdd` mirror of `_cwd`; added to `EXTREME_RAINFALL_GRIDFIRST_SLUGS`; added dispatcher elif branch; relaxed metric-registry invariant from `==` to `.issubset(...)` since the frozenset is now the dispatcher source-of-truth and may carry co-located non-bundle metrics. | `pytest -q` passes incl. 2 new CDD grid-first tests and the relaxed bundle-membership invariant. Yearly-ensemble CDD artifacts wrote without failure. | ea98c3e | Hydro/sub-basin CDD remains on the legacy polygon-first path (explicitly out of v2 dossier scope). |
| CHG-0031 | SUGGESTED | `tools/optimized/build_processed_optimised.py` | Reconcile builder plan vs. parity auditor plan for `--level admin` non-scoped geometry. `_geometry_tasks` should plan `adm1.geojson` and `admin_block_index.parquet` by default (without requiring `--include-shared-admin-artifacts`) when no `--state` is provided, so the auditor's expected artifacts match the builder's produced artifacts. Last toucher: `2a83461` (CHG-0026 work). | Add a planner test asserting `geometry == 4` for `--level admin` without state scope. Verify `parity_report.json` `issue_count` drops to 0 after re-run. | ea98c3e | Not blocking CHG-0028/CHG-0029 — the issue is in a code path neither CHG touched; surfaced during CHG-0029 verification. `--include-shared-admin-artifacts` does NOT currently cure the issue (geometry remains at 2). |
| CHG-0032 | APPLIED (user-confirmed) | `india_resilience_tool/config/proposal_bundles.py`, `docs/lens_scoring_methodology.md` (new §12 + renumber §12→§13, §13→§14), `tests/test_proposal_bundle_config.py`, `tests/test_proposal_bundle_builder.py`, `tests/test_proposal_bundle_per_lens_persistence.py`, `README.md`, `docs/proposal_bundle_methodology.md`, `docs/bundle_calculation_audit.md`, `MANIFEST.md` | Agricultural Risk lens migration: 7 rules rewritten with absolute+change+impact decomposition, `weight_mode="explicit_normalized"`, `min_available_rule_weight_fraction=0.70`. TXx impact band reset to self-derived 35-45°C (rice/wheat reproductive heat-sterility onset; IMD plains heatwave saturation) replacing the retired 40-45°C human-heatwave band. Six other rules carry self-derived LOW-confidence bands per §4 protocol; TNle10 zone caveat deferred to BL-0020. Rule-weight reshuffle: TXx 0.10→0.15, TNle10 0.20→0.15. | `pytest -q tests/test_proposal_bundle_config.py tests/test_proposal_bundle_builder.py tests/test_proposal_bundle_per_lens_persistence.py` = 37 passed, 0 failed. Full-suite 10 failures are pre-existing baseline (env: openpyxl missing; unrelated code paths: ribbon/master_freshness/timeseries/bundle_scores/SPI smoke/R95p test-spec mismatch) — none introduced by this CHG. | ea98c3e (dirty) | Artifact rebuild deferred to BL-0021 (CHG-0032-RT). This box never carried the upstream `master_metrics_by_*.csv` files for the 6 non-TXx rules (and 3 of those rules — SPI3 episodes, SPI3 longest, TNle10 — are absent from `processed_optimised/metrics/` too), so the rebuild belongs on the data-prod environment. |
| CHG-0230 | APPLIED (user-confirmed) | `docs/dead_code_redundancy_purge_plan.md` | Reachability-inventory amendment: AST import-closure method (dashboard seed `main.py` + 21 pipeline seed modules from `prepare_dashboard`), verified live-feature map (85/98 non-test package modules dashboard-live, 11 pipeline-only live, 1 truly dead: `app/adm2_cache.py`), dynamic-import pitfalls (`app/assets` via `ASSET_PACKAGE`, `compute_indices_bootstrap` via `__import__`), plus new deletion work packages CHG-0231 (orphan code/scripts), CHG-0232 (stale-docs disposition), CHG-0233 (doc-only tools); CHG-0227 sweep list extended; ledger updated. | Evidence commands re-run at 861f44c (adm2_cache refs = 4 expected files; 20 unique `tools.*` planned strings; closure counts match; 4 diagnostics zero-ref). Doc-only change; no pytest per risk table. | 861f44c | CHG-0231/0232/0233 remain SUGGESTED — each executes later under its own approval + CHG-0223 quarantine. `compute_indices.py` retirement is an ASK-USER checkpoint; `compute_indices_bootstrap.py` confirmed LIVE (corrects prior draft). |
| CHG-0349 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py` | Decoupled roster/area loading from map rendering and state-filtered geometry loading; added strict/degraded geometry modes. | Included in 9/9 targeted tests and successful three-state no-map smoke. | a0ab120 | Review found strict mode did not reject a partially missing requested shard or every unusable district area; follow-up tracked under CHG-0355. |
| CHG-0350 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py` | Composite and sub-composite coverage denominators now use total configured bundle weights; added metric fit report and warning for pool-wide missing metrics. | Targeted pilot tests: 9 passed. Smoke fitted 14/14 metrics under both rulers. | a0ab120 | Diagnostic correctness change; does not alter fitted metric rulers. |
| CHG-0351 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py` | Scoring and coverage share a finite-value mask, so infinities are not reported as covered. | Targeted pilot tests: 9 passed. | a0ab120 | — |
| CHG-0352 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tools/README.md`, `MANIFEST.md` | Removed unsupported block mode; pilot is explicitly district-only. | Targeted tests and `--help` completed successfully. | a0ab120 | — |
| CHG-0353 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py`, `docs/national_absolute_scale_pitfalls.md` | Exact empirical mid-rank CDF now drives the CDF ruler; 21-knot approximation error is measured and exact support is written separately. | Smoke support: 5,782 knots across 14 metrics; support counts/monotonicity verified. Max metric grid21 score error: 4.1443. | a0ab120 | Methodology-impacting change. Exact CDF has an open score range by construction. |
| CHG-0354 | APPLIED (user-confirmed) | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py`, `tools/README.md`, `MANIFEST.md`, `docs/national_absolute_scale_pitfalls.md` | Canonical geometry roster defines the expected universe; coverage is emitted by state × metric × slice with national totals and roster reconciliation. | Three-state smoke: 413 master/grid rows, zero missing roster districts, zero orphan master keys. | a0ab120 | Reconciliation labels and degraded-mode output need follow-up under CHG-0356. |
| CHG-0355 | SUGGESTED | — | Original combined follow-up proposal for strict requested-shard and per-district area validation. | — | a0ab120 | The dirty implementation split this proposal into CHG-0356 and CHG-0357. Resolve/supersede this ledger entry when the user rules on that split. |
| CHG-0356 | SUGGESTED | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py` | Fail strict mode when any explicitly requested state geometry shard is missing; allow partial shards only in degraded mode. | New tests are present but not yet run or reviewed. | a0ab120 + dirty | Uncommitted; not user-confirmed. |
| CHG-0357 | SUGGESTED | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py`, `docs/national_absolute_scale_pitfalls.md` | Require finite `area_m2 > 0` for every retained district in strict mode; warn/exclude invalid areas in degraded mode. | New tests are present but not yet run or reviewed. | a0ab120 + dirty | Uncommitted; not user-confirmed. |
| CHG-0358 | SUGGESTED | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py`, `docs/national_absolute_scale_pitfalls.md` | Separate roster districts with no master row from those with master rows but no finite value. | New tests are present but not yet run or reviewed. | a0ab120 + dirty | Uncommitted; not user-confirmed. |
| CHG-0359 | SUGGESTED | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py` | Preserve the `roster_reconciliation.csv` output contract in degraded/no-roster and empty-master runs. | New test is present but not yet run or reviewed. | a0ab120 + dirty | Uncommitted; not user-confirmed. |
| CHG-0360 | SUGGESTED | `tools/diagnostics/heat_risk_national_ruler_pilot.py`, `tests/test_heat_risk_national_ruler_pilot.py`, `tools/README.md`, `MANIFEST.md` | Avoid concatenating empty CDF support frames and correct the stale `--states` help/contract text. | Changes are present but not yet run or reviewed. | a0ab120 + dirty | Uncommitted; not user-confirmed. |
| CHG-0361 | APPLIED (user-confirmed) | `docs/HANDOFF.md` | Persisted the Heat Risk pilot implementation review, validation evidence, live dirty-tree state, pending follow-ups, and exact resume steps for compaction. | Documentation-only; checked diff and repository status. | a0ab120 + dirty | Administrative handoff update requested directly by the user. |
| CHG-0001 | SUGGESTED |  |  |  |  |  |

Statuses:
- SUGGESTED
- APPLIED (user-confirmed)
- REJECTED (user-confirmed)
- SUPERSEDED (by CHG-xxxx)

---

## Per-File Change Ledger

Add sections per file as needed:

### path/to/file.py
| Change ID | Status | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

### tools/diagnostics/heat_risk_national_ruler_pilot.py

| Change ID | Status | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|
| CHG-0349–CHG-0354 | APPLIED (user-confirmed) | District-only pilot hardened with canonical roster coverage, configured-weight gates, finite masks, and exact mid-rank CDF support. | 9 targeted tests pass; three-state smoke passes; full-suite baseline is 15 unrelated failures. | a0ab120 | Committed. |
| CHG-0356–CHG-0360 | SUGGESTED | Dirty implementation intended to close strict validation, reconciliation/degraded-output, warning, and help-text gaps found in review. | New tests exist but no post-edit tests have been run. | a0ab120 + dirty | Review the entire working set before requesting user confirmation or committing. |

---

## Open Threads / Known Issues

- [ ] CHG-0031: `--level admin` non-scoped optimized-bundle build under-plans geometry (2 tasks, missing `adm1.geojson` + `admin_block_index.parquet`); parity audit reports `issues=1`. `--include-shared-admin-artifacts` flag does not fix it. See ledger entry.
- [ ] Sectoral lens rework: Life & Livelihood Loss Risk dossier + config migration still outstanding (the other 7 sectoral bundles are migrated).
- [ ] CHG-0032-RT: Agricultural Risk artifact rebuild deferred to data-prod environment — see BL-0021 for commands and prerequisites.
- [ ] Branch `add_flood_depth` is 2 commits ahead of `origin/add_flood_depth`; not pushed.
- [ ] CHG-0026, CHG-0027, CHG-0030 acceptance pending (committed/deferred per in-chat ledger; user has not issued `Applied CHG-xxxx`).
- [ ] CHG-0355: resolve the original combined strict-validation proposal against the dirty implementation's split IDs CHG-0356/CHG-0357.
- [ ] CHG-0356–CHG-0360: review the uncommitted implementation and tests for strict shard/area gates, reconciliation status separation, stable degraded output, warning-free support assembly, and CLI/docs accuracy.
- [ ] Decide whether to retain or remove the two untracked smoke directories and `notebooks/irt_place_map.html`; do not delete them without explicit approval.

---

## Resume Checklist (fast)

1) Confirm snapshot:
   - `git status --short --branch`
   - `git rev-parse --short HEAD`
2) Inspect the uncommitted pilot diff before editing:
   - `git diff -- tools/diagnostics/heat_risk_national_ruler_pilot.py`
3) Review CHG-0356 through CHG-0360 against the five recorded findings and resolve whether CHG-0355 is superseded by the split. Do not mark anything applied without user confirmation.
4) If the user approves completion, add focused tests for partial shard loss, invalid areas, reconciliation status separation, degraded output, warning-free support assembly, and CLI help.
5) Run validation in the Windows `irt` environment:
   - `python -m pytest -q tests/test_heat_risk_national_ruler_pilot.py`
   - `python -m tools.diagnostics.heat_risk_national_ruler_pilot --states "Telangana,Himachal Pradesh,Kerala" --no-maps --out-dir docs/diagnostics/heat_risk_pilot_smoke_v3`
6) Treat the existing full-suite result as a non-green baseline: 15 failed, 1426 passed, 2 skipped. Re-run only after the focused work is stable, and triage any delta rather than claiming the suite is green.
7) Ask the user how to handle the untracked smoke outputs and notebook HTML; do not delete or commit them implicitly.
