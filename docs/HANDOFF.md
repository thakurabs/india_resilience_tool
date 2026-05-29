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
- Commit: ea98c3e
- Working Tree: clean (4 commits ahead of origin/add_flood_depth, not pushed)
- Last Updated: 2026-05-28 (Asia/Kolkata)
- Notes:
  - Sectoral lens-based rework: 6 of 8 bundles migrated (Health, Industrial, Investment/Financial, Infrastructure, Asset Thermal, Asset Hydro). Agricultural Risk and Life & Livelihood Loss Risk still pending dossier + config migration.
  - CDD (`pr_consecutive_dry_days_lt1mm`) now flows through the admin grid-first compute path (mirror of CWD). Hydro/sub-basin remains on the legacy polygon-first path.
  - Optimized bundle parity audit reports `issues=1` for `geometry/admin/adm1.geojson` under `--level admin` non-scoped builds. Tracked as CHG-0031; not blocking CHG-0028/CHG-0029.

---

## Global Change Ledger

| Change ID | Status | Files | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|---|
| CHG-0028 | APPLIED (user-confirmed) | `india_resilience_tool/config/proposal_bundles.py`, `tests/test_proposal_bundle_config.py`, `docs/lens_scoring_methodology.md`, `MANIFEST.md` | Industrial Risk bundle rewritten to lens dossier §7 (4 rules: rx1day, rx5day, cdd, txge35) with absolute+change+impact weights, `weight_mode="explicit_normalized"`, `min_available_rule_weight_fraction=0.70`. | `pytest -q` on `tests/test_proposal_bundle_config.py` passes (incl. new `test_industrial_risk_matches_lens_dossier_section_7`). Yearly-ensemble/yearly-model artifacts (4790+2063 tasks) wrote without failure on `--level admin` rebuild. | ea98c3e | Source-masters provenance verified before commit per user constraint. |
| CHG-0029 | APPLIED (user-confirmed) | `india_resilience_tool/compute/extreme_rainfall_gridfirst.py`, `tests/test_extreme_rainfall_gridfirst.py`, `tests/test_metrics_registry.py`, `docs/extreme_rainfall_flash_flood_methodology_v2.md`, `docs/climate_risk_indicator_inventory.md`, `MANIFEST.md` | Migrated CDD (`pr_consecutive_dry_days_lt1mm`) admin path to grid-first compute via new `_cdd` mirror of `_cwd`; added to `EXTREME_RAINFALL_GRIDFIRST_SLUGS`; added dispatcher elif branch; relaxed metric-registry invariant from `==` to `.issubset(...)` since the frozenset is now the dispatcher source-of-truth and may carry co-located non-bundle metrics. | `pytest -q` passes incl. 2 new CDD grid-first tests and the relaxed bundle-membership invariant. Yearly-ensemble CDD artifacts wrote without failure. | ea98c3e | Hydro/sub-basin CDD remains on the legacy polygon-first path (explicitly out of v2 dossier scope). |
| CHG-0031 | SUGGESTED | `tools/optimized/build_processed_optimised.py` | Reconcile builder plan vs. parity auditor plan for `--level admin` non-scoped geometry. `_geometry_tasks` should plan `adm1.geojson` and `admin_block_index.parquet` by default (without requiring `--include-shared-admin-artifacts`) when no `--state` is provided, so the auditor's expected artifacts match the builder's produced artifacts. Last toucher: `2a83461` (CHG-0026 work). | Add a planner test asserting `geometry == 4` for `--level admin` without state scope. Verify `parity_report.json` `issue_count` drops to 0 after re-run. | ea98c3e | Not blocking CHG-0028/CHG-0029 — the issue is in a code path neither CHG touched; surfaced during CHG-0029 verification. `--include-shared-admin-artifacts` does NOT currently cure the issue (geometry remains at 2). |
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

---

## Open Threads / Known Issues

- [ ] CHG-0031: `--level admin` non-scoped optimized-bundle build under-plans geometry (2 tasks, missing `adm1.geojson` + `admin_block_index.parquet`); parity audit reports `issues=1`. `--include-shared-admin-artifacts` flag does not fix it. See ledger entry.
- [ ] Sectoral lens rework: Agricultural Risk and Life & Livelihood Loss Risk dossiers + config migrations still outstanding (the other 6 sectoral bundles are migrated).
- [ ] Branch `add_flood_depth` is 4 commits ahead of `origin/add_flood_depth`; not pushed.
- [ ] CHG-0026, CHG-0027, CHG-0030 acceptance pending (committed/deferred per in-chat ledger; user has not issued `Applied CHG-xxxx`).

---

## Resume Checklist (fast)

1) Confirm snapshot:
   - `git status --short --branch`
   - `git rev-parse --short HEAD`
2) Run fast tests:
   - `python -m pytest -q`
3) If changes involve UI:
   - run the Streamlit entrypoint specified in `README.md`
