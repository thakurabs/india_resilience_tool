# CarbonPlan shade WBGT — release notes (CHG-0700..0703)

**Status:** **published 2026-10-11T06:20:39Z** (release `shade-cp-20261011`,
processed mode `aside`). Rollback window open: ≥ 7 days and sign-off before
`close` (not before 2026-10-18).

## What changes

The four shaded-WBGT metrics keep their slugs and labels but change method:

| | Before (legacy) | After (CarbonPlan-aligned) |
|---|---|---|
| Identity | `legacy-original-stull-gridfirst-v2` (unsigned files, pinned by the sidecar) | `carbonplan-shade-gridfirst-qdm-v1` (every file signed) |
| Formula | Stull wet-bulb on daily means | CarbonPlan shade WBGT: tas, tasmax, huss, pressure from elevation; RH at tasmax; no direct solar |
| Bias correction | none | per-cell QDM against UHE-Daily `wbgtmax`, trained 1985–2014 |
| Aggregation | grid-first | grid-first: each cell-day thresholded, cell-year metrics area-weighted (fractional day counts) |
| Models | — | 18 NEX-GDDP-CMIP6 models; historical 1985–2014, SSPs 2020–2080 |
| Coverage gate | none | units with < 0.95 retained area are blank, with the reason shown |

Slugs: `wbgt_shade_stull_annual_mean`, `wbgt_shade_stull_days_ge_28`,
`wbgt_shade_stull_days_ge_30`, `wbgt_shade_stull_days_ge_32`. Composite scoring is
unchanged; none of the four feeds a composite.

## Disclosures

**1. Days ≥ 32 °C got worse in some states (holdout validation).** Scored
out-of-sample (train 1985–1999, test 2000–2010) against UHE-Daily, all 18 models,
frozen rule in [SPEC.md](SPEC.md). Verdict `BLOCK_USER_RULING`; overruled, see
[HOLDOUT_RULING.md](HOLDOUT_RULING.md).

- 32 of 2,664 model × state × metric cuts are *worsened* (≥ 2/3 of units worse and
  the 90% bootstrap lower bound above 0); 144 had too little support to classify.
  29 of the 32 are days ≥ 32 °C, plus two ≥ 30 °C (Dadra–Daman & Diu, Goa) and one
  ≥ 28 °C (Bihar), all from CMCC-CM2-SR5.
- States with a worsened ≥ 32 °C cut, by number of models: Dadra, Nagar Haveli,
  Daman & Diu 7; Chhattisgarh 6; Madhya Pradesh 6; Rajasthan 3; Jharkhand 2;
  Tripura 2; Gujarat, Maharashtra, Telangana 1 each. The increase in median
  absolute error ranges from +0.8 to +10.9 days/yr.
- **No national cut worsened for any model.** National median absolute error
  change across the 18 models (median, range): annual mean −3.52 °C
  (−3.64 … −0.72); ≥ 28 °C −76.5 d/yr (−84.2 … −10.8); ≥ 30 °C −47.7 d/yr
  (−50.8 … −8.4); ≥ 32 °C −1.22 d/yr (−1.83 … +2.00, positive for 3 models but
  not classified worsened).

**2. 169 units are now blank.** 7,752 of 7,921 district/block units pass the 0.95
coverage gate. Of the 169 excluded, 165 fall short because UHE-Daily reference
support is missing over part of the unit (mostly coastal and island units) and 4
because of NEX grid gaps. All 169 had a legacy value; the dashboard now shows the
exclusion reason instead. List: [coverage_loss_units.csv](coverage_loss_units.csv).

**3. Sun-adjustment control: historical differs by 2.9e-7 °C — accepted by the project owner on 2026-10-11 as not material, cause disclosed as unexplained.**
The control rebuilds the published Route B outdoor values for ACCESS-CM2 from the
same cached shade plus the signed solar drivers, to prove the cache this release
reads is the one Route B was validated on.

| Scenario | Values | Mean / max | Day counts | Verdict |
|---|---|---|---|---|
| ssp245 | 2,364,360 | ≤ 3.4e-13 °C | exact | PASS |
| ssp585 | 2,364,360 | ≤ 3.4e-13 °C | exact | PASS |
| historical 1990–2010 | 813,960 | ≤ 2.9e-7 °C (325,583 above 5e-13) | exact | FAIL |

The difference is present per cell before aggregation (all 4,938 cells, 2005). It
is not the historical window, float32 driver storage, a different shade source, or
inputs rewritten after Route B ran (drivers and cache predate Route B's outputs).
Cause not identified. The shade metrics do not use the sun term.
Evidence: [evidence/sun_control.json](evidence/sun_control.json).

**4. Technical note lags.** The technical-note HTML still describes the legacy
shade method. It is regenerated in RUNBOOK step 15, after the rollback window
closes; until then the in-app metric description (which reads the deployed
identity) is authoritative.

## Gate evidence

| Gate | Result | Evidence |
|---|---|---|
| Stage, 18 models | PASS (7,752 eligible units each) | `scratch/wbgt_shade_carbonplan_release/<model>/status.json` |
| Holdout scoring | BLOCK_USER_RULING → overruled | [evidence/holdout_gate.json](evidence/holdout_gate.json), [evidence/holdout_all_cuts.csv](evidence/holdout_all_cuts.csv) |
| Sun-adjustment control | historical FAIL at 2.9e-7 °C (accepted, 2026-10-11); futures PASS | [evidence/sun_control.json](evidence/sun_control.json) |
| Staged processed tree | 36 states, 3,517,788 files, 2,991 s | [evidence/carbonplan_shade_release_manifest.json](evidence/carbonplan_shade_release_manifest.json) |
| Staged optimised build | 4 metrics, 288 masters, 3 h 06 min | `scratch/wbgt_shade_carbonplan_build.log` |
| Strict parity audit, four slugs | issue_count 0 | [evidence/staged_parity_report.json](evidence/staged_parity_report.json) |
| Staged dashboard check | 39 / 39 PASS | [evidence/staged_dashboard_check.txt](evidence/staged_dashboard_check.txt) |
| Full test suite vs [tests_baseline.json](tests_baseline.json) | 15 failed (all 15 in the baseline, 0 regressions), 2,249 passed, 5 skipped; 1 h 59 min | `scratch/wbgt_shade_carbonplan_pytest.log` |
| Legacy identity sidecar (step 9) | 872 optimised files, 288 masters; sha256 `0d80b8a6…` | `processed_optimised/shade_method_identity.json` |
| Cutover prepare (step 10) | PASS; estimated downtime 5.1 min (limit 30) | `_shade_cutover/shade-cp-20261011/prepare.json` |
| Swap (step 11) | PUBLISHED; 4 dirs inode+inventory verified; manifest `37f03c4b…` → `360ef3ea…` | `_shade_cutover/shade-cp-20261011/journal.json` |
| Live identity | all 872 shade files `carbonplan-shade-gridfirst-qdm-v1` | `scratch/wbgt_shade_carbonplan_release_ops/check_live_identity.py` |
| Scoped production audit (step 12) | issue_count 0 | [evidence/production_scoped_parity_report.json](evidence/production_scoped_parity_report.json) |
| Production dashboard check | 40 / 40 PASS (incl. landing view, incumbents) | [evidence/production_dashboard_check.json](evidence/production_dashboard_check.json) |

## Rollback

Until the window is closed (≥ 7 days **and** sign-off), `cutover_carbonplan_shade
rollback` restores the legacy files from the journal, and the legacy identity
sidecar keeps them displayable. Routine optimised builds and state-value runs skip
the shade slugs until then.
