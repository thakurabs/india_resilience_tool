# Shade release evidence

Formula frozen before confirmation-window evaluation. No national outputs published.

| Window | Old RMSE (C) | Candidate RMSE (C) | Old count MAE (days/year) | Candidate count MAE | Improvement rule |
|---|---:|---:|---:|---:|---|
| 2005-2014 | 2.934 | 0.693 | 35.27 | 8.41 | PASS |
| 1990-2004 | 2.991 | 0.645 | 31.58 | 7.01 | PASS |

All 12 site/window pairs meet median absolute error <1 C and RMSE <1.5 C. The +/-20% annual count target fails for 37 of 128 pairs with >=50 reference events. Every exception is recorded in `absolute_count_targets.csv`; this target was not a release veto under the accepted policy.

All 138 deployment site-year >=32 pairs have fewer than 50 reference events. The metric is retained; this evidence does not validate it through a 50-event gate. Absolute errors, false positives, missed events and year-block intervals are reported separately.

## Reproduction report

Original 2005-2014 calendar and day-selection reproduced for 30 comparisons. Maximum numeric difference: 0. See `reproduction_scores.csv` and `reproduction_parity.json`. The reproduction-policy comparison uses complete finite reference hours and retains leap days.

## Deployment-policy report

Drop February 29, require all 365 daily peaks, and omit incomplete boundary years. The first year in each window is excluded because the UTC cache begins after the local day begins. See `report.json` for every site/policy exclusion. All methods share the same valid dates; hourly Bernard is the primary reference and hourly Stull is separately scored.

Daily scores and seasonal errors: `site_scores.csv`, `seasonal.csv`. Annual errors and uncertainty: `annual_counts.csv`, `annual_uncertainty.csv`. Bootstrap uses 1,000 year-block draws with seed 583; intervals describe temporal sampling uncertainty, not reference-method or climate-model uncertainty.

## Remaining publication work

National build and staged promotion have not run. `tools/pipeline/build_shade_release.py` is the
budget and staging runner (CHG-0587, hardened by CHG-0588..0591); it writes only under its `--stage`
directory and has no publication path. Promotion and rollback remain unimplemented: nothing consumes
`release_ready.json`. The three-state single-model/year pilot is in `pilot_report.json`. Complete downstream staging cost, full calendar audit, release verification and rollback rehearsal are still required. The sibling `twb_days_ge_*` false-zero defect remains unresolved; W-04/F-05 are not fixed across Heat Stress.

## Staged production pilot

All four metrics were built for ACCESS-CM2 historical 1990–2010 in Kerala, Rajasthan and
Himachal Pradesh at district and block levels. Compute, ensemble, master, optimized and
state-value stages completed. Strict parity reported **zero issues** for 24 masters,
24 yearly-model outputs, 24 yearly-ensemble outputs and six geometry outputs.
See `staged_parity.json`, `staged_downstream_timings.json` and `staged_disk_budget.json`.
This is a pilot, not national publication.

The full calendar-header audit (`source_calendars.csv`) yields 19 supported models and
2,717 model-years (`release_roster.json`), excluding KACE-1-0-G. The published baseline
side-by-side zero/NaN comparison is `published_baseline_comparison.csv`.
See [NEX residuals](NEX_RESIDUALS.md) for remaining model bias without QDM.

## Verification

- Dedicated shade suite `tests/test_wbgt_shade_release.py`: 27 passed (17 before CHG-0591;
  `verification.json` previously mis-recorded this as 16).
- Focused compute/registry/marker/bootstrap/state-value/ensemble tests: 135 passed.
- Full `python -m pytest -q tests --tb=short`: 1,673 passed, 3 skipped, 15 failures.
  The same 15 failure cases reproduce on untouched `269be91`; no new failure remains.
- New modules pass Ruff. Whole-runtime Ruff reports 86 findings; untouched HEAD has 88.
- `python -m black --check india_resilience_tool/` and `python -m mypy india_resilience_tool/`
  could not run: those modules are not installed. No dependencies were changed.
- `git diff --check` passed.
