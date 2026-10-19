# Count-metric missing-data review (F-05)

Date: 2026-10-19. Change: CHG-0717. Production computation and published data were not changed.

## Scope and evidence

`tests/test_count_missing_data_review.py` exercises ten production reducer paths:
Heat Risk >=, >, <=, < counts; Cold Risk <=, < counts and longest <= run;
tropical nights >28 C; wet-bulb days >=28 and >=30 C. Fixtures use small synthetic
daily arrays, without downloads or national recomputation.

- 30 missing-input cases: empty time axis, one missing observation, and 365 missing days.
- 60 observed-input controls: no events, all events, one observation, partial missingness,
  exact threshold equality, and extreme finite values.
- 10 island-fill cases: a missing overlapping cell with finite neighbouring donors.
- 10 mixed-polygon cases: equal overlap weights on a missing cell and an observed event cell.
- One control verifies that a genuinely NaN field permits IDW, no donors produce no fill,
  and an entirely missing polygon remains NaN.

Result: **61 passed, 50 strict expected failures**. The expected failures assert the desired
missing-data contract, rather than accepting fabricated zeros. Only AssertionError is allowed
as an expected failure; unexpected errors remain test failures. A future fix will produce strict
XPASS failures until the expected-failure markers are removed deliberately.

## Confirmed behavior

All ten reducers return zero for entirely missing input. These zeros appear finite to
spatial aggregation. They prevent subcell IDW and halve a count of one in the synthetic
polygon with equal missing/observed cell weights. This mixed-polygon example demonstrates
potential impact; it does not establish nationwide geographic prevalence.

Observed zero events correctly produce zero. Inclusive and strict threshold comparators
behave as specified. Missing days break cold runs. Partial series count their observed events;
this test does not establish a suitable annual completeness threshold.

## Current published-data spot check

Read-only inspection of these four files under
`D:/projects/irt_data/processed_optimised/metrics/{slug}/masters/admin/block/state=Lakshadweep.parquet`
confirmed the September pattern in the historical 1990–2010 ensemble mean:

| Block | TX >=35 C days | TN >25 C nights | Twb >=28 C days | TXx (C) / provenance |
|---|---:|---:|---:|---|
| Andrott | 0 / native | 0 / native | 0 / native | 29.060217 / idw |
| Bitra | 0 / native | 0 / native | 0 / native | 29.265347 / idw |
| Chetlat | 0 / native | 0 / native | 0 / native | 28.808233 / idw |

A zero by itself is not evidence of a defect: TX >=35 C is zero in all ten island blocks
in this slice, and may be a genuine absence of events in observed cells. The issue is the
missing-input reduction and misleading `native` provenance, supported by the synthetic
reproduction and the mean-metric IDW contrast. These checks did not reopen raw NetCDFs,
trace every model/year, or audit every national unit.

## Correction to assess next

1. Preserve NaN where there are no valid observations in each count/run path, using the
   original input validity mask (comparisons alone lose missingness).
2. Separately decide how partially observed years should be handled; do not silently
   introduce a new completeness policy with the all-missing fix.
3. Audit affected models, years and polygon support, including partial overlaps, before
   selecting a rebuild footprint. Preserve genuine observed zeros.
4. Version or invalidate affected annual caches, rebuild affected masters and optimized
   shards, and verify value/provenance parity. No composite rebuild is needed for the
   retired dashboard, although legacy offline composites would remain stale unless rebuilt.
5. Remove strict xfail markers only after the desired contracts pass. Inspect affected
   island values and flags in the dashboard and verify unaffected units retain their values.

## Validation notes

The combined targeted dashboard, registry, documentation and build-planning run passed
161 tests, skipped 3 optional environment-dependent tests and recorded the 50 expected
F-05 failures. The broader optimized-bundle suite hit the same eight missing-boundary
fixture failures listed in `docs/pytest_baseline_failures.md`; those tests require temporary
canonical `districts_4326.geojson`/`blocks_4326.geojson` fixtures. The relevant Glance
build-planning regressions passed.

Manual dashboard check (not executed in a browser): launch the app, select district and
block metrics in thematic and sectoral domains, and confirm no composite choice or
Back to Glance action is offered. Reopen a session with old Glance/composite state and
check that the metric ribbon selects a valid individual metric. Open Read the Docs and
check the revised domain/grid, ensemble explanation, figures and navigation.
