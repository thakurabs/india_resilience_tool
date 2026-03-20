# AGENTS.md — Analysis Layer (metrics, ranking, portfolio, case studies)

Applies to: `india_resilience_tool/analysis/`

Goal: protect scientific correctness and make analytical logic testable.

## Scientific guardrails

### Missing data behavior (required)
Assume NaNs/masked values are common.
Every non-trivial data function must define behavior explicitly (docstring):
- raises error, OR
- returns NaNs, OR
- returns partial results + warning/log

### Units / baselines / time/space
- Be explicit about units and conversions.
- Be explicit about baseline and projection periods.
- Treat temporal aggregation carefully (rolling windows, calendar assumptions).
- Treat spatial aggregation carefully (CRS, grid vs polygon aggregation).

### Methodology changes
Never change ranking logic, thresholds, baselines, or aggregation silently.
If a change impacts outputs:
- call it out explicitly
- add/adjust a pytest test asserting expected ordering on a small synthetic dataset (including ties).

## Engineering guidance
- Prefer pure functions for computations; separate I/O from compute where possible.
- Use type hints for non-trivial functions and public APIs.

## Validation
- Provide a manual sanity-check plan (e.g., one known district/block; expected monotonicity).
- Add/adjust tests for ranking/scoring changes.
