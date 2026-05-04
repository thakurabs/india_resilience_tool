# CLAUDE.md — Analysis Layer

Applies to: `india_resilience_tool/analysis/`

Modules: `bundle_scores.py`, `map_enrichment.py`, `metrics.py`, `portfolio.py`, `timeseries.py`

Goal: protect scientific correctness and keep analytical logic testable.

---

## Missing data behavior (required)

Assume NaNs and masked values are common.

Every non-trivial data function must define and document its NaN behavior explicitly in its docstring:
- raises an error, OR
- returns NaNs, OR
- returns partial results + warning/log

Never silently drop, coerce, or fill NaNs without documentation.

---

## Units, baselines, time, and space

- Be explicit about units and any conversions applied.
- Be explicit about baseline periods vs projection periods.
- Treat temporal aggregation carefully: rolling windows, calendar assumptions, leap years.
- Treat spatial aggregation carefully: CRS consistency, grid-vs-polygon aggregation semantics.

---

## Methodology changes (strictly controlled)

Never change ranking logic, thresholds, baselines, or aggregation silently.

If a change impacts outputs:
- Call it out explicitly in the proposal
- Add or adjust a pytest test asserting expected ordering on a small synthetic dataset, including tie behavior

---

## Engineering guidance

- Prefer pure functions for computations; separate I/O from compute where possible.
- Use type hints for non-trivial functions and public APIs.
- This layer is **Streamlit-free** — no `import streamlit` anywhere here.

---

## Validation checklist

For any ranking or scoring change:
- Manual sanity check: pick one known district/block; verify expected monotonicity
- Pytest test: assert ordering + tie behavior on synthetic inputs
- Run: `python -m pytest -q`
