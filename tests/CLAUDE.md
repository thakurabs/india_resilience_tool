# CLAUDE.md — Tests (pytest)

Applies to: `tests/`

Goal: targeted tests that protect scientific correctness and data contracts. Do not add tests reflexively — test what breaks expensively.

---

## Core principle: test what breaks expensively

A test earns its place if it would catch a real bug that is hard to detect manually and costly to have in production. Tests that just assert a function runs without error, or that duplicate what an upstream package already validates, add maintenance burden without protection.

---

## Risk tiers

**Tier 1 — Always test (scientific compute and data contracts)**
- Ranking and ordering logic: assert ordering + tie behavior on synthetic inputs
- SPI / climate index calculations: assert against a reference value or known output
- Baseline and threshold handling: confirm values are computed from the correct period
- Master column naming: assert the `{metric}__{scenario}__{period}__{stat}` format is preserved
- Identifier column presence: `state`, `district`, `block`, `basin_id`, etc.
- Known regression guards: any bug that was previously silent and hard to catch

**Tier 2 — Add selectively (only when behavior is non-obvious)**
- NaN handling: only when the fallback is non-trivial or was previously a source of bugs
- Spatial matching edge cases: only when fallback or no-match behavior changes
- Config validation: only when new validation logic is introduced, not for every new slug

**Tier 3 — Skip or manual-only (low test value)**
- UI widget and layout changes → manual click-path check
- Chart and plot rendering → manual visual check
- Export file generation → manual open-and-inspect check
- Pipeline script behavior → `--dry-run` check
- Pure refactors with no logic change → run existing tests only

---

## Conventions

- Framework: `pytest`
- File names: `test_*.py`, test functions: `test_*`
- Use small synthetic inputs — no large datasets, no network access
- One assertion per logical invariant — do not bundle unrelated checks

---

## Commands

```bash
# Fast (preferred)
python -m pytest -q

# Single file
python -m pytest tests/test_<name>.py -q

# With coverage (use sparingly — for audit, not routine)
python -m pytest --cov=india_resilience_tool
```

---

## Known baseline failures

Check `docs/pytest_baseline_failures.md` before treating a failure as a regression. If a listed failure starts passing, note it — it may mean a known defect was resolved.

