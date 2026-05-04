# CLAUDE.md — Tests (pytest)

Applies to: `tests/`

Goal: fast, reliable tests that protect scientific and UX-critical logic.

---

## Conventions

- Framework: `pytest`
- File names: `test_*.py`
- Test names: `test_*`
- Prefer **small synthetic inputs** — avoid large datasets and network access in tests.

---

## High-value coverage areas

Always add or update tests for:
- **Ranking and ordering logic**: assert ordering + tie behavior explicitly
- **NaN handling**: empty inputs, all-NaN, partial-NaN — test each case
- **Deterministic transforms** used by plots (test the data prep, not pixels)
- **Configuration defaults and validation** (metric slugs, registry shape, bundle weights)
- **Master column naming contract**: `{metric}__{scenario}__{period}__{stat}` double-underscore format
- **Boundary identifier contracts**: `state`, `district`, `block`, `basin_id`, etc.

---

## Commands

```bash
# Fast (preferred for iteration)
python -m pytest -q

# With coverage
python -m pytest --cov=india_resilience_tool

# Single test file
python -m pytest tests/test_<name>.py -q

# Single test
python -m pytest tests/test_<name>.py::test_<function> -q
```

---

## Known baseline failures

Check `docs/pytest_baseline_failures.md` before interpreting a failure as regression. If a test there starts passing, that is worth noting — it may mean a defect was resolved.

---

## Adding tests for a new change

When proposing a change, always include:
- Suggested test file path
- Test function name
- Core assertion (what invariant it checks)
- At least one edge case (NaN, empty, ties, extremes)
