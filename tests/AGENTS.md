# AGENTS.md — Tests (pytest)

Applies to: `tests/`

Goal: fast, reliable tests that protect scientific and UX-critical logic.

## Conventions
- Use pytest.
- File names: `test_*.py`
- Test names: `test_*`
- Prefer small synthetic inputs; avoid large datasets and network access.

## High-value coverage
- ranking/order logic: assert ordering + tie behavior
- NaN-handling: empty inputs, all-NaN, partial-NaN
- deterministic transforms used by plots (data prep)
- configuration defaults and validation

## Commands
- fast: `python -m pytest -q`
- coverage: `python -m pytest --cov=india_resilience_tool`
