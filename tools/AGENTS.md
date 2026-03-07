# AGENTS.md — Tools (ops / diagnostics / data prep)

Applies to: `tools/`

Goal: keep non-runtime scripts organized, documented, and safe to run.

## Rules
- `tools/` is **not runtime**: code under `india_resilience_tool/` must not import `tools/`.
- Prefer scripts runnable via `python -m tools.<package>.<module> --help`.
- Prefer `argparse` over hard-coded paths (support `IRT_DATA_DIR` / `IRT_PROCESSED_ROOT` when relevant).
- Keep side effects explicit (write paths, overwrites, deletes). Add `--dry-run` where destructive.
- Keep every script documented in `tools/README.md` (purpose + command + inputs/outputs).

## Validation
- For any tools refactor: run the module with `--help` and one minimal no-op/validation path if available.

