# AGENTS.md — Notebooks (exploratory)

Applies to: `notebooks/`

Goal: keep notebooks useful for exploration without polluting runtime or repo history.

## Rules
- Notebooks are **non-runtime**; no production imports should depend on notebooks.
- Avoid committing large embedded outputs; clear outputs before committing when feasible.
- Do not store secrets, tokens, or credentials in notebooks.
- Prefer parameterized paths (env vars or clearly marked constants) over machine-specific absolute paths.
- If a notebook graduates into a tool, move logic into `tools/` (and keep the notebook as a thin driver if needed).

