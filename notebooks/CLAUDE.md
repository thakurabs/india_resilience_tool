# CLAUDE.md — Notebooks (exploratory)

Applies to: `notebooks/`

Goal: keep notebooks useful for exploration without polluting runtime or repo history.

---

## Rules

- Notebooks are **non-runtime**: no production code in `india_resilience_tool/` or `tools/` should import from notebooks.
- Do not commit large embedded outputs — clear outputs before committing when feasible.
- Do not store secrets, tokens, API keys, or credentials in notebooks.
- Prefer parameterized paths (env vars or clearly marked constants) over machine-specific absolute paths.

---

## Graduating notebook logic

If a notebook develops into reusable logic:
- Move the logic into `tools/` (and optionally keep the notebook as a thin driver).
- Document the new tool in `tools/README.md` and `MANIFEST.md`.
- Do not leave duplicated logic in both the notebook and the tool.

---

## What Claude Code should not do here

- Do not refactor notebook structure unless explicitly asked.
- Do not add production imports (Streamlit, app-layer modules) to notebooks.
- Do not commit notebooks with outputs unless the user explicitly confirms.
