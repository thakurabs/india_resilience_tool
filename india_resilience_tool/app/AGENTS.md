# AGENTS.md — App Layer (Streamlit)

Applies to: `india_resilience_tool/app/`

Goal: keep the UI responsive and predictable. Avoid expensive recomputation and Streamlit key/state pitfalls.

## Rules
- Do not restructure UI unless explicitly asked.
- Any new widget MUST have an explicit, stable, unique `key=...`.
- Prefer extracting non-UI logic into helpers for testability.
- Avoid writing to `st.session_state` in tight loops; gate updates behind user actions.
- If adding caching, be explicit about cache keys and invalidation.

## Validation
- For UI changes: provide a click-path manual test plan.
- If extracting helpers: suggest a small pytest test in `tests/` for the helper logic.

## Common pitfalls
- duplicate widget keys
- rerun loops from session_state writes
- expensive computations executed on every interaction
