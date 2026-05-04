# CLAUDE.md — App Layer (Streamlit)

Applies to: `india_resilience_tool/app/`

Modules: `runtime.py`, `sidebar.py`, `ribbon.py`, `map_pipeline.py`, `landing_runtime.py`, `details_runtime.py`, `left_panel_runtime.py`, `portfolio_ui.py`, `portfolio_state_runtime.py`, `master_cache.py`, `geo_cache.py`, `adm2_cache.py`, and all `views/` modules.

Goal: keep the UI responsive and predictable. Avoid expensive recomputation and Streamlit key/state pitfalls.

---

## UI structure rules

- Do not restructure UI layout unless explicitly asked.
- Any new widget **must** have an explicit, stable, unique `key=...` argument.
- Prefer extracting non-UI logic into helper functions for testability.
- Avoid writing to `st.session_state` in tight loops; gate state writes behind user actions.
- If adding caching (`@st.cache_data`, `@st.cache_resource`): be explicit about cache keys and invalidation strategy.

---

## Common Streamlit pitfalls to avoid

- **Duplicate widget keys** — causes Streamlit to raise at runtime; always check for key collisions when adding widgets.
- **Rerun loops** — writing to `st.session_state` unconditionally on every render can cause infinite reruns; use guards.
- **Expensive computations on every interaction** — move heavy work into cached functions or precompute offline.

---

## Engineering guidance

- This is the only layer where Streamlit imports are permitted within `india_resilience_tool/`.
- Extract non-UI logic into `analysis/`, `compute/`, or `data/` and keep `app/` thin.
- `master_cache.py` and `geo_cache.py` are the canonical session-state caches; do not duplicate their responsibility elsewhere in `app/`.

---

## When to add tests

The app layer is the lowest testing priority — Streamlit is hard to test well and UI changes frequently.

| Change | Requirement |
|--------|-------------|
| UI layout, widget, or label change | Manual click-path check only (state → action → expected result); no pytest |
| Helper logic extracted from UI into a pure function | Add a small pytest test for the extracted function only |
| Session-state logic change | Manual click-path check; add pytest only if the logic is complex and reusable |
| Caching or performance change | Manual check that the UI renders correctly without rerun loops |

Run existing tests after any change to confirm nothing regressed: `python -m pytest -q`
