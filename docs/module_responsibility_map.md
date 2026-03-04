# Module Responsibility Map — India Resilience Tool (IRT)

This document is the 1-page map of **what each module does** and the **allowed import boundaries**.

## 1) Runtime execution chain (dashboard)

Primary entrypoint (only supported):
- `india_resilience_tool/app/main.py`

Execution order (high-level):
1. `india_resilience_tool/app/main.py`
   - initializes Streamlit session state via `ensure_session_state`
   - calls the app runner
2. `india_resilience_tool/app/runtime.py`
   - owns `st.set_page_config`
   - orchestrates sidebar + ribbon + map/rankings + details panels
3. `india_resilience_tool/app/map_pipeline.py`
   - builds merged GeoDataFrame + enriched columns + legend + Folium map + rankings table
4. `india_resilience_tool/app/sidebar.py`
   - renders selectors (admin level, analysis mode, view selection, hover toggle)
5. `india_resilience_tool/app/views/*`
   - renders left-panel views and right-panel detail views
6. Downstream layers (Streamlit-free):
   - `india_resilience_tool/data/*` for I/O, discovery, merges
   - `india_resilience_tool/analysis/*` for computations (rank/percentile/portfolio/timeseries)
   - `india_resilience_tool/viz/*` for charts/tables/exports
   - `india_resilience_tool/config/*` + `india_resilience_tool/utils/*` for declarative config + utilities

## 2) Responsibilities by module

### `india_resilience_tool/app/` (Streamlit UI + state + orchestration)
- `main.py`: Streamlit script entry; calls `run()`
- `runtime.py`: app routing + orchestration (no business logic beyond UI wiring)
- `state.py`: session_state key contract + defaults; level-aware helpers
- `sidebar.py`: sidebar widgets and navigation/jump flags
- `views/map_view.py`: Folium map rendering + click event extraction + inline add-to-portfolio control
- `views/rankings_view.py`: rankings table rendering (district + block) and portfolio add/remove parity
- `views/details_panel.py`: single-unit “Climate Profile” rendering + export buttons
- `views/state_summary_view.py`: optional state-wide climate profile summaries
- `portfolio_ui.py`: portfolio panel UI (bundle-first selection, compare table, charts)
- `point_selection_ui.py`: add-by-location UI (single + batch coordinates)
- `portfolio_multistate.py`: multi-state portfolio helpers (concat + summary)
- `adm2_cache.py`: boundary caching/simplification helpers
- `geography.py`: filesystem-backed discovery for selector options
- `perf.py`: opt-in perf timing helpers (UI-visible)

### `india_resilience_tool/data/` (Streamlit-free)
- boundary loading and normalization (ADM2/ADM3)
- processed artifact discovery (yearly ensemble files, etc.)
- deterministic merges (boundary ↔ master)
- robust CSV loading + schema normalization/parsing

### `india_resilience_tool/analysis/` (Streamlit-free)
- ranking/percentiles/risk classes
- portfolio state logic (stored in session_state, but no Streamlit dependency)
- time-series loading and normalization
- case-study helpers (data prep for exports)

### `india_resilience_tool/viz/` (Streamlit-free)
- figure/chart builders (Matplotlib/Plotly)
- table assembly helpers
- export builders (PDF/ZIP) — returns bytes / file outputs
- styling/colors/formatting (deterministic)

### `india_resilience_tool/config/` + `india_resilience_tool/utils/` (Streamlit-free)
- declarative registries and constants
- path semantics (processed-root resolution)
- name normalization and small helpers

### `tools/` (non-runtime scripts; may import package modules)
- pipelines, diagnostics, data acquisition/prep
- never imported by runtime package modules

## 3) Import boundaries (enforced)

Hard rules:
- `india_resilience_tool/analysis/`, `data/`, `viz/`, `compute/`, `config/`, `utils/` must **not import Streamlit**.
- `india_resilience_tool/app/` may import downstream layers.
- `india_resilience_tool/` package code must **not import `tools/`**.

If a refactor needs shared logic:
- put Streamlit-free logic in `analysis/`, `data/`, `viz/`, `config/`, or `utils/`
- keep `app/` as the orchestrator/glue layer only
