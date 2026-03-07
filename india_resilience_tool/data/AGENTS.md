# AGENTS.md — Data Layer (loaders, schemas, spatial matching)

Applies to: `india_resilience_tool/data/`

Goal: keep data contracts explicit and stable, and keep this layer **Streamlit-free**.

## Non-negotiables
- **No Streamlit imports** in this directory (`analysis/`, `data/`, `viz/` are Streamlit-free).
- Prefer pure functions (compute) separated from I/O (read/write paths).
- No hard-coded machine-specific paths; respect `IRT_DATA_DIR` / processed-root helpers.

## Contracts to protect
- **CRS:** boundary loaders must return geometries in **EPSG:4326** (or clearly document/convert).
- **ADM2 (district) identifiers:** normalized to `state_name`, `district_name`, `geometry`.
- **ADM3 (block) identifiers:** normalized to `state_name`, `district_name`, `block_name`, `geometry`.
- **Master tables:** preserve identifier columns:
  - district master: `state`, `district`
  - block master: `state`, `district`, `block`
- **Master metric column naming:** `{metric}__{scenario}__{period}__{stat}`.

## Spatial matching
- Matching must be deterministic: given the same point/feature, return the same unit.
- Any fallback logic (e.g., point-in-polygon) must be documented in the function docstring.
- Be explicit about behavior when no match is found (raise vs return `None` vs warning).

## Validation
- Prefer small synthetic tests in `tests/` for schema helpers and matching edge cases.
- Fast command: `python -m pytest -q`

