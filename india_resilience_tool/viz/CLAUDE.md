# CLAUDE.md — Visualization Layer (plots, exports, styling)

Applies to: `india_resilience_tool/viz/`

Modules: `charts.py`, `colors.py`, `exports.py`, `folium_featurecollection.py`, `formatting.py`, `style.py`, `tables.py`

Goal: deterministic figures and safe export behavior. This layer is Streamlit-free.

---

## Non-negotiables

- **No Streamlit imports** in this directory — viz is Streamlit-free.
- Keep plotting **deterministic and reproducible**: same inputs must always produce the same figure.
- Avoid global style mutations (`plt.rcParams`, global color scales) unless explicitly asked.

---

## Autoscaling and normalization

If changing autoscaling, color limits, or normalization logic:
- Validate across multiple metrics and edge cases (all-NaN, single value, negative values)
- Document the new behavior explicitly

---

## Exports (PDF / images)

Any export change must include a manual validation checklist:
1. Generate one sample export
2. Verify the file opens and pages render correctly
3. Verify key labels and legends are present

---

## Performance

- Coordinate with app-layer caching (`app/geo_cache.py`, `app/master_cache.py`) — avoid redoing expensive data prep on every render.
- Prefer testing the **data transforms** that feed plots rather than pixel-perfect image comparisons.

---

## Validation

Run: `python -m pytest -q`

Key test files after viz changes:
- `tests/test_viz_charts.py`
- `tests/test_viz_colors.py`
- `tests/test_viz_exports.py`
- `tests/test_viz_tables.py`
