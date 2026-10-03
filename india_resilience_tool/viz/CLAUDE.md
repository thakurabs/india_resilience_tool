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

## When to add tests

Visualization is low testing priority — prefer testing the data transforms that feed plots, not the rendering itself.

| Change | Requirement |
|--------|-------------|
| Color scale or normalization logic | Add a test for the data transform only if the logic is non-trivial |
| Autoscaling or limit calculation | Manual check across 2-3 metrics with edge cases (all-NaN, single value) |
| Export change (PDF/image) | Manual checklist: file opens, pages render, labels/legends present |
| Chart layout or label change | Manual visual check only; no pytest |
| Formatting helper change | Run existing tests; add a test only if the helper feeds downstream logic |

Run existing tests after any change: `python -m pytest -q`
