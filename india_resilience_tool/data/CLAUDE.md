# CLAUDE.md — Data Layer (loaders, schemas, spatial matching)

Applies to: `india_resilience_tool/data/`

Modules: `adm2_loader.py`, `adm3_loader.py`, `hydro_loader.py`, `master_loader.py`, `master_columns.py`, `merge.py`, `discovery.py`, `crosswalks.py`, `spatial_match.py`, `optimized_bundle.py`, `river_loader.py`, `river_topology.py`

Goal: keep data contracts explicit and stable, and keep this layer Streamlit-free.

---

## Non-negotiables

- **No Streamlit imports** in this directory — data is Streamlit-free.
- Prefer pure functions (compute) separated from I/O (read/write paths).
- No hard-coded machine-specific paths; always resolve through `IRT_DATA_DIR` / processed-root helpers in `paths.py`.

---

## Contracts to protect

### CRS
Boundary loaders must return geometries in **EPSG:4326**, or clearly document and apply conversion.

### Identifier columns

| Level | Required identifiers |
|-------|---------------------|
| District master | `state`, `district` |
| Block master | `state`, `district`, `block` |
| Basin master | `basin_id`, `basin_name` |
| Sub-basin master | `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name` |

Never rename or drop these columns without explicit approval and a migration plan.

### Master metric column naming

```
{metric}__{scenario}__{period}__{stat}
```

Double-underscore separators are load-bearing — parsers depend on this format.

### Boundary identifier normalization

- ADM2 (district): `state_name`, `district_name`, `geometry`
- ADM3 (block): `state_name`, `district_name`, `block_name`, `geometry`

---

## Spatial matching

- Matching must be **deterministic**: given the same point or feature, always return the same unit.
- Any fallback logic (e.g., point-in-polygon) must be documented in the function docstring.
- Be explicit about behavior when no match is found: raise vs return `None` vs emit a warning.

---

## Optimized bundle preference

The dashboard prefers `processed_optimised/` over the legacy `processed/` tree at runtime:
- Parquet masters and yearly facts from `processed_optimised/metrics/...`
- Simplified geometry from `processed_optimised/geometry/...`
- Compact selector metadata from `processed_optimised/context/...`

Do not bypass this preference without explicit discussion.

---

## When to add tests

Data contracts are high-risk — broken identifiers or CRS mismatches corrupt merges silently across the whole dashboard.

| Change | Requirement |
|--------|-------------|
| Identifier column rename or drop | Always add a regression test asserting the column is present post-load |
| Master column naming format change | Always add a test; the double-underscore format is load-bearing |
| CRS handling change | Always add a test confirming output is EPSG:4326 |
| Spatial matching logic change | Add a test if fallback behavior or no-match behavior changes |
| Schema helper or loader refactor | Run existing tests; add a test only if new edge-case behavior is introduced |
| Pure I/O path change | Run existing tests; no new test required |

Key test files: `tests/test_master_loader.py`, `tests/test_merge.py`, `tests/test_naming.py`, `tests/test_crosswalk_context.py`

Run: `python -m pytest -q`
