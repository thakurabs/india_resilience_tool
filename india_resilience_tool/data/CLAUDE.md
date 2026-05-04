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

## Validation

Prefer small synthetic tests in `tests/` for schema helpers and matching edge cases.

Key test files to run after data layer changes:
- `tests/test_master_loader.py`
- `tests/test_merge.py`
- `tests/test_naming.py`
- `tests/test_crosswalk_context.py`

Run: `python -m pytest -q`
