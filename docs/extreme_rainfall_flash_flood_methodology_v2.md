# Extreme Rainfall | Flash Flood Risk v2 Methodology

This note documents the admin-only v2 methodology for the active
`Extreme Rainfall | Flash Flood Risk` bundle.

## Scope

The v2 implementation applies to admin `district` and `block` outputs only.
Public metric slugs, value columns, processed CSV paths, composite weights,
Glance selectors, and optimized artifact contracts are unchanged.

Hydro `basin` and `sub_basin` outputs continue to use the legacy
polygon-average-first compute path until a hydro migration is explicitly
scoped.

## Metrics

The active bundle remains the six existing metrics:

| Metric slug | v2 behavior |
|---|---|
| `pr_max_1day_precip` | Annual maximum finite daily precipitation per grid cell |
| `pr_max_5day_precip` | Annual maximum strict rolling 5-day precipitation total per grid cell; any NaN invalidates the affected window |
| `r20mm_very_heavy_precip_days` | Count of finite days with precipitation `>= 20 mm/day` |
| `r95p_very_wet_precip` | Total precipitation on days strictly greater than the baseline cell threshold |
| `r95ptot_contribution_pct` | R95p total as percent of wet-day precipitation |
| `cwd_consecutive_wet_days` | Longest run of days with precipitation `>= 1 mm/day`; NaN breaks runs |

## Spatial Method

The pipeline computes annual metric values on each climate grid cell first.
It then area-weights finite cell values to admin polygons using the shared
grid-first spatial overlap cache under `processed/_internal/spatial_weights/`.

Polygon aggregation drops NaN cells and requires at least 50% of the polygon's
overlapped cell weight to remain finite. Polygons below that retained-weight
floor receive `NaN` for the year.

## Missing Data

Annual cell coverage uses the actual number of timestamps loaded for that
cell-year after dropping February 29 where applicable. A cell-year must retain
at least 90% finite daily values. Coverage-failed cell-years emit `NaN`.

For CWD, coverage-passed all-dry years emit `0`. For R95pTOT,
coverage-passed years with no wet-day denominator emit `0`; coverage or
baseline retention failures emit `NaN`.

## R95p And R95pTOT

Admin v2 uses an internal percentile contract:

- baseline years: `1990-2010`
- wet day: `>= 1 mm/day`
- percentile: `95`
- quantile method: `linear`
- exceedance operator: strict `>`

These settings are implemented inside
`india_resilience_tool/compute/extreme_rainfall_gridfirst.py`. The registry
keeps the legacy R95p/R95pTOT params so hydro outputs do not change silently.

## Private Caches

Annual grid metrics are cached under:

```text
processed/_internal/extreme_rainfall/grid_metrics/<slug>/<model>/<grid_id>/<scenario>/<year>.nc
```

R95p thresholds are cached under:

```text
processed/_internal/extreme_rainfall/thresholds/<model>/<grid_id>/baseline=1990-2010/p95.nc
```

Each cache has a JSON sidecar with method version, grid id, input hashes, and
blob hash. Missing, stale, mismatched, or torn sidecars are treated as cache
misses.

## Not In This Version

- basin and sub-basin migration to grid-first
- R99p
- two-day heavy rainfall events
- sector-specific thresholded rainfall indicators
- JRC flood depth inside this bundle

JRC flood-depth metrics are handled by the separate `Riverine Flood` bundle.
