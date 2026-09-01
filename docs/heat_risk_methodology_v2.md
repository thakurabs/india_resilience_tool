# Heat Risk Methodology v2

Scope: documents the v2 scientific methodology for Heat Risk metrics in IRT. Records what changes, what stays, and which known issues are deferred.

This methodology is delivered by the lean Heat Risk v2 plan and is intentionally narrow: it covers only the changes required for scientific correctness in Heat Risk compute. Output paths, slugs, units, and column names are unchanged.

---

## 1. Scope

Applies to Heat Risk metric slugs in `india_resilience_tool/config/metrics_registry.py`:

- `tas_annual_mean`
- `tasmax_summer_mean`
- `tas_summer_mean`
- `txge30_hot_days`
- `tasmin_tropical_nights_gt25`
- TX90p hot-days percentage
- TN90p warm-nights percentage
- WSDI warm-spell duration indicator
- HWFI heatwave-frequency days
- HWFI heatwave-frequency events
- HWA heatwave amplitude
- TXx annual maximum daily TX
- TNx annual maximum daily TN

Other bundles (water, agriculture, exposure, hydro) are out of scope for this methodology change. They will be brought to scientific consistency in successive passes.

---

## 2. What changes

### 2.1 Compute order — grid first, then aggregate

Previous: daily fields were reduced to a polygon area-mean series first (boolean rasterized mask), then indicators were computed on that polygon series.

v2: indicators are computed per grid cell first (per-cell DOY 90p calibration, per-cell annual exceedance counts, per-cell annual extremes), then aggregated to admin polygons by area-weighted overlap.

Rationale: under the previous order, percentile-day metrics are biased because area-mean daily values lose the within-polygon distribution. ETCCDI percentile indices are defined per series and are not invariant under pre-averaging.

### 2.2 Spatial aggregation — exact area-weighted overlap

Previous: `all_touched=True` boolean rasterization, polygon = union of touched cells with equal weight.

v2: per-cell weights are `area(polygon ∩ cell)` in an equal-area projection (EPSG:6933). Cells outside the polygon contribute zero; partial cells contribute their fractional area.

Aggregation rules per metric semantic bucket:

- **Mean-style** (areal mean of a per-cell quantity): `aggregated = sum(w_i * x_i) / sum(w_i)` over cells with valid data. This now covers `tas_annual_mean`, `tasmax_summer_mean`, `tas_summer_mean`, `txx_annual_max`, and `tnx_annual_max`.
- **Percent-of-days** (TX90p, TN90p): `aggregated = 100 * sum_i(w_i * exceed_days_i) / sum_i(w_i * valid_days_i)`. Denominator is the area-weighted count of *valid* days, not nominal days. Cells with missing days reduce both numerator and denominator proportionally.
- **Count-style** (`txge30_hot_days`, `tasmin_tropical_nights_gt25`, WSDI, HWFI-days): area-weighted mean of per-cell annual counts.
- **Areal extreme** (TXx, TNx, HWA, HWFI-events): area-weighted mean of per-cell annual extremes. See §6 for the naming caveat.

### 2.3 Quantile method — linear

Per-cell DOY 90p thresholds are computed with `numpy.quantile(..., method="linear")`. The previous default `method="nearest"` snapped to a sample value and interacted with the operator choice in a way that masked the strict-vs-inclusive distinction. `method="linear"` interpolates between order statistics and is consistent with ETCCDI references.

### 2.4 Warm exceedance — strict `>`

All Heat Risk warm-side exceedance metrics use strict `>` against the DOY 90p threshold. The operator audit covers absolute and percentile thresholds. `>=` is not used in Heat Risk warm exceedance.

Combined with linear quantiles, this ensures the in-base TX90p exceedance frequency is well-defined under ties.

---

## 3. Calibration baseline period

Calibration period in v2 remains 1990–2010, matching the existing `SCENARIOS["historical"]["periods"]` token used in `tools/pipeline/compute_indices_multiprocess.py`.

This is **deferred from this pass**. The registry advertises `baseline_years = (1981, 2010)` for several Heat Risk metrics, and aligning calibration to registry intent requires renaming the historical column from `historical__1990-2010__*` to `historical__1981-2010__*` for Heat Risk slugs, which is a data-contract change. That work is held for a dedicated follow-up paired with a metric-aware baseline-label refactor across `master_columns.py`, the ribbon and map layers, and the composite/proposal builders.

---

## 4. Percent-metric denominator

For TX90p and TN90p the area-weighted formula is:

```
TX90p_pct = 100 * sum_{cells c in poly} ( w_c * exceed_days_c ) / sum_{cells c in poly} ( w_c * valid_days_c )
```

`w_c` is the area of `cell c ∩ poly` in EPSG:6933 square metres.
`valid_days_c` is the count of days in the year with non-missing data at cell c.
`exceed_days_c` is the count of days in the year with `TX_c > DOY90p_c` (or TN, by analogue).

A cell with all-missing days contributes 0/0 → excluded. A cell with partial coverage contributes proportionally to both numerator and denominator.

---

## 5. Bootstrap policy

In-base TX90p / TN90p frequencies are computed **without** the Zhang et al. 2005 bootstrap. Inhomogeneity at the calibration-window edges is known to bias in-base exceedance frequencies slightly low. The magnitude is small for 21-year windows; explicit bootstrapping is deferred as a future improvement.

Reference: Zhang, X., Hegerl, G., Zwiers, F. W., & Kenyon, J. (2005). Avoiding inhomogeneity in percentile-based indices of temperature extremes. J. Climate. https://doi.org/10.1175/JCLI3366.1

---

## 6. Areal interpretation of TXx, TNx, HWA, HWFI-events

Under v2 these are *area-weighted means of cellwise annual extremes / event counts*, not the polygon's hottest day or integer event count.

- TXx areal value: areal mean of `max_{day in year}(TX_c)` across cells. Lower than the polygon's hottest single cell.
- TNx areal value: areal mean of `max_{day in year}(TN_c)`.
- HWA areal value: areal mean of cellwise heatwave amplitude peaks.
- HWFI-events areal value: areal mean of cellwise integer event counts. Fractional values are expected.

UI labels and `value_col` strings still describe "Annual Max" / "Peak" / event integer counts. **Label and `value_col` corrections are deferred** to a housekeeping pass to preserve the data contract in this pass. The methodology doc is the canonical reference for the correct interpretation until that housekeeping is done.

---

## 7. HWFI-events variable

The HWFI-events metric is computed against `tasmax`, consistent with IMD operational heatwave criteria (IMD Met Monograph on Cold and Heat Waves).

The registry slug `hwfi_events_tmean_90p` is historically misleading: the slug suggests `tmean` while the metric correctly uses `tasmax`. Slug rename is **deferred** to avoid a public-API change in this pass. Consumers and the methodology doc treat the metric as `tasmax`-based.

Reference: IMD Met Monograph on Cold and Heat Waves. https://mausam.imd.gov.in/imd_latest/contents/Met_Monograph_Cold_Heat_Waves.pdf

---

## 8. State composite

The state-level composite Heat Risk score remains the unweighted mean of district composites in `india_resilience_tool/compute/glance_view_model.py`. This is a known limitation: districts contribute equally regardless of area or population. Correction is **deferred** to a dedicated CHG once district-level Heat Risk is scientifically correct under v2.

---

## 9. Spatial weights and threshold caches

Two private caches support v2 compute:

- `processed/_internal/heat_risk/thresholds/<model>/<var>/1990-2010.nc` — per-cell DOY 90p thresholds with a JSON sidecar recording `(input_file_hash, baseline_years, methodology_note)`. Sidecar mismatch invalidates the cache.
- `processed/_internal/spatial_weights/<level>__<grid_id>.parquet` — sparse `{admin_id, cell_index, area_m2}` weights with a JSON sidecar recording `(grid_id, crs_epsg=6933, boundary_file_hash)`.

Both directories are private (`_internal/` prefix). No existing reader consumes them. They are not part of the data contract.

Spatial weights are produced by a new build-time tool, `tools/pipeline/build_spatial_weights.py`, which uses `exactextract`. `exactextract` is a build-time dependency only; app runtime never imports it.

---

## 10. Data contract guarantees

This methodology change preserves the data contract:

- Same `processed/<slug>/<level>/...` paths.
- Same column names, including `historical__1990-2010__*`.
- Same slugs, units, `value_col` strings.
- Same registry public API.

Only the *values inside* the existing columns change. Downstream readers, composite and proposal builders, the optimized bundle, Glance, and UI layers continue to read the same surface.

---

## 11. Deferred items (tracked here, not in `docs/BACKLOG.md`)

To be promoted to `docs/BACKLOG.md` only on explicit instruction.

- Baseline period alignment to registry-declared 1981–2010, paired with metric-aware baseline-label refactor across all Heat Risk consumers.
- HWFI-events slug rename (`hwfi_events_tmean_90p` → `hwfi_events_tasmax_90p`) and downstream migration.
- TXx, TNx, HWA, HWFI-events UI display-label and `value_col` corrections to reflect areal-mean-of-cellwise-extreme semantics.
- State composite math redesign.
- ETCCDI in-base bootstrap (Zhang et al. 2005).
- Optimized-direct writes, methodology-version provenance, manifest, atomic publish, mixed-version detection, version resolver.
- Worker auto-sizing, RAM/disk preflight, model-exclusion policy reporting.
- Block-tie ranking policy at coarse-grid scale.
- Cache namespace versioning beyond sidecar invalidation.
- Promotion of `exactextract` from build-time to runtime dependency.

These will be addressed after Heat Risk and the other bundles are brought to scientific consistency under v2.

---

## 12. References

- Zhang et al. 2005, percentile bootstrap context — https://doi.org/10.1175/JCLI3366.1
- ETCCDI index notes — https://etccdi.pacificclimate.org/list_27_indices.shtml
- WMO ETCCDI overview — https://wmo.int/climate-change-detection-and-indices
- IMD heatwave criteria — https://mausam.imd.gov.in/imd_latest/contents/Met_Monograph_Cold_Heat_Waves.pdf
