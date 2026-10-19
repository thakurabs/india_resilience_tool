# Outdoor WBGT (Route B) — national release specification

CHG-0663. Frozen before national scale-out, at `GIT:add_flood_depth@0cb621f`.
This specification is the contract the national campaign is validated against.
It is **not** a publication approval.

Supersedes nothing. The shade release (`wbgt_shade_stull_*`) keeps its separate
authority and is not touched by this contract.

**User rulings, 2026-10-06** (see §9 for what they settled):

- Publish the outdoor metrics **alongside** `swbgt_empirical_*`. Retire nothing
  in this release. Do not substitute empirical values for missing outdoor ones,
  do not replace empirical inputs in sector composites, and do not reuse their
  scoring calibration.
- Keep the **0.95** unit-coverage gate. Retain the complete administrative
  roster, carrying coverage fractions and exclusion reasons into the exports.
- Hold the unfitted roster until the window-blanking loss is quantified by
  measurement. Do **not** switch to annual QDM application merely to reduce
  missingness, do not drop affected cells for whole scenarios, and do not fill
  screened inputs with fabricated values.

## 1. Model roster

**18 models.** Re-verified on 2026-10-06 by direct file existence over every
required model/scenario/year/variable combination, and by reading each model's
calendar — not inherited from the hard-coded `ENSEMBLE_MODELS` tuple.

```
ACCESS-CM2  ACCESS-ESM1-5  CMCC-CM2-SR5  CMCC-ESM2  CanESM5
EC-Earth3   EC-Earth3-Veg-LR  GFDL-CM4   GFDL-ESM4  INM-CM4-8
INM-CM5-0   KIOST-ESM      MPI-ESM1-2-HR  MPI-ESM1-2-LR
MRI-ESM2-0  NorESM2-LM     NorESM2-MM     TaiESM1
```

Six of the 24 models carrying historical `tas` are excluded, with the exact
missing requirement measured rather than quoted:

| Model | Reason | Measured |
|---|---|---|
| BCC-CSM2-MR | missing inputs | 286 files: `rsds`, `sfcWind` |
| IITM-ESM | missing inputs | 438 files: `rsds`, `sfcWind`, `tasmax` |
| NESM3 | missing inputs | 438 files: `huss`, `rsds`, `sfcWind` |
| IPSL-CM6A-LR | wrong humidity variable | 152 files: `huss` absent; carries `hurs` |
| MIROC6 | wrong humidity variable | 152 files: `huss` absent; carries `hurs` |
| **KACE-1-0-G** | **unalignable calendar** | inputs complete; runs `360_day` |

`IPSL-CM6A-LR` and `MIROC6` carry `hurs` but not the `huss` the pinned CarbonPlan
shade formula consumes. Substituting `hurs` would mix humidity chains inside one
ensemble, so they stay excluded rather than approximated.

**KACE-1-0-G was found during scale-out, not at inventory.** Its inputs are
complete, so an input-existence check passes it, and the original 19-model roster
included it on that basis. Driver extraction then aborted on
`Unsupported calendar: 360_day`. Relaxing that gate would not help: a 360-day year
of 30-day months has a 30 February, `annual_metrics` requires a year's dates to
equal the real calendar minus 29 February, the UHE reference is on a real
calendar, and the pinned QDM groups by day of year. Aligning it would mean
inventing five to six days per model-year, which is fabrication rather than a
gate relaxation. Every other model runs `proleptic_gregorian`, `365_day` or
`noleap`, all supported. Note that the incumbent `swbgt_empirical_*` ensemble
does include KACE-1-0-G, so the two rosters differ by this model as well.

## 2. Geography and identifiers

- Roster source: `districts_4326.geojson` (sha256 `da5799…3684e2`) and
  `blocks_4326.geojson` (sha256 `361fef…4d4b74`), frozen in
  `support_manifest.json`.
- 7,921 units: 784 districts and 7,137 blocks. District **is** a navigation
  level, not only a container.
- Canonical identifier: `unit_index` into `units.csv`, carrying `state_name`,
  `level`, `district_name`, `block_name`, `lgd_code`. Production keys are
  `district_key` / `block_key` as the incumbent metrics already spell them.
- 5,082 intersected NEX cells; 16 excluded for absent elevation (0.31%, under
  the 5% abort gate).

## 3. Scenarios, years, calendars

| Scenario | Years | Count |
|---|---|---|
| historical | 1990–2010 inclusive | 21 |
| ssp245 | 2020–2080 inclusive | 61 |
| ssp585 | 2020–2080 inclusive | 61 |

Calibration is 1985–2014 inclusive and is **separate** from the 1990–2010
reporting baseline. Each model keeps its native calendar (`standard`,
`gregorian`, `proleptic_gregorian`, `noleap`, `365_day`); the calendar is
verified per shard and a calendar change between years aborts. February 29 is
dropped explicitly before any annual reduction, in every calendar.

**The envelope is input-bound, not methodological.** `rsds` and `sfcWind` exist
locally only for historical 1990–2010 and the SSPs 2020–2080. The incumbent
`swbgt_empirical_*` yearly series run historical 1951–2014 and SSP 2015–2100.
See §9, decision D1.

## 4. Inputs and units

| Variable | Required for | Unit gate |
|---|---|---|
| `tas` | all years | `K` |
| `tasmax` | all years | `K` |
| `huss` | all years | `1` or `kg kg-1` |
| `rsds` | historical 1990–2010, SSPs 2020–2080 | `W m-2` |
| `sfcWind` | historical 1990–2010, SSPs 2020–2080 | `m s-1` |

A unit-attribute mismatch aborts the shard. Calendars must be complete and
duplicate-free against the expected date range. Non-finite values are retained,
not filled, so downstream eligibility gates can see them.

Elevation: `elevation_cells.csv`, sha256 `43b966…66a8f6`, CarbonPlan elevations
plus one recovered cell. GMTED is **not** substituted and sea level is **not**
fabricated for cells without an elevation; those cells are excluded and listed.

## 5. Reference, calibration, holdout

- Reference: UHE-Daily `wbgtmax`, 10,957 days, 1985–2014, native 0.05°,
  conservatively area-averaged onto the exact NEX grid, retaining valid-reference
  area fractions. UHE is provisional calibration evidence, **not** observed
  outdoor WBGT truth.
- Calibration: 1985–2014 full period for the released correction.
- Holdout: a contiguous temporal block, trained on 1985–1999 and scored on
  2000–2014. Scored for seasonal distribution and upper quantiles only. There
  is no same-date NEX-to-observation RMSE or correlation score, because NEX
  weather is not synchronised with observations.

## 6. Method

Route B: reproduce CarbonPlan's shade → QDM → sun chain on IRT support.

1. Pinned CarbonPlan shade formula from `08_shade_sun_adjustment.ipynb`
   (source lock `f662b37`), consuming `tas`, `tasmax`, `huss`.
2. Additive quantile delta mapping, trained **per cell** against the regridded
   UHE reference, using CarbonPlan's own pinned `train_bias_correction` from
   `06_bias_correction.ipynb` — 100 quantiles, `time.dayofyear` grouping,
   31-day window. Training is cached by model and spatial support; future
   scenarios reuse the historical mapping rather than retraining.
3. Pinned sun stage per cell, with radiation promoted to float64 before the
   solar function and a 1e-8 W/m² daily-mean conservation gate.
4. Admin summaries use production intersection-area weights.

**Aggregation order is part of the contract.** Two estimands are produced and
must never be conflated:

| Method | Definition | Released as |
|---|---|---|
| `grid_first_cell_metrics` | threshold each cell-day, reduce annually per cell, then area-weight | the production counts and maxima |
| `grid_first_series` | area-weight the daily cell series, then threshold | retained comparator only |

Counts are therefore **area-weighted cell exceedance days** and may be
fractional. Maxima are **area-weighted cell annual maxima**; maximum and spatial
mean do not commute. Annual mean is identical under both orders and carries no
ambiguity. Thresholding a spatial mean is **not** an acceptable substitute for
count-first aggregation.

## 7. Eligibility, completeness and missingness

| Gate | Rule |
|---|---|
| Cell | valid-reference area fraction ≥ 0.50, finite reference, finite historical shade |
| Unit | eligible-cell area coverage ≥ 0.95 |
| Year | the year's dates must equal the complete expected calendar, Feb 29 removed |
| Cell-year | every day finite, else the cell-year is missing |

An incomplete cell-year never produces a low count. A genuine zero count is
preserved and distinguished from missing. Excluded units stay present in the
roster with missing values — never absent, never zero.

**Measured national exclusions (ACCESS-CM2 support, 2026-10-06): 169 of 7,921
units (2.1%) — 31 districts and 138 blocks.** The driver is the 0.95 unit
coverage gate against UHE reference support, not elevation: only 21 excluded
units sit below 0.25 retained fraction, while 96 sit between 0.75 and 0.95.
The loss is systematically coastal and insular, and includes Chennai (0.601),
Visakhapatanam (0.921), Surat (0.927), Thiruvananthapuram (0.948) and
Ernakulam (0.949). All of Lakshadweep district is at 0.000. See §9, decision D2.

## 8. Metric slugs

New slugs, additive. The existing `swbgt_empirical_*` and `wbgt_shade_stull_*`
slugs are not renamed, and no empirical output is relabelled as outdoor WBGT.

| Slug | Value column | Unit | Source |
|---|---|---|---|
| `wbgt_outdoor_route_b_annual_mean` | `wbgt_outdoor_route_b_annual_mean_C` | °C | `mean_c` |
| `wbgt_outdoor_route_b_annual_max` | `wbgt_outdoor_route_b_annual_max_C` | °C | `max_c`, cell-first |
| `wbgt_outdoor_route_b_days_ge_28` | `wbgt_outdoor_route_b_days_ge_28_days` | days | `days_ge_28`, cell-first |
| `wbgt_outdoor_route_b_days_ge_30` | `wbgt_outdoor_route_b_days_ge_30_days` | days | `days_ge_30`, cell-first |
| `wbgt_outdoor_route_b_days_ge_32` | `wbgt_outdoor_route_b_days_ge_32_days` | days | `days_ge_32`, cell-first |

Thresholds are inclusive (`>=`). Group: `temperature`. Display-only, under Heat
Stress, consistent with the eight existing WBGT/sWBGT diagnostics: **not** scored
in `composite_heat_stress`, no `bundle_weights.py` change, and no entry in
`colour_scale.json`. The incumbent empirical scoring calibration is **not**
reused merely because the units look similar.

Method provenance carried into the processed layer:
`method_version = route-b-outdoor-v1`, `aggregation_method = gridfirst_cell_area_weighted`.
These differ from the incumbent's `heat-stress-v2-gridfirst-2` /
`gridfirst_area_weighted_mean` so the two can never be silently mixed.

## 9. Decisions that gate publication

Both are the user's call. Computation continues while they are open; neither
blocks fitting, assembly or staged building.

**D1 — coverage envelope.** The release carries 18 models, historical
1990-2010 and SSPs 2020-2080. The incumbent carries 22 models, historical
1951-2014 and SSPs 2015-2100. Master and period views are unaffected: every
master period (`historical 1990-2010`, `ssp* 2020-2040 / 2040-2060 /
2060-2080`) lies inside the new envelope. The yearly trend charts lose
1951-1989, 2011-2019 and 2081-2100, and four models.

- D1a — publish as new additive slugs, retire nothing. No coverage is lost;
  the dashboard shows both. *Default assumed while D1 is open.*
- D1b — publish and retire `swbgt_empirical_*`, accepting the shorter yearly
  range and smaller ensemble.
- D1c — extend the `rsds`/`sfcWind` acquisition to 1951–2100 before publishing,
  then retire. This is a large additional NEX download and must be budgeted
  separately.

**D2 — coastal and insular coverage.** 169 units cannot be served under the
0.95 gate.

- D2a — accept the documented exclusions; the dashboard shows them as missing,
  never zero. Requires D1a, or 169 units go blank.
- D2b — lower the unit coverage gate. A gate of 0.75 recovers 96 units,
  including Ernakulam and Thiruvananthapuram but not Chennai; 0.50 recovers
  about 125. This is a methodology change and must be measured and reported,
  not applied silently.
- D2c — extend reference support for coastal cells. Not fabricating sea-level
  elevation and not filling missing ocean cells; this would need a documented
  reference-side method, not a threshold tweak.

Publication requires either validated coverage restoration or explicit
acceptance of documented exclusions, with correct missing-data behaviour in the
dashboard.

## 10. Acceptance criteria

Declared before any national metric values were inspected. Only the support and
eligibility tables of ACCESS-CM2 had been read when this section was written;
no `yearly_metrics.csv` existed yet.

Structural — any failure blocks the release:

1. Cache set complete and signature-current for every model; zero stale caches.
2. Every model × scenario × year present exactly once per method; counts 21 / 61 / 61.
3. `unit_index` unique and equal to the `units.csv` roster; all 7,921 present.
4. Excluded units present with missing values, never zero, in every metric.
5. `0 <= days_ge_N <= 365`; `days_ge_28 >= days_ge_30 >= days_ge_32` per row.
6. `max_c >= mean_c` per row; `mean_c` within −20 °C to 60 °C.
7. `grid_first_cell_metrics` and `grid_first_series` counts differ for at least
    one eligible unit-year, proving no accidental substitution of one estimand
    for the other.
8. Ensemble membership verified by model identity, scenario and year — not by
    row count. An 18-model ensemble is never computed from a smaller subset.
9. Staged `processed_optimised` passes strict parity, freshness and identity
    checks against the actual published paths.

Scientific — a failure is reported and decided, not silently accepted:

10. Held-out seasonal distribution and upper-quantile errors against UHE are
    reported per state, with geographic outliers named. The known Rajasthan
    upper-tail deterioration from the pilot must be re-measured nationally.
11. Paired comparison against the frozen W1 incumbent on common support only,
    with the differing estimands stated. This is a method comparison, not an
    accuracy test.
12. Model spread reported; a single model is never presented as the ensemble.

Fitting success is not scientific validity, and a zero-exit-code run is not a
passed gate.

## 11. Known numerical warning

Every ACCESS-CM2 shard recorded xclim's
`Strange results could be returned when using dayofyear grouping on data defined
in the proleptic_gregorian calendar`. It arises because `time.dayofyear` shifts
by one day after February 28 between leap and non-leap years, and day 366 exists
only in leap years. The grouping is CarbonPlan's own pinned choice, inherited
with the source lock, not an IRT decision. Three mitigations are already in the
code: the 31-day window absorbs a one-day shift, `adjust()` converts back with
`align_on="year"` and interpolates only introduced leap dates, and
`annual_metrics` drops February 29 before any annual reduction. The residual
exposure is the sparser day-366 group. It is **measured**, not argued, by the
validator's leap diagnostic; zero process failures do not resolve it.

## 12. Provenance

Every assembled model writes `run_manifest.json` carrying the source lock
(`carbonplan_commit f662b37…`, `metsim_commit edbd68f…` and the sha256 of all
ten pinned upstream files), the support signature, the sha256 of
`wbgt_route_b_run.py` and `wbgt_route_b.py`, units eligible/total, cells
eligible, every captured warning, elapsed seconds and peak RSS. A shard cannot
write a cache that an assembling run would accept under different inputs.
