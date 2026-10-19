# Outdoor WBGT engineering pilot — frozen specification

**Status: FROZEN before any pilot result was computed (CHG-0619).**
Milestone 4. Predecessors: `wbgt_outdoor_feasibility` (milestone 1),
`wbgt_outdoor_selection` (milestone 2), `wbgt_outdoor_humidity` (milestone 3).

This document is the contract for the run. It is written first so that the
budget, the tolerances, the aggregation semantics and the stop conditions
cannot be chosen after seeing the numbers. Predecessor specifications and
their results are not edited by this milestone.

---

## 1. The question

Can the **unchanged milestone-2 W1 method** be driven from actual NASA NEX
files on a climate grid, aggregated to districts and blocks, and run inside a
measured, practical budget alongside the active national shade rebuild?

This is an **engineering** question. It asks whether the pipeline is correct
and affordable, not whether its outputs are scientifically accurate.

Verdicts: `ENGINEERING PASS` / `ENGINEERING CONDITIONAL` / `ENGINEERING FAIL`.
An engineering pass is **not** permission for national publication.

## 2. The method is frozen

The pilot computes milestone 2's **W1**, retained as milestone 3's candidate
**B**:

| Element | Source, unchanged |
|---|---|
| Temperature | `m1.reconstruct_temperature_c`, Parton & Logan (1981), a=1.86 h, b=2.2 |
| Radiation | `m1.reconstruct_radiation_w_m2`, TOA interval-mean shape, Erbs et al. (1982) split |
| Humidity | `m1.reconstruct_humidity_pct`, constant daily vapour pressure |
| Wind | `m2.wind_w1_dtr_ms`, `clip(0.03·DTR, 0.10, 0.80)` on the zero-mean cos-zenith shape |
| Pressure | `m1.barometric_pressure_hpa`, ISA from cell elevation |
| Solver | `m1.liljegren_wbgt_c` → `thermofeel.calculate_wbgt_liljegren` |
| Daily maximum | `m1.daily_max` |

Explicitly **not** done in this milestone: the milestone-3 input-consistent
humidity (rejected); any new wind shape; any coefficient tuning; any bias
correction or QDM; any change to gates or reference definitions; any humidity
or wind candidate search.

W1 is the selected **development** method. Its wind shape is a declared
assumption, not an established physical reconstruction, and it may benefit
from compensating reconstruction errors. That limitation travels with every
artifact this milestone writes.

## 3. Scope

| Item | Value |
|---|---|
| States | Kerala, Rajasthan, Himachal Pradesh |
| Levels | district, block |
| Model | ACCESS-CM2, `r1i1p1f1` |
| Scenario | `historical` |
| Target year | **2005** |
| Second model | MRI-ESM2-0, **Himachal Pradesh only**, ranking sensitivity (§10) |
| Second method | C1 (constant daily-mean wind), same grid/model/year, ranking sensitivity (§10) |

**Why 2005.** It is interior to the local `rsds`/`sfcWind` coverage
(1990–2010), so both padding days exist; it is not a leap year, so the
complete-365 policy needs no Feb-29 drop; and it is the year the shade pilot
used, which keeps the two pilots' costs comparable.

**Padding.** `reconstruct_temperature_c` reads `tasmin`/`tasmax` at the
previous and next day. The pilot therefore loads 2004-12-31 and 2006-01-01 in
addition to all of 2005. A target year is rejected if either padding day is
missing. Losing a complete target year to absent padding is a defect, not an
acceptable outcome.

## 4. Inputs

Six variables, all required, all entering the calculation numerically:

| Variable | Root | Units expected |
|---|---|---|
| `tas`, `tasmin`, `tasmax` | `<main>/r1i1p1f1` | K → °C |
| `hurs` | `<main>/r1i1p1f1` | % |
| `rsds` | `<wbgt-v2>/r1i1p1f1` | W m-2 |
| `sfcWind` | `<wbgt-v2>/r1i1p1f1` | m s-1 |

`rsds` and `sfcWind` are **not** provenance-only: `rsds` sets the daily
radiation total that the TOA shape is rescaled to conserve, and `sfcWind`
sets the daily-mean wind that W1's shape modulates. The runner asserts both
are consumed.

Verified before compute, per §5.

## 5. Input checks (all must pass)

1. All six files present for the target year and both padding days.
2. Identical `lat`/`lon` coordinates across all six variables — **no silent
   regridding**. A mismatch is an error.
3. Units match the table in §4. A mismatch is an error, not a conversion.
4. Calendar is `proleptic_gregorian` or `standard`/`gregorian`/`noleap`. A
   360-day calendar is **rejected**, not converted.
5. Day coverage: expected day count present, no duplicate dates, no
   unexpected missing dates.
6. Finiteness and physical validity on the pilot subset: `hurs` in [0, 100],
   `rsds` ≥ 0, `sfcWind` ≥ 0, temperatures within [-90, 60] °C.
7. `tasmin <= tasmax` on every valid cell-day. Violations are counted,
   reported, and the offending cell-days are marked **invalid**, never
   silently reordered or clipped.

**Day-boundary semantics.** No NEX file publishes `time_bnds`. The daily
timestamp is at 12:00 and is normalised to midnight, and the resulting day is
treated as the local day for reconstruction — the **inferred** convention
carried forward unchanged from milestones 1–3. Timestamp normalisation does
not prove daily interval semantics, and this milestone does not claim it
does. An unexplained coverage mismatch is rejected rather than joined.

## 6. Elevation — a declared gap

`SiteStatic.elevation_m` feeds ISA pressure. **No local per-cell elevation
source exists** (no DEM, no `orog`, no elevation column on the boundaries).

The pilot therefore runs every cell at **0 m**, declared in the method
signature as `elev-sea-level-constant-no-dem`, so no future elevation-aware
cache can be mistaken for this one.

Measured consequence: at fixed drivers (35 °C, 40 % RH, 2 m s-1, 850 W m-2),
Liljegren WBGT changes by **0.250 °C** between 1013.25 hPa (0 m) and
768.09 hPa (2276 m, Shimla). The error is therefore bounded by roughly
0.25 °C at the highest Indian elevations and is far smaller in Kerala and
most of Rajasthan. It is a **real but bounded** limitation, reported per
state, and it is a precondition for any later national work — not something
this pilot resolves.

## 7. Isolation

Bulk arrays, caches and temporary files: `scratch/wbgt_outdoor_pilot/`.
Compact evidence: `docs/diagnostics/wbgt_outdoor_pilot/`.

Every output path is **resolved** (symlinks followed) and refused if it is
equal to, or nested within, any protected root:

- `IRT_DATA_DIR` and the NEX source roots
- `processed/`, `processed_optimised/`
- `scratch/wbgt_shade_national/`, the shade stage and release directories
- `docs/diagnostics/wbgt_outdoor_feasibility/`
- `docs/diagnostics/wbgt_outdoor_selection/`
- `docs/diagnostics/wbgt_outdoor_humidity/`
- `docs/diagnostics/wbgt_shade_release/`

No isolated output path is handed to a downstream tool that resolves
production paths independently. The pilot imports production helpers
read-only and modifies none of them; no production module imports this tool.

CLI: `--dry-run`, `--source-root`, `--wbgt-root`, `--boundary-root`,
`--out-dir`, `--work-dir`, `--states`, `--model`, `--year`, `--levels`,
`--sample-cells`, `--resume`, `--overwrite`, `--workers`.

Default is **no overwrite**. `--resume` reuses a cell result only when the
full signature matches: method signature, model, scenario, year, grid id,
input file identities, boundary content hash, and the elevation convention.

## 8. Computation order (grid-first)

```
daily NEX inputs (per cell)
  -> reconstructed within-day drivers (24 h per day)
  -> Liljegren WBGT at each reconstructed hour
  -> daily maximum per cell
  -> annual statistics per cell
  -> area-weighted district / block statistics
```

Prohibited: averaging weather across an admin unit before the nonlinear WBGT
step; averaging WBGT across a unit and then counting threshold days;
assembling a synthetic WBGT from independently selected driver maxima;
loading the national archive into memory.

Only cells that actually intersect a pilot polygon are computed. Hourly
intermediates are retained only for the cell being solved. Spatial batching
is bounded; if temporal chunking is introduced it must preserve the
adjacent-day dependency and prove chunk parity (§9).

All-invalid or incomplete data stays **missing**. It never becomes zero.

## 9. Parity, frozen before scoring

The pilot must reproduce the frozen W1 diagnostic path exactly where the
numerical path is the same.

| Check | Tolerance |
|---|---|
| Pilot cell path vs `m2.nex_candidate_daily_max` on identical daily inputs | `max abs diff <= 1e-9 °C` |
| One cell alone vs the same cell inside a vectorised batch | **bit-identical** |
| Different spatial chunk sizes | **bit-identical** |
| Resume vs uninterrupted | **bit-identical** |
| Target-year-only hours vs a full 3-year hour span, on the target year | `max abs diff <= 1e-9 °C` |
| Annual threshold counts, all of the above | **exactly equal** |

The first and last target days are tested explicitly, because they are the
days whose reconstruction reads the padding.

**Stop condition: if parity fails, the pilot stops before full-state
execution** and reports the failure. It does not proceed with a caveat.

## 10. Annual and aggregation contract

Per cell, for a complete valid target year:

- `wbgt_annual_mean_c` — **annual mean of daily maximum outdoor WBGT**, °C.
  Never abbreviated to "daily mean WBGT".
- `days_ge_28`, `days_ge_30`, `days_ge_32` — annual counts of days whose
  **daily maximum** WBGT reaches the threshold.
- `expected_days`, `valid_days`, `complete_year`, and invalidity diagnostics
  separating input invalidity from solver invalidity.

Completeness: the frozen W1 contract is complete-365 with Feb 29 dropped.
2005 has 365 days and needs no drop; the policy is still applied and
asserted. An unsupported calendar is rejected, not converted.

Admin aggregation uses the production `build_area_weights` /
`aggregate_cell_values` path: actual polygon-by-cell **intersection areas**
in EPSG:6933, not centroid membership. Canonical identifiers
(`state_name`, `district_name`, `block_name`, LGD codes, `block_key`) are
preserved.

Reported per unit: area-weighted value over valid cells; valid intersected
area; total represented intersected area; valid-area fraction; counts of
valid and invalid contributing cells; spatial-support method; exclusions.

A polygon's area-weighted annual exceedance count may be fractional and is
labelled **"area-weighted mean annual cell exceedance days"**. It is *not*
the number of days every location exceeded the threshold, *not* the number
of days any location did, and *not* the number of days the polygon-average
WBGT did.

**No spatial gap-filling.** Temporal gaps are never filled spatially. A unit
without valid supporting cells is retained as `NaN` with a stated reason.
Sub-cell IDW is **disabled** in this pilot; if it were ever enabled it would
be separately labelled and would not rescue temporally incomplete cells.

## 11. Admin correctness checks

- Canonical admin keys unique at each level.
- Expected units either represented or explicitly excluded with a reason.
- District/block hierarchy consistent; no unintended duplicate joins.
- `days_ge_32 <= days_ge_30 <= days_ge_28` on every row.
- Valid annual counts within `[0, 365]`.
- `NaN` preserved, never coerced to 0.
- Each weighted mean lies within the min–max of its contributing valid cell
  values.
- A small hand-checkable fixture aggregated independently and compared.
- District values compared against block-to-district re-aggregation weighted
  by **valid intersected area**, never by whole-block area. Discrepancies are
  explained by geometry coverage or support, not absorbed.

The six reference sites' observed annual-mean range is **not** used as a
universal physical validity bound.

## 12. Staged execution and budget

| Stage | Content |
|---|---|
| A | Preflight: dry-run, file/contract/isolation checks, parity (§9) |
| B | Small-cell benchmark: measured wall time, peak RSS, artifact size |
| C | Kerala, district + block, validated before any other state |
| D | Rajasthan and Himachal Pradesh, if the measured budget holds |

**Declared budget**, set from the measured single-cell benchmark
(1.1 s per cell-year of solver time) and the active shade workload:

| Resource | Limit | Stop condition |
|---|---|---|
| Wall time | 3600 s total | Checkpoint and stop; report completed scope |
| Peak RSS | 4 GB | Abort the stage and report |
| Scratch artifacts | 5 GB | Abort and report |
| Workers | **1** | Not raised while shade is active |

These are budget ceilings, not predictions. Completion estimates are made
only after Stage B measures the real rate. No national extrapolation is made
from six-point diagnostic runtimes.

If the budget is exceeded the run checkpoints cleanly and delivers the
completed scope plus an exact continuation command. Equally, the run does not
stop early while it remains inside the declared scope and measured budget.

## 13. Ranking sensitivity (diagnostic only)

Predeclared here, before execution, so the scope cannot grow in response to
results:

1. **Method sensitivity** — W1 vs C1, same grid/model/year, all three states,
   district and block.
2. **Model sensitivity** — ACCESS-CM2 vs MRI-ESM2-0, **Himachal Pradesh
   only**, the smallest of the three states.

Both run only if they fit the measured Stage-B budget. If either does not, it
is reported as **outstanding**, and the pilot is not expanded to accommodate
it.

Reported: Spearman rank correlation; absolute rank shifts; largest changes;
coverage-related exclusions; near-tie effects. Method sensitivity and model
sensitivity are reported separately and never merged.

Rank **stability** is not rank **accuracy**, and agreement between two
reconstructions validates neither against reality. Neither claim is made.

## 14. Limitations that travel with every artifact

Every manifest and every report states:

- **Diagnostic-only.** No output enters the dashboard, the production metric
  registry, composites, frozen rulers or published maps.
- Uncorrected NEX inputs; no bias correction applied.
- Six-site reconstruction evidence does not establish national accuracy.
- Remaining NEX distribution and count errors are unresolved.
- W1 may benefit from compensating reconstruction errors.
- `>= 32 °C` outcomes include reference-sensitive results.
- Rare-event regimes remain insufficiently evaluated.
- Spatial ranking accuracy is **not** established.
- Daily time-boundary semantics remain **inferred**.
- Cell elevation is a sea-level constant; no DEM is available (§6).

All thresholds — including `>= 28` and `>= 30 °C` — remain diagnostic-only on
NEX. An ERA5 reconstruction pass does not promote them.

## 15. Stop conditions

The run stops and reports rather than continuing when:

1. Any §5 input check fails.
2. Any §9 parity check fails.
3. A §12 budget ceiling is reached.
4. A write is attempted against a protected root.
5. An admin-correctness check in §11 fails structurally.

No candidate is added, no gate is widened and no tolerance is relaxed in
response to a result. The milestone ends with the engineering verdict.
