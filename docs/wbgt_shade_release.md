# Shade WBGT correction — implementation and release status

This implements CHG-0572 and the code/validation portions of CHG-0583–0586.
CHG-0587 provides the national staging runner, hardened by CHG-0588..0592 (streaming/cached budget,
resume safety, roster preflight, boundary-coverage validation, tests and this documentation) and by
CHG-0593..0598 (live-lock and lock-ownership safety, rollback cache bound to its published root,
preflight derived from the real compute archive, incomplete measurements barred from a space verdict).
Complete national execution, release verification and promotion remain outstanding.
No published metric directories have been replaced. Do not treat the new source code as evidence
that existing processed or optimized shade artifacts were rebuilt.

## Calculation and contract

The four existing `wbgt_shade_stull_*` slugs and value columns remain. Annual mean means
**Shaded WBGT — Annual Mean of Estimated Daily Maxima**. The calculation is:

```
es(T) = 6.112 exp(17.62 T / (243.12 + T)) hPa
RH_peak = hurs_percent es(tas_C) / es(tasmax_C)
shade_peak = 0.7 Stull(tasmax_C, RH_peak) + 0.3 tasmax_C
```

This is a sheltered-shade daily-maximum estimate, not an hourly humidity reconstruction or
CarbonPlan's specific-humidity implementation. Units come from metadata; absent metadata uses
the declared NEX contracts (Kelvin, percent). Unknown units, mismatched coordinates and conflicting
source identities raise errors. Nonfinite inputs, invalid humidity and `tasmax < tas` invalidate days.
Reconstructed humidity clipping and Stull-domain excursions are recorded. There is no 5% floor.

After excluding February 29, every one of 365 expected days must be finite. An incomplete cell-year
has NaN annual mean and all three counts. Gregorian and no-leap calendars are supported;
360-day and other calendars raise an explicit error. Counts use inclusive >=28, >=30 and >=32 C.
Annual cell artifacts include valid days, missing-input days, physical-invalid days, domain excursions,
clipping and incomplete-year flags. Missing dates are detected by expected-day completeness.

Both compute routes calculate cells before area weighting; polygon counts may be fractional.
All weighted units remain present, including NaN units. Valid-area fraction is reported without a new
coverage cutoff. Structural sub-cell IDW remains available only when the polygon has no jointly valid
native source support anywhere in the selected source period. Temporal failures cannot trigger IDW;
only complete annual cell values are donors. IDW provenance remains `climate_fill_method="idw"`.

The shade signature includes formula, variables, humidity, calendar, completeness and aggregation.
It is independent of `HEAT_STRESS_GRIDFIRST_METHOD_VERSION`. Old shade caches/markers are stale;
Twb and empirical sWBGT numerical calculations and cache versions are unchanged. Master,
optimized master, yearly and state-value outputs carry shade provenance. Optimized builds preflight
all shade masters and require the four shade metrics together before writing.

Outdoor labels now say **Empirical sWBGT**. Their calculations still use daily-mean inputs, with no
explicit solar radiation or wind; their annual means are not labelled daily maxima.
The `twb_days_ge_*` false-zero sibling defect is still unresolved. W-04/F-05 must not be reported as
fixed across all Heat Stress metrics.

## Evidence and limits

[Evidence tables](diagnostics/wbgt_shade_release/README.md) separate exact original reproduction
from production calendar/completeness evaluation. Original 2005–2014 tables reproduce exactly.
The frozen candidate passes the accepted improvement rule in both 2005–2014 and 1990–2004.
Mean site RMSE changes from 2.934 to 0.693 C and 2.991 to 0.645 C respectively. Annual threshold-count
MAE changes from 35.27 to 8.41 days and 31.58 to 7.01 days. Every site's RMSE improves.

All 12 site/window daily-error target pairs pass. However, 37 of 128 sufficiently populated annual
count pairs miss +/-20%. All 138 >=32 site-year pairs are sparse. Absolute errors and year-block
uncertainty are provided; sparse evidence is not validated by a 50-event gate.

The published baseline audit has 4,032 metric/state/admin-level/scenario/period records for all eight
WBGT metrics. Zero fractions, NaN fractions, model membership and unavailable coverage metadata
are explicit. Published summaries cannot diagnose partial years. Local input intersection gives
20 models/2,860 model-years; KACE-1-0-G has unsupported 360-day inputs. The completed all-input calendar audit yields 19 supported models and 2,717 model-years.
Use `release_roster.json`, not the unfiltered inventory roster.

Shade-specific NEX residuals compare historical distributions and annual counts with ERA5, not
same-date weather. See `nex_residuals.csv` and `nex_exclusions.json`. No QDM has been implemented or
borrowed from outdoor work. The Tier-2 probe was shade plus a linear adjustment, not a reconstructed
hourly Liljegren ceiling. Radiation conservation has not been established by this shade experiment.

The single-model/year pilot covers Kerala, Rajasthan and Himachal Pradesh at district and block
levels. Peak process RSS was 748,228,608 bytes. Its early conservative 20-model extrapolation was
123.1 hours and 14.3 GB of annual caches with one worker; that figure used the unfiltered 20-model
roster and excluded downstream and rollback cost, so it is **superseded** and must not be quoted as
the national budget.

The budget from `build_shade_release` on the supported 19-model roster is **an extrapolation, not a
measured national runtime**: **188.5 hours** with one worker (513,939 s compute plus 164,510 s
downstream) and **228.7 GB** of reserved staged output, scaled from 21 measured pilot model-years and
656 pilot units to 2,717 model-years and 7,921 units. Only the pilot is measured; the national
figures inherit its per-unit and per-model-year cost. Record the actual national cost after staging. Pilot model-years are measured from the staged tree rather than assumed. Staging and the
published tree share one volume here, so the space check sums both requirements against its 6.45 TB
free. Rollback storage for the four published shade trees is measured separately and cached, so a space
requirement quoted before that walk completes excludes it; see `budget.json` and
`rollback_sizes.json` under the stage directory. The cache records the published root it was
measured from and is discarded rather than reused for another `--data-dir`. A tree that exists but
cannot be fully read is reported incomplete and withholds the space verdict instead of contributing
an understated zero (CHG-0594, CHG-0596).
District-first cache warming makes the subsequent block pass warm even on its first iteration;
`cached=false` denotes the first pass for that level, not necessarily a cold cache.

## Chronological operator commands (Windows PowerShell)

Run at the repository root in the existing `irt` conda environment. These commands use local inputs;
none download data. Do not install or change geospatial packages.

1. Verify interpreter dependencies and cached site input readability (read-only):

```powershell
python -c "import pandas, pyarrow, xarray, thermofeel; print(pandas.__version__, pyarrow.__version__, xarray.__version__, thermofeel.__version__); print(pandas.read_parquet('scratch/wbgt_deployed_vs_reference_cache/kochi_2005.parquet').shape)"
```

2. Audit published summaries and local variable/year availability (writes diagnostic reports):

```powershell
python -m tools.diagnostics.wbgt_shade_inventory --data-root D:/projects/irt_data
python -m tools.diagnostics.wbgt_shade_inventory --data-root D:/projects/irt_data --calendars-only
```

3. Reproduce and evaluate the frozen formula, then assess model residuals (writes reports only):

```powershell
python -m tools.diagnostics.wbgt_shade_release --dry-run
python -m tools.diagnostics.wbgt_shade_release
python -m tools.diagnostics.wbgt_shade_nex --source-root D:/projects/irt_data/r1i1p1f1
```

4. Run numerical and contract regressions, then measure the three-state pilot (writes private caches):

```powershell
python -m pytest -q tests/test_wbgt_shade_release.py tests/test_heat_stress_gridfirst.py tests/test_compute_indices_heat_stress_metrics.py tests/test_compute_indices_marker_validation.py tests/test_metrics_registry.py
python -m tools.diagnostics.wbgt_shade_pilot --data-root D:/projects/irt_data
```

5. Verify the isolated destination, then build one model's full historical Kerala pilot (writes only
staged compute artifacts; source files remain under the existing data root):

```powershell
python -c "from pathlib import Path; p=Path('scratch/wbgt_shade_stage/processed').resolve(); print('Staging:', p); assert p != Path('D:/projects/irt_data/processed').resolve()"
python -m tools.pipeline.compute_indices_multiprocess --state Kerala --models ACCESS-CM2 --scenarios historical --metrics wbgt_shade_stull_annual_mean wbgt_shade_stull_days_ge_28 wbgt_shade_stull_days_ge_30 wbgt_shade_stull_days_ge_32 --workers 1 --level both --yearly-cleanup-policy preserve --output-root scratch/wbgt_shade_stage/processed
python -m tools.pipeline.build_master_metrics --processed-root scratch/wbgt_shade_stage/processed --state Kerala --level both --metrics wbgt_shade_stull_annual_mean wbgt_shade_stull_days_ge_28 wbgt_shade_stull_days_ge_30 wbgt_shade_stull_days_ge_32 --workers 1
```

`--output-root` sets `IRT_COMPUTE_OUTPUT_ROOT` for spawned workers. It changes compute destinations,
not raw input paths. Do not point it at the published tree while testing.

6. National budget, then staging. The supported-roster audit, the full-period three-state pilot and
its downstream optimized/state-summary parity are **complete** (see the two sections below); the
remaining prerequisites are the total staging+rollback disk budget and the promotion/rollback
rehearsal. Budget first, which writes only under `--stage`:

```powershell
python -m tools.pipeline.build_shade_release --data-dir D:/projects/irt_data --stage scratch/wbgt_shade_national --dry-run
python -m tools.pipeline.build_shade_release --data-dir D:/projects/irt_data --stage scratch/wbgt_shade_national
```

`--dry-run` answers everything except free space in seconds. The default run additionally walks the
four published `processed/<slug>` trees and the four `processed_optimised/metrics/<slug>` trees, which
costs minutes *per tree*; each measurement is cached in `<stage>/rollback_sizes.json` as it completes,
so an interrupted walk resumes rather than restarting. Use `--remeasure-rollback` once those published
trees change. Only the default run produces a space verdict, so `--dry-run` cannot authorize `--build`.

Then stage all four metrics together in the isolated tree:

```powershell
python -m tools.pipeline.build_shade_release --data-dir D:/projects/irt_data --stage scratch/wbgt_shade_national --source-root D:/projects/irt_data/r1i1p1f1 --build
```

Every build preflights every rostered model-year input before the first compute stage, because the
compute CLI derives years from whatever inputs exist while release validation demands the exact roster.
The archive preflighted is the one compute resolves from `--data-dir`; `--source-root` asserts that it
is the path the operator expected and fails on disagreement, rather than substituting a path compute
never opens (CHG-0595). Each state's roster coverage is checked as soon as its compute finishes, not
days later at the end. Resuming a stage built from a different roster, state list, formula signature,
published root or boundary layer is refused rather than silently mixed. A build lock whose holder is
provably alive is always refused; `--force` applies only where the holding PID cannot be probed on
this platform, and a lock is released only while this process still owns it (CHG-0593).

This stages outputs and validates identifiers/slices/roster/signatures and missing values. It does
**not** publish. Promotion of the four metrics together with a tested rollback is still unimplemented:
nothing consumes `release_ready.json`, and that workflow has not been rehearsed.

Manual review: choose each shade metric in map, table and export; confirm missing units are shown
as missing, never zero. Check the annual-mean wording and unchanged empirical-outdoor values.
Verify a partial cell-year has NaN mean/counts and no temporal IDW fill. Compare both compute routes
on the heterogeneous fixture in `tests/test_wbgt_shade_release.py`.

### Completed staged historical pilot

The ACCESS-CM2 historical 1990–2010 pilot subsequently completed compute, ensembles, masters,
optimized artifacts and state values for all three states. Strict parity returned zero issues
(24 masters, 24 yearly-model outputs, 24 yearly-ensemble outputs and six geometry outputs).
The staged tree uses 110,718,407 logical bytes including its three-state boundaries.
Rajasthan compute took 135.0 s, Himachal Pradesh 91.5 s; downstream masters took 32.5 s,
optimized conversion 50.6 s, state values 6.2 s and strict parity 16.0 s. Kerala compute/ensembles
reported 53.4 s district plus 49.0 s block. These results do not constitute a national run or
an atomic promotion/rollback rehearsal. Reports are retained beside the reference-validation evidence.
