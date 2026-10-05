# Outdoor WBGT: Route B programme

CHG-0644. User-approved direction, 2026-10-03, starting at
`GIT:add_flood_depth@7bd2ddb`. This is a diagnostic research programme, not a
production-method replacement. The shade release retains its separate authority.

## Decisions adopted

**2026-10-05 scope update (CHG-0654):** the user stopped further two-model
calculations and selected a working **grid-first, ACCESS-CM2-only** pilot.
Both the recovery coordinator and CMCC worker were terminated and verified
absent. CMCC's 135 cell and 3 regional cached series remain intact for potential
future work; no automatic resume is scheduled. These partial fits are not a
completed model and are not used in the pilot. The earlier two-model comparison
programme below is retained as history, not a prerequisite for the working pilot.

Open [the working pilot](diagnostics/wbgt_outdoor_route_b_pilot/working_pilot/index.html)
directly in a browser. It provides district/block selection, historical and future
periods, annual charts, baseline differences, coverage and CSV downloads from the
completed grid-first cell metrics. Build with
`python -m tools.diagnostics.wbgt_route_b_pilot --build` (offline artifact writes,
no climate compute). The default command is a no-write plan. The viewer,
`pilot_yearly.csv` and the source `ACCESS-CM2/yearly_metrics.csv` are git-ignored
for size and exist only locally; `working_pilot/validation.json` and
`ACCESS-CM2/run_manifest.json` are committed and pin their hashes and provenance.
Because the source metrics are the output of the now-stopped pilot compute, a
clone cannot regenerate the viewer without re-running `wbgt_route_b_run`. Counts are explicitly
area-weighted cell exceedance days; maxima are area-weighted cell annual maxima.
Missing units remain visible. Production and composite membership are unchanged.

| Decision | Contract |
|---|---|
| Method | Route B: reproduce CarbonPlan's shade → QDM → sun chain on IRT support. |
| Aggregation | Grid-first selected for the working pilot. Completed region-first results remain diagnostic comparators. |
| Models | ACCESS-CM2 only for the working pilot; further CMCC computation stopped by user choice. |
| Reference | UHE-Daily is provisional calibration evidence, not observed outdoor WBGT truth. |
| Calibration | 1985–2014 inclusive. Radiation is unnecessary for shade calibration. |
| Reporting baseline | 1990–2010 inclusive; separate from calibration. |
| Output envelope | Proposed historical 1990–2010 and SSP245/SSP585 2020–2080, conditional on complete common-input inventory. |
| Geography | Kerala, Rajasthan, Himachal Pradesh; verify current district/block roster, rather than assume predecessor counts. |
| Destination | Diagnostic-only. Display release and composite/ruler membership require subsequent acceptance decisions. |
| Route C | Deferred challenger; no direct outdoor-to-shade-reference QDM. Define compatible quantities before testing it. |

The implementation now includes acquisition, native-support QC, conservative
input preparation, both pilot aggregation paths, held-out evaluation, reporting
and a resumable local completion coordinator (CHG-0649/CHG-0650).
The [12-date acquisition sample](diagnostics/wbgt_outdoor_route_b_reference/README.md)
has passed integrity checks; independent observational accuracy remains unestablished.
Follow-up [reference QC](diagnostics/wbgt_outdoor_route_b_reference/QC_SPEC.md)
has established source identity: CarbonPlan's published reference uses overlapping
0.25° footprints on 0.05° UHE centres. Reproducing that wiring matches 36 sample
values to 2.63e-14 °C. IRT retains true native footprints; the published wiring
is a separate control. This difference and incomplete island support are explicit.
The [frozen paired-pilot specification](diagnostics/wbgt_outdoor_route_b_reference/PILOT_SPEC.md)
covers two models and the verified 75-district/581-block roster, intersecting 784
NEX cells before reference/input coverage screens. All 304 model/scenario/year
driver shards have been extracted locally. All 10,957 reference days were acquired
and passed local integrity verification. The first ACCESS-CM2 run completed all
774 cell and 630 regional QDM fits, then stopped at the solar conservation gate:
float32 daily radiation multiplication incurred a roughly 1e-5 W/m² residual.
CHG-0651 promotes radiation to float64 before the pinned solar function; the
1e-8 W/m² conservation gate is unchanged. A representative real-input check
reduced the residual to 8.53e-14 W/m². Recovery verifies that only the downstream
solar function changed, checks original source/input signatures, and preserves
all QDM arrays and original fit timings. The audit and recovery script are under
`docs/diagnostics/wbgt_outdoor_route_b_pilot/`. Scientific results require the
completed two-model report. Execution state is
recorded in `docs/diagnostics/wbgt_outdoor_route_b_pilot/execution_status.json`.
National scale-out and publication remain later decisions.

Before the scope change, the 2026-10-05 recovery used the offline, locked
`docs/diagnostics/wbgt_outdoor_route_b_pilot/scripts/finish_recovery.py --run`
coordinator. **That chain is now stopped; it is not needed for the working pilot.**
Its implementation waits for the already-running ACCESS solar/output stage, verifies
its manifest against the cache audit, applies the existing 12-hour QDM cost gate
to CMCC, runs that model, then writes the standard report and stratified
`REVIEW.md`/`review_tables/` via `scripts/review_results.py`. Both scripts plan
without writes unless `--run` is supplied. This recovery entrypoint is specific
to the audited 9979326 solar failure; use the normal completion coordinator for
a fresh run. Recovery logs are under `scratch/wbgt_route_b/recovery_*.log`.
The acquired compressed reference occupies 9,482,300,289 bytes across 10,957
files, with all 30 annual reference shards prepared.

ACCESS-CM2 completed on 2026-10-05; CMCC-CM2-SR5 was subsequently stopped by the user.
The [ACCESS interim review](diagnostics/wbgt_outdoor_route_b_pilot/INTERIM_ACCESS_REVIEW.md)
records the completed first-model evidence: small aggregation-order effects but
substantial count-definition differences, improved seasonal held-out performance
with a Rajasthan upper-tail deterioration, and material future application-window
sensitivity. It is not a completed two-model verdict or release approval.

## Evidence and limits

The [pinned reproduction](diagnostics/wbgt_outdoor_carbonplan_reproduction/README.md)
matched every threshold count across nine city-years of ACCESS-CM2. The ordinary
outdoor residual was at most 7.1e-5 °C, but a 0.0168 °C tied-day exception remains;
the declared continuous-parity gates did not pass. Do not describe this as
unqualified exact continuous reproduction. Use the self-consistent 3600 s
radiation wiring and retain a daily-mean radiation-conservation check.

The measured sign-changing QDM correction supports testing bias correction. It
does not establish observational superiority over W1: the decomposition combines
a 30-year shade correction with three years of outdoor bias and differs in other
pipeline choices. CarbonPlan parity is an implementation control; W1 is a method
comparison. Neither is an independent observational accuracy test.

UHE's source work derives WBGT from CHIRTS temperature and humidity information.
CHIRTS combines station, satellite and reanalysis information; do not label it
either direct WBGT observations or wholly devoid of Indian observations.
Its urban application does not itself prove rural invalidity. Rural, mountain,
coastal and seasonal support must be assessed explicitly.

Sources:
- [CarbonPlan code](https://github.com/carbonplan/extreme-heat); use the existing
  [source lock](diagnostics/wbgt_outdoor_carbonplan_reproduction/source_lock.json),
  including xclim 0.44.0 and the reproduction environment. Do not install these
  old dependencies into IRT's geo environment.
- [Tuholske et al., 2021](https://pmc.ncbi.nlm.nih.gov/articles/PMC8521713/).
- [CHC daily rasters](https://data.chc.ucsb.edu/people/cascade/UHE-daily/wbgtmax/).
  Preserve source URLs, dates, validators and subset hashes. Public access alone
  is not a redistribution licence; attribution/reuse terms must accompany a release.

## Stage 1: reference acquisition and QC

`python -m tools.data_acquisition.uhe_daily` prints a plan by default and performs
no network access or writes. `--download` explicitly acquires; `--verify` is local
and read-only. The tool uses existing NumPy and requests, with no geo-library
installation or PROJ/GDAL configuration changes.

The window is **68–98°E, 6–38°N**, source-cell-edge aligned, 640 rows × 600 columns.
It is a rectangular reference subset, not an India land/admin mask. Check it
against the canonical roster before full acquisition. Data remain at 0.05° with
native pixel-centre coordinates, north-to-south rows, EPSG:4326, degrees Celsius.
The source encodes 0.05° as `0.05000000074505806`; nominal request bounds select
cell indices, while stored coordinates and resolution preserve that native value.
No regridding, polygon weighting, leap-day removal, or interpolation occurs here.

The 1985–2014 plan contains 10,957 Gregorian dates. At the measured source layout,
each day needs a 65,536-byte header plus 640 complete 7,200-cell float32 rows:
about **202.7 GB** before retries, and **16.8 GB** of uncompressed cropped values.
Stored compressed NPZ size and runtime require measurement. These estimates are
not a promised elapsed time or storage guarantee.

Each day is one atomic compressed NPZ containing `wbgtmax_c`, `latitude`,
`longitude`, and a JSON `metadata` scalar. Metadata records date, URL, source
size/validator, downloaded byte ranges, header and row-payload SHA-256, normalized
array SHA-256, grid, units, acquisition time, and valid/missing counts and extrema.
The source hashes describe acquired subsets, **not the entire remote TIFF**.
Reruns verify existing dates before skipping them; they refuse conflicting or
corrupt outputs. An interrupted day is retried; no missing day becomes zero.

The reader accepts only the verified classic TIFF layout: one float32 band,
uncompressed contiguous one-row strips, known dimensions/georeferencing, and
PixelIsArea EPSG:4326. Layout changes abort rather than trigger a full download.
HTTP 206, exact Content-Range, identity encoding, and stable source validators
are mandatory. Requests are single-stream, spaced at least two seconds apart.
There are at most two retries for connection/timeout/server errors; HTTP 429
stops the run. Attempted response lengths, including retries, consume the cap.
HTTP overhead is not included in byte estimates. The default 0.25 GB cap covers
the 12-date sample but refuses the complete calibration download at preflight.

Source nodata tags, if present, and non-finite samples become NaN. Without a
nodata tag, finite sentinel-like values are not guessed away. Empty/all-missing
windows and finite values outside -100..100 °C fail acquisition QC. That broad
range is a corruption screen, not a scientific plausibility/acceptance threshold.

### Chronological operator steps

Run from the repository root in the existing `irt` Python environment.

1. **Verify dependencies and offline contracts**; no download:

   ```bash
   python -c "import numpy, requests; print(numpy.__version__, requests.__version__)"
   python -m pytest -q tests/test_uhe_daily.py
   ```

2. **Plan the bounded sample**; no writes or download:

   ```bash
   python -m tools.data_acquisition.uhe_daily --sample --dry-run --output-dir scratch/uhe_daily
   ```

3. **Acquire the sample** (12 dates: January/April/July/October 15 in
   1985/2000/2014; about 222 MB before retries). Writes diagnostic NPZ artifacts:

   ```bash
   python -m tools.data_acquisition.uhe_daily --sample --download --output-dir scratch/uhe_daily --max-transfer-gb 0.25
   ```

4. **Verify every sample artifact locally**; no network or writes:

   ```bash
   python -m tools.data_acquisition.uhe_daily --sample --verify --output-dir scratch/uhe_daily
   ```

5. **Review scientific/support QC before the complete archive**: compare valid
   masks across sample dates, inspect the three pilot states and islands, inspect
   seasonal ranges and mountain/coastal cells, confirm the raster WBGT definition
   and agreement with CarbonPlan's regional reference under matching weights,
   and record measured transfer/storage/runtime. Acquisition integrity alone
   does not pass this gate. Check independent Indian evidence and its overlap
   with UHE inputs; related products are not automatically independent.

6. **Plan the complete calibration archive**; still no download or writes:

   ```bash
   python -m tools.data_acquisition.uhe_daily --start 1985-01-01 --end 2014-12-31 --dry-run --output-dir scratch/uhe_daily --max-transfer-gb 230
   ```

   The complete download was subsequently authorized and started. For a fresh
   acquisition, run the reviewed plan with `--download`, then the identical date
   range with `--verify`. Use a durable diagnostic output directory with measured
   free space. A transfer cap exhausted by retries requires an explicit resume
   budget; it must not be disabled. Coordinate host etiquette before a bulk pull;
   no external message is sent automatically.

### Active execution and resumption (CHG-0650)

The initial single-stream acquisition stopped on a read timeout after 2007-03-05,
having reserved 149.701 GB. The active resume uses an **80 GB attempt cap**;
combined reservations stay below the original 230 GB envelope. Do not launch a
second downloader or reset that budget while a download is active. Existing days
are verified on resume. Network time is variable; the initial 21-hour estimate
is not a completion guarantee.

Input preparation uses the existing Windows IRT geo environment. QDM execution
uses the existing isolated Linux Python 3.10 environment at
`/tmp/irt-carbonplan-py310/bin/python`, with xclim 0.44.0 and thermofeel 1.3.0.
No environment was upgraded. The elevation input is
`scratch/wbgt_route_b/elevation_cells.csv`: previously cached CarbonPlan elevations
plus the missing 32.125°N, 78.875°E cell (5268 m), retrieved from cached elevation
chunks. Its source hashes are in `elevation_supplement_provenance.json`; the
support manifest freezes the complete elevation table and all support artifacts.

The ordered local stages are:

1. `python -m tools.diagnostics.wbgt_route_b_prepare --stage support --run`
   builds/verifies canonical polygon intersections and reference mapping in the
   Windows geo environment. This step and driver extraction are complete locally.
2. `python -m tools.diagnostics.wbgt_route_b_prepare --stage drivers --run`
   extracts local NEX data, enforcing units, exact cells and complete calendars.
3. `/tmp/irt-carbonplan-py310/bin/python -m tools.diagnostics.wbgt_route_b_finish`
   prints the completion plan without writes or network. Adding `--run` starts
   the locked local coordinator. It waits for all 10,957 reference dates and all
   304 driver shards, verifies the reference, reduces it to NEX support, reruns
   integration smoke, probes each model, runs both pilots sequentially, and writes
   the comparison report. It makes no network requests and never publishes.

The coordinator records progress/failure atomically in `execution_status.json`
and stage logs in `scratch/wbgt_route_b/finish_*.log`. An hour without acquisition
log progress stops the wait for inspection. Each model's measured QDM estimate
must fit a 12-hour diagnostic budget; actual model execution has a 24-hour cap.
Input waiting is limited to 36 hours. These are bounded diagnostic execution
limits, not estimates or national-run authorization. Cache signatures prevent
silent reuse after code/input/support changes. On an input or numerical failure,
inspect the named log and resolve it before rerunning the coordinator.

Validation: focused offline tests cover geometry, missing support, complete years,
different threshold estimands, holdout separation, future windows, cache identity,
period completeness and comparison labels. The real pinned integration smoke
checks Gregorian and no-leap additive QDM, formula parity and solar conservation.
Manual acceptance: verify both model manifests are complete, inspect excluded
units and clipping, compare held-out shade distributions and aggregation gaps,
then review the scientific limitations before any release decision.

## Stage 2: paired three-state pilot

Before compute, freeze the exact model/input intersection, roster and geometry
hashes, valid-support policy, calendar, quantile nodes/interpolation/extrapolation,
application windows, weights and metric semantics. The reported 21 models are an
inventory hypothesis until checked across every required variable/scenario/year.
Cache training by model and spatial support; do not retrain historical mappings
independently for each future scenario.

Run a representative small model/spatial subset first to measure QDM training,
application, I/O, peak memory and cache size. Extrapolate national cost separately
for cell counts and admin-unit counts, not only model × scenario × unit count.

### Two paths and two distinguishable metrics

- **Region-first control:** aggregate raw shade and UHE daily reference to the
  same polygon and fixed weight definition, train additive dayofyear/31-day QDM,
  apply the pinned region-level sun stage. On CarbonPlan control footprints,
  reproduce their population weighting and source support. IRT area weighting
  is a documented variant, not exact CarbonPlan reproduction.
- **Grid-first candidate:** conservatively area-average valid UHE source cells
  onto the exact NEX grid, retain valid-reference area fractions, train QDM per
  cell, apply the sun stage per cell, and use production intersection-area
  weights for admin summaries. Define coastal coverage gates before scoring;
  do not fill missing ocean/land cells or renormalize changing support silently.

For a controlled aggregation-order comparison, use the same coarsened reference
support and weights in both paths. Separately report sensitivity to using native
0.05° UHE directly in regional reference aggregation; otherwise regridding and
QDM-order effects are confounded. Coarsening to 0.25° does not create 5 km output
detail or independent block variation within one climate cell.

For each common support, distinguish:

1. **Regional-series exceedance days:** threshold the daily regional-average
   outdoor WBGT, then count. This permits comparison of the two paths' daily
   regional series under a common metric definition.
2. **Area-weighted cell exceedance days:** threshold each cell's daily outdoor
   WBGT, reduce annually, then aggregate. This is the proposed IRT grid-first
   metric and can be fractional. It is not the same quantity as (1).

Also report annual mean/maximum semantics explicitly; maximum and spatial mean
do not commute. Apply annual completeness at the declared support; no incomplete
cell-year may silently produce a low count. Preserve genuine zero counts.

### Evaluation and decision gates

- Retain the original CarbonPlan-footprint reproduction as a control, including
  tied-day residuals, exact threshold counts, and radiation conservation.
  Merely overlapping admin polygons are not a parity target.
- Evaluate held-out contiguous temporal blocks for seasonal distributions,
  upper quantiles and annual >=28/30/32 °C counts against UHE. Historical
  in-sample fit is not a validation score. NEX weather is not synchronized with
  observations: no same-date NEX-to-observation RMSE/correlation scoring.
- Compare B with frozen W1 on identical model inputs/support; show paired changes
  by state, elevation/coast, metric and model. Do not call this accuracy testing.
- Include SSP245 and SSP585 future tests, including the 2060–2080 tail. Compare
  QDM application-window choices (upstream windows versus a declared continuous
  annual-series approach) before selecting one. Check quantile-delta behaviour,
  year-boundary discontinuities, and radiation/wind clipping frequencies. Do not
  silently stitch overlapping windows or assume historical fit transfers to 2080.
- Report support coverage, district/block consistency, effect sizes and model
  spread. Record acceptance tolerances before scored pilot execution; this
  programme does not invent an unsupported observational accuracy threshold.

Proceed nationally only with a reference-QC verdict, selected metric semantics
and aggregation path, future-window decision, explicit limitations, and measured
resource budget. Display-only release precedes any separate composite decision.

## Validation and change record

CHG-0644 — APPLIED (user-confirmed direction): this programme, bounded acquisition
tool, offline tests, and navigation updates in README/MANIFEST/tools README.
CHG-0649 — APPLIED (user-confirmed continuation): reference QC tool/tests, native
support evidence and preserved native-versus-published-footprint controls.
CHG-0650 — APPLIED (user-confirmed continuation): paired-pilot kernels, input
preparation, pinned runner, complete-period reporting, common-support W1
comparison, logged completion coordinator, tests and navigation. Input acquisition
and scientific pilot execution remain in progress; code completion is not a
scientific acceptance verdict.
No production formulas, metric slugs, scoring membership, published artifacts,
HANDOFF or BACKLOG entries are changed.

CHG-0651 — APPLIED (user-confirmed continuation, 2026-10-05): solar float64
input promotion, regression coverage, audited reuse of unchanged QDM arrays,
and retention of original compute timings during recovery. The isolated pinned
smoke passed; the IRT regression suite passed 50 tests before reporting changes.

Offline tests cover dry-run isolation, exact spatial windows, changed TIFF
layouts, HTTP fallback/range/encoding/truncation, source mutation, transfer caps,
leap-date planning, empty/all-NaN/single-point/extreme/partial-missing QC, atomic
round-trip provenance, wrong-date cache refusal, and tamper detection.
Manual validation follows operator steps 2–5. Run evidence belongs in a separate
diagnostic report; the scientific QC gate must not be inferred from passing tests.
