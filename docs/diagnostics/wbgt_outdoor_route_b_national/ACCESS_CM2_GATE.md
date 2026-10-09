# ACCESS-CM2 national release gate — result

CHG-0666. The first national model, assembled 2026-10-06 and validated against
`RELEASE_SPEC.md` section 10. **Verdict: PASS.** This is the structural gate. It
is not a publication approval, and passing it does not settle the three open
decisions.

## 1. Run facts

| Quantity | Measured |
|---|---|
| Assembly elapsed | 2,622.6 s (43.7 min) |
| Peak RSS | 18,751 MB |
| Cell caches consumed | 4,938 of 4,938, zero stale |
| Cells eligible | 4,938 |
| Units eligible | 7,752 of 7,921 |
| Yearly rows | 2,265,406, exactly the declared roster |
| Years per scenario | historical 21, ssp245 61, ssp585 61 |
| Assembly-level warnings | none |

Fit-time warnings were **not** re-emitted, because the assembling run reused
caches. They live in the 24 shard reports and the validator now reads both; see
section 5.

## 2. Structural criteria

| Criterion | Verdict | Evidence |
|---|---|---|
| Manifest and provenance pinned | PASS | support signature matches; source lock and code hashes present |
| Roster complete, once each | PASS | 2,265,406 rows = expected; 7,921 units; no duplicates, none missing, none extra |
| Excluded units missing, never zero | PASS | 169 units, 48,334 rows, **zero** zero-values and **zero** finite values |
| Bounds and monotonicity | PASS | see section 3 |
| Estimands genuinely distinct | PASS | see section 4 |
| Held-out skill reported with outliers | PASS | see section 6 |
| Day-366 exposure measured | PASS | see section 5 |

## 3. Values

| Quantity | Range |
|---|---|
| `mean_c` | 1.34 to 35.37 °C |
| `max_c` | 14.28 to 42.71 °C |
| `days_ge_28` | 0 to 365 |
| `days_ge_30` | 0 to 365 |
| `days_ge_32` | 0 to 338.61 |

Genuine zeros are preserved and counted: 14,088 unit-years at exactly zero
`days_ge_28`, 34,727 at `days_ge_30`, 67,738 at `days_ge_32`.

**Saturation is a result, not an error.** 3,698 unit-years reach the calendar
ceiling on `days_ge_28` and 13 on `days_ge_30` — every day of the year above the
threshold. They concentrate in Kerala (208), Tamil Nadu (174) and Andhra Pradesh
(144), overwhelmingly under SSP585 (452 of 560), which is the expected direction
for humid coastal south India late century.

A first validator run failed this criterion on an exact `> 365` test. The measured
worst excess was **1.14e-13 days**: counts are area-weighted sums of per-cell
annual counts, and the intersection-area weights sum to 1.0 only to
floating-point precision, so a fully saturated unit lands a few ULP above 365.
The bound now carries a 1e-6 day tolerance, documented in the code, and a test
proves the tolerance admits 365 + 1.1e-13 while still rejecting 366. The
saturation count is reported separately so the tolerance can never hide a real
over-count.

## 4. Aggregation order matters, measurably

Cell-first counts and series counts differ for roughly 1.0 million of 1.1 million
eligible unit-years, with maximum gaps of **82.2, 88.5 and 78.0 days** at the 28,
30 and 32 °C thresholds. Thresholding a spatial mean is therefore not a close
approximation of counting per cell — it is a different quantity by up to a
quarter of a year. The published slugs take the cell-first estimand.

The annual mean commutes as it must: the largest order gap is 2.49e-14 °C.

## 5. The dayofyear warning, measured

All 24 fitting shards recorded xclim's
`Strange results could be returned when using dayofyear grouping on data defined
in the proleptic_gregorian calendar`. The grouping is CarbonPlan's pinned choice,
inherited with the source lock.

Measured exposure: leap years are 5 of 21 reporting historical years (23.8%) and
16 of 61 in each SSP (26.2%). Day 366 therefore exists in about a quarter of
years, giving its quantile group fewer samples than its neighbours. Three
mitigations are already in the code — the 31-day window draws in neighbouring
days, `adjust()` converts back with `align_on="year"` and interpolates only
introduced leap dates, and `annual_metrics` removes February 29 before any annual
reduction. The residual is the sparser day-366 group.

The earlier validator read only the assembling run's manifest, which is empty
because caches were reused — the warning would have looked resolved. It now reads
the shard reports too, and reports the count per distinct warning.

## 6. Held-out skill, 2000-2014 against UHE-Daily

Trained on 1985-1999, scored on 2000-2014, all 7,752 eligible units scored.

| Statistic | Mean | Median | p05 | p95 |
|---|---|---|---|---|
| Annual mean bias (°C) | -0.083 | -0.092 | -0.231 | +0.089 |
| Monthly climatology MAE (°C) | 0.287 | 0.276 | 0.151 | 0.426 |

The worst annual-mean biases are about -0.30 °C, in Telangana blocks (Warangal /
Nalla Belli, Hanumakonda / Shayampet). This is held-out distributional skill
against a provisional calibration reference, not an independent observational
accuracy test, and UHE-Daily is not observed outdoor WBGT truth.

The pilot's Rajasthan upper-tail deterioration must still be re-measured
nationally against this national holdout; that comparison is not in this gate.

## 7. Finding: NEX v2 negative humidity, and the window amplification behind it

384 of 1,108,536 eligible unit-year rows (0.035%) are missing despite belonging to
eligible units. They are not scattered: they fall in exactly three contiguous
blocks that align precisely with the declared QDM application windows
`((2020, 2039), (2040, 2059), (2060, 2080))`.

| State | Scenario | Years | Rows | Units |
|---|---|---|---|---|
| Mizoram | ssp245 | 2020-2039 (window 1) | 260 | 8 blocks |
| Ladakh | ssp245 | 2040-2080 (windows 2 and 3) | 82 | 1 block (Leh) |
| Ladakh | ssp585 | 2060-2080 (window 3) | 42 | 1 block (Leh) |

No district is affected; sibling cells cover the parent districts.

### The chain, traced end to end

1. The raw NEX drivers contain **zero non-finite values**, and their units,
   calendars and completeness all pass the extraction gates.
2. They do contain **negative `huss`** — physically impossible specific humidity.
   Measured examples:

   | Date | Cell | Elevation | `huss` (kg/kg) | Implied RH at tasmax |
   |---|---|---|---|---|
   | 2023-03-31 | 23.125°N, 92.875°E | 632 m | -5.71e-4 | -2.2% |
   | 2023-03-31 | 23.125°N, 92.625°E | 433 m | -2.80e-4 | -1.0% |
   | 2064-07-17 | 35.625°N, 73.875°E | 2,246 m | -3.48e-4 | -0.7% |
   | 2064-07-17 | 35.625°N, 74.125°E | 3,463 m | -2.49e-4 | -0.7% |
   | 2044-08-27 | 36.375°N, 72.625°E | 3,368 m | -8.25e-5 | -0.4% |

   The most negative value anywhere in ACCESS-CM2 is -1.38e-3 kg/kg. This is the
   same class of artefact as the already-documented unbounded `hurs` > 100% in
   NEX v2, which carries no `valid_range` attribute.
3. **`raw_shade` already screens this deliberately.** The implementation carries an
   explicit physical-validity gate — `(huss < 0) | (huss >= 1)`, alongside
   non-finite and non-positive temperature screens — and sets those cell-days to
   NaN. This is correct, intentional, conservative behaviour that was already in
   the code, not an accidental numerical blow-up. The resulting prepared shade
   series carries 21 non-finite cell-days in ssp245 across 6 cells, 2 in ssp585
   across 1 cell, and **none in the historical period**.
4. `adjust()` returns all-NaN for any window containing a non-finite day, by
   design, so it never fabricates a value.
5. An area-weighted unit aggregate containing one NaN cell is NaN, so the unit
   loses the entire window.

So **23 screened input values blank 384 unit-years** — an amplification of roughly
17 unit-years per screened cell-day. The amplification lives in step 4, the
window granularity, not in step 3.

### What this means for the roster

The exposure is strongly model-dependent, and ACCESS-CM2 is the mild case. A
read-only scan of the extracted drivers measured:

| Model | Negative `huss` cell-days | Cells affected | Model-scenario-years affected | Most negative (kg/kg) |
|---|---|---|---|---|
| ACCESS-CM2 | 23 | 6 of 4,938 | 6 | -1.38e-3 |
| ACCESS-ESM1-5 | 2,004 | 451 of 4,938 | 21 | -2.69e-3 |

ACCESS-ESM1-5 has **75 times** the affected cell-days and **451** affected cells
rather than 6, so its unit-year loss cannot be extrapolated from ACCESS-CM2's
0.035% — it must be measured. The full roster scan is recorded in
`negative_huss_scan.json`.

Two structural consequences follow, and both are reasons not to fit the remaining
roster before the decision:

- **Eligibility could diverge between models.** Cell eligibility requires the
  *historical* shade to be entirely finite. ACCESS-CM2 and ACCESS-ESM1-5 both
  keep 4,938 cells because their negative `huss` falls only in the SSPs. A model
  with negative `huss` in 1985-2014 would lose those cells outright, giving a
  different unit roster. `publish_route_b_outdoor` refuses to average two
  different spatial supports into one ensemble, so that would stop publication.
- **Ensemble membership would vary by year and window.** A unit-year missing in
  some models but not others reduces that cell's `n_models` rather than failing
  outright, so the ensemble would silently rest on a varying subset unless the
  contract says otherwise.

### Why nothing was changed

Clipping `huss` to zero would convert a deliberate physical screen into
fabricated data: -2.7e-4 kg/kg is not 0, and pretending otherwise invents a
humidity that the model never produced. The existing screen is the correct
treatment of an impossible value, so **clipping is not offered as an option.**

Reducing the amplification means changing the QDM application granularity — and
the methodology document already lists exactly that as an open decision, to be
settled by comparing upstream windows against a declared continuous annual-series
approach. Either change edits `wbgt_route_b_run.py` or `wbgt_route_b.py`, both
covered by the shared cache signature, so either **invalidates every fitted cache
for every model** and forces a complete refit. One model's measured refit cost is
about 80 minutes on a quiet 24-shard machine, and that cost grows with every
model completed before the decision is taken.

This is put to the user as decision D3 rather than applied silently. Under the
current behaviour the release is self-consistent and conservative: these
unit-years are missing, the dashboard must render them as missing, and no
published value anywhere is wrong.


---

# ACCESS-ESM1-5 national release gate — result

CHG-0666. The second national model, assembled 2026-10-06. **Verdict: PASS.**

| Quantity | ACCESS-CM2 | ACCESS-ESM1-5 |
|---|---|---|
| Assembly elapsed | 2,622.6 s | 6,274.2 s |
| Peak RSS | 18,751 MB | 19,109 MB |
| Units eligible | 7,752 of 7,921 | 7,752 of 7,921 |
| Cells eligible | 4,938 | 4,938 |
| Yearly rows | 2,265,406 | 2,265,406 |
| Excluded units carrying zeros | 0 | 0 |
| Missing rows among eligible units | 384 | 19,733 |
| Saturated at calendar ceiling (`days_ge_28`) | 3,698 | 2,831 |
| Count rows where the two estimands differ | ~1.01 M | ~1.00 M |
| Assembly-level warnings | none | none |

The longer assembly is contention, not a change in cost: three jobs shared the
machine for most of this run.

**Eligibility is byte-identical between the two models** — the same 7,752 units,
the same 4,938 cells, the same `retained_fraction` to twelve decimal places. This
is checked directly from the two `unit_support.csv` artifacts rather than inferred,
and it confirms that one ensemble rests on one consistent spatial support.

Its 19,733 missing unit-years were **predicted exactly before the model finished
assembling** — 374 units and 19,733 unit-years, with no unit predicted that was
not observed and none observed that was not predicted. The loss concentrates in
Rajasthan (10,077 under SSP245 and 7,115 under SSP585, 87% of the total), as
predicted. See `EXPOSURE_REPORT.md`.

The structural criteria that passed are the same seven as for ACCESS-CM2. Both
models are display-ready under the release contract; neither is published.
