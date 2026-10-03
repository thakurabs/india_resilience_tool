# CarbonPlan shade WBGT pilot in IRT — Kerala (CHG-0639..CHG-0642)

**What this is.** CarbonPlan's shade WBGT method run inside IRT for one state
(Kerala), one model (ACCESS-CM2) and the historical period. Area weights replace
CarbonPlan's population weights. Results are compared per district and per
block with IRT's deployed `shade-peak-v1`. It is an implementation pilot. It
does not explain *why* the two differ; that was ruled out of scope. Contract:
[`SPEC.md`](SPEC.md), written before any score. Its two pre-score corrections
are recorded inline there.

## Result in one paragraph

The wiring is proven. The same code run with CarbonPlan's own population
weights reproduces their published shade threshold counts **exactly**
(1985-2014, 3 of 3 regions). Area-weighted, the CarbonPlan method gives Kerala
much the same **annual mean** as `shade-peak-v1`: +0.22 °C across districts,
with blocks ranging -2.2 to +1.8 °C. But it gives **far more warm days**.
`days_ge_28` averages +40 d/yr per district and `days_ge_30` +10 d/yr, while
`days_ge_32` is near zero in both (pilot 0.04, IRT 0.14 d/yr). The two methods
**disagree on which places are hottest**: Spearman across units is only
0.46-0.67 for districts and 0.52-0.60 for blocks, on every metric except
`days_ge_32`. Year-to-year co-variation is high for the annual mean (r ≈ 0.97)
and lower for the counts (0.67-0.90).

## Method (see SPEC.md for the full contract)

1. Raw shade per 0.25° NEX cell, using CarbonPlan notebook 02 (huss, pressure
   from elevation, xclim RH at tasmax, thermofeel 1.3 WBT/BGT,
   `0.7·WBT + 0.2·BGT + 0.1·Tmax`).
2. Cells → CarbonPlan units by **intersection area only**. Units are the 86
   Indian `hierid` regions plus the 63 city polygons intersecting Kerala: 149
   in total, over 117 cells. Null-WBGT cells (2, with no elevation) are dropped
   before normalising, as notebook 05 does.
3. QDM per unit with notebook 06's own `train_bias_correction`, trained
   1985-2014 against UHE-Daily.
4. Units → IRT districts and blocks by intersection area.
5. IRT conventions: Feb 29 dropped, complete 365-day years, inclusive
   thresholds, and the period value as the mean of the 1990-2010 yearly values.
   Counts are region-first (per unit-year, then area-weighted), mirroring IRT's
   cell-first rule. Counts taken on the unit daily series are reported
   alongside them.

## Correctness check (wiring proof)

`results/correctness_check.csv`: the 3 units with the largest Kerala intersection area,
population-weighted, against CarbonPlan's published `historical-WBGT-shade`
(ACCESS-CM2, 10,957 days each).

| processing_id | hierid | days ≥28 / ≥30 / ≥32 (pilot = published) | max abs diff °C | days > 1e-3 °C |
|---|---|---|---|---|
| 19567 | IND.18.251.962 | 2876 / 427 / 0 | 5.5e-5 | 0 |
| 20346 | IND.18.245.940 | 1 / 0 / 0 | 0.181 | 2 |
| 20372 | IND.18.245.941 | 64 / 0 / 0 | 0.0067 | 2 |

The 4 days above 1e-3 °C are the known QDM tie-break artefact. Each is one of
a pair of days in the same day-of-year group whose raw values differ by
1-2e-5 °C. The two residuals are equal and opposite: 1988-10-25 and
2007-10-26 at ∓0.181; 2008-05-18 and 2004-05-18 at ∓0.0067. The adjustment
factors were exchanged between indistinguishable days. They are reported and
not corrected. The vectorised raw-shade path equals the predecessor
`reproduce.raw_shade` to 0.0 °C.

## Comparison (pilot minus shade-peak-v1, 1990-2010 period)

`results/summary.csv`:

| level | metric | pilot mean | IRT mean | diff median | diff range | Spearman |
|---|---|---|---|---|---|---|
| district (14) | annual_mean °C | 26.35 | 26.13 | +0.37 | -1.62 .. +1.14 | 0.46 |
| district | days_ge_28 | 95.2 | 54.9 | +38.0 | -0.1 .. +80.7 | 0.54 |
| district | days_ge_30 | 16.0 | 6.1 | +9.4 | -0.2 .. +36.7 | 0.67 |
| district | days_ge_32 | 0.04 | 0.14 | -0.06 | -0.25 .. +0.01 | 0.83 |
| block (152) | annual_mean °C | 26.91 | 26.66 | +0.32 | -2.17 .. +1.81 | 0.60 |
| block | days_ge_28 | 114.0 | 71.0 | +42.0 | -47.6 .. +119.5 | 0.52 |
| block | days_ge_30 | 21.4 | 8.5 | +10.5 | -9.9 .. +47.1 | 0.55 |
| block | days_ge_32 | 0.07 | 0.21 | -0.09 | -0.69 .. +0.13 | 0.76 |

Per district (annual mean °C; days ≥28; days ≥30, each as pilot / IRT):

| district | mean | ≥28 | ≥30 |
|---|---|---|---|
| Alappuzha | 27.8 / 27.4 | 155.8 / 94.2 | 24.7 / 11.6 |
| Ernakulam | 26.4 / 27.0 | 108.8 / 90.0 | 24.3 / 11.9 |
| Idukki | 23.0 / 23.5 | 9.8 / 7.0 | 0.3 / 0.5 |
| Kannur | 26.7 / 26.6 | 95.8 / 74.4 | 12.6 / 9.9 |
| Kasaragod | 27.1 / 26.4 | 107.1 / 50.9 | 12.5 / 3.0 |
| Kollam | 27.2 / 26.3 | 112.7 / 32.0 | 15.6 / 0.6 |
| Kottayam | 26.9 / 26.9 | 108.5 / 72.9 | 16.3 / 8.1 |
| Kozhikode | 26.9 / 26.4 | 104.7 / 64.2 | 16.5 / 7.2 |
| Malappuram | 26.9 / 26.6 | 104.6 / 76.3 | 19.4 / 11.6 |
| Palakkad | 26.1 / 26.4 | 74.8 / 71.0 | 11.7 / 10.0 |
| Pathanamthitta | 26.6 / 25.7 | 90.4 / 33.9 | 11.6 / 1.7 |
| Thiruvananthapuram | 27.2 / 26.1 | 104.7 / 25.6 | 12.7 / 0.3 |
| Thrissur | 27.8 / 26.8 | 152.8 / 73.1 | 45.4 / 8.6 |
| Wayanad | 22.4 / 24.0 | 2.5 / 2.6 | 0.2 / 0.1 |

Read-outs, without attribution:
- The pilot is cooler than IRT in the highlands (Wayanad -1.6 °C;
  Kothamangalam, Nedumkandom and Kattappana blocks -1.9 to -2.2 °C). It is
  warmer in the south and on the coast (Thiruvananthapuram +1.1 °C; Chalakkudy
  and Vellanad blocks +1.8 °C).
- The largest count differences are in the south, where IRT shows almost no
  ≥30 °C days: Thiruvananthapuram 0.3, Kollam 0.6, Pathanamthitta 1.7, against
  the pilot's 12-16.
- The secondary rule (counts taken on the unit daily series) gives smaller but
  same-signed count differences: +31 instead of +40 d/yr for district
  `days_ge_28`.

## Limitations

- One model and the historical period only. The QDM is fitted on the same
  1985-2014 window it is evaluated within, so this is not an out-of-sample test.
- Spatial detail is capped at CarbonPlan's units. A block smaller than its
  region inherits the region's series.
- 7 units cover less than 0.95 of their area with CarbonPlan geometry. All are
  backwater or coastal blocks (Thycattussery 0.55, Aryad 0.69, Kanjikkuzhy,
  Vypeen, Pallom, Pattanakkad) plus one district (0.93). Values are
  renormalised over the covered area and flagged, not filled.
- Local NEX files may differ in version from CarbonPlan's 2023 download. The
  correctness check bounds the effect at ≤ 5.5e-5 °C outside tie-break days.
- Area weights are the only intended departure from CarbonPlan. Population
  weighting is deferred to the decision the user makes after this comparison.

## Reproduce

From the repository root:

```bash
# 1. IRT env (Windows python from WSL): inputs under scratch/carbonplan_kerala
/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe docs/diagnostics/wbgt_shade_carbonplan_kerala/scripts/prepare_inputs.py
# 2. Pinned py3.10 env (see ../wbgt_outdoor_carbonplan_reproduction/README.md)
/tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_shade_carbonplan_kerala/scripts/run_pilot.py
# 3. Either env: comparison tables into results/
/tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_shade_carbonplan_kerala/scripts/compare.py
```

- The scripts import `Archive`, `load_function`, `raw_shade` and
  `verify_sources` from `../wbgt_outdoor_carbonplan_reproduction/scripts/`.
  They need its checksum-locked upstream sources in
  `scratch/carbonplan_reproduction/upstream`.
- Preparation reads that reproduction's chunk cache read-only. It streams
  CarbonPlan's 190 MB geometry file without storing it (declared in the SPEC).
- Total transfer stayed under the 2 GB cap:
  - The pilot's own chunk cache holds 508 MB.
  - The geometry was streamed four times in full and once partially, because
    setup was re-run after the two pre-score corrections. That is at most
    about 0.95 GB.
- Runtimes were not formally timed.

## Files

- `SPEC.md`: the contract, with two pre-score corrections (null-cell mask; city
  units).
- `scripts/prepare_inputs.py`, `scripts/run_pilot.py`, `scripts/compare.py`.
- `results/summary.csv`: per level and metric, the difference distribution,
  Spearman across units and mean yearly correlation.
- `results/unit_period_comparison.csv`: per unit and metric, the pilot value
  (both count rules), the IRT value, the differences and coverage.
- `results/unit_yearly_comparison.csv`: the same per year.
- `results/unmatched_units.csv`: empty, since all 166 units matched.
- `results/comparison_manifest.json`.
- Scratch evidence (`scratch/carbonplan_kerala/run/`):
  - `correctness_check.csv`
  - `region_daily_corrected.csv`
  - `region_yearly.csv`
  - `unit_yearly.csv`
  - `unit_periods.csv`
  - `run_manifest.json`, which holds the QDM warnings and package versions.

## Population-weighted variant (CHG-0643)

This variant uses CarbonPlan's own weights (area × GHS-POP 2030) for cells →
CarbonPlan units. Units → IRT districts and blocks stay area-weighted. Run it
with `run_pilot.py --weights population --out-dir scratch/carbonplan_kerala/run_population`,
then `compare.py --run-dir scratch/carbonplan_kerala/run_population --out-dir .../results_population`.
The evidence is in `results_population/`, and `population_vs_area.csv` gives
each unit side by side.

**It is CarbonPlan's published product.** All 149 Kerala units were checked
against the published ACCESS-CM2 historical shade series for 1985-2014:
- 141 match every ≥28/30/32 count exactly.
- Each of the other 8 is off by a single day on one threshold over 30 years.
  On every flipped day both values sit within 5e-5 °C of the threshold (for
  example 27.99995 against 28.00000). That is input-version float noise, not a
  method difference.
- The median of the per-unit maximum daily difference is 0.0067 °C. Larger
  single days are the known QDM tie-break pairs.

**Weights barely matter after QDM.** Population against area:
- Annual mean differs by at most 0.002 °C in every unit.
- `days_ge_28/30` differ by at most 0.43 d/yr per block and 0.27 d/yr per
  district.
- Unit rankings are essentially identical (Spearman 0.98-1.00).

QDM maps each unit's raw series onto the same UHE-Daily reference
distribution, so a weighting change that shifts the raw series is almost
entirely absorbed. Individual days can still differ by up to 3.2 °C, with a
mean absolute difference of 0.019 °C, but annual statistics do not move.
Consequently the comparison with `shade-peak-v1` is unchanged from the
area-weighted pilot to the reported precision: district annual mean +0.22 °C,
`days_ge_28` +40 d/yr, `days_ge_30` +10 d/yr, and unit-rank Spearman
0.46-0.84.
