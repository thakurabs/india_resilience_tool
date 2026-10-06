# CarbonPlan shade-WBGT working pilot (CHG-0652..CHG-0655)

Status: **PILOT COMPLETE, 2026-10-05. Not a release.** The method was frozen
in [`METHOD.md`](METHOD.md) before any run, with no amendments since.

- National execution is not authorised.
- `shade-peak-v1` and its staging tree were only read, never written.
- Nothing was committed or published.

> **Two models are not enough for a release ensemble.** These outputs come from
> ACCESS-CM2 and CMCC-CM2-SR5 only. They show that the pipeline works end to end.
> They are not an ensemble estimate and must not be read as a model spread.

## What was produced

- **Scope:**
  - three states: Kerala, Rajasthan and Himachal Pradesh;
  - two models: ACCESS-CM2 and CMCC-CM2-SR5;
  - historical and SSP2-4.5 runs;
  - district and block level;
  - the four IRT shade metrics.
- **Format:** the same file layout and CSV columns as IRT's processed tree, plus
  pilot columns. The output is isolated under `scratch/carbonplan_shade_pilot/processed/`.
- **Signature:** `carbonplan-shade-pilot-v1:...`, written on every row. The full
  string is in METHOD.md.

| Artifact | Path |
|---|---|
| Admin CSVs (20,992 files, 333 MB, 367,360 admin-year rows) | `scratch/carbonplan_shade_pilot/processed/wbgt_shade_stull_<slug>/<State>/{districts,blocks}/…/<model>/<scenario>/*_{yearly,periods}.csv` |
| Calibrated daily unit series (535 MB) | `scratch/carbonplan_shade_pilot/calibrated/<model>/*.npz` |
| All admin-years in one file | `scratch/carbonplan_shade_pilot/admin_yearly_all.csv.gz` |
| Coverage and overlap, all 656 admins | `results/coverage_and_overlap.csv`, `results/overlap_all_nonzero.csv` |
| Excluded admins | `results/excluded_admins.csv` |
| Parity | `results/parity/{parity_summary.json, qdm_parity_vs_published.csv, qdm_parity_threshold_flips.csv}` |
| Reader verification | `results/reader_verification.json` |
| Comparison with shade-peak-v1 | `results/comparison/pilot_vs_shade_peak_v1_{summary.csv, units.csv.gz}` |
| Runtime and memory, per stage | `results/stage_log.jsonl` |

**Periods** are as frozen in the method:
- historical: 1990–2010;
- SSP2-4.5: 2020–2039 and 2040–2059.

IRT's labelled 2020–2040 and 2040–2060 periods are **not** produced. They would
need 2060, and no published adjustment window covers it.

Yearly files hold every adjusted year:
- historical: 1985–2014;
- SSP2-4.5: 2020–2059.

The pilot has **no incomplete years**. Every non-excluded admin has all years in
every period.

## Correctness checks

| Check | Result |
|---|---|
| Raw formula (G1), all 736 units × 2 models | max abs 0.0 °C, **PASS** |
| QDM vs the published product, ACCESS-CM2 Kerala, historical | 149 units: 141 have exact counts; 10 flip days, all explained (8 threshold proximity, 2 tie pairs). **PASS** |
| … ACCESS-CM2 Kerala, ssp245 2020–2039 | 146/149 exact; 7 flips (3 proximity, 4 tie). **PASS** |
| … ACCESS-CM2 Kerala, ssp245 2040–2059 | 144/149 exact; 7 flips (5 proximity, 2 tie). **PASS** |
| … CMCC-CM2-SR5 Kerala, historical (no-leap calendar path) | 145/149 exact; 4 flips (all proximity). **PASS** |
| Determinism vs the validation's earlier ACCESS Kerala outputs | max diff 7.1e-15 °C in every window. **Not bit-identical** (floating-point order), but numerically equal. |
| Calendar | ACCESS (proleptic Gregorian): 0 gap days. CMCC (365_day): 7 / 5 / 5 interpolated Feb 29s per unit-window, as expected. All Feb 29s are dropped before counting. |
| Stock IRT reader refuses pilot files | refused for all 4 slugs. **PASS** |
| Reader round-trip, signature patched in-process only | 24/24 slug × state × level checks: every value identical, 0 unmatched. `build_master_metrics` builds all 24 masters. **PASS** |

Parity results hold only for the units and windows tested: Kerala only. CMCC was
checked for the historical window only.

The largest single-day QDM differences are 0.51–1.71 °C. They occur on
tie-ordered days, the same artefact already documented in the validation. Median
per-unit maxima are ≤0.007 °C.

## Coverage and excluded admins

Coverage is now the area of the **union** of intersections (CHG-0652):
- the maximum is 1.0000000000000064;
- the retired summed-intersection method reached 1.2145.

375 admins have some overlap. All overlaps that are not excluded are at most
2.9e-7 of the admin area (≤153 m²).

**Excluded admins.** These are written as missing, with
`pilot_exclusion_reason = unresolved_source_polygon_overlap`. No ownership rule
was chosen and no weights were renormalised.

| State | Level | Admin | Overlap fraction | Source units |
|---|---|---|---|---|
| Kerala | block | Ernakulam / Kothamangalam | 0.0039 | 8 |
| Kerala | district | Ernakulam | 0.0012 | 19 |
| Himachal Pradesh | block | Shimla / Basantpur | **0.2145** | 3 |
| Himachal Pradesh | block | Shimla / Mashobra | 0.0121 | 11 |
| Himachal Pradesh | district | Shimla | 0.0024 | 24 |

**Coverage below 0.95.** These admins are kept, and their coverage is written per
row in `coverage_union_fraction`.

| Admin | Coverage |
|---|---|
| Thycattussery | 0.548 |
| Aryad | 0.694 |
| Kanjikkuzhy | 0.840 |
| Vypeen | 0.849 |
| Pallom | 0.896 |
| Pattanakkad | 0.911 |
| Pooh | 0.925 |
| Alappuzha district | 0.927 |

## ⚠ Rajasthan ≥32 °C: a known deterioration, retained unchanged

The validation holdout (train 1985–1999, test 2000–2014) found that **QDM makes
Rajasthan days ≥32 °C worse for both models**:
- the median absolute error rose from 1.4 to **5.3** d/yr for ACCESS-CM2, and
  from 1.3 to **6.4** d/yr for CMCC-CM2-SR5;
- signed errors moved to **+5.3 / +6.4 d/yr**, an overestimate;
- 78–85% of units got worse.

Rajasthan is the only tested state with widespread ≥32 °C support. As instructed,
the pilot **keeps** the 32 °C threshold:
- the values are not clipped or tuned;
- no other method is substituted.

The pilot values are therefore likely to **overstate** Rajasthan ≥32 °C days, by
a margin of the order of the holdout error. That holdout used a different training
window, so the size of the bias here is not measured.

| Rajasthan blocks, median days ≥32 °C | 1990–2010 | 2020–2039 | 2040–2059 |
|---|---|---|---|
| Pilot, ACCESS-CM2 | 1.9 | 24.6 | 63.8 |
| Pilot, CMCC-CM2-SR5 | 2.0 | 10.3 | 24.8 |

The holdout says nothing direct about future windows. A finding from the
validation still stands: QDM output depends on the window it is applied to.

## Comparison with shade-peak-v1

The comparison uses each admin's period mean of yearly values. It covers only
admins that are complete in both products for the period. Medians are taken over
those admins. The table shows district level; block level is in
`results/comparison/pilot_vs_shade_peak_v1_summary.csv`. Here Δ means the median
difference, pilot minus v1, and ρ is the Spearman rank correlation.

| State | Model | Metric | Δ 1990–2010 | ρ | Δ 2040–2059 | ρ |
|---|---|---|---|---|---|---|
| Kerala | ACCESS | annual mean °C | +0.46 | 0.59 | +0.50 | 0.54 |
| Kerala | ACCESS | ≥28 d/yr | +40.3 | 0.49 | +33.5 | 0.48 |
| Kerala | ACCESS | ≥30 | +9.3 | 0.58 | +48.1 | 0.62 |
| Kerala | ACCESS | ≥32 | −0.04 | 0.80 | +4.6 | 0.76 |
| Kerala | CMCC | annual mean °C | −0.56 | 0.57 | −0.45 | 0.55 |
| Kerala | CMCC | ≥28 | −31.4 | 0.62 | −25.2 | 0.53 |
| Kerala | CMCC | ≥30 | −8.8 | 0.60 | −11.0 | 0.59 |
| Kerala | CMCC | ≥32 | −2.4 | 0.48 | −5.5 | 0.88 |
| Rajasthan | ACCESS | annual mean °C | +1.77 | 0.88 | +1.53 | 0.67 |
| Rajasthan | ACCESS | ≥28 | +33.1 | 0.90 | +23.8 | 0.59 |
| Rajasthan | ACCESS | ≥30 | +16.0 | 0.92 | +12.2 | 0.79 |
| Rajasthan | ACCESS | ≥32 | −2.6 | 0.76 | +3.2 | 0.80 |
| Rajasthan | CMCC | annual mean °C | +0.82 | 0.88 | +0.68 | 0.86 |
| Rajasthan | CMCC | ≥28 | −3.0 | 0.89 | +1.2 | 0.85 |
| Rajasthan | CMCC | ≥30 | −9.6 | 0.93 | −9.8 | 0.88 |
| Rajasthan | CMCC | ≥32 | −5.8 | 0.88 | −9.6 | 0.90 |
| Himachal | ACCESS | annual mean °C | −0.46 | 0.99 | −0.54 | 0.98 |
| Himachal | ACCESS | ≥28 | −2.9 | 0.95 | −12.7 | 0.95 |
| Himachal | CMCC | annual mean °C | −1.38 | 0.99 | −1.44 | 1.00 |
| Himachal | CMCC | ≥28 | −21.5 | 0.94 | −17.0 | 1.00 |

**What the comparison shows:**
- The two products differ systematically, and the direction depends on the model.
  For ACCESS-CM2 the pilot is warmer than v1 in Kerala and Rajasthan. For
  CMCC-CM2-SR5 it is cooler, except for the Rajasthan annual mean.
  - This is expected: QDM removes each model's own bias against UHE-Daily, and
    v1 does not.
  - With two models, the pilot cannot tell whether the corrected ensemble would
    be warmer or cooler than v1.
- The ranking between products agrees well in Rajasthan and Himachal (ρ 0.76–1.00
  at district level). Agreement is weak in Kerala (ρ 0.48–0.88), consistent with
  the earlier Kerala pilot.
- Block-level differences reach **200 d/yr** for single admin units. These are in
  the ≥28 °C counts in Kerala, where a fixed threshold sits near the median of the
  distribution.

**shade-peak-v1 has missing years, a defect found by this comparison.** v1 writes
NaN for whole admin-years in yearly files whose rows are otherwise complete:
- Rajasthan ACCESS-CM2 ssp245 blocks: 2020 is NaN in 122 of 348 blocks, 2022 in
  118, 2050 in 117, and there are smaller gaps through 2080.
- In the historical run, 18 Rajasthan blocks are missing 1–2 years.
- In Himachal CMCC 2040–2059, 25 blocks and 2 districts are affected.

This is why some rows above compare fewer admins, for example 153 of 348
Rajasthan blocks for ACCESS 2040–2059. The cause is **not attributed here**. A
plausible but untested candidate is the cell-year loss from NEX `hurs`>100%,
which was seen in the outdoor pilot. The defect needs a separate look before any
v1 promotion. The pilot has no such gaps.

## Runtime and memory (measured, `results/stage_log.jsonl`)

| Stage | Environment | Wall time | Peak RSS |
|---|---|---|---|
| coverage | irt | 153 s | 238 MB |
| fetch-parity (CMCC Kerala, 319 MB) | irt | 62 s | 417 MB |
| calibrate ACCESS-CM2 (736 units) | pinned | 5,113 s (85 min) | 1,937 MB |
| calibrate CMCC-CM2-SR5 (736 units), run concurrently | pinned | 5,433 s (91 min) | 1,941 MB |
| parity | pinned | 61 s | 634 MB |
| write | irt | 428 s | 459 MB |
| verify | irt | 355 s | 312 MB |
| compare | irt | 102 s | 301 MB |

Calibration averaged about 7 s per unit-model, with two processes running side by
side. Later units were slower, at about 15 s each. No national
estimate is given: the per-state unit mix is not measured.

**Downloads:** 318.6 MB were downloaded in this task. The task ledger stands at
933.4 MB of the 2 GB cap.

## Reproduction

Run from the repository root.
- `IRT` is `/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe`.
  This is the Windows irt environment; it cannot see `/tmp`.
- `PIN` is `/tmp/irt-carbonplan-py310/bin/python` (Python 3.10, xclim 0.44.0,
  thermofeel 1.3.0).
- Validation caches must already be present under
  `scratch/carbonplan_shade_validation/`.

```bash
$IRT -m tools.diagnostics.wbgt_shade_cp_pilot coverage
$IRT -m tools.diagnostics.wbgt_shade_cp_pilot fetch-parity
$PIN -m tools.diagnostics.wbgt_shade_cp_pilot calibrate --model ACCESS-CM2
$PIN -m tools.diagnostics.wbgt_shade_cp_pilot calibrate --model CMCC-CM2-SR5
$PIN -m tools.diagnostics.wbgt_shade_cp_pilot parity
$IRT -m tools.diagnostics.wbgt_shade_cp_pilot write     # refuses a non-empty output root
$IRT -m tools.diagnostics.wbgt_shade_cp_pilot verify
$IRT -m tools.diagnostics.wbgt_shade_cp_pilot compare
$IRT -m pytest -q tests/test_wbgt_shade_cp_validation.py   # 22 passed
```

Any stage accepts `--dry-run`. The two calibrate commands can run concurrently.

## Tests

- The focused tests for the changed compute and transfer paths pass: 22 passed in
  `tests/test_wbgt_shade_cp_validation.py`. They cover:
  - union coverage;
  - refusing the old summed coverage;
  - excluding an overlapping admin as missing;
  - reporting an overlap below tolerance without excluding it.
- Generated files were checked through the IRT reader (see Correctness checks).
- As instructed, the full two-hour suite and the holdout experiments were **not**
  rerun.

## Not done (deferred until the pilot is reviewed)

- Residual attribution: the validation's 118/116 unattributed days, and window
  dependence.
- A national SPEC, the release roster (more than two models), and the IRT
  2020–2040 / 2040–2060 period mapping.
- QDM parity outside Kerala, and CMCC future windows.
- A decision on the overlap-excluded admins (an ownership rule) and on the Alappuzha
  coverage gaps.
- The cause of the missing years in shade-peak-v1.
