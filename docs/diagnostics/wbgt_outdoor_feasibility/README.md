# Outdoor-WBGT reconstruction feasibility — milestone 1

**Recommendation: REVISE.**

Generated 2026-09-29 against `GIT:add_flood_depth@9b38565` (working tree dirty; untracked
diagnostics only). Contract frozen in [SPEC.md](SPEC.md) before any candidate score was computed.
Change IDs: CHG-0602 (spec), CHG-0603 (tool), CHG-0604 (tests), CHG-0605 (this report).

The question was:

> Can the daily climate inputs available to IRT support a sufficiently accurate estimate of daily
> maximum open-sky WBGT, using a physical WBGT calculation and a defensible within-day
> reconstruction?

**Answer, in one paragraph.** Yes for the level and for day-to-day accuracy, not yet for the
threshold counts. A physical Liljegren calculation run on hourly drivers reconstructed from only
the six daily fields NEX ships clears the pre-registered daily bar — median absolute daily error
< 1.0 °C and RMSE < 1.5 °C — at **all six sites in both evaluation periods, 12/12 pairs**, which
no previous outdoor candidate has done (the Tier-2 shade-plus-linear-adjustment "ceiling" passed
4 of 6). The annual mean of daily maxima lands within **+0.04 to +0.45 °C**. But the separate,
separately predeclared count gate **fails**: each of the two viable candidates misses two of
thirty gated site-threshold pairs, at ≥ 32 °C, and they miss at *different* sites. The unresolved
term is the within-day wind treatment. Separately, the reference itself needed a correction, and
the available model roster is smaller and shorter than either previously quoted figure.

---

## 1. What was demonstrated

### 1.1 The daily gate passes everywhere, in both periods

Against the **audited** hourly ERA5 reference, on matched IST local days, all seasons pooled
(`candidate_scores.csv`, `season == "ALL"`):

| Site | Window | C1 median abs err (°C) | C1 RMSE (°C) | C1 bias (°C) | C2 median abs err | C2 RMSE | gate |
|---|---|---:|---:|---:|---:|---:|:--|
| Kochi | **1990-2004** | 0.712 | 0.983 | +0.231 | 0.661 | 0.956 | PASS |
| Kolkata | **1990-2004** | 0.638 | 0.945 | +0.333 | 0.508 | 0.867 | PASS |
| Bikaner | **1990-2004** | 0.726 | 1.160 | +0.041 | 0.622 | 1.232 | PASS |
| Lucknow | **1990-2004** | 0.711 | 1.035 | +0.048 | 0.611 | 1.088 | PASS |
| Hyderabad | **1990-2004** | 0.629 | 0.898 | +0.286 | 0.463 | 0.868 | PASS |
| Shimla | **1990-2004** | 0.857 | 1.107 | +0.274 | 0.710 | 1.118 | PASS |
| Kochi | 2005-2014 | 0.782 | 1.089 | +0.345 | 0.708 | 0.995 | PASS |
| Kolkata | 2005-2014 | 0.703 | 1.033 | +0.452 | 0.578 | 0.913 | PASS |
| Bikaner | 2005-2014 | 0.811 | 1.144 | +0.204 | 0.666 | 1.166 | PASS |
| Lucknow | 2005-2014 | 0.738 | 1.040 | +0.116 | 0.611 | 1.070 | PASS |
| Hyderabad | 2005-2014 | 0.625 | 0.903 | +0.273 | 0.473 | 0.868 | PASS |
| Shimla | 2005-2014 | 0.903 | 1.128 | +0.344 | 0.703 | 1.098 | PASS |

Bars: median absolute daily error < 1.0 °C **and** RMSE < 1.5 °C, per site, no pooling across
sites and **no warm-day subsetting** (the earlier Stage A harness restricted to warm days; that
restriction is not applied here, so Stage A numbers are not directly comparable).

**1990-2004 is the primary, independent period.** No candidate parameter was chosen by looking at
it. 2005-2014 is present only for continuity with the existing six-site tables. The two windows
agree closely, which is the point of including both.

Year-block bootstrap 95 % intervals (1,000 draws of whole years, seed 20260929;
`uncertainty.csv`) keep C1 clear of both bars at every site — the widest RMSE interval is Bikaner
1990-2004 at [1.126, 1.189] °C against a 1.5 °C bar, and the widest median-absolute-error
interval is Shimla 2005-2014 at [0.857, 0.941] °C against a 1.0 °C bar. Shimla is the tightest
margin and it is still inside. These intervals describe temporal sampling uncertainty only — not
reference-method and not climate-model uncertainty.

### 1.2 Against the incumbents and the retired chain

Same reference, season `ALL`, 1990-2004 (`candidate_scores.csv`, `kind == "comparison baseline"`):

| Candidate | median abs err (°C) | RMSE (°C) | r | daily gate |
|---|---:|---:|---:|:--|
| **C1 recon-baseline** | 0.63 – 0.86 | 0.90 – 1.16 | 0.885 – 0.979 | **PASS 6/6** |
| **C2 recon-wind-diurnal** | 0.46 – 0.71 | 0.87 – 1.23 | 0.882 – 0.979 | **PASS 6/6** |
| C3 recon-constant-rh | 2.16 – 2.35 | 2.30 – 2.65 | 0.882 – 0.973 | FAIL 0/6 |
| empirical sWBGT, as deployed | 1.56 – 3.41 | 2.26 – 3.91 | 0.417 – 0.944 | FAIL 0/6 |
| shade Stull at `tasmax` + RH-at-`tasmax` | 2.87 – 4.92 | 3.37 – 5.30 | 0.763 – 0.965 | FAIL 0/6 |
| Tier-2 (shade + retired linear adjustment) | 0.84 – 1.71 | 1.24 – 2.29 | 0.856 – 0.974 | FAIL (passes only Hyderabad) |

The reconstruction is a step change, not an increment: the incumbent "Outdoor WBGT" runs
0.5 – 3.5 °C cold with r as low as 0.42 at Kochi, while C1 runs +0.04 to +0.33 °C with
r ≥ 0.885 everywhere.

### 1.3 Where the remaining error comes from

Oracle ablations (each reconstructs one driver group and leaves the rest at actual hourly ERA5;
**not deployable candidates**). Median absolute daily error, 1990-2004, °C:

| Ablation | Reconstructed | Kochi | Kolkata | Bikaner | Lucknow | Hyderabad | Shimla |
|---|---|---:|---:|---:|---:|---:|---:|
| `A2` | radiation + direct fraction | 0.200 | 0.129 | 0.131 | 0.119 | 0.143 | 0.223 |
| `A3` | wind (daily-mean constant) | 0.657 | 0.279 | 0.361 | 0.445 | 0.273 | 0.863 |
| `A1` | temperature + humidity | 0.426 | 0.463 | 0.668 | 0.401 | 0.427 | 0.386 |
| `A4` | pressure (ISA from elevation) | 0.0005 | 0.0015 | 0.0076 | 0.0029 | 0.0027 | 0.0217 |
| `C1` | everything | 0.712 | 0.638 | 0.726 | 0.711 | 0.629 | 0.857 |

Four conclusions follow, and the third and fourth overturn earlier written claims:

1. **Radiation is the *least* problematic driver.** Disaggregating a daily-mean `rsds` by a
   top-of-atmosphere shape and splitting it with Erbs (1982) costs 0.12 – 0.22 °C. The
   reconciliation document's worry that "disaggregation by solar geometry is mandatory, not
   optional" is right that it is mandatory; it is also cheap and accurate.
2. **Wind is the dominant unresolved term, and it is a *tail* error.** `A3`'s median error is
   modest but its conditional error on the reference's hottest 1 % of days reaches **−2.58 °C**
   (Bikaner), **−2.75 °C** (Lucknow) and **−2.66 °C** (Hyderabad). A daily mean does not imply a
   known hourly wind, and at the hot-dry inland sites the hours that set the WBGT peak are
   precisely the hours whose wind the daily mean misrepresents.
3. **Pressure is a non-issue, so NEX publishing no `ps` costs nothing measurable.** Substituting
   ISA barometric pressure computed from site elevation changes the daily maximum by at most
   **0.022 °C**, at Shimla, *even though* the ISA value there (768.1 hPa, from 2,276 m) disagrees
   with the ERA5 grid cell's mean surface pressure (806.5 hPa) by 38 hPa, because the 0.25° cell
   smooths the Himalaya. The reconciliation document listed "no pressure correction, which biases
   high-elevation Himalayan cells" as a real gap (P4); for the *outdoor daily maximum* it is not
   one. CarbonPlan's elevation-synthesised `ps` buys nothing here.
4. **The moisture invariant matters, and constant vapour pressure is the right one.** C3, which
   holds relative humidity constant through the day instead, runs **+1.93 to +2.55 °C** and fails
   every site. This is direct confirmation, at hourly resolution, of the reasoning behind
   CarbonPlan's RH-at-`tasmax` route and IRT's own shade fix.

### 1.4 Reconstruction self-consistency (`reconstruction_consistency.csv`)

Measured, not asserted:

| Diagnostic | Result |
|---|---|
| Daily `rsds` energy conservation | max relative error **3.6e-16** (tolerance 1e-9) |
| Shortwave in an hour whose whole interval is dark | **exactly 0.0** at every site-window |
| Minimum reconstructed shortwave | **0.0** (never negative) |
| Reconstructed hourly peak minus input `tasmax` | **−0.031 to −0.004 °C** |
| Reconstructed hourly trough minus input `tasmin` | +0.47 to +0.69 °C |
| Reconstructed 24-h mean minus input `tas` | **+0.18 to +0.65 °C** (reported, not forced away) |
| Reconstructed hourly-mean RH minus input `hurs` | **−2.16 to +0.70 %** mean, p05/p95 span −4.94 to +3.80 % |
| RH clipped to [0, 100] | 0.1 % – 7.3 % of hours |
| Liljegren non-finite rate | 1.0e-4 – 1.6e-4, entirely the two record-boundary local days |

Two honest caveats the table encodes. First, Parton-Logan is driven by `tasmin`/`tasmax`, so its
24-hour mean is **not** consistent with the supplied `tas`: it runs 0.18 – 0.65 °C warm. No offset
is applied, because forcing the mean would break the extremes the target depends on. Second,
constant vapour pressure does not preserve daily-mean `hurs`, by up to about 2 % in the mean and
5 % in the tails; that is inherent to the assumption and is why the residual is published.

One label was wrong and is corrected here: the diagnostic originally named
`night_shortwave_max_w_m2` in `site_diagnostics.json` was computed over hours whose *midpoint*
solar cosine is zero, which includes the hour straddling sunrise or sunset. That hour legitimately
carries a non-zero interval mean (up to 29.8 W/m²). The correct pair of statistics is now
`fully_dark_hour_max_shortwave_w_m2` (exactly 0.0) and `straddling_hour_max_shortwave_w_m2`; both
are in `reconstruction_consistency.csv`, and the cached diagnostics were regenerated.

---

## 2. The reference audit — and a defect in the existing reference

### 2.1 Legacy reproduced bit-for-bit

`legacy_reproduction.json`: the legacy identity reproduces the committed
`docs/diagnostics/wbgt_deployed_vs_reference/daily_series.parquet` at **all six sites**, 3,651
compared days each, **maximum absolute difference 0.0**, and `days_ge_32` identical
(Kochi 2113, Kolkata 1439, Bikaner 897, …). The historical evidence was read, never written.

### 2.2 The correction, and why it was needed

Open-Meteo documents `shortwave_radiation` and `direct_radiation` as the **"average of the
preceding hour"** while `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m` and
`surface_pressure` are **"Instant"**. The legacy reference pairs the hour-mean flux with the solar
geometry of the *labelled* instant. The audited identity evaluates `cossza` at the midpoint of
the radiation interval, `H − 30 min`, which is the instant that mean represents.

This is not a cosmetic half-hour. The Liljegren globe balance carries a direct-beam projection
`fdir * (1/(2·cos z) − 1)`, and `thermofeel` treats the sun as up whenever
`cos z > CZA_MIN = 0.00873`. At `CZA_MIN` that factor is **56.3**. So the hour that straddles
sunrise or sunset gets a small mean flux amplified into a large radiant load.

Worked example, Kochi, 2010-03-01, 13:00 UTC — the sunset hour (`reference_low_sun_worst_hours.csv`):

```
shortwave_radiation = 83 W/m2 (mean over 12:00-13:00)   fdir = 0.590
cossza at the label   = 0.0097  -> beam factor  50.6  -> legacy  WBGT = 39.12 C
cossza at the midpoint= 0.1367  -> beam factor   3.2  -> audited WBGT = 28.82 C
genuine solar-noon peak that day (06:00 UTC)           = 32.60 / 32.76 C
```

The legacy reference's **daily maximum for that day is the 39.12 °C sunset artefact**, 6.4 °C
above the real afternoon peak.

Frequency, measured at all six sites (`reference_low_sun_audit.csv`):

| Site | Window | days | legacy peak in a low-sun hour | % of days | mean inflation on those days (°C) | max inflation (°C) |
|---|---|---:|---:|---:|---:|---:|
| Kochi | 1990-2004 | 5478 | 669 | **12.21** | 3.13 | 13.32 |
| Kolkata | 1990-2004 | 5478 | 146 | 2.67 | 2.41 | 8.33 |
| Bikaner | 1990-2004 | 5478 | 423 | 7.72 | 1.48 | 8.10 |
| Lucknow | 1990-2004 | 5478 | 248 | 4.53 | 2.68 | **13.98** |
| Hyderabad | 1990-2004 | 5478 | 255 | 4.66 | 1.30 | 9.24 |
| Shimla | 1990-2004 | 5478 | 286 | 5.22 | 3.28 | 13.40 |
| Kochi | 2005-2014 | 3651 | 413 | **11.31** | 3.15 | 10.12 |
| Kolkata | 2005-2014 | 3651 | 99 | 2.71 | 2.88 | 11.68 |
| Bikaner | 2005-2014 | 3651 | 293 | 8.03 | 1.41 | 9.06 |
| Lucknow | 2005-2014 | 3651 | 150 | 4.11 | 2.67 | 11.10 |
| Hyderabad | 2005-2014 | 3651 | 133 | 3.64 | 1.24 | 5.35 |
| Shimla | 2005-2014 | 3651 | 177 | 4.85 | 3.06 | 13.43 |

### 2.3 What this does and does not invalidate

Scoped carefully, because the temptation is to overstate it.

**It contaminates the reference's tail statistics.** Audited minus legacy at the 99th percentile
of the legacy daily maximum reaches **−5.72 °C** (Kochi MAM), **−3.10 °C** (Kolkata DJF) and
**−2.86 °C** (Shimla DJF); the conditional mean error on the legacy reference's hottest 1 % of
days reaches **−9.94 °C** (Shimla DJF) and **−8.23 °C** (Lucknow DJF). Any `p99_bias_c` column in
`wbgt_deployed_vs_reference/scores.csv` or `wbgt_tier2_probe/per_site.csv` is computed against a
reference whose own top percentile is partly artefact, and should not be quoted.

**It barely moves the annual counts or the annual mean.** The spurious spikes mostly land on days
that already exceeded the threshold, so `days_ge_32` shifts by **0 to −4.9 days/year** and the
annual mean of daily maxima by **−0.11 to −0.34 °C** (`reference_audit.csv`, season
`ANNUAL COUNT`). The count columns of the existing published tables therefore survive; the level
columns shift by a few tenths.

**Neither identity is exactly right for the straddling hour.** The hour's mean flux is real, and
its mean solar geometry over the *sunlit fraction* of the interval is higher than the midpoint
value. The audited identity removes an unphysical 50× amplification; it does not claim to resolve
the straddling hour correctly. A sub-hourly treatment would, and is not in scope here.

### 2.4 Wind height — a documented claim in the repository is wrong

`docs/diagnostics/wbgt_reconciliation/README.md` §2 P2 states: "`sfcWind` is a 10 m value;
ISO/Liljegren want ~2 m. Worth +0.3..0.8 °C … A log-law conversion is needed."

**For this implementation that is incorrect**, and the correction is read from the installed
source rather than inferred. `thermofeel.calculate_wbgt_liljegren` (v2.3.0) documents
`:param va: wind speed at 10 metres [m/s]` and does the conversion itself:

```python
va = np.maximum(va, _LILJEGREN_MIN_WIND_10M)      # 0.62 m/s, KNMI 10 m floor
speed = _liljegren_wind_speed_2m(va, cossza, ssrd) # Pasquill-Gifford power law, floored 0.13 m/s
```

So `tools/diagnostics/wbgt_deployed_vs_reference.py` passing `wind_speed_10m` is **correct**, and
applying a log-law conversion before the call would double-count. Measured, the choice of profile
is nearly irrelevant anyway: swapping the Liljegren stability profile for the generic `brode` log
profile changes the daily maximum by a median of **0.052 – 0.066 °C** and an RMSE of
**0.056 – 0.090 °C** at every site (`candidate_scores.csv`, `sens_wind_brode`).

What matters about wind is not its measurement height. It is its *within-day shape* — §1.3
point 2.

---

## 3. Why the count gate fails

The count-acceptance policy was predeclared in SPEC.md 10.2 and is separate from the daily gate,
because daily accuracy does not license threshold counts. A pair is gated only where the
reference mean annual count is ≥ 5 days/year; pass requires
`|candidate − reference| ≤ max(20 % × reference, 2 days/year)`.

| Candidate | gated pairs | passing | failing | not gated |
|---|---:|---:|---:|---:|
| C1 recon-baseline | 30 | 28 | **2** | 6 |
| C2 recon-wind-diurnal | 30 | 28 | **2** | 6 |
| C3 recon-constant-rh | 30 | 6 | 24 | 6 |

The failures (`annual_counts.csv`):

| Candidate | Site | Window | Threshold | reference (d/yr) | candidate (d/yr) | error | tolerance |
|---|---|---|---:|---:|---:|---:|---:|
| C1 | Bikaner | 1990-2004 | ≥ 32 °C | 73.07 | 92.00 | **+18.93** | 14.61 |
| C1 | Bikaner | 2005-2014 | ≥ 32 °C | 87.22 | 107.44 | **+20.22** | 17.44 |
| C2 | Hyderabad | 1990-2004 | ≥ 32 °C | 39.50 | 28.93 | **−10.57** | 7.90 |
| C2 | Hyderabad | 2005-2014 | ≥ 32 °C | 45.00 | 35.22 | **−9.78** | 9.00 |

Read together these are one finding, not two. C1 holds wind constant at the daily mean, which
under-ventilates the peak hour and so **over**-counts at hot-dry Bikaner. C2 raises daytime wind
on a declared mean-preserving shape, which fixes Bikaner (+8.1 and +9.0, both inside tolerance)
and **over-corrects** at Hyderabad, where it now under-counts. Both candidates reproduce the
level and the day-to-day ordering; the residual is a wind-shape error that flips sign between
regimes. That is exactly the signature §1.3's `A3` ablation predicted.

Both candidates also share a tail bias: the conditional mean error on the reference's hottest 1 %
of days is **negative at all four inland sites** for both (−1.5 to −2.6 °C). They are slightly too
cool on the very hottest days, and ≥ 32 °C is a tail statistic.

### 3.1 Two honest weaknesses in the gate itself

Reported rather than fixed, because the policy was frozen before the results were seen.

1. **The 20 % band is vacuous where the count saturates.** At Kochi ≥ 28 °C the reference is
   356 – 359 days/year, so the tolerance is ±71 days against a physical ceiling of 365. That pair
   cannot fail. Of the 30 gated pairs, the ones that actually discriminate are the ≥ 32 °C pairs
   and the mid-range ≥ 30 °C ones.
2. **Shimla contributes no count evidence at all.** Its reference is 0.57 – 0.78 days/year at
   ≥ 28 °C and exactly 0 at ≥ 30 and ≥ 32, so all six of its pairs are correctly marked
   `NOT GATED (rare event)` and count as neither pass nor fail. The elevation regime is validated
   on daily error only. This is a coverage gap in the evidence base, not a candidate defect.

---

## 4. NEX daily-input inventory (read-only)

Three tiers, reported separately and never conflated. Tier 2 is a **bounded header scan**, not a
national data-quality audit.

### 4.1 Tier 1 — files present (`nex_files_present.csv`, 27,713 rows)

| Variable | Models | Historical years | Projection years | Tree |
|---|---:|---|---|---|
| `tas` | 24 | 1951-2014 | 2015-2100 | main NEX |
| `tasmax` | 23 | 1951-2014 | 2015-2100 | main NEX |
| `tasmin` | 23 | 1951-2014 | 2015-2100 | main NEX |
| `hurs` | 22 | 1951-2014 | 2015-2100 | main NEX |
| `rsds` | **21** | **1990-2010** | **2020-2080** | `nex_gddp_cmip6_v2_wbgt` |
| `sfcWind` | **21** | **1990-2010** | **2020-2080** | `nex_gddp_cmip6_v2_wbgt` |

Required-variable intersection (`nex_required_intersection.csv`, `nex_roster.json`), computed from
disk — neither the "21 models" figure nor the shade release's roster of 19 was carried over:

| Scenario | complete model-years | models | year span |
|---|---:|---:|---|
| historical | 441 | 21 | 1990-2010 |
| ssp245 | **1280** | 21 | 2020-2080 |
| ssp585 | 1281 | 21 | 2020-2080 |

Excluded for missing `rsds`/`sfcWind` entirely: **BCC-CSM2-MR**, **NESM3**, **IITM-ESM** (which
additionally lacks `tasmax`/`tasmin` in the projections and has only 364 days in historical 1990).
One further gap: **KIOST-ESM ssp245 2058 is missing `hurs`** — the single model-year that makes
ssp245 1280 rather than 1281.

**Two hard coverage constraints follow, and both are product constraints, not bugs.** An outdoor
metric can cover **historical 1990-2010** and **projections 2020-2080** only. 2011-2014,
2015-2019 and 2081-2100 are unavailable at any roster size, because `rsds` and `sfcWind` were
never acquired for them. The shade metrics, built on `tas`/`tasmax`/`hurs`, are not so limited, so
an outdoor family would not align with the shade family's windows.

### 4.2 Tier 2 — metadata and calendar compatibility (`nex_header_scan.csv`, 134 rows, all OK)

Uniform and correct across all 21 models and all six variables for the sampled year: grid
128 × 120, latitude 6.125 – 37.875, longitude 68.125 – 97.875; units `K` for the three
temperatures, `%` for `hurs`, `W m-2` for `rsds`, `m s-1` for `sfcWind`; `frequency = day`;
member `r1i1p1f1`; **zero duplicate timestamps**.

Three findings:

1. **KACE-1-0-G uses a `360_day` calendar** (360 timesteps per year) and cannot satisfy a
   complete-365 annual policy. The **metadata-compatible outdoor roster is therefore 20, not 21.**
   This is the same exclusion the shade release made, reached independently here.
2. **KIOST-ESM labels its daily timestamps at 00:00 UTC**; the other 20 models label at 12:00 UTC.
   Mixing two day-label conventions in one ensemble is a real hazard for a metric whose whole
   point is a daily peak. Pending resolution, a defensible roster is **19**.
3. **No file publishes `time_bnds`.** The day boundary is therefore not recoverable from the local
   headers.

### 4.3 The NEX-day versus IST-civil-day mismatch — assessed, not assumed

The 12:00 UTC label with no published bounds is consistent with the CMIP6 convention of a 00-24
UTC day, which for India (UTC+05:30) spans **05:30 IST of day D to 05:30 IST of day D+1**. The
Indian afternoon peak of local day D falls inside that window, so `tasmax` and daily-mean `rsds`
for NEX day D do describe local day D's daytime — the mismatch is mild for this metric. But this
is **inference from the label plus the CMIP6 convention, not verification**: without `time_bnds`
it cannot be confirmed from the data on disk, and KIOST-ESM's 00:00 label may denote something
different again. This is an open item, and the six-site ERA5 experiment does not test it, because
that experiment defines its own IST day from hourly data.

### 4.4 Tier 3 — valid sampled six-site data (`nex_site_validity.csv`)

ACCESS-CM2 and MRI-ESM2-0, historical 1990-1999, all six sites, all six variables, 3,652 days
each: **zero non-finite values, zero out-of-range values, zero days with `tasmax < tas`, zero days
with `tasmin > tas`.** Ranges are physically plausible at every site (e.g. Bikaner `tasmax` to
47.1 °C, Shimla `tasmin` to −5.4 °C, `hurs` 6.7 – 99.5 %, `rsds` 56 – 402 W/m², `sfcWind`
0.46 – 7.5 m/s). This is a two-model sample, not a national audit.

### 4.5 The NEX distribution shift dominates the counts

Candidate C1 run on **NEX** daily inputs (`nex_candidate_distribution.csv`), against the
ERA5-driven audited reference. This is a **distribution** comparison. A NEX day is not an IST
civil day and NEX carries no weather for a particular ERA5 date, so no same-date difference and no
correlation between the two is computed, and none should be read as skill.

| Site | ERA5 ref annual mean (°C) | NEX C1 mean (°C) | ERA5 ref ≥32 (d/yr) | NEX C1 ≥32 (d/yr) |
|---|---:|---:|---:|---:|
| Kochi | 31.94 | 31.28 / 31.28 | 193.4 | 100.5 / 109.7 |
| Kolkata | 29.78 | 31.04 / 31.00 | 127.1 | 195.5 / 196.1 |
| Bikaner | 27.51 | 28.00 / 27.97 | 73.1 | 118.9 / 123.0 |
| Lucknow | 28.58 | 28.99 / 28.92 | 115.4 | 145.1 / 143.6 |
| Hyderabad | 28.59 | 28.95 / 29.01 | 39.5 | 34.9 / 42.5 |
| Shimla | 19.62 | 21.27 / 21.22 | 0.0 | 0.0 / 0.0 |

(ACCESS-CM2 / MRI-ESM2-0. NEX window 1990-1999; ERA5 window 1990-2004; NEX values are grid-cell
means, ERA5 values are the nearest 0.25° cell to a point. Both differences are stated rather than
corrected for.)

**The level transfers; the counts do not.** Annual means differ by −0.66 to +1.65 °C, but
`days_ge_32` differs by **−48 % at Kochi and +63 % at Bikaner, +54 % at Kolkata**. The
climate-model distribution difference is an order of magnitude larger than the reconstruction
error the six-site gate measures. This is P3 of the reconciliation document — the residual joint
distribution left over by variable-by-variable BCSD — and it means **passing the six-site count
gate would not by itself deliver correct national threshold counts.** Quantile delta mapping is
the named candidate remedy and was out of scope for this milestone, deliberately.

---

## 5. What remains uncertain

1. **The within-day wind shape.** The only term blocking the count gate. Both the constant
   treatment and the one declared diurnal shape miss, with opposite signs, at different regimes.
   The C2 shape is an assumed form with no observational fit claimed, and it must not be tuned to
   these sites.
2. **Threshold counts under the NEX distribution.** §4.5. Unaddressed, and larger than the
   reconstruction error.
3. **The elevation regime carries no count evidence.** §3.1 point 2.
4. **≥ 32 °C outside the four inland sites.** Kochi's reference (193 – 210 d/yr) and Shimla's
   (0 d/yr) bracket the useful range; the discriminating evidence is four sites wide.
5. **The NEX day boundary.** §4.3. Not verifiable from the local files.
6. **The reference is model-based, not observational.** Candidate and reference call the *same*
   Liljegren solver, so every score here isolates reconstruction error. Nothing in this milestone
   validates Liljegren against instruments, and the psychrometric-versus-natural wet bulb and
   Stull-approximation caveats of the reconciliation document's P4 still cap absolute accuracy for
   IRT and CarbonPlan alike.
7. **The sunrise/sunset straddling hour.** §2.3. Both reference identities approximate it.
8. **Transmissivity is held constant through the day** in the radiation reconstruction — the
   standard metsim/MTCLIM assumption, and the largest known weakness of an otherwise accurate
   driver (`A2` = 0.12 – 0.22 °C).

Where the failures come from, attributed:

| Source | Contribution |
|---|---|
| Reference conventions | Real and now corrected; contaminates tail statistics, moves counts by ≤ 4.9 d/yr |
| Reconstruction — wind shape | **The binding constraint on the count gate** |
| Reconstruction — temperature/humidity | Second largest, within the daily bar |
| Reconstruction — radiation | Small |
| Reconstruction — pressure | Negligible (≤ 0.022 °C) |
| Input availability | Roster 20 (or 19); historical 1990-2010, projections 2020-2080 only |
| Climate-model distribution | **Larger than all reconstruction error for threshold counts** |

---

## 6. Recommendation: REVISE

Feasibility is demonstrated for the level and for day-to-day accuracy, and a named, bounded set of
gaps blocks acceptance. Specifically:

- The daily gate **passes** at 12/12 site-window pairs, including the independent 1990-2004
  period, with bootstrap intervals clear of both bars.
- The count gate **fails** for both viable candidates, 2 of 30 gated pairs each, at ≥ 32 °C, with
  opposite signs at different regimes.
- Input coverage is sufficient in models (19-20) but **restricted in time** in a way that does not
  match the shade family.

This is not DEFER: the errors that were measured are small, the failing term is identified, and
nothing measured says the daily inputs are inadequate in principle. It is not PROCEED: a
predeclared gate failed, and converting that into a pass would be moving the goalposts.

### Smallest next experiment

**One measurement, no new data, no tuning.** Score two further *predeclared* within-day wind
treatments on the same six sites and both windows, chosen on physical grounds rather than fitted:

1. a shape whose amplitude is set by the day's own diurnal temperature range (convective mixing
   scales with surface heating, which is what distinguishes Bikaner from Hyderabad), and
2. a shape derived from the ERA5 climatological hourly wind cycle by site-month, which is static
   information a production path could ship and is therefore **not** leakage — provided it is
   fitted on a period disjoint from the evaluation window and that is stated.

Acceptance: the existing frozen gates, unchanged. Cost: about 25 minutes of compute on the cache
already on disk, at `--workers 1`. If a predeclared shape clears the count gate at all 30 gated
pairs across both windows, that is PROCEED for the six-site evidence base; if none does, the
defensible reading is that the daily inputs support an outdoor **level and ranking** product but
not outdoor **threshold counts**, which is a publishable conclusion in its own right and matches
what §4.5 says about the NEX distribution.

### Is production integration justified?

**Not yet, and not on this evidence alone.** Three things must land first, in order:

1. The wind-shape experiment above.
2. A decision on threshold counts in light of §4.5 — because a reconstruction that passes the
   six-site count gate would still not deliver correct national counts without bias correction,
   and the eight WBGT slugs are display-only diagnostics that move no composite score
   (`metrics_registry.py` declares them so; there is no WBGT frozen ruler).
3. An explicit ruling on the time-coverage mismatch: an outdoor family limited to historical
   1990-2010 and projections 2020-2080 sitting beside a shade family with wider windows is a
   product decision, not an engineering detail.

Nothing in this milestone changed a production metric, rebuilt a national output, installed a
dependency, published an artifact, altered a composite weight or applied any bias correction. The
running national shade build (`scratch/wbgt_shade_national`) was untouched; the tool refuses to
start if its output paths resolve inside `irt_data`, `processed_optimised` or a shade stage.

---

## 7. Artifacts

| File | Contents |
|---|---|
| `SPEC.md` | The frozen contract (CHG-0602) |
| `README.md` | This report |
| `run_manifest.json` | Git snapshot, package versions, method signatures, gates, exact command |
| `legacy_reproduction.json` | Bit-for-bit parity against the committed historical series |
| `reference_audit.csv` | Legacy vs audited by site, season, daily maximum and annual count |
| `reference_low_sun_audit.csv` | Frequency and magnitude of the low-sun beam amplification |
| `reference_low_sun_worst_hours.csv` | The ten worst hours per site-window, with all drivers |
| `candidate_scores.csv` | 720 rows: every candidate/ablation/baseline × site × window × season |
| `annual_counts.csv` | 576 rows: annual means, counts, signed/absolute errors, count gate |
| `uncertainty.csv` | Year-block bootstrap intervals, 1,000 draws, seed 20260929 |
| `reconstruction_consistency.csv` | Energy conservation, night zeros, input-residual diagnostics |
| `site_diagnostics.json` | Per site-window NaN rates, pressures, runtime, method signatures |
| `gate_verdicts.json` | Both predeclared gates applied |
| `daily_series.parquet` | Every daily maximum series, all identities, both windows |
| `nex_files_present.csv` | Tier 1, 27,713 rows |
| `nex_required_intersection.csv` | Complete/incomplete model-years |
| `nex_roster.json` | Required-variable roster computed from disk |
| `nex_header_scan.csv` | Tier 2, bounded header sample |
| `nex_site_validity.csv` | Tier 3, sampled six-site validity |
| `nex_candidate_distribution.csv` | NEX-driven candidate distribution (distribution comparison only) |

Bulky intermediates stay outside this tree, under `scratch/wbgt_outdoor_feasibility/`
(git-ignored): per-site daily parquets, provenance-stamped diagnostics for `--resume`, and the
NEX-driven daily maxima.

### Runtime and memory

Per site-window, single worker, on a machine concurrently running the national shade build:
**242 – 278 s** for 1990-2004 (5,478 local days, 131,472 hours) and **50 – 70 s** for 2005-2014
(3,651 days). Total experiment wall time **1,921 s** across 12 site-windows, plus about 6 min for
the low-sun audit, 22 s for the reconstruction-consistency stage and about 7 min for the NEX
inventory including the two-model sampled read. Peak memory stayed within an ordinary process:
each site-window holds a 131k-row hourly frame and ten solver outputs of the same length, on the
order of 100 MB. Cost scales with the Liljegren fixed-point solver, which is called once per
identity per site-window.

### Reproduction

```bash
python -m tools.diagnostics.wbgt_outdoor_feasibility --self-test
python -m tools.diagnostics.wbgt_outdoor_feasibility --dry-run
python -m tools.diagnostics.wbgt_outdoor_feasibility --stage all --nex-sample-models ACCESS-CM2,MRI-ESM2-0 --nex-sample-years 1990-1999
python -m pytest tests/test_wbgt_outdoor_feasibility.py -q
```

Reruns are free with `--resume`: per-site daily results are cached under `--work-dir` with the
reference signature stamped in, and a stale stamp triggers recomputation rather than silent reuse.
