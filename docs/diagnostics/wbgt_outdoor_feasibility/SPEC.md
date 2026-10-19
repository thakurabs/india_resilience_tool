# Frozen specification — outdoor-WBGT reconstruction feasibility (milestone 1)

**Status:** FROZEN 2026-09-29 before any candidate score was computed.
**Snapshot at freeze:** `GIT:add_flood_depth@9b38565`, working tree dirty (untracked diagnostics only).
**Change IDs:** CHG-0602 (this file), CHG-0603 (tool), CHG-0604 (tests).

This file is the contract. Anything measured after it was written is reported against these
definitions, not against definitions chosen once the numbers were visible. Where the contract
turned out to be unachievable, the README says so explicitly rather than editing this file.

---

## 1. Question

> Can the daily climate inputs available to IRT support a sufficiently accurate estimate of
> daily maximum **open-sky** WBGT, using a physical WBGT calculation and a defensible
> within-day reconstruction?

This is Step 3 of `docs/diagnostics/wbgt_reconciliation/README.md` §4, with the linear
shade→sun adjustment permanently retired (CHG-0567) and with no bias correction (QDM is
explicitly out of scope for this milestone).

## 2. Target quantity

| Property | Value |
|---|---|
| Quantity | Open-sky (outdoor, solar-loaded) WBGT |
| Unit | degrees Celsius |
| Temporal primitive | **daily maximum of hourly physical WBGT** |
| Annual level statistic | annual mean of valid daily maxima |
| Annual count statistics | annual counts of daily maxima >= 28, >= 30, >= 32 degC |

The daily maximum is taken **after** WBGT is evaluated at every reconstructed hour. Taking
independently selected maxima of temperature, humidity and radiation and then evaluating WBGT
once is forbidden and is not implemented.

## 3. Sites

The six established sites (`tools/diagnostics/wbgt_method_validation.SITES`, reused unchanged):

| Site | lat | lon | elevation (m) | regime |
|---|---:|---:|---:|---|
| Kochi | 9.93 | 76.27 | 3 | hot-humid coastal |
| Kolkata | 22.57 | 88.36 | 9 | monsoon delta |
| Bikaner | 28.02 | 73.31 | 242 | hot-dry Thar |
| Lucknow | 26.85 | 80.95 | 123 | Gangetic pre-monsoon |
| Hyderabad | 17.39 | 78.49 | 505 | semi-arid inland |
| Shimla | 31.10 | 77.17 | 2276 | elevation |

Elevation is the **site** elevation from that table and is used for the barometric pressure
assumption. The ERA5 grid-cell elevation returned by the archive API is recorded in the run
manifest where present but is not substituted silently.

## 4. Evaluation periods

| Period | Role |
|---|---|
| **1990-2004** | **Primary, independent.** No candidate parameter was chosen by looking at it. |
| 2005-2014 | Continuity window. Every published six-site number in `wbgt_reconciliation` §1.1 and `wbgt_tier2_probe` uses it. |

Both windows are available in full for all six sites in the existing hourly ERA5 cache
`scratch/wbgt_deployed_vs_reference_cache` (150 parquet files, 1990-2014, verified complete and
non-null on 2026-09-29). No download is required and none is performed.

Seasons, for seasonal breakdowns: DJF, MAM, JJAS (Indian monsoon), ON.

## 5. Calendar, day boundary and time zone

- The hourly ERA5 cache is indexed in **UTC**.
- Days are **India Standard Time civil days** (UTC+05:30). A local day owns the 24 UTC hours
  whose IST-shifted timestamp falls on that date.
- A local day is used only if **all 24 hours are present**. Partial boundary days at each end of
  the record are dropped, never padded. This matches the existing harness
  (`build_daily_frame`, `counts == 24`).
- Two policies are kept separate and both reported:
  - **Reproduction policy** — retain 29 February; score every day that has 24 hours. Used only to
    reproduce the legacy 2005-2014 experiment bit-for-bit.
  - **Evaluation policy** — drop 29 February; an annual count is emitted only for a year with
    365 valid daily maxima; incomplete years are excluded and listed. This matches the shipped
    shade contract (`SHADE_METHOD_SIGNATURE`: `complete-365`, `drop-feb29`).
- Solar geometry is always computed on **UTC** timestamps with the site longitude. The IST offset
  never enters the physics.

## 6. Units and driver conventions

### 6.1 Required by `thermofeel.calculate_wbgt_liljegren` (v2.3.0, read from source)

| Argument | Unit | Note |
|---|---|---|
| `t2_k` | K | |
| `rh` | percent | converted to a fraction internally |
| `pressure` | hPa | |
| `va` | m/s at **10 m** | see 6.2 |
| `ssrd` | W/m2, **instantaneous** | not an accumulation; a J/m2 hourly accumulation would be wrong by 3600 |
| `fdir` | dimensionless fraction of `ssrd` that is direct beam **on the horizontal plane**, 0-1 | clamped internally to [0, 0.9] and set to 0 when `cossza < 0.00873` |
| `cossza` | dimensionless, cosine of solar zenith | |

Return value is **K**; the caller subtracts 273.15.

### 6.2 Wind measurement height — resolved, no second conversion

`calculate_wbgt_liljegren` takes the **10 m** wind and converts internally:

```
va    = max(va, 0.62)                      # MIN_WIND_10M, KNMI 10 m floor
speed = wind_speed_2m(va, cossza, ssrd)    # Liljegren/Pasquill-Gifford power law, floored at 0.13 m/s
```

(`thermofeel/thermofeel.py` lines 743-805 and `thermofeel/liljegren.py` lines 244-276, plus the
public docstring "`:param va:` wind speed at 10 metres [m/s]".)

Therefore `tools/diagnostics/wbgt_deployed_vs_reference.py` passing `wind_speed_10m` is
**correct**, and applying a log-law 10 m -> 2 m conversion before the call would double-count.
`docs/diagnostics/wbgt_reconciliation/README.md` §2 P2 ("`sfcWind` is a 10 m value; ISO/Liljegren
want ~2 m. A log-law conversion is needed") is therefore **wrong for this implementation** and is
corrected in the README of this milestone. The `wind_scaling="brode"` generic log profile is
retained as a declared **sensitivity**, not a correction.

This is documented library behaviour read from the installed source, not an assumption.

### 6.3 Open-Meteo archive interval conventions (primary documentation)

From the Open-Meteo historical-weather API documentation:

- `shortwave_radiation`: "Shortwave solar radiation as average of the preceding hour."
- `direct_radiation`: "Direct solar radiation as average of the preceding hour on the horizontal
  plane and the normal plane." The horizontal-plane series is the one returned under this name;
  `direct_normal_irradiance` is a different variable and is not used.
- `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `surface_pressure`: "Instant".

So the value labelled hour *H* for radiation is the mean over **[H-1h, H]**, while the
thermodynamic drivers labelled *H* are instantaneous at *H*. This is a 30-minute phase offset
between the radiation and the solar geometry it is paired with, and it is the basis of the
reference audit in section 7.

`fdir = direct_radiation / shortwave_radiation` is dimensionally correct for `thermofeel`
because both are horizontal-plane fluxes and the solver applies the
`fdir * (1/(2*cos z) - 1)` projection itself.

### 6.4 Pressure

- **Reference (ERA5):** the cached `surface_pressure` in hPa, used as given.
- **Candidate (daily-input path):** NEX publishes **no** `ps` for any of the 21 models carrying
  `rsds`/`sfcWind` (verified on disk). Pressure is therefore the ISA barometric value from the
  **site elevation**:
  `p(z) = 1013.25 * (1 - 2.25577e-5 * z)^5.25588` hPa.
  This is an explicitly justified elevation-based alternative, declared here in advance, and its
  magnitude is measured as a sensitivity (section 9).

## 7. Reference identities

Two reference identities exist. Neither overwrites the other, and historical evidence is not
modified.

| Identity | Definition |
|---|---|
| **A. Legacy reference** | Exactly `tools/diagnostics/wbgt_deployed_vs_reference.py` as shipped: `cossza` evaluated at the labelled UTC hour, paired with the preceding-hour-mean radiation. Reproduced unchanged for continuity. Signature `liljegren-legacy-v1:cossza-at-label`. |
| **B. Audited reference** | Identical except `cossza` is evaluated at the **midpoint of the radiation interval**, `H - 30 min`, which is the instant the preceding-hour-mean flux represents. Thermodynamic drivers stay at the instant `H`. Signature `liljegren-audited-v1:cossza-at-radiation-interval-midpoint`. |

Everything else is shared: thermofeel 2.3.0 `calculate_wbgt_liljegren`, `wind_scaling="liljegren"`,
10 m wind passed unconverted, ERA5 `surface_pressure`, `fdir` from the horizontal direct/global
ratio clipped to [0, 1] before the library's own [0, 0.9] clamp.

**B is the reference for every candidate score.** A exists to quantify what the correction is
worth and to keep the legacy tables reproducible. The legacy-vs-audited difference is reported by
site, season, daily maximum and annual threshold count.

Both are **model-based references**, not observational ground truth. Because candidate and
reference call the *same* physical solver, a candidate score isolates **reconstruction error**. It
does not validate Liljegren against instruments, and no claim of that kind is made.

### 7.1 Solver behaviour, audited from source (`thermofeel/liljegren.py`)

- Globe and natural-wet-bulb temperatures are each solved by damped fixed-point iteration
  (`0.9*prev + 0.1*new`), tolerance `CONVERGENCE = 0.02` K, cap `MAX_ITER = 500`. Non-convergence
  returns NaN.
- NaN inputs never converge, so they return NaN. Invalid input therefore stays invalid and can
  never become a zero-exceedance day. This is asserted in the test suite.
- Low wind: floors at 0.62 m/s (10 m) and 0.13 m/s (2 m, inside the Reynolds number).
- `CZA_MIN = 0.00873` (cos 89.5 deg) is the sun-down threshold; `fdir` is zeroed below it.
- Natural wet bulb uses `rad=1`; the psychrometric wet bulb (`rad=0`) is not used here.
- Pressure enters air density, diffusivity and the `(pair - ewick)` denominator, so elevation
  matters physically and not only cosmetically.

## 8. Candidate set — predeclared, three members

Reconstruction may read **only**: the daily inputs of section 8.1, static site information
(latitude, longitude, elevation) and the declared parameters below. Access to actual hourly
weather from inside a candidate is a leakage bug and is blocked by construction and by test.

### 8.1 Daily inputs

Formed by aggregating the cached hourly ERA5 over the **IST local day**, so the experiment
measures reconstruction error with climate-model error removed:

| Daily input | Definition | NEX counterpart |
|---|---|---|
| `tas` | mean of `temperature_2m` | `tas` |
| `tasmin` | min of `temperature_2m` | `tasmin` |
| `tasmax` | max of `temperature_2m` | `tasmax` |
| `hurs` | mean of `relative_humidity_2m` | `hurs` |
| `rsds` | mean of `shortwave_radiation` | `rsds` |
| `sfcWind` | mean of `wind_speed_10m` | `sfcWind` |

No pressure input: see 6.4.

### 8.2 Shared reconstruction elements

**Hourly grid.** The candidate reconstructs on the *same* 24 UTC timestamps the reference uses for
that local day, under the *same* interval convention (section 6.3): thermodynamic fields
instantaneous at `H`, radiation the mean over `[H-1h, H]`, `cossza` at `H - 30 min`. Convention is
therefore identical on both sides and cannot masquerade as reconstruction skill.

**Temperature — Parton and Logan (1981).** Daylight: a sine from `tasmin` at sunrise to `tasmax`
lagged `a = 1.86 h` after solar noon,
`T = tasmin + (tasmax - tasmin) * sin(pi * (t - t_rise) / (DL + 2a))` on `[t_rise, t_set]`.
Night: exponential relaxation from `T(t_set)` toward the *next* day's `tasmin`,
`T = tasmin_next + (T_set - tasmin_next) * exp(-b * dt / Z)`, `b = 2.2`, `Z` the night length.
Sunrise and sunset come from the site's solar geometry. The scheme is driven by `tasmin`/`tasmax`
because the target is a daily *peak*; the 24-hour mean of the result therefore need not equal the
supplied `tas`. **That residual is measured and reported, not hidden**, as
`recon_tas_minus_input_tas_c`. No offset is applied to force agreement, because doing so would
break the extremes the target depends on.

**Humidity — constant vapour pressure (baseline).** `e_day = (hurs/100) * es(tas)` with the Magnus
form already shipped in `heat_stress_gridfirst.py` (6.112 / 17.62 / 243.12). Then
`RH(t) = 100 * e_day / es(T(t))`, clipped to [0, 100]; clip counts are reported. Vapour pressure is
the conserved invariant because `e` is far more stable through the day than `RH`. The preservation
diagnostic `recon_hurs_minus_input_hurs_pct` (hourly mean of reconstructed RH minus the supplied
daily-mean `hurs`) is reported per site and season; it is expected to be non-zero because RH is
nonlinear in `T`.

**Radiation.** The top-of-atmosphere horizontal flux
`I_toa(t) = S0 * E0(doy) * max(cossza(t), 0)`, `S0 = 1361 W/m2`, `E0` the standard
eccentricity factor, is integrated over each `[H-1h, H]` interval by 10-minute sub-sampling to
give an interval-mean shape, then the whole day's 24 interval means are multiplied by one scalar
so their mean equals the daily input `rsds`. Consequences, all asserted in tests:
- night-time shortwave is exactly 0,
- daytime values are non-negative,
- the reconstructed daily mean reproduces the input `rsds` to a relative tolerance of **1e-9**.
Days with zero insolation shape return all zeros rather than dividing by zero. Transmissivity is
implicitly constant through the day; this is the standard metsim/MTCLIM assumption and is the
largest known weakness of the radiation reconstruction.

**Direct/diffuse partition — Erbs et al. (1982)** on the hourly clearness index
`kt = I / I_toa` (interval means, `I_toa > 0`):
`kt <= 0.22`: `Kd = 1 - 0.09 kt`;
`0.22 < kt <= 0.80`: `Kd = 0.9511 - 0.1604 kt + 4.388 kt^2 - 16.638 kt^3 + 12.336 kt^4`;
`kt > 0.80`: `Kd = 0.165`. Then `fdir = clip(1 - Kd, 0, 1)`, a horizontal-plane direct fraction,
which is what section 6.3 established the solver wants.

**Wind.** `sfcWind` is a **daily mean at 10 m**. A daily mean does not imply a known hourly wind.
The baseline treatment is therefore explicitly labelled **"wind held constant at the daily mean"**,
not "hourly wind". The library's own 10 m floor and 10 m -> 2 m profile then apply.

**Pressure.** ISA barometric from site elevation (6.4), constant through the day.

### 8.3 The three candidates

| Id | Name | Differs from C1 by |
|---|---|---|
| **C1** | `recon-baseline` | — (the defensible baseline defined in 8.2) |
| **C2** | `recon-wind-diurnal` | Wind given a mean-preserving diurnal shape, `w(t) = wbar * (1 + 0.4 * s(t))` where `s` is the day's normalised `cossza` profile recentred to zero mean, so daytime wind exceeds the daily mean and night-time wind falls below it. Addresses the largest acknowledged reconstruction unknown: convective mixing is strongest at the hour WBGT peaks, and wind is the dominant control on both `Tg` and `Tnw`. The shape is an **assumed** form; no observational fit is claimed. |
| **C3** | `recon-constant-rh` | Humidity conserved as constant **relative** humidity (`RH(t) = hurs`) instead of constant vapour pressure. Tests whether the moisture invariant matters, which is the assumption CarbonPlan's `RH`-at-`tasmax` route and IRT's shade fix both turn on. |

No other variants are scored as candidates. **No coefficient search of any kind is performed**:
`a`, `b`, `S0`, the Erbs coefficients, the 0.4 wind amplitude and the ISA constants are all fixed
literature or declared values, and none is tuned against any site, season or period.

## 9. Ablations and sensitivities — diagnostic only, never deployable

Each row replaces one driver group with its reconstruction and leaves the rest at the actual
hourly ERA5 values. Rows marked *oracle* consume hourly weather that the production path would
not have and are **not** candidates.

| Id | Reconstructed | Actual hourly | Kind |
|---|---|---|---|
| `A0` | — | all | audited reference (identity) |
| `A1` | temperature + humidity | rsds, fdir, wind, pressure | oracle ablation |
| `A2` | radiation + fdir | T, RH, wind, pressure | oracle ablation |
| `A3` | wind (daily-mean constant) | T, RH, rsds, fdir, pressure | oracle ablation |
| `A4` | pressure (ISA barometric) | T, RH, rsds, fdir, wind | oracle ablation |
| `C1` | everything | — | deployable candidate |

Sensitivities: `wind_scaling="brode"` versus `"liljegren"` on the audited reference
(wind-height treatment), and ISA barometric versus ERA5 `surface_pressure` on the audited
reference (`A4` supplies this).

Comparison baselines, clearly labelled and not candidates:
- `empirical sWBGT, as deployed` — `0.567*Ta + 0.393*e + 3.94` on daily-mean `tas`/`hurs`.
- `shade Stull at tasmax + RH-at-tasmax` — the shipped shade metric's daily-peak form.
- `Tier-2 (shade + linear sun adjustment)` — the retired chain, for continuity with
  `wbgt_tier2_probe`.

## 10. Acceptance criteria — set before any candidate result was inspected

### 10.1 Daily-error gate (continuity with the earlier harness)

Per **site x window**, on matched local days, against reference B:

- median absolute daily error `< 1.0 degC`
- RMSE `< 1.5 degC`

Interpretation verified against `tools/diagnostics/wbgt_deployed_vs_reference.py:compare_series`:
`median_abs_bias_c = median(|candidate - reference|)` and `rmse_c = sqrt(mean((candidate -
reference)^2))`, both over paired non-null days. These definitions are carried over unchanged; the
metric has **not** been redefined.

Evaluated **separately for each site**, i.e. per regime. No pooling across sites, and no
warm-day subsetting (the earlier Stage A harness restricted to warm days; that restriction is
**not** applied here, and any comparison to Stage A numbers must say so).

### 10.2 Count-acceptance policy — separate gate, predeclared

Daily-error gates do **not** establish threshold-count validity, so counts have their own rule.
Per site x threshold x window, using mean annual count over the window's complete years:

- **Gated** when the reference mean annual count `>= 5.0 days/year`. Pass requires
  `|candidate - reference| <= max(0.20 * reference, 2.0 days/year)`. The absolute floor of
  2 days/year exists because a pure relative band on a small base is noise.
- **Not gated** when the reference mean annual count `< 5.0 days/year`. These pairs are reported
  with **absolute** signed and unsigned errors and are explicitly marked `NOT GATED (rare event)`.
  They are never counted as passes and never as failures.
- Relative error is left blank, not zero, where the reference is 0.

A candidate passes the count gate only if **every gated pair passes**. The number of gated pairs
is reported, because a gate that can only judge two pairs has said little.

### 10.3 Reported quantities

Per site x season x window, and pooled over seasons:

n available days, n valid days, mean bias, median absolute daily error, RMSE, Pearson r
(secondary), quantile differences at p50/p90/p95/p99 (candidate quantile minus reference
quantile), **and separately** the mean error conditional on the reference's hottest 1 % of days.
Those last two are different statistics and are reported under different names
(`q99_difference_c` and `cond_mean_error_hottest_1pct_c`); the legacy column `p99_bias_c` is the
conditional one, and the README says so.

Annual: mean annual maximum-WBGT level, annual counts at 28/30/32, signed and absolute count
errors, missing/invalid-day accounting with reasons.

Uncertainty: **year-block bootstrap**, resampling whole years with replacement, 1000 draws,
seed 20260929. Daily samples are never treated as independent. Intervals describe temporal
sampling uncertainty only — not reference-method and not climate-model uncertainty.

## 11. NEX daily-input inventory

Read-only. Three separately reported tiers, never conflated:

1. **Files present** — filesystem enumeration, variable x model x scenario x year.
2. **Metadata/calendar compatible** — header scan: units, calendar, time coverage, duplicate
   timestamps, grid shape and coordinates, model/member/scenario identity.
3. **Valid sampled six-site data** — the actual six grid points, checked for non-finite and
   physically invalid values.

Tier 2 is a header scan on a bounded sample of years and is **not** a national data-quality audit;
the README states this. Tier 3 is bounded to the models and years named on the command line.

The required-variable intersection is computed from disk, not assumed. Neither the "21 models"
figure nor the shade release roster of 19 is carried over as the outdoor roster without
verification. Historical and future coverage are reported separately because `rsds`/`sfcWind`
cover 1990-2010 historical and 2020-2080 projected, while `tas`/`tasmax`/`hurs` cover 1951-2014.

NEX daily time semantics are inspected, not assumed, and the NEX-day versus IST-civil-day mismatch
is assessed explicitly. A NEX-driven comparison is a **distribution** comparison: same-date
NEX-versus-ERA5 differences are not forecast skill and no correlation between them is interpreted
as such.

## 12. Out of scope for this milestone

Production metric changes; national outdoor builds; publishing; new dependencies; composite
weights; quantile delta mapping or any other bias correction; anything that writes under
`IRT_DATA_DIR` or under the running shade stage `scratch/wbgt_shade_national`.

## 13. Verdict vocabulary

- **PROCEED** — a candidate passes 10.1 and 10.2 across the required domains with sufficient
  input coverage.
- **REVISE** — promising, but a named reconstruction assumption or validation gap blocks
  acceptance.
- **DEFER** — the available daily inputs or the measured errors do not support the product at the
  required accuracy.

"Not evaluated" is reported as not evaluated. It is never converted into a pass or a fail.
