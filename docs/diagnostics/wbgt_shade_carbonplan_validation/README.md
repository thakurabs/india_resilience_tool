# CarbonPlan shade WBGT: held-out validation (CHG-0645..CHG-0648)

**What this is.** A bounded scientific validation of CarbonPlan's shade-WBGT
pipeline (notebooks 02 → 05 → 06, commit `f662b372`). It informs whether IRT
should write a national SPEC to replace `shade-peak-v1`. It answers three
separate questions:

1. **Fidelity:** does IRT reproduce CarbonPlan?
2. **Temporal transfer:** does a correction trained on 1985–1999 improve
   2000–2014 shade estimates against UHE-Daily?
3. **Product suitability:** what happens when results are carried to IRT
   districts and blocks?

The contract is [`SPEC.md`](SPEC.md), frozen before any score. Its dated
amendments A1–A5 change no scoring or decision rule. Nothing here is a
production change; no method is selected or promoted.

**Recommendation: PROCEED CONDITIONALLY to a national SPEC.** This is the
outcome of the predeclared rule (SPEC §9):
- Fidelity gates G1, G2 and G3 pass, but the claim is narrow. Published
  threshold-count reproduction passes **for ACCESS-CM2 in Kerala, under the
  declared numerical exception rules**. G3 leaves 118 and 116 future-window
  days with residuals above 1e-3 °C unattributed (amendment A4); none of them
  flips a count.
- In the primary experiment (Kerala, ACCESS-CM2, population weights), held-out
  QDM improves every supported primary metric.
- But days ≥32 °C **worsen** in Rajasthan for both models, and p99 is not
  improved in 3 of 6 state × model cases. Rajasthan is the only tested state
  with widespread support at ≥32 °C: 300 of 479 units, against 5 of 108 in
  Himachal and none in Kerala.
- **The spatial overlap check FAILED** (review correction, CHG-0655). Section 4
  explains the failure and which admin values are provisional.
- So the *proceed* rule ("no primary family worsens anywhere") is not met.
  Neither is the *do-not-proceed* rule. The conditions are listed at the end.

## Evidence framing (carried from the brief)

- The earlier Kochi "57 raw / 208 corrected / 212 reference days" result was
  **outdoor** WBGT against ERA5-Liljegren. It was already a temporal holdout,
  and it is not a shade/UHE validation.
- The CHG-0643 Kerala population reproduction matched counts exactly in 141 of
  149 units. Each of the other eight was off by one day at one threshold over
  30 years. This run reproduces that outcome exactly (below).
- UHE-Daily (Tuholske et al. 2021, aggregated by CarbonPlan per unit) is an
  **observation-informed derived** WBGT reference, not instrument WBGT.
- "Raw" means **no WBGT-level QDM**. The NEX inputs are already BCSD-adjusted.
- Historical model years are not synchronised with observed weather. Daily
  paired RMSE and correlation are diagnostic only, never criteria.
- A good historical holdout is not proof of future accuracy.

## 1. Implementation fidelity: PASS (ACCESS-CM2, Kerala only, under the declared numerical exception rules)

`results/fidelity/` (Kerala, ACCESS-CM2, 149 units):

| Gate | Window | Units with exact ≥28/30/32 counts | Flip days | Unexplained | Max abs daily residual | Verdict |
|---|---|---|---|---|---|---|
| G1 raw parity, pop + area | 1985–2014 | — | — | — | **0.0 °C** (all 12 holdout runs too) | PASS |
| G2 historical | 1985–2014 | **141 / 149** | 10 | 0 | 0.753 °C | PASS |
| G3 SSP2-4.5 | 2020–2039 | 146 / 149 | 7 | 0 | 0.514 °C | PASS |
| G3 SSP2-4.5 | 2040–2059 | 144 / 149 | 7 | 0 | 1.710 °C | PASS |

- **Overlap:** 10,957 historical and 7,305 future finite days per unit, with
  no gaps.
- **Residual distribution (G2):** median 9.5e-6 °C; p99 4.8e-5 °C; p99.9
  6.5e-5 °C.
- **Every one of the 216 G2 days above 1e-3 °C is a QDM tie pair.** Such a pair
  is two days of one unit, within ±15 day-of-year, whose raw values differ by
  ≤1e-4 °C and whose residuals are equal and opposite.
- **Every threshold flip is listed** in `g2_g3_threshold_flips.csv`, with both
  values' distances from the threshold:
  - 16 of the 24 flips are input-version noise: both values lie within
    5e-5 °C of the threshold, e.g. 27.99995 vs 28.00002.
  - The other 8 belong to tie pairs, e.g. unit 20395 on 2042-03-26 and 2050-03-26
    (±1.19 / ∓0.52 °C).
- The future windows were reproduced with **two separate adjustment calls**, as
  notebook 06 does.
- Counts use inclusive `>=` on unrounded values. Tolerances only attribute
  residuals.
- Published-product reproduction is **not** independent validation.

### Application windows matter (AW diagnostic)

`results/fidelity/aw_window_sensitivity.csv`, three fixed units. The same
trained QDM, applied over a different window, changes almost every value on
the overlapping dates:

| Comparison (same trained model) | Days changed > 1e-6 °C | Mean abs | Max abs | Count change over the window |
|---|---|---|---|---|
| 2020–2039 alone vs inside one 2020–2059 call | 6791–6864 of 7305 | 0.16–0.28 °C | 1.27–1.66 °C | up to +95 days ≥30 °C |
| 2020–2039 alone vs inside one 2020–2080 call | 6939–6945 of 7305 | 0.24–0.45 °C | 1.44–2.10 °C | up to +151 days ≥30 °C |
| 2040–2059 alone vs inside 2020–2059 / 2020–2080 | ~6800–6870 of 7305 | 0.12–0.31 °C | 2.2–3.4 °C | −79 to +77 |
| 2000–2014 one call vs 2000–2007 + 2008–2014 | ~4100 of 5479 | 0.15–0.20 °C | 2.5–3.3 °C | −4 to +7 |

The cause is that xclim's QDM ranks each value within the series being
adjusted. So the application window is part of the method.

A national SPEC must freeze:
- the windows (CarbonPlan's 2020–2039 / 2040–2059);
- the training period (1985–2014); and
- a rule that adding a horizon (e.g. 2060–2080) is a **new, separately
  adjusted window**, never a re-run over a longer span.

Otherwise extending the horizon silently rewrites values already published.
No window was chosen by score.

## 2. Temporal transfer: held-out 2000–2014 vs UHE-Daily

Train 1985–1999 (UHE and model), apply 2000–2014 in one call, IRT reporting
calendar (Feb 29 dropped, 15 complete 365-day years). Population weights. No
unit or unit-year was excluded in any run (`holdout_exclusions.csv` is empty):
UHE was finite on every day for all 736 units, and every model input passed
the inventory. Each cell below is the across-unit median |error|, raw → QDM,
then the screening verdict (*impr* = improves, *mixed* =
mixed/inconclusive, *WORSE* = worsens). Full rows, with bootstrap intervals
and shares, are in `results/screening_summary.csv`.

| State · model (units) | Seasonal °C | p90 °C | p95 °C | p99 °C | ≥28 d/yr | ≥30 d/yr | ≥32 d/yr |
|---|---|---|---|---|---|---|---|
| Kerala · ACCESS-CM2 (149) **primary** | 0.80→0.17 impr | 0.89→0.10 impr | 0.80→0.09 impr | 0.64→0.18 impr | 42.1→5.4 impr | 18.3→3.5 impr | 0.07→0.40 mixed† |
| Kerala · CMCC-CM2-SR5 (149) | 0.74→0.32 impr | 0.76→0.44 mixed | 0.67→0.48 mixed | 0.79→0.72 mixed | 38.1→18.7 impr | 10.3→14.8 mixed | 1.03→1.77 mixed† |
| Rajasthan · ACCESS-CM2 (479) | 2.01→0.35 impr | 0.71→0.31 impr | 0.55→0.44 mixed | 0.35→0.48 mixed | 32.2→3.9 impr | 23.6→6.7 impr | **1.4→5.3 WORSE** |
| Rajasthan · CMCC-CM2-SR5 (479) | 2.02→0.30 impr | 0.90→0.39 impr | 0.90→0.53 impr | 0.66→0.82 mixed | 31.2→6.8 impr | 30.2→10.7 impr | **1.3→6.4 WORSE** |
| Himachal Pradesh · ACCESS-CM2 (108) | 1.41→0.40 impr | 0.73→0.13 impr | 0.61→0.27 impr | 0.71→0.32 impr | 4.2→0.5 mixed‡ | 1.0→0.4 mixed‡ | 0.4→0.3 mixed‡ |
| Himachal Pradesh · CMCC-CM2-SR5 (108) | 1.47→0.38 impr | 0.81→0.38 impr | 0.68→0.38 impr | 0.71→0.33 impr | 11.4→2.2 impr | 2.6→2.1 mixed‡ | 0.47→0.07 mixed‡ |

† No Kerala unit has reference support at ≥32 °C (116 zero, 33 sparse, 0
supported). QDM adds about 0.4 d/yr (ACCESS) or 1.8 d/yr (CMCC) where UHE has
almost none, so this is not a supported-threshold verdict.
‡ Himachal thresholds are mostly zero/zero: at ≥28, 58 of 108 units have no
reference day. Few units are classified, so the share criterion fails even
where median errors fall. That is a support limit, not evidence of harm.

**Event support** (reference days in 2000–2014; full counts in
`screening_summary.csv`):

| State | ≥28 °C supported units | ≥30 °C supported | ≥32 °C supported |
|---|---|---|---|
| Kerala | 133 / 149 | 123 / 149 | 0 / 149 |
| Rajasthan | 479 / 479 | 476 / 479 | 300 / 479 (median 2.2 d/yr) |
| Himachal Pradesh | 42 / 108 | 26 / 108 | 5 / 108 |

**Where it improves.**
- The seasonal cycle improves in every state and model; the median monthly
  bias falls 2.0 → 0.3 °C in Rajasthan.
- Counts at ≥28 and ≥30 °C improve wherever they are supported, except Kerala
  CMCC at ≥30.
- In the primary Kerala ACCESS-CM2 case, every interval lies well below zero.
  For example, the change in median |error| for ≥28 °C counts is −39.0 to
  −29.3 d/yr (90%).

**Where it worsens or stalls.**
- **Rajasthan ≥32 °C, both models:** errors go from −0.2/−1.3 d/yr signed raw
  to **+5.3/+6.4 d/yr** after QDM. 78–85% of units worsen, and the intervals
  lie entirely above zero.
- **p99:** QDM overshoots the upper tail in Rajasthan (median signed +0.48 /
  +0.82 °C) and Kerala CMCC (+0.72 °C). The Kerala ACCESS p99 regressions
  cluster on the central coast (Alappuzha, Ernakulam, Kottayam: +0.25 to
  +0.4 °C).
- **Kerala ≥32 °C regressions** concentrate in Thrissur: all 11 of its units
  worsen.
- **Kerala CMCC-CM2-SR5** is the weakest case: ≥30 °C goes from 10.3 to
  14.8 d/yr error, and tails are mixed.
- **Seasonal regressions** (median |monthly bias| up by more than 0.05 °C):
  - Himachal March (ACCESS 0.56 → 1.15 °C; CMCC 0.63 → 0.70);
  - Kerala CMCC May (0.60 → 0.71).
  - Residual QDM biases of at least 0.5 °C remain in Himachal Jan/Feb/May and
    Rajasthan Jan/Feb.
- **Cities and non-city regions** give the same verdicts in every case
  (`subset` column).

**Exploratory interpretation (A5, post hoc, not a test).** In every state and
model, the raw model's p99 warms more from 1985–1999 to 2000–2014 than UHE's:

| | ACCESS-CM2 | CMCC-CM2-SR5 |
|---|---|---|
| Model p99 change minus UHE p99 change | +0.16 to +0.41 °C | +0.42 to +0.72 °C |
| Model mean change minus UHE mean change | −0.06 to +0.09 °C | +0.23 to +0.30 °C |

QDM preserves the model's own quantile change by design. A model whose tail
warms faster than the reference could therefore produce corrected tails that
overshoot in later years. The Rajasthan ≥32 °C regression is **consistent
with** that mechanism at the one tested threshold where the tail is
populated, but this is a post-hoc hypothesis that has not been tested.

Three things are not established:
- that this mechanism is the cause;
- whether the effect carries into the future windows, since there is no
  future reference;
- that implementation error is ruled out. G1–G3 found none, but they were
  run for ACCESS-CM2 in Kerala only, and Rajasthan was never checked against
  the published product.

The adverse ≥32 °C finding stands as measured.

**In-sample context (H0, not validation).** Fitting on 1985–2014 and scoring
2000–2014 gives Kerala ACCESS p99 |error| 0.04 °C and ≥28 °C 1.4 d/yr, against
0.18 °C and 5.4 d/yr held out. The gap is the cost of genuine transfer.

**Uncertainty.** Whole-year bootstrap (B = 1000), with model and reference
years drawn independently and one draw shared by all units. Intervals are
**conditional on the fitted correction**, with no refitting. Neighbouring
units are not treated as independent, and no single significance test is used
as proof.

## 3. Population vs area weighting

`weighting_heldout_comparison.csv` and `weighting_future_comparison.csv`. Each
weighting was trained on its own raw series. The band is 0.05 °C or 0.5 d/yr.

| | Before QDM (raw) | After held-out QDM | Future (Kerala ACCESS, trained 1985–2014) |
|---|---|---|---|
| Annual mean, p90–p99 | Differ a lot: up to 6.4 °C in Himachal, 1.6 °C in Kerala; 45–81 units over band | Annual mean ≤0.09 °C and p90–p95 ≤0.17 °C everywhere (the maxima are Himachal CMCC) | Annual mean ≤0.04 °C; p90–p99 ≤0.051 °C in every unit |
| p99 detail | | Himachal up to 0.37 °C (8–12 units over band) | |
| Counts | | Up to 1.5 d/yr (ACCESS) or 2.2 d/yr (CMCC) in Kerala; 16–30 units over band at ≥28 | 2020–2039: up to 2.6 d/yr (26 units over band at ≥28). 2040–2059: up to 4.3 d/yr ≥28 (36 units) and 1.65 d/yr ≥32 (6 units) |

**Equivalence holds for temperature statistics out of sample, but not fully
for counts.** QDM absorbs most of the weighting difference. The residual count
differences grow in the future windows, because thresholds amplify small
distribution differences. The CHG-0643 claim ("weights barely matter") holds
for the calibration fit and for annual means. It should not be extended to
future threshold counts without this caveat.

## 4. Product suitability: transfer to IRT districts and blocks

`results/transfer/`, ACCESS-CM2 population held-out series. Counts are taken
per unit-year **first**, then area-weighted. They are labelled "area-weighted
regional exceedance days (fractional)".

| Check | Kerala | Rajasthan | Himachal Pradesh |
|---|---|---|---|
| Admin units (districts + blocks) | 14 + 152 | 49 + 348 | 12 + 81 |
| CarbonPlan units used | 149 (63 cities) | 479 (212 cities) | 108 (26 cities) |
| Weight-sum max deviation | 2e-16 | 2e-16 | 2e-16 |
| Zero-coverage units | 0 | 0 | 0 |
| Coverage < 0.95 (original admin area) | 6 backwater/coastal blocks (min Thycattussery 0.55) + Alappuzha district 0.93 | 0 (min 0.97) | 1 block (Kinnaur/Pooh 0.925) |
| Unit-polygon double counting (SPEC: ≤ 0.1%) | **FAIL**: 2 admin units > 0.1% (max 0.39%) | pass (max 3e-7) | **FAIL**: 3 admin units > 0.1% (**max 21%**) |
| Units per block, median (min) | 5 (2) | 5 (1) | 5 (1) |
| Dominant-unit weight, block median | 0.67 | 0.86 | 0.77 |
| Blocks effectively single-source (dominant ≥ 0.99) | 0 | 8 | 3 |
| Blocks sharing an identical weight vector | 0 | 0 | 0 |

- **Double counting is in CarbonPlan's own geometry.** Region `IND.14.176.693`
  (12.5 km²) lies entirely inside `IND.14.176.690`, which has no matching hole.
  Two Kerala/Tamil Nadu border regions also overlap by 3.8 km²
  (`provenance/unit_overlaps.csv`). Reported, not corrected; nothing was
  filled.
- **This is a FAILED spatial check, and the coverage column was defective**
  (review correction, CHG-0652/0655). `coverage.csv` divides the *sum* of
  intersections by admin area, not their *union*, so overlapping polygons
  report more than full coverage:

  | Admin unit | Reported coverage |
  |---|---|
  | Basantpur | 121.4% |
  | Mashobra | 101.2% |
  | Kothamangalam | 100.4% |
  | Shimla district | 100.2% |

  The transfer weights count the overlapped land twice as well. Values and
  ranks in `results/transfer/` for the five affected admin units
  (Kothamangalam, Ernakulam district, Basantpur, Mashobra, Shimla district) are
  **PROVISIONAL**.
- These outputs were not recomputed, and no overlap-ownership rule was chosen.
  The coverage fix is in `wbgt_shade_cp_validation.crosswalk_weights`
  (CHG-0652), which now refuses summed coverage. The pilot
  (`../wbgt_shade_carbonplan_pilot/`) excludes these units and reports them,
  rather than renormalising their weights.
- Union-based coverage leaves the < 0.95 list unchanged: the same eight units.
- **Data contract:** 29 of Rajasthan's 49 districts share a
  `district_lgd_code` (amendment A1). Any national crosswalk must not key
  districts on LGD code.
- **Rank agreement with UHE across blocks** (Spearman, 2000–2014):

  | State | raw | QDM |
  |---|---|---|
  | Kerala | 0.42–0.63 | 0.96–0.99 (0.67 at ≥32 °C) |
  | Rajasthan | 0.74–0.90 | 0.98–1.00 |
  | Himachal Pradesh | 0.69–0.98 | 0.80–1.00 |

  QDM's agreement is **largely by construction**: QDM imports each unit's UHE
  climatology from the training years, so this is not independent skill. For
  near-zero metrics, ties dominate: at ≥32 °C, 93 of 152 Kerala blocks and 66
  of 81 Himachal blocks have zero reference days. Ranks there are not
  meaningful.
- **`shade-peak-v1` (descriptive only)**, 2000–2010, block median |x − UHE|,
  with UHE transferred through the same coarse crosswalk:

  | Metric | Kerala v1 | Kerala QDM | Rajasthan v1 | Rajasthan QDM |
  |---|---|---|---|---|
  | ≥28 °C (d/yr) | 44.0 | 4.4 | 33.2 | 4.0 |
  | ≥32 °C (d/yr) | — | — | 3.2 | 4.7 |

  `shade-peak-v1` behaves like raw CarbonPlan shade (cold in Kerala and
  Rajasthan). The QDM product is close to UHE by construction. This is **not**
  a validation of either method against a defensible common reference.
- **Geographic detail is capped** at CarbonPlan units (median 5 per block).
  Every block still gets a distinct weight vector.

## Answers to the brief's questions

- **Does held-out shade performance improve?** Yes in the primary case
  (Kerala, ACCESS-CM2): seasonal, p90–p99 and supported counts at ≥28/30 °C
  all improve, with intervals clear of zero. Broadly yes for the seasonal
  cycle, p90 and ≥28/30 °C counts across all three states and both models.
- **Where does it worsen?**
  - **Climates / thresholds:** the hot-dry tail (Rajasthan ≥32 °C, both
    models; p99 in Rajasthan).
  - **Models:** CMCC-CM2-SR5 in humid Kerala (≥30 °C, tails).
  - **Seasons:** Himachal March; Kerala CMCC May.
  - **Places:** Thrissur (≥32 °C) and the central Kerala coast (p99).
  - **Inconclusive by sampling or support:** Himachal counts; Kerala ≥32 °C.
- **Is population/area equivalence supported outside calibration?** For annual
  means, yes (≤0.09 °C). For tail quantiles, mostly: ≤0.07 °C outside
  Himachal, but up to 0.17 °C (p90–p95) and 0.37 °C (p99) there. For threshold counts, only approximately: up to 2.2 d/yr held out
  and 4.3 d/yr in 2040–2059. Weighting is a product decision for counts.
- **Does the future implementation reproduce CarbonPlan's published
  windows?** For ACCESS-CM2 in Kerala, yes, under the declared numerical
  exception rules: both SSP2-4.5 windows pass G3 for all 149 units, and every
  count flip is explained. The 118 and 116 residual days above 1e-3 °C are
  not attributed (A4). No other model or state was checked.
- **Which decisions remain open?**
  - **Calendar:** CarbonPlan interpolates Feb 29 for 365-day models (4 days
    per unit in 2000–2014). IRT's reporting calendar drops them. The SPEC must
    state which calendar published values use.
  - **Coverage:** coastal/backwater blocks below 0.95; CarbonPlan's nested
    polygon in Himachal; Rajasthan LGD duplicates.
  - **Application window:** must be frozen per §1.
  - **Weighting:** for counts.
  - **≥32 °C semantics:** where the tail is populated, QDM inherits model
    tail warming.
- **What is needed before national production?**
  1. Resolve or accept the tail-overshoot behaviour, with a decision on ≥32 °C
     reporting.
  2. Repeat M1 for more models; only 2 of the 19-model roster were tested.
  3. Run the G3-style future check on a second model and a second state.
  4. Freeze windows, calendar, weighting and the district key in the SPEC.
  5. Add CarbonPlan geometry QC (overlaps and holes) to the crosswalk build.
  6. Measure the national compute budget (one worker here: about 1 GB RSS and
     17–20 min per 480-unit state-model).
  7. Decide whether the reference (UHE-Daily, ending 2016) suffices for
     national acceptance.

## Limitations

- One temporal split (15 + 15 years), two models, three states. The 19-model
  release roster's full required-year intersection was **not** verified; only
  ACCESS-CM2 and CMCC-CM2-SR5 were inventoried (1985–2014 and 2020–2059;
  ACCESS-CM2 also 2060–2080 for the AW cells).
- Bootstrap intervals omit refitting uncertainty. 15 years are few for rare
  thresholds.
- UHE-Daily is itself a model-derived reference, so "improvement" means
  closer to UHE.
- Local NEX files are version 1.0 and may differ from CarbonPlan's 2023
  download. That accounts for the 1e-5 °C residuals.
- The screening bands (0.05 °C, 0.5 d/yr) and the 2/3 share rule are
  predeclared reporting conventions, not accuracy standards.
- The `shade-peak-v1` comparison is descriptive.

## Reproduce (chronological, from the repository root)

```bash
IRT=/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe   # geo/NetCDF env
CP=/tmp/irt-carbonplan-py310/bin/python                                   # pinned xclim 0.44.0 / thermofeel 1.3.0
S=docs/diagnostics/wbgt_shade_carbonplan_validation/scripts
$IRT $S/prepare.py geometry      # streams the 190 MB geometry once (ledger)
$IRT $S/prepare.py plan          # HEAD-prices every uncached chunk against the 2 GB cap
$IRT $S/prepare.py fetch         # cells/weights, UHE, published shade (Kerala ACCESS-CM2)
$IRT $S/prepare.py drivers       # NEX inventory + extraction (local only)
$IRT $S/prepare.py crosswalk     # IRT district/block x unit intersections
$CP -W ignore $S/run_validation.py fidelity --out-dir scratch/carbonplan_shade_validation/fidelity
# one per state x model x weighting, sequentially (12 runs):
$CP -W ignore $S/run_validation.py holdout --state Kerala --model ACCESS-CM2 --weights population \
    --out-dir scratch/carbonplan_shade_validation/runs/Kerala__ACCESS-CM2__population
$CP -W ignore $S/score.py        # -> results/ (screening, per-unit scores, weighting)
$CP -W ignore $S/transfer.py     # -> results/transfer/
$IRT $S/plots.py                 # -> results/*.png
```

The actual order was: geometry, plan, fetch, drivers, fetch (re-run to write
npz, cache only), crosswalk (re-run after A1), fidelity, Kerala ACCESS holdouts,
the 10-run chain (3 CMCC runs failed, A3), the CMCC re-runs, score, transfer,
plots.

## Resources

- **Downloads:** **614,795,325 bytes** cumulative (`provenance/transfer_ledger.json`)
  against the 2 GB cap:
  - the geometry stream, 190.2 MB;
  - the future published Kerala chunks, 424.6 MB;
  - metadata.
  
  UHE, population, elevation and historical published chunks for all three
  states were already cached.
- **Runtime (single worker; peak RSS from `getrusage` or psutil):**
  - fidelity: 1069 s, 1.97 GB;
  - holdouts: 254–1212 s each, 1.36–1.95 GB; Rajasthan is about 18–20 min per
    run;
  - scoring: 3537 s, 0.46 GB;
  - transfer: 81 s;
  - prepare stages: 9–187 s, ≤0.73 GB.
- The crosswalk stage and one transfer trial overlapped with holdout runs.
  These were geometry and pandas work only, so holdout wall times are slightly
  inflated.

## Files

- `SPEC.md`: prospective contract plus amendments A1–A5.
- `scripts/`: `prepare.py` (irt env), `run_validation.py`, `score.py` and
  `transfer.py` (pinned env), `plots.py` (irt env). The pure logic lives in
  `tools/diagnostics/wbgt_shade_cp_validation.py`, with tests in
  `tests/test_wbgt_shade_cp_validation.py`.
- `results/screening_summary.csv`: per state × model × weighting × subset ×
  metric. Medians, signed medians, 90% intervals, improved/unchanged/worsened
  shares, zero/zero counts, support and verdict. Also the H0 context rows.
- `results/units/`: per-unit scores (`scores__*`), worst regressions,
  monthly-bias climatology and worsened-by-district tables per run.
- `results/pooled_summary.csv`: descriptive only; per-state verdicts govern.
- `results/weighting_*`: population vs area comparisons.
- `results/fidelity/`: G1–G3 tables, every flip, tie-pair days, residual
  quantiles, the AW table.
- `results/transfer/`: coverage, block detail, rank agreement, admin
  yearly/period values, the `shade-peak-v1` descriptive tables, integrity checks.
- `results/provenance/`: transfer ledger, remote plan, NEX driver inventory,
  UHE coverage, unit overlaps, crosswalk checks, geometry SHA-256, stage logs.
- `results/run_manifests/`: per-run versions, timings, G1, exclusions, QDM
  warnings.
- Large intermediates (daily series) stay in `scratch/carbonplan_shade_validation/`.
