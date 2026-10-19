# Reproducing published CarbonPlan outdoor WBGT: Kochi, Bikaner and Shimla

Completed diagnostic. CarbonPlan's published outdoor-WBGT chain is re-run from
its own pinned code against local ACCESS-CM2 inputs at three Indian cities and
scored day-by-day against its released product. Nothing in production, in the
shade artifacts, or in predecessor diagnostic evidence was changed, and nothing
here is deployed.

**Result.** One unchanged chain reproduces the published product at all three
cities and all nine city-years, matching every >=28/30/32 degC threshold count
exactly, with a worst daily error of 7.1e-5 degC once a tied-day ordering
artefact is set aside. The same comparison gives the frozen W1 pilot a daily
error of 0.80 to 2.62 degC. Reproducing CarbonPlan is therefore roughly five
orders of magnitude closer to its product than W1 is, and the gap is not a
calibration offset: it is the bias-correction step W1 omits.

Verdict token: `COUNTS_EXACT_CONTINUOUS_PARITY_NOT_MET`. Every threshold count
is reproduced exactly; the pre-declared continuous gates are not met. Both facts
are stated because either alone misreports the run. See
[Gates and what failed](#gates-and-what-failed).

---

## Scope

ACCESS-CM2 historical. Shade calibration 1985-2014 against UHE-Daily; daily
outdoor comparison in 2005, 2007 and 2009.

| City | processing_id | Climate cells | CarbonPlan elevation | Why included |
|---|---:|---:|---:|---|
| Kochi | 7886 | 5 | 0-70 m | Humid coastal; the original single-city case |
| Bikaner | 6558 | 2 | 215-251 m | Semi-arid plain; W1 reads **low** here |
| Shimla | 6822 | 1 | 1635 m | Mountain; W1 reads **high** here |

The three cities and three years are exactly the subset of the predecessor
five-city W1 comparison for which local ACCESS-CM2 `rsds` and `sfcWind` exist
(1990-2010 only). Bikaner and Shimla were chosen, before scoring, because W1's
disagreement with CarbonPlan has **opposite sign** at them. Hyderabad and
Kolkata are in input range and deliberately excluded; adding them is follow-up
work, not a result withheld after scoring. Three years of one model do not
quantify long-run or ensemble uncertainty, and Shimla's single cell limits any
spatial inference.

## What is reproduced, and from whose code

Every stage runs CarbonPlan's own published code, loaded out of its notebooks by
AST and executed, not reimplemented. Sources are pinned by SHA-256 in
[`source_lock.json`](source_lock.json) at CarbonPlan `extreme-heat`
`f662b37200fe219db912ebd09ceb52fdac979861` and its MetSim fork
`edbd68fecd7decf0f03b32d1226f44327bd297b7`; a checksum mismatch aborts the run.

| Stage | Source | Inputs |
|---|---|---|
| City weights | intersection area x population, `ESRI:53034` | CarbonPlan's published city polygon, its GHS-POP raster resampled to its grid, its elevation raster |
| Raw shade WBGT | notebook 02 | local NEX `tas`, `tasmax`, `huss`; RH at `tasmax`; thermofeel 1.3.0 `calculate_wbt`/`calculate_bgt`; 0.7/0.2/0.1 |
| Bias correction | notebook 06's own `train_bias_correction` | xclim 0.44.0 QDM, `time.dayofyear` grouping with a 31-day window, trained against UHE-Daily 1985-2014 |
| Radiation | pinned MetSim `solar_geom` + `shortwave` | local NEX `rsds` |
| Sun adjustment | notebook 08 | 16 Kong/Huber points refitted; radiation clipped 300-900 W/m2, wind 0.5-3 m/s |

Local NEX inputs drive the shade chain, so the shade stage is an independent
reconstruction. CarbonPlan's published shade series is used only for a separately
labelled sun-stage-only comparison, never as an input to the end-to-end score.
No coefficient, weight, calendar or calibration period is tuned, and nothing is
fitted to the released WBGT series.

## Scores

End-to-end independent chain against the released `historical-WBGT-sun` product,
under the self-consistent hourly radiation interpretation:

| City | Year | Published mean | Bias | MAE | Max abs | >=28 | >=30 | >=32 |
|---|---:|---:|---:|---:|---:|:--:|:--:|:--:|
| Kochi | 2005 | 31.069 | +5.8e-7 | 1.1e-5 | 4.0e-5 | 361 = 361 | 266 = 266 | 110 = 110 |
| Kochi | 2007 | 30.409 | +4.7e-5 | 5.7e-5 | 1.7e-2 | 320 = 320 | 210 = 210 | 80 = 80 |
| Kochi | 2009 | 30.710 | +3.5e-7 | 1.1e-5 | 4.4e-5 | 341 = 341 | 225 = 225 | 101 = 101 |
| Bikaner | 2005 | 28.715 | +5.2e-7 | 1.5e-5 | 6.8e-5 | 217 = 217 | 189 = 189 | 142 = 142 |
| Bikaner | 2007 | 28.873 | +4.6e-8 | 1.3e-5 | 6.4e-5 | 227 = 227 | 190 = 190 | 129 = 129 |
| Bikaner | 2009 | 28.924 | +2.5e-7 | 1.2e-5 | 7.1e-5 | 232 = 232 | 198 = 198 | 127 = 127 |
| Shimla | 2005 | 18.974 | -8.1e-7 | 1.7e-5 | 6.0e-5 | 13 = 13 | 1 = 1 | 0 = 0 |
| Shimla | 2007 | 19.101 | +2.7e-6 | 1.7e-5 | 6.4e-5 | 0 = 0 | 0 = 0 | 0 = 0 |
| Shimla | 2009 | 18.865 | -4.8e-7 | 1.7e-5 | 6.4e-5 | 8 = 8 | 2 = 2 | 0 = 0 |

All units degC; counts shown as reproduced = published. The shade stage alone,
scored independently over all 10,957 calibration days per city, also matches
every count exactly, with worst daily errors of 8.3e-5 degC at Bikaner and
Shimla. Applying only the sun stage to CarbonPlan's published shade isolates
that stage further, to 2.7e-7 degC.

Shimla 2007 has zero exceedance days in both products at all three thresholds.
That is agreement on an absence and carries little evidence; its continuous
scores carry the weight there.

## Gates and what failed

Two gates were declared before scoring, and neither was moved afterwards.

- **Declared parity, max abs <= 1e-6 degC with identical counts: NOT MET.** The
  measured worst daily error is 4.0e-5 to 7.1e-5 degC across the eight city-years
  free of the tied-day artefact, on an MAE of 1.1e-5 to 1.7e-5 degC. Local NEX
  files are not assumed
  identical in version to CarbonPlan's 2023 download, and this floor is the size
  expected from that lineage difference, but that is an assumption about inputs,
  not a demonstration.
- **Input-version band, max abs <= 1e-3 degC: NOT MET, by two days out of 32,871.**
  Both are at Kochi, both on 7 August (1989 and 2007), both 0.0168 degC.
- **Threshold counts at every city-year: MATCHED EXACTLY, 9 of 9.**

### The two failing days are a tied-day ordering artefact

Those two days have raw shade values **3e-6 degC apart** inside one day-of-year
QDM group, and their adjustment factors are exchanged:

| Day | Our factor | CarbonPlan's factor |
|---|---:|---:|
| 1989-08-07 | -0.491397 | -0.474629 |
| 2007-08-07 | -0.474623 | -0.491410 |

Our 1989 factor is CarbonPlan's 2007 factor and vice versa, each to about
1e-5 degC. QDM maps ranked quantiles, so the rank order of two days whose inputs
are indistinguishable is decided at float precision and can legitimately differ
between runs. Re-pairing the two drops the residual to 6e-6 and 1.3e-5 degC, the
ordinary floor. This is reported in `tie_break_pairs.csv` and **left in every
score**; no re-pairing is applied and no threshold count is affected.

### A defect in CarbonPlan's published wiring

The pinned source is internally inconsistent. MetSim's constants set
`SW_RAD_DT = 30` s, while notebook 07 passes `SW_RAD_DT = 3600` s into
`shortwave`. Run literally, the hourly disaggregation does not conserve the daily
mean radiation and the product cannot be reproduced. All three interpretations
are reported for every city-year:

| Interpretation | Daily-mean radiation residual | Worst MAE vs published | Counts |
|---|---:|---:|---|
| `literal` (geometry 30 s, wrapper 3600 s) | 337.6 W/m2 | 1.743 degC | mismatch at 8 of 9 city-years |
| `consistent30s` (both 30 s) | 9.1e-13 W/m2 | 0.042 degC | mismatch at 5 of 9 |
| `consistent3600s` (both 3600 s) | 1.1e-13 W/m2 | 5.7e-5 degC | exact at 9 of 9 |

Both self-consistent readings conserve the daily mean; only the hourly one
reproduces the product, which identifies 3600 s as CarbonPlan's effective
intent. The verdict is keyed to that interpretation and the literal source is
reported in its own field, because keying the verdict to the literal source
reports a defect in CarbonPlan's published wiring as a failure of this
reproduction. Neither consistent reading tunes a coefficient; both use a value
already present in the pinned sources.

## How much better than W1

Same cities, same years, same footprints, same published target. W1 numbers are
the predecessor comparison's, unchanged.

| City | Year | Reproduction MAE | W1 MAE | W1 bias | W1 corr | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| Kochi | 2005 | 1.1e-5 | 0.803 | +0.230 | 0.793 | 74,000x |
| Kochi | 2007 | 5.7e-5 | 0.906 | +0.580 | 0.877 | 16,000x |
| Kochi | 2009 | 1.1e-5 | 0.948 | +0.556 | 0.861 | 86,000x |
| Bikaner | 2005 | 1.5e-5 | 1.547 | -0.919 | 0.969 | 101,000x |
| Bikaner | 2007 | 1.3e-5 | 1.614 | -0.999 | 0.965 | 120,000x |
| Bikaner | 2009 | 1.2e-5 | 1.628 | -1.101 | 0.976 | 132,000x |
| Shimla | 2005 | 1.7e-5 | 2.603 | +2.454 | 0.957 | 156,000x |
| Shimla | 2007 | 1.7e-5 | 2.497 | +2.346 | 0.955 | 146,000x |
| Shimla | 2009 | 1.7e-5 | 2.623 | +2.469 | 0.970 | 155,000x |

Median ratio 120,000x. The count comparison is sharper than the ratio. At Kochi
2005 W1 gives 116 days >=32 degC against CarbonPlan's 110, which looks like
agreement, but only 78 are days both products exceed: 32 published-only plus
38 W1-only, about 70 days of timing disagreement hidden inside a net difference
of 6. Day-level overlap per city-year-threshold is in
`threshold_day_level.csv`. The reproduction matches day for day, so its
day-level disagreement is zero wherever counts match.

### What accounts for W1's error

W1 is a physical reconstruction with no accepted derived-WBGT bias correction.
This run measures the step it omits. The QDM correction CarbonPlan applies is
large and **changes sign by city**, and at each city it runs opposite to W1's
bias:

| City | Raw chain minus UHE-Daily | Mean QDM correction | W1 bias vs published | Sum |
|---|---:|---:|---:|---:|
| Kochi | -0.552 | +0.541 | +0.455 | +1.00 |
| Bikaner | -1.787 | +1.781 | -1.007 | +0.77 |
| Shimla | +1.311 | -1.343 | +2.423 | +1.08 |

All degC; QDM correction is the 1985-2014 mean of corrected minus raw shade, and
is stable year to year (Shimla -1.23/-1.18/-1.35 across 2005/07/09).

CarbonPlan's own *uncorrected* physical chain carries the same sign-flipping
error structure W1 does: 1.8 degC low at Bikaner, 1.3 degC high at Shimla. QDM
against UHE-Daily is what removes it. W1 is a raw physical chain without that
step, so it inherits that structure. Adding the omitted correction back to W1's
bias leaves +0.77 to +1.08 degC, nearly constant across three climatically very
different cities, against W1 biases spanning 3.4 degC. In other words the
sign-flipping, location-dependent part of W1's disagreement is accounted for by
the missing bias correction, and what remains is close to a constant offset
attributable to the other differences (area-weighted versus population-weighted
aggregation, Liljegren versus the three-term ISO shade formula, and sun-stage
differences).

This is a three-city decomposition, and it is indicative rather than an
identity: the QDM correction is a shade-stage quantity measured over 30 years
while W1's bias is an outdoor-stage quantity over 3 years, and W1 differs from
CarbonPlan in more than the correction. The sum column is not expected to be
zero. Two candidate explanations are ruled out: a uniform national offset cannot
reconcile differences of opposing sign, and elevation cannot explain Shimla.
CarbonPlan's 1635 m against W1's 1481 m GMTED cell-mean is a 154 m difference,
and the milestone-4b pilot measured the whole elevation effect at **at most
0.261 degC even at 5308 m** ([wbgt_outdoor_pilot_qc README section
3.3](../wbgt_outdoor_pilot_qc/README.md)), so 154 m cannot account for a
2.42 degC bias.

## Limits and decision

This is a successful reproduction of a published product, not an observational
accuracy test. It establishes that CarbonPlan's pipeline is reproducible from its
published code and inputs, that its published radiation wiring contains a
timestep inconsistency that must be resolved to reproduce it, and that W1's
disagreement is dominated by the bias-correction step W1 omits. It does **not**
establish that CarbonPlan is more accurate than W1 against observations; UHE-Daily
is CarbonPlan's chosen reference, and reproducing a product calibrated to it is
not evidence about reality.

**Outdoor WBGT counts stay diagnostic-only.** Nothing here is promoted to
production, and no global offset or wind shape should be tuned from these
tables. The decision this run informs is narrower and now answerable: if outdoor
WBGT is ever to ship, the missing piece is an accepted bias correction against a
declared reference, not a better wind shape or a per-region offset. Choosing that
reference, and whether an India-specific one is needed in place of UHE-Daily,
remains open and is a methodology decision, not an engineering one.

## Evidence

Durable evidence is this directory. Run products are under
`scratch/carbonplan_reproduction/run_final/` (git-ignored, regenerable):

- `summary_all.csv` — every stage, city, year and interpretation
- `reproduction_vs_w1.csv` — the head-to-head table above
- `threshold_day_level.csv` — day-level exceedance overlap, exposing W1's cancellation
- `residual_scales.csv` — residual magnitude bands across all 32,871 calibration days
- `tie_break_pairs.csv` — the two tied-day exchanges
- `rollup.json`, `run_manifest.json` — verdicts, per-city diagnostics, source lock, package versions
- `<City>/shade_daily_1985_2014.csv`, `<City>/daily_<year>.csv`, `<City>/raw_shade_cell_daily.csv`

Per-city input provenance, including SHA-256 of every remote object and of every
selected local array slice, is in
`scratch/carbonplan_reproduction/cities/<City>/input_manifest.json`.

No figures are produced. The headline is a pair of error magnitudes five orders
of magnitude apart, which these tables state exactly; the predecessor
comparison's figures remain the place for seasonal shape.

## Reproducing this run

The reproduction stage needs CarbonPlan's pinned library versions, which are
older than IRT's geo environment and must not be installed into it. Two
interpreters are used deliberately: IRT's conda environment for input extraction
(it has the geo stack), and a throwaway Python 3.10 environment for the scored
chain. **No IRT dependency is added or changed.**

```bash
# 1. Pinned-version environment for the scored chain (throwaway; /tmp is volatile)
uv venv --python 3.10 /tmp/irt-carbonplan-py310
uv pip install --python /tmp/irt-carbonplan-py310/bin/python 'numpy==1.26.4' 'pandas==2.0.3' 'xarray==2023.8.0' 'xclim==0.44.0' 'cf-xarray==0.8.4' 'pint==0.22' 'scipy==1.11.4' 'numba==0.60.0' 'dask==2023.8.1' 'thermofeel==1.3.0'

# 2. Scoring-gate contract checks (no downloads, no inputs needed)
/tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/contract_checks.py

# 3. Verify the checksum-pinned upstream sources into the work directory
python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/fetch_sources.py --work-dir scratch/carbonplan_reproduction

# 4. Extract bounded public CarbonPlan inputs and local NEX drivers per city (IRT conda env)
python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/prepare_inputs.py --work-dir scratch/carbonplan_reproduction

# 5. Score the chain into a FRESH output directory (existing directories are refused)
/tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/reproduce.py --work-dir scratch/carbonplan_reproduction --out-dir scratch/carbonplan_reproduction/run_final

# 6. Roll up across cities and against W1
/tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/summarize.py --out-dir scratch/carbonplan_reproduction/run_final
```

Step 4 downloads about 529 MB of public CarbonPlan objects on a cold cache,
capped at 160 MB per object and cached by URL under the work root, shared across
cities. Steps 4-6 are single-worker. Two independent executions of steps 5-6
produced byte-identical daily output.

`fetch_sources.py --verify-remote` re-fetches and re-checks the pinned sources
against GitHub. It is the one step that has not completed here: raw.githubusercontent.com
reset the connection from both WSL and the Windows interpreter. The sources were
therefore verified against `source_lock.json` from the local copies, which the
scored run also does on every execution; an independent remote re-verification
remains outstanding.

## Attribution

CarbonPlan's `extreme-heat` data and code are **CC BY 4.0**; credit CarbonPlan.
Only bounded portions of its public inputs and released outputs were downloaded;
no global archive was retrieved. Local NEX-GDDP-CMIP6 inputs are NASA's, and
their version is not assumed identical to CarbonPlan's 2023 download.

- Pre-registered contract and the two post-score amendments: [`SPEC.md`](SPEC.md)
- Predecessor product comparison: [`../wbgt_outdoor_carbonplan_multicity/`](../wbgt_outdoor_carbonplan_multicity/)
- Single-city precursor: [`../wbgt_outdoor_carbonplan_compare/`](../wbgt_outdoor_carbonplan_compare/)
