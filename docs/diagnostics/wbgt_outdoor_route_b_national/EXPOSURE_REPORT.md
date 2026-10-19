# Window-blanking exposure, measured across the roster without fitting

CHG-0667. The user held the unfitted roster and asked for the actual loss to be
quantified — "affected district/block unit-years by scenario and period,
geographic concentration, and effects on ensemble membership, not just affected
cell counts" — and for the remaining models to be estimated from input screening
rather than by fitting them. This is that measurement.

## 1. The prediction is exact, not an estimate

The three steps that turn a screened input day into a missing unit-year are all
deterministic functions of the drivers:

1. `raw_shade` screens physically impossible inputs — negative or supersaturated
   `huss`, non-positive temperature, non-finite values — to NaN.
2. `adjust` returns all-NaN for any QDM application window containing a
   non-finite day, so it never fabricates a value.
3. `fixed_aggregate` yields NaN for a unit when any cell with **strictly
   positive** area weight is missing; zero-weight cells cannot poison a unit.

`tools.diagnostics.wbgt_route_b_exposure` replays all three from the extracted
drivers. It has been checked against both models that were actually fitted and
assembled — and the second check was a **true out-of-sample prediction**, made and
published before that model finished assembling:

| | ACCESS-CM2 | ACCESS-ESM1-5 |
|---|---|---|
| Units predicted | 15 | 374 |
| Units actually missing | 15 | 374 |
| Unit-years predicted | 384 | 19,733 |
| Unit-years actually missing | 384 | 19,733 |
| Predicted but not observed | 0 | 0 |
| Observed but not predicted | 0 | 0 |

ACCESS-ESM1-5 carries 51 times ACCESS-CM2's loss, so the agreement is not an
artefact of a small number. The predictor reproduced a model it had never seen
assembled, exactly, at both unit and unit-year granularity, and its geographic
prediction held too: Rajasthan accounts for 17,192 of the 19,733 missing
unit-years, 87% of the total.

The figures below are therefore the loss each remaining model **would** have, not a
sample or an extrapolation. No model was fitted to obtain them.

## 2. Roster-wide loss, all 18 models

Eligible unit-years per model are 7,752 units x 143 years = 1,108,536.

| Model | Unit-years lost | Share | Screened cell-days | Cells screened |
|---|---|---|---|---|
| ACCESS-ESM1-5 | 19,733 | 1.780% | 2,004 | 451 |
| MPI-ESM1-2-HR | 10,880 | 0.982% | 214 | 173 |
| MPI-ESM1-2-LR | 8,722 | 0.787% | 336 | 326 |
| CanESM5 | 1,640 | 0.148% | 54 | 42 |
| CMCC-ESM2 | 920 | 0.083% | 27 | 26 |
| INM-CM5-0 | 892 | 0.081% | 146 | 50 |
| INM-CM4-8 | 609 | 0.055% | 85 | 64 |
| CMCC-CM2-SR5 | 460 | 0.042% | 4 | 4 |
| ACCESS-CM2 | 384 | 0.035% | 23 | 6 |
| EC-Earth3 | 80 | 0.007% | 7 | 4 |
| EC-Earth3-Veg-LR | 0 | 0.000% | 0 | 0 |
| GFDL-CM4 | 0 | 0.000% | 0 | 0 |
| GFDL-ESM4 | 0 | 0.000% | 0 | 0 |
| KIOST-ESM | 0 | 0.000% | 0 | 0 |
| MRI-ESM2-0 | 0 | 0.000% | 0 | 0 |
| NorESM2-LM | 0 | 0.000% | 0 | 0 |
| NorESM2-MM | 0 | 0.000% | 0 | 0 |
| TaiESM1 | 0 | 0.000% | 0 | 0 |

Across the full roster the total is **44,320 unit-years of 19,953,648**, or
**0.22% of the whole model-unit-year space**.

Four findings matter more than the totals:

- **Eight of eighteen models lose nothing at all.** EC-Earth3-Veg-LR, GFDL-CM4,
  GFDL-ESM4, KIOST-ESM, MRI-ESM2-0, NorESM2-LM, NorESM2-MM and TaiESM1 contain no
  screened day anywhere in the declared envelope.
- **Cell and unit eligibility never diverge.** All 18 models keep exactly 4,938
  cells and 7,752 eligible units, because no model has a screened day inside the
  1985-2014 calibration series. The structural risk that one ensemble would
  average two different spatial supports does **not** materialise, and
  `publish_route_b_outdoor`'s refusal to mix supports will not trigger.
- **Three models carry almost all of it.** ACCESS-ESM1-5 (1.78%), MPI-ESM1-2-HR
  (0.98%) and MPI-ESM1-2-LR (0.79%) are 89% of the roster total. The remaining
  fifteen together lose under 0.5% of one model's years.
- **Cell counts mislead.** MPI-ESM1-2-HR screens 173 cells and loses 10,880
  unit-years; MPI-ESM1-2-LR screens 326 — nearly twice as many — and loses 8,722.
  CanESM5 screens 42 cells and loses more than INM-CM4-8's 64. Cell counts are
  not a usable proxy for what a reader sees, which is why they are not the
  headline.

## 3. The worst model in detail: ACCESS-ESM1-5

### By scenario, QDM window and administrative level

| Scenario | Window | Level | Units affected | Years in window | Unit-years lost |
|---|---|---|---|---|---|
| ssp245 | 2020-2039 | district | 27 | 20 | 540 |
| ssp245 | 2020-2039 | block | 127 | 20 | 2,540 |
| ssp245 | 2040-2059 | district | 26 | 20 | 520 |
| ssp245 | 2040-2059 | block | 134 | 20 | 2,680 |
| ssp245 | 2060-2080 | district | 44 | 21 | 924 |
| ssp245 | 2060-2080 | block | 216 | 21 | 4,536 |
| ssp585 | 2020-2039 | district | 26 | 20 | 520 |
| ssp585 | 2020-2039 | block | 116 | 20 | 2,320 |
| ssp585 | 2040-2059 | district | 4 | 20 | 80 |
| ssp585 | 2040-2059 | block | 9 | 20 | 180 |
| ssp585 | 2060-2080 | district | 43 | 21 | 903 |
| ssp585 | 2060-2080 | block | 190 | 21 | 3,990 |

Districts and blocks are both affected, in roughly a 1-to-5 ratio that tracks the
roster's own shape. No level is spared.

### By master reporting period

The QDM windows and the master reporting periods **do not align**, and that is the
most consequential detail in this report.

| Scenario | Period | Years in period | Max years lost | Units affected | Units losing the whole period |
|---|---|---|---|---|---|
| ssp245 | 2020-2040 | 21 | 20 | 160 | 0 |
| ssp245 | 2040-2060 | 21 | 20 | 260 | 0 |
| ssp245 | 2060-2080 | 21 | 21 | 260 | 260 |
| ssp585 | 2020-2040 | 21 | 20 | 142 | 0 |
| ssp585 | 2040-2060 | 21 | 20 | 233 | 0 |
| ssp585 | 2060-2080 | 21 | 21 | 233 | 233 |

Window 3 is 2060-2080 and the master period 2060-2080 is identical, so **260 units
under SSP245 and 233 under SSP585 lose that period entirely** from this model. The
other two periods straddle a window boundary and lose at most 20 of their 21
years — which would leave a "21-year" period mean resting on a single year.

That is why the converter now **requires a complete period**: a partial period is
published as missing rather than as a one-year value, because the master carries
mean and median alone and has no years-used column that would let a reader tell
the two apart. The honest years-used count is still written to each
`*_periods.csv`. MPI-ESM1-2-HR, by contrast, loses no complete period at all —
only partial windows — so without this rule its master would have looked
untouched while resting on as little as one year.

### Geographic concentration differs by model

| Model | Top affected states (unit-windows) |
|---|---|
| ACCESS-ESM1-5 | Rajasthan 839, Haryana 65, Punjab 17, Madhya Pradesh 14, Sikkim 9 |
| MPI-ESM1-2-HR | Telangana 315, Maharashtra 95, Uttar Pradesh 55, Bihar 31, Madhya Pradesh 20 |
| MPI-ESM1-2-LR | Chhattisgarh 138, Madhya Pradesh 105, Maharashtra 93, Odisha 51, Karnataka 18 |

Each model has its own dry-cell signature: ACCESS-ESM1-5 is overwhelmingly
Rajasthan, MPI-ESM1-2-HR is Telangana, MPI-ESM1-2-LR is Chhattisgarh and Madhya
Pradesh. The common thread is physical rather than regional — BCSD downscaling
undershoots very low specific humidity into negative values, and each model is
driest in different places. This is a different population of units from the 169
excluded by the coastal coverage gate, which are coastal and insular.

## 4. Effect on ensemble membership

| Models missing per unit | Units |
|---|---|
| 7 | 2 |
| 2 | 168 |
| 1 | 1,214 |

**1,384 of 7,752 eligible units** lose at least one
model somewhere. The overwhelming majority lose exactly one of eighteen.

| State | District | Block | Level | Models missing |
|---|---|---|---|---|
| Ladakh | Leh Ladakh | — | district | 7 |
| Ladakh | Leh Ladakh | Leh | block | 7 |
| Chhattisgarh | Kanker | Bhanupratappur | block | 2 |
| Chhattisgarh | Kanker | Charama | block | 2 |
| Chhattisgarh | Kanker | Kanker | block | 2 |
| Chhattisgarh | Kanker | Narharpur | block | 2 |
| Chhattisgarh | Khairgarh Chhuikhadan Gandai | Chhuikhadan | block | 2 |
| Chhattisgarh | Khairgarh Chhuikhadan Gandai | Khairagarh | block | 2 |
| Chhattisgarh | Mahasamund | Bagbahara | block | 2 |
| Chhattisgarh | Mahasamund | Basna | block | 2 |

**Leh Ladakh is the outlier: 7 of 18 models missing**, at both district and block
level. Its end-century values would rest on an 11-model ensemble where every other
unit has 18. That is the single place where the thinning is large enough to affect
how a value should be read, and it is exactly the high-altitude, cold, dry regime
where negative specific humidity is most frequent.

A unit-year missing from some models but not others does not fail outright: it
reduces that unit-year's `n_models` and the ensemble rests on the remaining
subset. The published `n_models` and `values_per_model` columns record exactly
which models contributed, so the thinning is visible per unit, per scenario and
per period instead of being averaged away silently.

**No nominal 18-model ensemble is ever computed from a smaller available subset
without saying so.** That is enforced by `load_assembly`, which refuses a partial
roster outright, and by `n_models` being published rather than assumed.

## 5. What this does not settle

The user's instruction was explicit: do not switch to annual QDM application
merely to reduce missingness, and first establish how it would change the
adjustment and the resulting values. This report measures **how much** is
missing. It does not measure what annual application would do to the corrections
themselves, which is the separate question the methodology document already lists
as open — comparing upstream windows against a declared continuous annual-series
approach. The `_whole_window` sensitivity variant already in the runner is the
instrument for that comparison, and it has not been run nationally.

Nothing here argues that 1.78% is acceptable or unacceptable. It argues only that
the number is now measured rather than extrapolated, that it is concentrated in
one model and one state, and that it never silently becomes a zero or a one-year
climatology.
