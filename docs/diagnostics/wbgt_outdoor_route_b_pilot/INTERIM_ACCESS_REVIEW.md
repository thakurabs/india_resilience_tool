# ACCESS-CM2 interim evidence — CHG-0651

Checked 2026-10-05 after ACCESS-CM2 completed every scenario and output check.
This is **not** a completed two-model verdict. The user subsequently stopped
CMCC-CM2-SR5 and its coordinator, choosing the ACCESS-only grid-first working
pilot (CHG-0654). The findings below remain the completed pilot's evidence;
no combined two-model report is pending automatic execution.

## Outdoor aggregation: order versus metric definition

Across historical district results, changing only aggregation order has a small
effect on ≥30 °C period-average annual counts: median absolute differences are
0.286 days/year in Kerala, 0.143 in Rajasthan and 0 in Himachal Pradesh. The
largest historical district difference is 0.952 days/year. Across future
district windows, the maximum reaches 2.0 days/year; the corresponding maximum
annual-mean-temperature difference is 0.021 °C.

Changing the metric definition matters much more. Area-average **cell** counts
versus counts of the **regional-average daily series** differ by a median
14.50 days/year in Kerala's historical districts, and by up to 36.76 in Himachal.
The largest future district difference is 46.09 days/year at ≥30 °C. These are
different exposure quantities, not two estimates of the same count. The first
model therefore supports retaining grid-first as the development candidate,
while making the count definition explicit. It does not yet settle the final
two-model or national decision.

## Held-out shade evidence

Calibration 1985–1999; evaluation 2000–2014 against UHE-Daily. The following are
medians across eligible districts, not state-average climate values. Inputs use
the identical conservatively coarsened reference and fixed area support used by
the paired pilot. These are distributional errors, not synchronized-weather
scores and not independent outdoor-WBGT accuracy measurements.

| State | Raw seasonal MAE °C | Grid-first QDM seasonal MAE °C | Region-first QDM seasonal MAE °C |
|---|---:|---:|---:|
| Kerala | 0.929 | 0.125 | 0.127 |
| Rajasthan | 2.003 | 0.351 | 0.354 |
| Himachal Pradesh | 1.365 | 0.424 | 0.422 |

QDM improves the median seasonal error in all three states. The tail is mixed:
Rajasthan's median absolute ≥32 °C count bias rises from **1.27 days/year raw**
to **5.47 grid-first / 5.33 region-first**. Its p99 absolute bias rises from
0.321 °C to 0.462 / 0.450 °C. Lower-threshold Rajasthan count errors improve:
≥28 °C falls from 32.47 to 3.40 / 3.73 days/year, and ≥30 °C from 24.20 to
5.47 / 5.40. Zero median rare-event errors in cooler states do not establish
extreme-event accuracy. Both paths show similar seasonal improvement; this alone
does not decide outdoor aggregation order or threshold semantics.

## W1 comparison on matched 2005 cell support

Median district differences below are **Route B minus W1's combined
humidity/elevation diagnostic variant**, using identical complete cells and
fixed area weights, with at least 95% whole-polygon coverage.

| State | Annual mean difference °C | ≥30 °C count difference days/year |
|---|---:|---:|
| Kerala | -1.205 | -41.21 |
| Rajasthan | +0.876 | +13.81 |
| Himachal Pradesh | -2.084 | -9.57 |

The differences change sign by location. They combine correction and other
method choices; they cannot demonstrate observational superiority. Baseline W1
is retained separately in the eventual report. There is no matching CMCC W1
predecessor cache.

## Coverage and cost

ACCESS retains 774/784 cells and 630/656 admin units. Exclusions are confined to
Kerala: 5 districts and 21 blocks fail the declared 95% whole-area support gate.
Their headline results remain missing. The eligible sample is 9 Kerala,
49 Rajasthan and 12 Himachal districts, plus 131/348/81 blocks respectively.

Measured QDM fitting/application work totals **6.047 hours** over 774 cell and
630 regional series, including full and held-out fitting and both future
scenarios. Original timings were retained through the solar-only cache recovery.
Solar, I/O and geometry preparation are additional costs. CMCC's separate
conservative probe estimate is 9.335 hours, below the existing 12-hour QDM gate.

## Application windows and sun-stage limits

The preselected Palakkad district's separate-window versus whole-2020–2080 QDM
comparison changes an individual year's outdoor count by up to 45 days at ≥28 °C
and 34 days at ≥30 °C under SSP585, despite much smaller 61-year mean differences.
This is a substantive application-window sensitivity. Lahul and Spiti reaches
an 11.71 °C single-day shade difference, on a cold winter day (2070-12-17:
-20.34 versus -8.63 °C); that extreme does not change the tested heat counts.
Jaisalmer also shows differences, including a 3.16 °C shade change on a hot
SSP585 day (2074-09-02). A small long-period mean shift cannot establish
day-by-day or year-by-year stability.

All ACCESS solar conservation checks passed, with maximum residual
2.84e-13 W/m². However, the empirical sun adjustment clips wind to its fitted
range on a median 34–39% of days across supports, depending on scenario/path;
some supports approach 100%. Conservation validates energy bookkeeping, not
that empirical adjustment outside its fitted range.

## Evidence status

These summaries use the actual signed ACCESS outputs under `ACCESS-CM2/`.
The independently calculated interim held-out table matches the final held-out
output exactly. The single-model report/review is under
`scratch/wbgt_route_b_access_review/`; selected summary CSVs are retained under
`access_interim_review/` here. ACCESS resumed wall time was 605 seconds and peak
RSS 3,598 MB; the six hours of original QDM work must be added when interpreting
total compute cost.

The current evidence supports continued diagnostic evaluation, with an explicit
Rajasthan upper-tail limitation. National scale-out, display publication and
composite membership remain undecided.
