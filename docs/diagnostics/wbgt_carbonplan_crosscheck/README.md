# IRT WBGT day counts vs CarbonPlan extreme-heat v1.0

Generated 2026-09-21 19:58 UTC.

**Metric:** days per year with WBGT >= 32 degC (the only threshold both products publish).

## Stated differences, not corrected for

| | CarbonPlan | IRT |
| --- | --- | --- |
| Historical window | 1985-2014 | 1990-2010 |
| Spatial unit | CIL impact region (2,257 in India) | LGD district (784) |
| Driver | `tasmax`, RH at `tasmax` | daily-mean `tas`, daily-mean `hurs` |
| Bias correction | against ERA5 (notebook 06) | none |

> The spatial units do not correspond, so **nothing below is a paired statistic**. These are two distributions over the same country, compared by shape. A per-place correlation is blocked -- see *What this does not establish* at the end.

## shade

| statistic | CarbonPlan | IRT | IRT / CP |
| --- | ---: | ---: | ---: |
| n | 2257.000 | 784.000 | 0.35 |
| mean | 4.268 | 0.669 | 0.16 |
| std | 7.791 | 0.924 | 0.12 |
| skew | 2.794 | 1.970 | 0.71 |
| frac_zero | 0.537 | 0.060 | 0.11 |

### Quantile curve

| quantile | CarbonPlan | IRT | IRT / CP |
| --- | ---: | ---: | ---: |
| 0.00 | 0.00 | 0.00 | n/a |
| 0.05 | 0.00 | 0.00 | n/a |
| 0.10 | 0.00 | 0.00 | n/a |
| 0.25 | 0.00 | 0.06 | n/a |
| 0.50 | 0.00 | 0.30 | n/a |
| 0.75 | 5.75 | 0.82 | 0.14 |
| 0.90 | 13.25 | 2.14 | 0.16 |
| 0.95 | 19.65 | 2.87 | 0.15 |
| 0.99 | 36.33 | 3.94 | 0.11 |
| 1.00 | 55.25 | 4.36 | 0.08 |

Mean agreement: **-84.3%** (ratio 0.157).  
Two-sample KS: D = 0.477, p = 8.28e-121.

## sun/outdoor

| statistic | CarbonPlan | IRT | IRT / CP |
| --- | ---: | ---: | ---: |
| n | 2257.000 | 784.000 | 0.35 |
| mean | 78.214 | 80.920 | 1.03 |
| std | 50.248 | 53.293 | 1.06 |
| skew | -0.113 | 0.124 | -1.10 |
| frac_zero | 0.102 | 0.001 | 0.01 |

### Quantile curve

| quantile | CarbonPlan | IRT | IRT / CP |
| --- | ---: | ---: | ---: |
| 0.00 | 0.00 | 0.00 | n/a |
| 0.05 | 0.00 | 0.40 | n/a |
| 0.10 | 0.00 | 6.77 | n/a |
| 0.25 | 34.75 | 34.46 | 0.99 |
| 0.50 | 81.75 | 85.94 | 1.05 |
| 0.75 | 121.00 | 121.99 | 1.01 |
| 0.90 | 145.00 | 151.71 | 1.05 |
| 0.95 | 151.00 | 167.75 | 1.11 |
| 0.99 | 161.61 | 190.88 | 1.18 |
| 1.00 | 178.75 | 209.75 | 1.17 |

Mean agreement: **+3.5%** (ratio 1.035).  
Two-sample KS: D = 0.101, p = 1.38e-05.

## What this does not establish

- **No per-place agreement.** CarbonPlan keys regions by CIL `hierid` (`IND.<state>.<district>.<region>`); the published CSVs and summary Zarrs carry no coordinates, and the 592 district-level prefixes do not correspond to IRT's 784 LGD districts. Unblocking this needs the CIL impact-region geometry, joined to IRT boundaries by point-in-polygon -- never by name.
- **A matching mean is not validation.** Read the quantile table, not the mean row: the tail is where a threshold-count metric is decided, and it is where these two products diverge most.
- **Neither product is ground truth here.** For a comparison against physical references rather than against another model's output, see `docs/diagnostics/wbgt_deployed_vs_reference/`.
