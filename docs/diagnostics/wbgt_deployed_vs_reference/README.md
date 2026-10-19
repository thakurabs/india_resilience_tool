# Deployed IRT WBGT vs the Lemke & Kjellstrom reference methods

Generated 2026-09-21 19:57 UTC.

**Window:** 2005-2014, all months.  
**Driver:** ERA5 hourly, Open-Meteo archive, nearest 0.25 deg cell.  
**References:** Liljegren et al. (2008) outdoor; Bernard et al. (1999) indoor -- the two methods recommended by Lemke & Kjellstrom (2012).

Every column below is driven by the *same* hourly series, so climate-model error cancels and what remains is method error.

## Liljegren NaN rate by site

| site | NaN rate |
| --- | --- |
| Kochi | 0.0000 |
| Kolkata | 0.0000 |
| Bikaner | 0.0000 |
| Lucknow | 0.0000 |
| Hyderabad | 0.0000 |
| Shimla | 0.0000 |

## Scores

`bias` is candidate minus reference. `p99 bias` is the same difference restricted to the hottest 1% of reference days, where the shipped threshold-count metrics are decided.

| site | reference | candidate | n | bias C | med abs C | RMSE C | r | p99 bias C |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kochi | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | +0.93 | 1.81 | 2.28 | 0.410 | -6.12 |
| Kochi | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -1.39 | 1.41 | 2.41 | 0.479 | -8.13 |
| Kochi | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.04 | 0.05 | 0.07 | 0.998 | -0.00 |
| Kochi | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -2.10 | 2.09 | 2.15 | 0.888 | -2.76 |
| Kochi | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -6.91 | 6.75 | 7.17 | 0.532 | -13.78 |
| Kolkata | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | +1.77 | 2.11 | 2.76 | 0.914 | -0.69 |
| Kolkata | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -0.50 | 1.84 | 2.50 | 0.906 | -2.89 |
| Kolkata | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.06 | 0.06 | 0.09 | 1.000 | +0.05 |
| Kolkata | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -2.22 | 2.03 | 2.37 | 0.990 | -2.28 |
| Kolkata | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -5.95 | 5.80 | 6.24 | 0.912 | -9.17 |
| Bikaner | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | +0.86 | 1.74 | 2.22 | 0.955 | +0.33 |
| Bikaner | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -1.93 | 2.10 | 3.25 | 0.942 | -1.93 |
| Bikaner | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.17 | 0.16 | 0.28 | 0.999 | +0.08 |
| Bikaner | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -2.95 | 2.79 | 3.15 | 0.992 | -2.15 |
| Bikaner | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -6.67 | 6.33 | 7.05 | 0.950 | -7.59 |
| Lucknow | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | +1.11 | 1.54 | 2.37 | 0.940 | +0.36 |
| Lucknow | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -1.97 | 2.48 | 3.37 | 0.918 | -2.64 |
| Lucknow | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.09 | 0.08 | 0.14 | 1.000 | +0.08 |
| Lucknow | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -3.07 | 3.03 | 3.27 | 0.990 | -2.53 |
| Lucknow | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -6.97 | 6.90 | 7.32 | 0.928 | -8.54 |
| Hyderabad | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | +1.21 | 1.43 | 2.02 | 0.825 | -1.88 |
| Hyderabad | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -1.52 | 1.83 | 2.63 | 0.730 | -4.98 |
| Hyderabad | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.06 | 0.07 | 0.10 | 1.000 | +0.05 |
| Hyderabad | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -2.71 | 2.60 | 2.84 | 0.961 | -2.91 |
| Hyderabad | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -6.23 | 6.19 | 6.51 | 0.758 | -9.88 |
| Shimla | liljegren_daily_max | irt_swbgt_hourly_max [outdoor, formula only] | 3651 | -0.35 | 0.71 | 1.62 | 0.948 | -2.07 |
| Shimla | liljegren_daily_max | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3651 | -3.45 | 3.28 | 4.00 | 0.926 | -5.97 |
| Shimla | bernard_daily_max | irt_shade_hourly_max [shade, formula only] | 3651 | -0.26 | 0.25 | 0.28 | 1.000 | -0.09 |
| Shimla | bernard_daily_max | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 3651 | -3.64 | 3.56 | 3.82 | 0.988 | -3.56 |
| Shimla | liljegren_daily_max | irt_shade_daily_mean_in [shade vs outdoor reference] | 3651 | -8.36 | 8.06 | 8.70 | 0.927 | -10.48 |

## Day counts at the shipped thresholds

| site | candidate | >=28 ref | >=28 cand | >=30 ref | >=30 cand | >=32 ref | >=32 cand |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kochi | irt_swbgt_hourly_max [outdoor, formula only] | 3595 | 3651 | 3272 | 3633 | 2113 | 3107 |
| Kochi | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 3595 | 3613 | 3272 | 3103 | 2113 | 776 |
| Kochi | irt_shade_hourly_max [shade, formula only] | 1275 | 1213 | 52 | 53 | 0 | 0 |
| Kochi | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 1275 | 11 | 52 | 0 | 0 | 0 |
| Kochi | irt_shade_daily_mean_in [shade vs outdoor reference] | 3595 | 11 | 3272 | 0 | 2113 | 0 |
| Kolkata | irt_swbgt_hourly_max [outdoor, formula only] | 2673 | 2674 | 2256 | 2451 | 1439 | 2152 |
| Kolkata | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 2673 | 2451 | 2256 | 2178 | 1439 | 1823 |
| Kolkata | irt_shade_hourly_max [shade, formula only] | 1801 | 1777 | 552 | 535 | 39 | 42 |
| Kolkata | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 1801 | 614 | 552 | 39 | 39 | 0 |
| Kolkata | irt_shade_daily_mean_in [shade vs outdoor reference] | 2673 | 614 | 2256 | 39 | 1439 | 0 |
| Bikaner | irt_swbgt_hourly_max [outdoor, formula only] | 2069 | 2045 | 1597 | 1749 | 897 | 1420 |
| Bikaner | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 2069 | 1629 | 1597 | 1348 | 897 | 980 |
| Bikaner | irt_shade_hourly_max [shade, formula only] | 1296 | 1270 | 512 | 528 | 18 | 24 |
| Bikaner | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 1296 | 548 | 512 | 21 | 18 | 0 |
| Bikaner | irt_shade_daily_mean_in [shade vs outdoor reference] | 2069 | 548 | 1597 | 21 | 897 | 0 |
| Lucknow | irt_swbgt_hourly_max [outdoor, formula only] | 2231 | 2286 | 1815 | 1974 | 1218 | 1698 |
| Lucknow | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 2231 | 1783 | 1815 | 1536 | 1218 | 1173 |
| Lucknow | irt_shade_hourly_max [shade, formula only] | 1441 | 1411 | 509 | 508 | 30 | 35 |
| Lucknow | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 1441 | 468 | 509 | 12 | 30 | 0 |
| Lucknow | irt_shade_daily_mean_in [shade vs outdoor reference] | 2231 | 468 | 1815 | 12 | 1218 | 0 |
| Hyderabad | irt_swbgt_hourly_max [outdoor, formula only] | 2262 | 2755 | 1205 | 2145 | 511 | 1033 |
| Hyderabad | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 2262 | 2019 | 1205 | 732 | 511 | 62 |
| Hyderabad | irt_shade_hourly_max [shade, formula only] | 499 | 496 | 14 | 18 | 0 | 0 |
| Hyderabad | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 499 | 2 | 14 | 0 | 0 | 0 |
| Hyderabad | irt_shade_daily_mean_in [shade vs outdoor reference] | 2262 | 2 | 1205 | 0 | 511 | 0 |
| Shimla | irt_swbgt_hourly_max [outdoor, formula only] | 15 | 19 | 1 | 0 | 0 | 0 |
| Shimla | irt_swbgt_daily_mean_in [outdoor, AS DEPLOYED] | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | irt_shade_hourly_max [shade, formula only] | 0 | 0 | 0 | 0 | 0 | 0 |
| Shimla | irt_shade_daily_mean_in [shade, AS DEPLOYED] | 0 | 0 | 0 | 0 | 0 | 0 |
| Shimla | irt_shade_daily_mean_in [shade vs outdoor reference] | 15 | 0 | 1 | 0 | 0 | 0 |

## Reading this table

- A large gap between the `formula only` and `AS DEPLOYED` rows for the same metric is **aggregation** error: it is caused by evaluating the formula on daily-mean inputs instead of hourly, not by the formula itself.
- A large `formula only` bias is **formula** error and cannot be fixed by changing the aggregation.
- A high `r` with a large bias means the metric ranks places correctly but reports the wrong absolute level -- which is benign for a frozen-CDF ruler and serious for an absolute threshold count.
