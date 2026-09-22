# Does the Tier-2 sun adjustment fix the outdoor WBGT metric?

Generated 2026-09-22 09:25 UTC.

**Window:** 2005-2014, all months.  
**Driver:** ERA5 hourly via the Open-Meteo archive, cached by CHG-0546.  
**Reference:** Liljegren et al. (2008), hourly, then the daily maximum.  
**Chain under test:** `WBGT_sun = WBGT_shade - (-2.1564 - 0.005375*rsds_max + 1.0424*sfcWind)`, clipped to rsds_max 300-900 W/m2 and wind 0.5-3 m/s.

`bias` is candidate minus reference. `p99 bias` is the same difference
restricted to the hottest 1% of reference days. `uplift` is the mean
degrees the adjustment adds to shade WBGT -- a candidate with a good
bias and a large uplift is cancelling errors, not measuring heat.

**Acceptance (pre-registered in CHG-0538, not tuned here):** median
absolute bias < 1 C **and** RMSE < 1.5 C, in every regime.

## Scores

| site | candidate | n | bias C | med abs C | RMSE C | r | p99 bias C | uplift C | verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| Kochi | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.39 | 1.41 | 2.41 | 0.479 | -8.13 | -- | FAIL |
| Kochi | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -3.58 | 3.36 | 3.87 | 0.786 | -9.62 | +3.32 | FAIL |
| Kochi | Tier 2 as specified (tasmax-driven shade) | 3651 | +0.84 | 0.98 | 1.62 | 0.814 | -3.75 | +3.32 | FAIL |
| Kochi | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.22 | 2.21 | 2.66 | 0.815 | -2.37 | +4.71 | FAIL |
| Kochi | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.14 | 0.78 | 1.40 | 0.784 | -6.16 | +4.71 | PASS |
| Kolkata | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -0.50 | 1.84 | 2.50 | 0.906 | -2.89 | -- | FAIL |
| Kolkata | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -3.57 | 3.36 | 3.82 | 0.951 | -5.95 | +2.38 | FAIL |
| Kolkata | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.16 | 1.23 | 1.86 | 0.925 | -0.66 | +2.38 | FAIL |
| Kolkata | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.44 | 2.44 | 2.87 | 0.923 | +0.93 | +3.66 | FAIL |
| Kolkata | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.13 | 0.46 | 0.98 | 0.969 | -2.28 | +3.66 | PASS |
| Bikaner | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.93 | 2.10 | 3.25 | 0.942 | -1.93 | -- | FAIL |
| Bikaner | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -4.42 | 4.08 | 4.93 | 0.957 | -5.09 | +2.25 | FAIL |
| Bikaner | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.01 | 1.49 | 1.99 | 0.962 | -0.41 | +2.25 | FAIL |
| Bikaner | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.29 | 2.51 | 2.90 | 0.963 | +1.05 | +3.54 | FAIL |
| Bikaner | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.35 | 1.14 | 1.69 | 0.967 | -1.56 | +3.54 | FAIL |
| Lucknow | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.97 | 2.48 | 3.37 | 0.918 | -2.64 | -- | FAIL |
| Lucknow | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -4.51 | 4.28 | 4.88 | 0.954 | -5.69 | +2.46 | FAIL |
| Lucknow | Tier 2 as specified (tasmax-driven shade) | 3651 | +0.93 | 1.16 | 1.79 | 0.954 | -0.48 | +2.46 | FAIL |
| Lucknow | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.18 | 2.26 | 2.69 | 0.953 | +1.00 | +3.71 | FAIL |
| Lucknow | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.28 | 0.71 | 1.34 | 0.970 | -1.58 | +3.71 | PASS |
| Hyderabad | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.52 | 1.83 | 2.63 | 0.730 | -4.98 | -- | FAIL |
| Hyderabad | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -3.92 | 3.76 | 4.12 | 0.889 | -6.58 | +2.31 | FAIL |
| Hyderabad | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.17 | 1.26 | 1.60 | 0.920 | -0.72 | +2.31 | FAIL |
| Hyderabad | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.46 | 2.56 | 2.74 | 0.912 | +0.71 | +3.60 | FAIL |
| Hyderabad | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | +0.03 | 0.50 | 0.84 | 0.951 | -2.22 | +3.60 | PASS |
| Shimla | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -3.45 | 3.28 | 4.00 | 0.926 | -5.97 | -- | FAIL |
| Shimla | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -5.10 | 4.76 | 5.60 | 0.940 | -7.12 | +3.26 | FAIL |
| Shimla | Tier 2 as specified (tasmax-driven shade) | 3651 | -0.13 | 0.99 | 1.74 | 0.951 | -2.53 | +3.26 | FAIL |
| Shimla | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +1.11 | 1.66 | 2.13 | 0.953 | -0.97 | +4.50 | FAIL |
| Shimla | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.48 | 0.89 | 1.75 | 0.961 | -1.94 | +4.50 | FAIL |

## Day counts at the shipped thresholds

| site | candidate | >=28 ref | >=28 cand | >=30 ref | >=30 cand | >=32 ref | >=32 cand |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kochi | INCUMBENT swbgt_empirical, AS DEPLOYED | 3595 | 3613 | 3272 | 3103 | 2113 | 776 |
| Kochi | Tier 2 on daily-mean inputs (today's aggregation) | 3595 | 2727 | 3272 | 764 | 2113 | 13 |
| Kochi | Tier 2 as specified (tasmax-driven shade) | 3595 | 3612 | 3272 | 3313 | 2113 | 2646 |
| Kochi | Tier 2, true hourly rsds_max (no disaggregation error) | 3595 | 3622 | 3272 | 3453 | 2113 | 3079 |
| Kochi | Tier 2 CEILING (hourly shade max + true rsds_max) | 3595 | 3613 | 3272 | 3336 | 2113 | 2266 |
| Kolkata | INCUMBENT swbgt_empirical, AS DEPLOYED | 2673 | 2451 | 2256 | 2178 | 1439 | 1823 |
| Kolkata | Tier 2 on daily-mean inputs (today's aggregation) | 2673 | 1861 | 2256 | 829 | 1439 | 96 |
| Kolkata | Tier 2 as specified (tasmax-driven shade) | 2673 | 2931 | 2256 | 2465 | 1439 | 1700 |
| Kolkata | Tier 2, true hourly rsds_max (no disaggregation error) | 2673 | 3131 | 2256 | 2728 | 1439 | 2220 |
| Kolkata | Tier 2 CEILING (hourly shade max + true rsds_max) | 2673 | 2572 | 2256 | 2178 | 1439 | 1420 |
| Bikaner | INCUMBENT swbgt_empirical, AS DEPLOYED | 2069 | 1629 | 1597 | 1348 | 897 | 980 |
| Bikaner | Tier 2 on daily-mean inputs (today's aggregation) | 2069 | 1275 | 1597 | 630 | 897 | 45 |
| Bikaner | Tier 2 as specified (tasmax-driven shade) | 2069 | 2231 | 1597 | 1887 | 897 | 1466 |
| Bikaner | Tier 2, true hourly rsds_max (no disaggregation error) | 2069 | 2408 | 1597 | 2100 | 897 | 1770 |
| Bikaner | Tier 2 CEILING (hourly shade max + true rsds_max) | 2069 | 1950 | 1597 | 1623 | 897 | 1126 |
| Lucknow | INCUMBENT swbgt_empirical, AS DEPLOYED | 2231 | 1783 | 1815 | 1536 | 1218 | 1173 |
| Lucknow | Tier 2 on daily-mean inputs (today's aggregation) | 2231 | 1390 | 1815 | 636 | 1218 | 75 |
| Lucknow | Tier 2 as specified (tasmax-driven shade) | 2231 | 2481 | 1815 | 2109 | 1218 | 1556 |
| Lucknow | Tier 2, true hourly rsds_max (no disaggregation error) | 2231 | 2636 | 1815 | 2341 | 1218 | 1911 |
| Lucknow | Tier 2 CEILING (hourly shade max + true rsds_max) | 2231 | 2211 | 1815 | 1797 | 1218 | 1279 |
| Hyderabad | INCUMBENT swbgt_empirical, AS DEPLOYED | 2262 | 2019 | 1205 | 732 | 511 | 62 |
| Hyderabad | Tier 2 on daily-mean inputs (today's aggregation) | 2262 | 542 | 1205 | 36 | 511 | 0 |
| Hyderabad | Tier 2 as specified (tasmax-driven shade) | 2262 | 2797 | 1205 | 1765 | 511 | 978 |
| Hyderabad | Tier 2, true hourly rsds_max (no disaggregation error) | 2262 | 3223 | 1205 | 2454 | 511 | 1504 |
| Hyderabad | Tier 2 CEILING (hourly shade max + true rsds_max) | 2262 | 2293 | 1205 | 1319 | 511 | 583 |
| Shimla | INCUMBENT swbgt_empirical, AS DEPLOYED | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 on daily-mean inputs (today's aggregation) | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 as specified (tasmax-driven shade) | 15 | 4 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2, true hourly rsds_max (no disaggregation error) | 15 | 152 | 1 | 1 | 0 | 0 |
| Shimla | Tier 2 CEILING (hourly shade max + true rsds_max) | 15 | 21 | 1 | 0 | 0 | 0 |

## Reading this table

- The four Tier-2 rows differ only in how much input error is removed.
  Compare them downward: `CEILING` is the adjustment model judged on
  perfect inputs, and everything above it is the cost of an input
  approximation the production pipeline would actually make.
- A row that passes on `bias` while `r` is low and `uplift` is large is
  the sWBGT failure mode repeating: the right average heat added to the
  wrong day-to-day signal.
- `p99 bias` decides the shipped `*_days_ge_*` slugs. A tail bias with
  the opposite sign to the mean bias means threshold counts will be
  wrong even where the annual mean looks right.
