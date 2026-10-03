# Does the Tier-2 sun adjustment fix the outdoor WBGT metric?

Generated 2026-09-22 17:46 UTC.

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
| Kochi | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -1.27 | 1.21 | 1.78 | 0.832 | -6.45 | +3.32 | FAIL |
| Kochi | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -1.27 | 1.21 | 1.78 | 0.832 | -6.45 | +3.32 | FAIL |
| Kochi | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | -0.41 | 0.82 | 1.41 | 0.799 | -5.67 | +4.19 | PASS |
| Kochi | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.22 | 2.21 | 2.66 | 0.815 | -2.37 | +4.71 | FAIL |
| Kochi | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.14 | 0.78 | 1.40 | 0.784 | -6.16 | +4.71 | PASS |
| Kolkata | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -0.50 | 1.84 | 2.50 | 0.906 | -2.89 | -- | FAIL |
| Kolkata | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -3.57 | 3.36 | 3.82 | 0.951 | -5.95 | +2.38 | FAIL |
| Kolkata | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.16 | 1.23 | 1.86 | 0.925 | -0.66 | +2.38 | FAIL |
| Kolkata | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -0.94 | 0.99 | 1.47 | 0.956 | -3.16 | +2.38 | PASS |
| Kolkata | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -0.94 | 0.99 | 1.47 | 0.956 | -3.16 | +2.38 | PASS |
| Kolkata | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | +0.91 | 1.18 | 1.66 | 0.934 | -1.75 | +4.23 | FAIL |
| Kolkata | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.44 | 2.44 | 2.87 | 0.923 | +0.93 | +3.66 | FAIL |
| Kolkata | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.13 | 0.46 | 0.98 | 0.969 | -2.28 | +3.66 | PASS |
| Bikaner | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.93 | 2.10 | 3.25 | 0.942 | -1.93 | -- | FAIL |
| Bikaner | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -4.42 | 4.08 | 4.93 | 0.957 | -5.09 | +2.25 | FAIL |
| Bikaner | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.01 | 1.49 | 1.99 | 0.962 | -0.41 | +2.25 | FAIL |
| Bikaner | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -0.96 | 1.17 | 1.92 | 0.964 | -2.44 | +2.25 | FAIL |
| Bikaner | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -0.96 | 1.17 | 1.92 | 0.964 | -2.44 | +2.25 | FAIL |
| Bikaner | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | +1.41 | 1.89 | 2.33 | 0.958 | -0.25 | +4.61 | FAIL |
| Bikaner | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.29 | 2.51 | 2.90 | 0.963 | +1.05 | +3.54 | FAIL |
| Bikaner | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.35 | 1.14 | 1.69 | 0.967 | -1.56 | +3.54 | FAIL |
| Lucknow | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.97 | 2.48 | 3.37 | 0.918 | -2.64 | -- | FAIL |
| Lucknow | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -4.51 | 4.28 | 4.88 | 0.954 | -5.69 | +2.46 | FAIL |
| Lucknow | Tier 2 as specified (tasmax-driven shade) | 3651 | +0.93 | 1.16 | 1.79 | 0.954 | -0.48 | +2.46 | FAIL |
| Lucknow | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -1.27 | 1.19 | 1.89 | 0.962 | -2.79 | +2.46 | FAIL |
| Lucknow | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -1.27 | 1.19 | 1.89 | 0.962 | -2.79 | +2.46 | FAIL |
| Lucknow | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | +0.67 | 1.28 | 1.83 | 0.944 | -0.74 | +4.40 | FAIL |
| Lucknow | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.18 | 2.26 | 2.69 | 0.953 | +1.00 | +3.71 | FAIL |
| Lucknow | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.28 | 0.71 | 1.34 | 0.970 | -1.58 | +3.71 | PASS |
| Hyderabad | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -1.52 | 1.83 | 2.63 | 0.730 | -4.98 | -- | FAIL |
| Hyderabad | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -3.92 | 3.76 | 4.12 | 0.889 | -6.58 | +2.31 | FAIL |
| Hyderabad | Tier 2 as specified (tasmax-driven shade) | 3651 | +1.17 | 1.26 | 1.60 | 0.920 | -0.72 | +2.31 | FAIL |
| Hyderabad | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -0.91 | 0.87 | 1.30 | 0.940 | -3.14 | +2.31 | PASS |
| Hyderabad | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -0.91 | 0.87 | 1.30 | 0.940 | -3.14 | +2.31 | PASS |
| Hyderabad | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | +1.22 | 1.45 | 1.66 | 0.909 | -1.41 | +4.44 | FAIL |
| Hyderabad | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +2.46 | 2.56 | 2.74 | 0.912 | +0.71 | +3.60 | FAIL |
| Hyderabad | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | +0.03 | 0.50 | 0.84 | 0.951 | -2.22 | +3.60 | PASS |
| Shimla | INCUMBENT swbgt_empirical, AS DEPLOYED | 3651 | -3.45 | 3.28 | 4.00 | 0.926 | -5.97 | -- | FAIL |
| Shimla | Tier 2 on daily-mean inputs (today's aggregation) | 3651 | -5.10 | 4.76 | 5.60 | 0.940 | -7.12 | +3.26 | FAIL |
| Shimla | Tier 2 as specified (tasmax-driven shade) | 3651 | -0.13 | 0.99 | 1.74 | 0.951 | -2.53 | +3.26 | FAIL |
| Shimla | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3651 | -1.81 | 1.57 | 2.47 | 0.952 | -4.49 | +3.26 | FAIL |
| Shimla | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3651 | -1.81 | 1.57 | 2.47 | 0.952 | -4.49 | +3.26 | FAIL |
| Shimla | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3651 | -0.80 | 1.17 | 2.01 | 0.941 | -3.83 | +4.27 | FAIL |
| Shimla | Tier 2, true hourly rsds_max (no disaggregation error) | 3651 | +1.11 | 1.66 | 2.13 | 0.953 | -0.97 | +4.50 | FAIL |
| Shimla | Tier 2 CEILING (hourly shade max + true rsds_max) | 3651 | -0.48 | 0.89 | 1.75 | 0.961 | -1.94 | +4.50 | FAIL |

## Day counts at the shipped thresholds

| site | candidate | >=28 ref | >=28 cand | >=30 ref | >=30 cand | >=32 ref | >=32 cand |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Kochi | INCUMBENT swbgt_empirical, AS DEPLOYED | 3595 | 3613 | 3272 | 3103 | 2113 | 776 |
| Kochi | Tier 2 on daily-mean inputs (today's aggregation) | 3595 | 2727 | 3272 | 764 | 2113 | 13 |
| Kochi | Tier 2 as specified (tasmax-driven shade) | 3595 | 3612 | 3272 | 3313 | 2113 | 2646 |
| Kochi | Tier 2 + RH at tasmax (CarbonPlan humidity) | 3595 | 3481 | 3272 | 2728 | 2113 | 1223 |
| Kochi | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 3595 | 3481 | 3272 | 2728 | 2113 | 1223 |
| Kochi | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 3595 | 3646 | 3272 | 3151 | 2113 | 1911 |
| Kochi | Tier 2, true hourly rsds_max (no disaggregation error) | 3595 | 3622 | 3272 | 3453 | 2113 | 3079 |
| Kochi | Tier 2 CEILING (hourly shade max + true rsds_max) | 3595 | 3613 | 3272 | 3336 | 2113 | 2266 |
| Kolkata | INCUMBENT swbgt_empirical, AS DEPLOYED | 2673 | 2451 | 2256 | 2178 | 1439 | 1823 |
| Kolkata | Tier 2 on daily-mean inputs (today's aggregation) | 2673 | 1861 | 2256 | 829 | 1439 | 96 |
| Kolkata | Tier 2 as specified (tasmax-driven shade) | 2673 | 2931 | 2256 | 2465 | 1439 | 1700 |
| Kolkata | Tier 2 + RH at tasmax (CarbonPlan humidity) | 2673 | 2495 | 2256 | 1804 | 1439 | 904 |
| Kolkata | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 2673 | 2495 | 2256 | 1804 | 1439 | 904 |
| Kolkata | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 2673 | 2784 | 2256 | 2452 | 1439 | 1793 |
| Kolkata | Tier 2, true hourly rsds_max (no disaggregation error) | 2673 | 3131 | 2256 | 2728 | 1439 | 2220 |
| Kolkata | Tier 2 CEILING (hourly shade max + true rsds_max) | 2673 | 2572 | 2256 | 2178 | 1439 | 1420 |
| Bikaner | INCUMBENT swbgt_empirical, AS DEPLOYED | 2069 | 1629 | 1597 | 1348 | 897 | 980 |
| Bikaner | Tier 2 on daily-mean inputs (today's aggregation) | 2069 | 1275 | 1597 | 630 | 897 | 45 |
| Bikaner | Tier 2 as specified (tasmax-driven shade) | 2069 | 2231 | 1597 | 1887 | 897 | 1466 |
| Bikaner | Tier 2 + RH at tasmax (CarbonPlan humidity) | 2069 | 1892 | 1597 | 1504 | 897 | 854 |
| Bikaner | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 2069 | 1892 | 1597 | 1504 | 897 | 854 |
| Bikaner | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 2069 | 2277 | 1597 | 1971 | 897 | 1611 |
| Bikaner | Tier 2, true hourly rsds_max (no disaggregation error) | 2069 | 2408 | 1597 | 2100 | 897 | 1770 |
| Bikaner | Tier 2 CEILING (hourly shade max + true rsds_max) | 2069 | 1950 | 1597 | 1623 | 897 | 1126 |
| Lucknow | INCUMBENT swbgt_empirical, AS DEPLOYED | 2231 | 1783 | 1815 | 1536 | 1218 | 1173 |
| Lucknow | Tier 2 on daily-mean inputs (today's aggregation) | 2231 | 1390 | 1815 | 636 | 1218 | 75 |
| Lucknow | Tier 2 as specified (tasmax-driven shade) | 2231 | 2481 | 1815 | 2109 | 1218 | 1556 |
| Lucknow | Tier 2 + RH at tasmax (CarbonPlan humidity) | 2231 | 2094 | 1815 | 1567 | 1218 | 796 |
| Lucknow | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 2231 | 2094 | 1815 | 1567 | 1218 | 796 |
| Lucknow | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 2231 | 2411 | 1815 | 2064 | 1218 | 1547 |
| Lucknow | Tier 2, true hourly rsds_max (no disaggregation error) | 2231 | 2636 | 1815 | 2341 | 1218 | 1911 |
| Lucknow | Tier 2 CEILING (hourly shade max + true rsds_max) | 2231 | 2211 | 1815 | 1797 | 1218 | 1279 |
| Hyderabad | INCUMBENT swbgt_empirical, AS DEPLOYED | 2262 | 2019 | 1205 | 732 | 511 | 62 |
| Hyderabad | Tier 2 on daily-mean inputs (today's aggregation) | 2262 | 542 | 1205 | 36 | 511 | 0 |
| Hyderabad | Tier 2 as specified (tasmax-driven shade) | 2262 | 2797 | 1205 | 1765 | 511 | 978 |
| Hyderabad | Tier 2 + RH at tasmax (CarbonPlan humidity) | 2262 | 1754 | 1205 | 919 | 511 | 270 |
| Hyderabad | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 2262 | 1754 | 1205 | 919 | 511 | 270 |
| Hyderabad | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 2262 | 2869 | 1205 | 1869 | 511 | 913 |
| Hyderabad | Tier 2, true hourly rsds_max (no disaggregation error) | 2262 | 3223 | 1205 | 2454 | 511 | 1504 |
| Hyderabad | Tier 2 CEILING (hourly shade max + true rsds_max) | 2262 | 2293 | 1205 | 1319 | 511 | 583 |
| Shimla | INCUMBENT swbgt_empirical, AS DEPLOYED | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 on daily-mean inputs (today's aggregation) | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 as specified (tasmax-driven shade) | 15 | 4 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 + RH at tasmax (CarbonPlan humidity) | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 + RH at tasmax, CarbonPlan three-term ISO form | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2 + RH at tasmax, wind frozen at 0.5 m/s (no sfcWind) | 15 | 0 | 1 | 0 | 0 | 0 |
| Shimla | Tier 2, true hourly rsds_max (no disaggregation error) | 15 | 152 | 1 | 1 | 0 | 0 |
| Shimla | Tier 2 CEILING (hourly shade max + true rsds_max) | 15 | 21 | 1 | 0 | 0 | 0 |

## Does `sfcWind` have to be downloaded at all?

The adjustment's only wind term is `+1.0424 * sfcWind`, clipped to 0.5-3 m/s, so freezing wind at CarbonPlan's 0.5 m/s is bounded above by 2.61 C by construction. What matters is the size it actually reaches here.

## Humidity, and the form of the equation

`hurs at tasmax` is mean daily RH re-expressed at the daily maximum temperature at fixed vapour pressure. `clamped days` counts days pushed below Stull's 5% validity floor by that conversion. `CP form max diff` is the largest absolute gap between CarbonPlan's three-term ISO WBGT and IRT's two-term form on identical inputs -- with `tmrt = tas`, thermofeel returns `BGT = Ta`, so the two are the same equation and this column should be numerically zero.

| site | wind mean m/s | wind p95 m/s | freeze mean abs C | freeze max abs C | hurs mean % | hurs at tasmax % | clamped days | CP form max diff C |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Kochi | 1.33 | 1.97 | 0.86 | 2.61 | 82.7 | 64.4 | 0 | 2.84e-13 |
| Kolkata | 2.53 | 4.61 | 1.85 | 2.61 | 76.9 | 58.8 | 0 | 2.88e-13 |
| Bikaner | 3.66 | 6.66 | 2.36 | 2.61 | 45.9 | 32.8 | 0 | 2.84e-13 |
| Lucknow | 2.61 | 4.55 | 1.94 | 2.61 | 65.2 | 47.4 | 0 | 2.88e-13 |
| Hyderabad | 3.13 | 6.04 | 2.13 | 2.61 | 62.5 | 46.5 | 0 | 2.88e-13 |
| Shimla | 1.47 | 2.30 | 1.01 | 2.61 | 70.6 | 51.4 | 0 | 2.70e-13 |

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
