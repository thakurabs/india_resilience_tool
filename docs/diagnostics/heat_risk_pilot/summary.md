# Heat Risk national-ruler pilot — summary

**Pilot-grade. Nothing here is frozen, published, or committed to config.**

Level: `district` · states: 36 · metrics: 14 · slices: 7 · coverage gate: 70%

Weight split — baseline-referenced 0.367 / absolute-threshold 0.633


## Realized composite spread per slice (P-02, P-03)

```
 ruler   scenario    period  n_scored  composite_min  composite_median  composite_max  composite_iqr  pct_score_ge_99  pct_score_le_1
linear historical 1990-2010       784          0.060            43.595         51.584         13.114            0.000           1.276
linear     ssp245 2020-2040       784          8.227            52.900         61.980         10.403            0.000           0.000
linear     ssp245 2040-2060       784         14.036            62.981         71.614         10.036            0.000           0.000
linear     ssp245 2060-2080       784         19.307            70.389         80.318          9.248            0.000           0.000
linear     ssp585 2020-2040       784          8.811            53.940         63.186         10.142            0.000           0.000
linear     ssp585 2040-2060       784         18.265            68.744         78.332          8.991            0.000           0.000
linear     ssp585 2060-2080       784         29.720            84.150         93.002          8.867            0.000           0.000
   cdf historical 1990-2010       784          1.084            26.645         49.065         18.510            0.000           0.000
   cdf     ssp245 2020-2040       784         10.852            38.236         61.416         18.278            0.000           0.000
   cdf     ssp245 2040-2060       784         17.617            54.917         74.133         21.672            0.000           0.000
   cdf     ssp245 2060-2080       784         24.623            67.149         85.376         22.383            0.000           0.000
   cdf     ssp585 2020-2040       784         12.298            40.705         63.815         18.155            0.000           0.000
   cdf     ssp585 2040-2060       784         23.485            63.809         82.929         21.673            0.000           0.000
   cdf     ssp585 2060-2080       784         33.495            83.497         95.542         23.917            0.000           0.000
```


## Roster reconciliation (P-09, P-10)

- 784 roster districts with at least one finite metric value
- 0 roster districts with master rows but **no finite value** in any slice (regeneration gap, P-10)
- 0 roster districts with **no master row at all** (roster/boundary gap, P-09)
- 0 master district_keys absent from the roster (dropped)


## Metric fit (P-08)

- Every configured metric fitted a ruler for every ruler kind. The coverage denominator is the full configured bundle weight.


## Ruler fit (P-05, P-06)

```
 ruler                 metric_slug  pooled_n  knot_low  knot_high  n_knots  tied_fraction  modal_mass_fraction  grid21_max_score_error  grid21_mean_score_error
linear      hwa_heatwave_amplitude      5488    19.368     43.141        2          0.000                0.000                     NaN                      NaN
linear       hwfi_events_tmean_90p      5488     1.617     12.735        2          0.000                0.000                     NaN                      NaN
linear              hwfi_tmean_90p      5488    12.693    282.669        2          0.000                0.000                     NaN                      NaN
linear             tas_annual_mean      5488     8.029     30.607        2          0.000                0.000                     NaN                      NaN
linear             tas_summer_mean      5488     7.871     34.581        2          0.000                0.000                     NaN                      NaN
linear          tasmax_summer_mean      5488    13.233     41.863        2          0.000                0.000                     NaN                      NaN
linear tasmin_tropical_nights_gt25      5488     0.079    252.838        2          0.000                0.000                     NaN                      NaN
linear       tn90p_warm_nights_pct      5488    10.237     81.038        2          0.000                0.000                     NaN                      NaN
linear              tnx_annual_max      5488    13.974     35.805        2          0.000                0.000                     NaN                      NaN
linear          tx90p_hot_days_pct      5488    10.281     68.483        2          0.000                0.000                     NaN                      NaN
linear             txge30_hot_days      5488     1.736    351.981        2          0.000                0.000                     NaN                      NaN
linear    txge35_extreme_heat_days      5488     0.004    202.921        2          0.000                0.005                     NaN                      NaN
linear              txx_annual_max      5488    25.418     48.634        2          0.000                0.000                     NaN                      NaN
linear        wsdi_warm_spell_days      5488     9.313    217.429        2          0.000                0.000                     NaN                      NaN
   cdf      hwa_heatwave_amplitude      5488    11.820     44.410     5488          0.000                0.000                   1.742                    0.221
   cdf       hwfi_events_tmean_90p      5488     1.340     14.307     5488          0.000                0.000                   3.964                    0.365
   cdf              hwfi_tmean_90p      5488    10.884    359.633     5488          0.000                0.000                   4.028                    0.423
   cdf             tas_annual_mean      5488    -1.105     31.543     5488          0.000                0.000                   1.762                    0.258
   cdf             tas_summer_mean      5488    -2.192     35.807     5488          0.000                0.000                   1.726                    0.235
   cdf          tasmax_summer_mean      5488     3.516     43.123     5488          0.000                0.000                   1.755                    0.240
   cdf tasmin_tropical_nights_gt25      5488     0.000    343.288     5488          0.000                0.000                   2.293                    0.256
   cdf       tn90p_warm_nights_pct      5488    10.182     97.991     5488          0.000                0.000                   4.235                    0.393
   cdf              tnx_annual_max      5488     7.588     36.958     5488          0.000                0.000                   1.566                    0.217
   cdf          tx90p_hot_days_pct      5488    10.230     98.871     5488          0.000                0.000                   4.238                    0.476
   cdf             txge30_hot_days      5488     0.000    362.784     5488          0.000                0.000                   1.156                    0.212
   cdf    txge35_extreme_heat_days      5488     0.000    238.184     5459          0.005                0.005                   2.069                    0.252
   cdf              txx_annual_max      5488    18.198     49.544     5488          0.000                0.000                   2.026                    0.235
   cdf        wsdi_warm_spell_days      5488     6.186    359.376     5488          0.000                0.000                   3.895                    0.539
```

`grid21_*_score_error` is how far the compact 21-knot quantile grid departs from the exact mid-rank CDF on the pooled sample. Small values mean a frozen artifact can carry the grid instead of the full support (P-05, P-11).


## Ruler disagreement (P-01)

```
ruler_a ruler_b   scenario    period  n_compared  mean_abs_delta  p95_abs_delta  max_abs_delta  pct_abs_delta_gt10  pct_abs_delta_gt20  spearman_rank_corr  worst_n  worst_n_overlap  worst_decile_overlap_pct
 linear     cdf historical 1990-2010         784          13.165         18.777         19.844              78.954               0.000               0.979       50               36                    78.205
 linear     cdf     ssp245 2020-2040         784          11.747         18.512         20.087              66.582               0.128               0.989       50               41                    82.051
 linear     cdf     ssp245 2040-2060         784           7.778         15.992         17.844              34.566               0.000               0.972       50               29                    65.385
 linear     cdf     ssp245 2060-2080         784           5.737         14.017         16.703              20.026               0.000               0.941       50               25                    57.692
 linear     cdf     ssp585 2020-2040         784          10.874         17.469         18.725              59.821               0.000               0.989       50               42                    80.769
 linear     cdf     ssp585 2040-2060         784           5.950         14.172         16.241              22.449               0.000               0.954       50               26                    56.410
 linear     cdf     ssp585 2060-2080         784           6.483         17.164         19.782              25.255               0.000               0.893       50               21                    50.000
```

A high `spearman_rank_corr` with a large `mean_abs_delta` means the rulers agree on the ordering and disagree only on how far apart the districts are — which is the whole of P-01: same map shape, different claim about magnitude. A `worst_decile_overlap_pct` below ~80 means the ruler choice changes *which* districts are called worst, and the choice can no longer be defaulted.


### Where they disagree most

```
ruler_a ruler_b         state         district   scenario    period  score_a  score_b   delta  rank_a  rank_b
 linear     cdf Uttar Pradesh           Bijnor     ssp245 2020-2040   49.782   29.695 -20.087 507.000 551.000
 linear     cdf   Uttarakhand Udam Singh Nagar     ssp245 2020-2040   50.408   30.550 -19.858 488.000 542.000
 linear     cdf        Punjab         Amritsar historical 1990-2010   39.937   20.092 -19.844 493.000 535.000
 linear     cdf       Mizoram        Lawngtlai     ssp585 2060-2080   73.618   53.835 -19.782 643.000 650.000
 linear     cdf     Jharkhand        Lohardaga     ssp245 2020-2040   45.176   25.473 -19.703 591.000 618.000
 linear     cdf         Bihar           Supaul     ssp245 2020-2040   46.220   26.588 -19.632 575.000 601.000
 linear     cdf        Punjab       Tarn Taran historical 1990-2010   41.551   21.929 -19.622 458.000 503.000
 linear     cdf        Punjab       Kapurthala historical 1990-2010   40.640   21.036 -19.604 478.000 519.000
 linear     cdf         Bihar           Araria     ssp245 2020-2040   45.285   25.692 -19.593 589.000 613.000
 linear     cdf        Punjab        Gurdaspur historical 1990-2010   38.721   19.163 -19.558 519.000 546.000
```

`disagreement_spotcheck.csv` carries all 140 metric rows behind these district-slices: each metric's raw physical value (degrees C, days, percent) beside its score under each ruler. Read those values before choosing — a large `delta` on a physically tight metric is manufactured contrast, and a large `delta` on a physically wide one is contrast the linear ruler is hiding.


## Coverage gate effect (P-08)

- `linear`: 0.00% of district-slices fall below the 70% weight-coverage gate (0 states affected).
- `cdf`: 0.00% of district-slices fall below the 70% weight-coverage gate (0 states affected).


## How to read this

- If `linear` and `cdf` disagree about *which* districts are worst, the ruler choice is methodologically load-bearing and must be argued, not defaulted (P-01).
- If `composite_iqr` is small under `cdf` but the map still looks differentiated, contrast is being manufactured by uniformization (P-01/P-02).
- If `pct_score_ge_99` climbs steeply between 2040-2060 and 2060-2080, the ruler is saturating and late-century differences are being lost (P-07).
- If the baseline slice is near-uniform, that is expected under a full-span pooled ruler and is a product decision, not a defect (P-03).
- The exact mid-rank `cdf` ruler cannot return a hard 0 or 100 by construction, so `pct_score_ge_99` / `pct_score_le_1` read lower for it than for `linear` at equal saturation. Compare each ruler against itself across slices, not across rulers.
- `roster_no_master_row` and `roster_master_no_finite_value` districts are scored NaN, not 0. They lower coverage, they do not lower the composite (P-09).
- Each map is rendered on both a fixed 0-100 domain and an auto-scaled one. Comparing the two rulers is only valid on the fixed domain; the auto panels show what each ruler resolves at full contrast, and the gap between the two renderings is itself the P-02 measurement.
