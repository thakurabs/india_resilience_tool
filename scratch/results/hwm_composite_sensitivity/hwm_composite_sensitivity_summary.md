# HWM Composite Sensitivity Summary

Data dir: `D:\projects\irt_data`
Rows: `781` district rows across `35` states
Fidelity max abs diff vs persisted parquet: `0`
NaN aligned: `True`
Shuffle seed: `20260715`
States with <3 districts: `4`

## Within-State View (Production-Matching Normalization)
```text
        view                     scenario  delta_n  delta_mean  delta_std  delta_p05  delta_median  delta_p95  delta_min  delta_max  median_abs_rank_shift  p95_abs_rank_shift  moved_gt_5  moved_gt_10  district_rows       seed  states  median_spearman  median_kendall  median_state_p95_abs_rank_shift
within_state            S1_correlated_hwa      781    0.296377   1.015515  -1.363803      0.314369   1.841544  -2.175578   2.662696                    0.0                 3.0           7            0            781        NaN      31         0.994216        0.960474                             1.00
within_state S2_cold_favoring_tas_inverse      781   -0.991788   2.559608  -4.387648     -1.417296   4.166667  -6.250000   6.250000                    0.0                 3.0          16            2            781        NaN      31         0.990196        0.948052                             1.75
within_state              S3_shuffled_hwa      781    0.296377   1.977072  -3.394957      0.470798   3.347704  -5.641815   6.181406                    1.0                 5.0          39           11            781 20260715.0      31         0.981931        0.913978                             2.90
```

## National-Pooled View (Caveated, Not Production)
```text
                    view                     scenario  delta_n  delta_mean  delta_std  delta_p05  delta_median  delta_p95  delta_min  delta_max  spearman  kendall  median_abs_rank_shift  p95_abs_rank_shift  moved_gt_5  moved_gt_10  district_rows       seed
national_pooled_caveated            S1_correlated_hwa      781    1.232209   0.452614   0.372198      1.382558   1.742241  -0.564315   1.859816  0.998304 0.971135                    6.0                24.0         403          237            781        NaN
national_pooled_caveated S2_cold_favoring_tas_inverse      781   -2.920065   1.572683  -4.155555     -3.408005   0.772850  -4.359653   5.718109  0.998569 0.971056                    7.0                24.0         436          262            781        NaN
national_pooled_caveated              S3_shuffled_hwa      781    1.232209   1.235372  -1.078941      1.393976   2.985961  -4.343104   5.410927  0.975775 0.890673                   20.0                97.0         607          510            781 20260715.0
```

## Notes
- `S1_correlated_hwa` is the reweight-only floor because proxied HWM follows normalized HWA.
- `S2_cold_favoring_tas_inverse` is an assumption-heavy anomaly-lens proxy using `100 - norm(tas_annual_mean)` per comparison frame.
- `S3_shuffled_hwa` is a reproducible noise ceiling using the recorded seed.
- The national-pooled pass normalizes raw component columns once across India and is not how production persisted composites are built.