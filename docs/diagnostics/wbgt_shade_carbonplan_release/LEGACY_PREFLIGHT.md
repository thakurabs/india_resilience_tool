# Deployed shade method preflight (CHG-0700/0701)

The currently published unsigned shade yearly files are the original
`0.7 × Stull(tas, hurs) + 0.3 × tas` method. They are **not** the later
`shade-peak-v1` computation and are not CarbonPlan QDM shade. The current
registry text describing `tasmax` and reconstructed humidity is inaccurate for
these deployed files and must be corrected by artifact-aware descriptions.

## Source and aggregation evidence

- The four production Wayanad/ACCESS-CM2/historical yearly files under
  `D:/projects/irt_data/processed/wbgt_shade_stull_*/Kerala/districts/Wayanad/`
  carry `method_version=heat-stress-v2-gridfirst-2`,
  `aggregation_method=gridfirst_area_weighted_mean`, and `source_file` entries
  for `tas` and `hurs` only. Their 1951 yearly-file SHA-256 values, in
  annual-mean, ≥28, ≥30, ≥32 order, are
  `a596d298c516a003e29bbe78e3011a9a701e059d8837f265f66bfa8e64757c02`,
  `40b5431d332b4c2c4e24e19499b9668e9a441c15f1da39e64e94ec9dba0884b9`,
  `5d11f8a6e60ca90068730559bb9e82dca15db98e2de6060a48498d801adf6bf4`,
  and `05c67098ef1bce3900d96683b9a661010465dd89419364c8957dcbee31ea833b`.
- The source implementation immediately before the `shade-peak-v1` change is
  commit `269be913150e65de14b55d119cacacb708e63d20`, file
  `india_resilience_tool/compute/heat_stress_gridfirst.py`, Git blob
  `e3955d6e2a147fcb97a8a7adc793d566fbab0649`, SHA-256
  `0a56ebfc6d5529f9c32f3927e891a8814a357bfd257af6a804d94b2a0385650a`.
  Its `wbgt_shade_stull_cell_c` returns `0.7 * stull_twb_c(tas_c, hurs) +
  0.3 * tas_c`.
- That same source computes `(field >= threshold)` on each daily **cell**,
  sums to an annual cell count, and only then calls `aggregate_cell_values`
  on the annual field. Thus the deployed legacy threshold counts and the new
  product both use cell-first thresholding and area transfer. Their formulas,
  support rules and missing-data handling still differ; the holdout scores are
  not adjusted for those differences.

This source-marker sample establishes the published method identity. The
production identity sidecar must separately hash every file in all four
optimised shade trees, including masters and `yearly_models`, before compatible
readers accept unsigned data. Unknown or mixed hashes remain invalid.
