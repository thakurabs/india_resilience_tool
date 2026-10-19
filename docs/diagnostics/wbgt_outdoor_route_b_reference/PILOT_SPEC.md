# Route B paired pilot — CHG-0650

Frozen before scoring the IRT-grid pilot, 2026-10-03; parent programme
`docs/wbgt_outdoor_route_b.md`. User approved completing the remaining work.
No publication, composite membership or national model-scale compute is implied.

## Scope and order

1. Acquire and verify every UHE date 1985–2014 (single stream, 230 GB cap).
2. Prepare the canonical three-state district/block support and local NEX inputs.
   ACCESS-CM2 is the first model; CMCC-CM2-SR5 tests the no-leap calendar. File
   presence is checked before extraction; header/calendar/units and selected
   arrays are checked on extraction. These two models do not validate the entire
   21-model roster. Code can resume at yearly input and per-series QDM boundaries.
3. Run historical 1990–2010 and future SSP245/SSP585 2020–2080 for both aggregation
   paths. Train full calibrations over 1985–2014; historical QDM application uses
   the full calibration series, then report 1990–2010. Future primary application
   windows are 2020–2039, 2040–2059, 2060–2080, with no overlapping-year splice.
4. Holdout: separately fit 1985–1999, apply 2000–2014. Compare seasonal means,
   p90/p95/p99 and annual >=28/30/32 counts to held-out **shade** reference. No
   outdoor WBGT is compared to a shade reference. No synchronized-weather score.
5. Application-window sensitivity: on one representative district per state
   (largest polygon area, selected before WBGT scores), compare the primary
   future windows with one 2020–2080 adjustment. Report changes without choosing
   a preferred window after seeing which matches the calibration target best.
6. Compare the 2005 results with predecessor W1 only at matching model/support
   and metric semantics. Any unmatched-support comparison is labelled separately.

## Numerical and spatial contract

- Raw shade and QDM reuse the existing checksum-pinned CarbonPlan sources and
  xclim 0.44.0 / thermofeel 1.3.0 environment. No dependency changes in IRT.
- UHE cells retain their true 0.05° footprints. The source investigation proved
  CarbonPlan's published UHE aggregation instead uses overlapping 0.25° footprints.
  That calculation is a separate reproduction control, not IRT calibration.
- Conservative reference remapping uses exact EPSG:6933 overlap areas and the
  full target-cell denominator. Missing/ocean area remains visible in fractions.
- The initial diagnostic cell-support screen is minimum UHE fraction >=0.50
  across the calibration period. Also report the >=0.95 screen as a coverage
  sensitivity. These are declared pilot support screens, not validated accuracy
  cutoffs or release rules. No missing reference days are filled.
- A model cell must have complete finite calibration inputs and finite physical
  shade output. Fixed polygon weights use retained cell intersections and are
  identical in both paths. Report the retained fraction of each whole polygon;
  units below 0.95 coverage have NaN headline comparisons. List every exclusion.
- Any invalid future driver window prevents that cell's QDM application for that
  window; zero-weight missing cells cannot contaminate unrelated units. A unit
  uses fixed weights, never a weather-dependent daily renormalization.
- Region-first uses area-weighted raw shade and the **same coarsened reference**
  as grid-first, isolating correction order. Region solar geometry uses weighted
  latitude/elevation and region-average rsds/wind. Grid-first computes these per
  cell. This is an IRT area-weighted adaptation, not exact product replication.
- MetSim uses consistent 3600 s geometry/disaggregation. Require daily-mean
  radiation conservation <=1e-8 W/m². Report radiation and wind clipping fractions.
- Preserve native model calendars for QDM; remove Feb 29 for IRT reporting and
  require a complete 365-day year. Missing source weather is never interpolated.
- Report regional-series exceedance counts for both paths, and area-weighted
  cell-level counts for grid-first. Likewise distinguish regional-series maxima
  from area-weighted cell maxima. Genuine zeros remain zero; missing years NaN.

## Gates and deliverables

Offline synthetic tests must demonstrate conserved constant fields, explicit
partial/outside support, NaN isolation, calendar completeness, and the expected
difference between count-first and regional-series threshold semantics. The QDM
integration smoke must reproduce a known constant additive adjustment, verify
train/test separation and check radiation conservation before long execution.

Measured pilot output includes source signatures, excluded cells/units, yearly
and period metrics, holdout errors, window sensitivity, clipping/conservation
diagnostics, timings/memory and cost extrapolation. No acceptance threshold is
invented for observational accuracy. A national/display/composite decision is
made only after these results exist and the limitations can be reviewed.
