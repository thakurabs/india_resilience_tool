# Sub-grid-cell climate fill (IDW) — methodology

**Status:** implemented (CHG-0305..0309). Effective scope today: Lakshadweep block level.
**Provenance:** every filled value is flagged in-data via a `climate_fill_method` master
column (`native` / `idw`).

## The problem

The climate grid is coarse (~25 km cells). A polygon far smaller than one cell — a coral
atoll of a few km² — can sit entirely inside a single grid cell. When that cell is
ocean-masked (all-NaN in the source raster), the polygon overlaps no real data even though
it overlaps the cell geometrically. Four Lakshadweep blocks are in this position: **Andrott,
Bitra, Chetlat, Kiltan**.

The symptom differs by which base aggregator runs, but the cause is one:

- **Temperature-family metrics** (`aggregate_cell_values`) drop the unit — its row is
  **absent** from the output.
- **Precip/drought metrics** (`aggregate_grid_values_with_retention`) keep the unit but
  set it **all-NaN** (the retained-weight floor NaNs a unit whose only cell is masked).

Lakshadweep at **district** level is unaffected — the single district polygon spans all the
valid island cells. **Kiltan is asymmetric**: its temperature cell is valid but its precip
cell is masked, so the fill fires only for its precip/drought metrics.

## The fill rule

For a unit that qualifies (below), estimate its value by an **inverse-distance-weighted
mean** of every finite cell on the same state-cropped grid:

- weight = `1 / max(great_circle_km, ε)^p`, with power **p = 2** and a distance floor
  **ε = 1 km** (keeps a near-coincident donor from dominating via divide-by-~0);
- distance measured from the polygon's area-weighted cell centroid;
- donors are only the finite cells inside the state crop, so they are always within the
  archipelago, never the mainland.

IDW was chosen by evidence, not preference. Leave-one-out cross-validation over the eight
valid cells in the Lakshadweep crop (widened to 24 models × historical/ssp245/ssp585 = 72
fields per family) compared three fills — global (district) mean, nearest cell, and IDW p2:

| Family | global-mean | nearest | **IDW p2** |
|---|---|---|---|
| Temperature (tas)      | 2.150 | 2.064 | **1.681** |
| Precip (pr5day)        | 19.614 | 15.209 | **14.683** |
| Drought (spi3)         | 0.565 | 0.532 | **0.497** |

IDW has the lowest mean **and** median LOO error in every family and is never badly wrong;
the two extremes each fail somewhere (global-mean near-catastrophic for precip, nearest
worst for temperature). The four atolls' real nearest-donor distances (27–83 km) all fall
inside the LOO holdout distance regime (27–202 km), so the CV is a representative — not
optimistic — proxy for the fill error.

Every fill is still an **estimate**: the model has no data over these atolls. Even IDW's
error is about the size of the local cell-to-cell spread (temperature RMSE ~1.7 °C). That is
why the fill is recorded in-data rather than made invisible.

## What fires, what does not (the guard)

A unit is filled **only if both** hold:

1. **Sub-cell** — the polygon area is smaller than half the largest grid cell it overlaps,
   both measured in the equal-area analysis CRS (EPSG:6933). Computing this in projected
   metres, not degrees, is load-bearing; a degree-area shortcut would misclassify at
   latitude. An atoll's ratio is ~0.008; a one-cell mainland block is ~1.0.
2. **No finite overlapping cell** — the unit has no real data of its own.

This deliberately leaves untouched three things with different root causes:

- **Arid SPI3 NaN** (~230 mainland blocks in Rajasthan/Gujarat/MP): these are **multi-cell**,
  so they fail the sub-cell test. SPI3 is genuinely undefined where 3-month precip ≈ 0.
- **Naming/roster mismatches** (single blocks in AP/Maharashtra/WB): a key problem, not a
  coverage problem.
- **Andaman & Nicobar**: 0 affected blocks; its blocks aggregate enough islands to carry a
  signal. No-op.

## Provenance in the data

The base aggregators are left byte-identical; the fill is a separate post-aggregation helper
(`gridfirst_spatial.subcell_idw_fill`). Each per-metric master gains a `climate_fill_method`
column, per **(unit, metric)**:

- Kiltan reads `native` in temperature masters and `idw` in precip masters.
- A **composite** reads `idw` for a unit iff **any** of its component metrics is `idw` for
  that unit, else `native`.

The flag is threaded through every stage that otherwise drops non-value columns: the yearly→
period roll-up in the pipeline, `master_builder._build_wide_master` (post-build join),
`compute_composite_master_frame` (read from the full component masters, not the value-only
wide frame), and the optimised publish whitelist
(`build_processed_optimised._select_master_columns`). Masters for metrics that never carry
the flag stay byte-identical — the column only appears where a fill actually occurred.

## Regeneration note

Filling is post-aggregation, so the cached per-cell grid fields are **not** invalidated — no
recompute-from-raw is needed. Because glance ranks and `all_states.parquet` are national, the
four new atoll values cause a tiny, expected national rank/distribution drift; verify the
national rollups, not only the Lakshadweep slice, after regeneration.
