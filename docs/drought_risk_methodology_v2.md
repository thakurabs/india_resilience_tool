# Drought Risk Methodology v2

Drought Risk v2 uses grid-first Standardized Precipitation Index (SPI) metrics. The canonical SPI implementation is `climate-indices==2.2.0`; the legacy z-score SPI path is non-conformant with WMO SPI methodology and is rejected by the pipeline.

## Metric Set

| Metric | Weight | Annual value | Period rollup |
|---|---:|---|---|
| `spi3_count_events_lt_minus1` | 0.08 | count of within-year runs where SPI3 `< -1.0` | `period_mean` |
| `spi6_count_events_lt_minus1` | 0.12 | count of within-year runs where SPI6 `< -1.0` | `period_mean` |
| `spi12_count_events_lt_minus1` | 0.20 | count of within-year runs where SPI12 `< -1.0` | `period_mean` |
| `spi3_max_spell_lt_minus1` | 0.12 | longest within-year SPI3 run `< -1.0` | `period_max` |
| `spi6_max_spell_lt_minus1` | 0.18 | longest within-year SPI6 run `< -1.0` | `period_max` |
| `spi12_max_spell_lt_minus1` | 0.30 | longest within-year SPI12 run `< -1.0` | `period_max` |

Calibration uses baseline years `(1981, 2010)`. The historical comparison period for anchored Drought composite normalization is `1990-2010`.

## Floors

| Parameter | Value |
|---|---:|
| `min_months_per_year` | 9 |
| `min_event_months` | 1 |
| `min_baseline_years_per_calendar_month_fraction` | 0.83 |
| `min_years_per_period_fraction` | 0.75 |
| `min_polygon_cell_weight_fraction` | 0.50 |
| `min_anchored_components` | 4 |

## Processing

Daily `pr` grids are converted to monthly totals with unit checks. Flux units such as `kg m-2 s-1`, `kg m**-2 s**-1`, `kg/m2/s`, and `kg/m^2/s` are multiplied by `86400`; daily depth/rate units such as `mm/day`, `mm d-1`, `mm/d`, and `mm` are used as-is. Blank or unknown units raise `ValueError`. A monthly total is valid only when at least `ceil(0.90 * days_in_month)` daily values are finite.

Before SPI math, monthly baseline and scenario data are reindexed to a contiguous month-start axis. Missing calendar gaps, such as 2011-2014 between a 1981-2010 baseline and SSP scenario data, are filled with NaN so rolling SPI windows cannot silently bridge non-adjacent months. The contiguous series is trimmed to complete Jan-Dec years before calling `climate-indices`.

SPI is computed per grid cell, then annual count or spell metrics are derived per cell. Period rollups happen per cell before polygon aggregation. Polygon values drop NaN cells, renormalize weights over finite cells, and emit NaN when retained overlap weight is below `0.50`. Period diagnostics use polygon-specific area-weighted retained cells rather than a global best-cell count.

Private diagnostics and caches live under `processed/_internal/drought_risk/`; no Drought GeoTIFF outputs are produced. Annual and period grid caches include `input_file_hashes`, `grid_id`, distribution, `climate_indices_version`, and a NetCDF `cache_blob_sha256`; mismatches are cache misses.

## Worked Examples

SPI12 warm-up: the first 11 months of a scenario do not have a complete 12-month accumulation window, so they are expected to be NaN and cannot contribute to annual validity.

Calibration coverage failure: for baseline `(1981, 2010)`, each calendar month needs at least 25 finite monthly precipitation values. A cell with only 24 finite Januaries emits all-NaN SPI.

Polygon NaN-cell retention: with weights `0.4, 0.4, 0.2` and values `6, NaN, 9`, retained weight is `0.6`; the polygon value is `(0.4*6 + 0.2*9) / 0.6 = 7.0`.

Year-boundary truncation: a drought spell from October through the following September is split by calendar year. Year Y gets a max spell of 3 months, Year Y+1 gets 9 months, and period max is 9, not 12.

Synthetic NaN probes for `climate-indices==2.2.0` are covered by a hygiene test when the package is installed in the test environment. Environments without the package skip that probe, while the adapter and grid-cell paths still avoid IRT-side NaN-to-zero coercion.

## Pre-Landing Audits

`--spi-legacy` references remain only as compatibility/rejection surfaces in `tools/pipeline/compute_indices_multiprocess.py`, `tools/runs/prepare_dashboard.py`, and `tests/test_spi_hygiene.py`. No CI script currently invokes the flag.

Inventory of actual `pr` unit strings across `IRT_DATA_DIR` should be run before production rebuilds; strict parsing accepts flux units (`kg m-2 s-1`, `kg m**-2 s**-1`, `kg/m2/s`) and daily depth/rate units (`mm/day`, `mm d-1`, `mm/d`, `mm`), and rejects unknown or blank values.
