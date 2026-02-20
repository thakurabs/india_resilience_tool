# India Resilience Tool (IRT) - Codebase Manifest

## Overview

The India Resilience Tool (IRT) is a Streamlit-based dashboard for exploring climate resilience metrics across Indian administrative units at **two levels**:

- **Districts (ADM2)**
- **Blocks / Sub-districts (ADM3)**

IRT visualizes ensemble climate model outputs and derived indices, enabling comparison of temperature and rainfall metrics across scenarios and time periods, and supports **portfolio comparison** of multiple districts or blocks.

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in  
**Tech Stack:** Python 3.10+, Streamlit, Pandas, GeoPandas, Folium, Matplotlib

---

## Quick Reference

### Entry Points
| Command | Purpose |
|---------|---------|
| `streamlit run dashboard_unfactored.py` | Launch dashboard (recommended shim) |
| `streamlit run india_resilience_tool/india_resilience_tool/app/main.py` | Launch dashboard (package entry) |
| `python build_master_metrics.py` | Rebuild master metrics (Parquet default; district + block) |
| `python compute_indices_multiprocess.py` | Build processed index artifacts (district default) |
| `python compute_indices_multiprocess.py --level block` | Build processed artifacts at block level |
| `python compute_indices_multiprocess.py --ensembles-only` | Rebuild ensembles from existing raw outputs |

### Key Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default state to load |
| `IRT_DATA_DIR` | (from `paths.py`) | Base data directory (boundaries + processed) |
| `IRT_PROCESSED_SUBDIR` | `processed` | Processed subdir under `DATA_DIR/` (e.g., `processed_test`) |
| `IRT_PROCESSED_ROOT` | (unset) | Processed data location override (wins over defaults) |
| `IRT_DEBUG` | `0` | Enable debug output |

---

## Project Structure

```
india_resilience_tool/
├── __init__.py
├── analysis/ # Data analysis & computation
│ ├── __init__.py
│ ├── case_study.py # Case-study exports and helpers
│ ├── metrics.py # Risk classification
│ ├── portfolio.py # Portfolio logic & state (district + block)
│ └── timeseries.py # Time series loading (district + block)
├── app/ # Streamlit application
│ ├── __init__.py
│ ├── adm2_cache.py # District boundary caching/simplification
│ ├── dashboard.py # Dashboard entry wrapper
│ ├── legacy_dashboard_impl.py # Main orchestrator (district + block + bundles)
│ ├── main.py # Streamlit entry module (run via Streamlit)
│ ├── orchestrator.py # Module executor
│ ├── perf.py # Performance helpers / timing
│ ├── point_selection_ui.py # Coordinate input & batch support (district + block)
│ ├── portfolio_ui.py # Portfolio management panel (district + block + bundles)
│ ├── sidebar.py # Sidebar controls & navigation (admin level + focus)
│ ├── state.py # Session state defaults & constants
│ └── views/ # View renderers
│ ├── __init__.py
│ ├── details_panel.py # Details panel (district + block; exports + case studies)
│ ├── map_view.py # Choropleth map (district + block)
│ ├── rankings_view.py # Rankings (district + block) + portfolio add parity
│ └── state_summary_view.py # State summary view (district-first, optional)
├── compute/ # Computation helpers
│ ├── __init__.py
│ └── spi_adapter.py # SPI adapter around climate-indices
├── config/ # Configuration
│ ├── __init__.py
│ ├── constants.py # App constants & styling
│ ├── metrics_registry.py # Unified metric definitions + bundles (single source of truth)
│ ├── paths.py # Library path semantics (mirrors root paths.py)
│ └── variables.py # Dashboard variable registry (imports from metrics_registry)
├── data/ # Data loading & processing
│ ├── __init__.py
│ ├── adm2_loader.py # GeoJSON district loading (ADM2)
│ ├── adm3_loader.py # GeoJSON block loading (ADM3)
│ ├── boundary_loader.py # Unified boundary loader API
│ ├── discovery.py # Processed-artifact discovery helpers
│ ├── master_loader.py # Master CSV loading (district + block)
│ └── merge.py # Merge utilities (ADM2/ADM3)
├── utils/ # Utilities
│ ├── __init__.py
│ └── naming.py # Name normalization & aliases
└── viz/ # Visualization
├── __init__.py
├── charts.py # Chart/figure generation
├── colors.py # Color scales & legends
├── exports.py # PDF/ZIP export generation
├── style.py # Shared plotting/table styling
└── tables.py # Table formatting

Root files:
├── dashboard_unfactored.py # Streamlit entry shim (recommended)
├── dashboard_unfactored_impl.py # Legacy monolithic implementation (kept for reference)
├── paths.py # DATA_DIR + processed-root configuration (canonical)
├── build_master_metrics.py # Master CSV builder script
├── compute_indices.py # Single-process index compute
├── compute_indices_multiprocess.py # Multi-process index compute
├── environment.yml # Conda environment (pinned)
├── requirements.txt # pip freeze (UTF-16)
├── docs/ # Additional docs/notes
└── tests/ # Test suite
```

---

## Data Contracts (District + Block)

### Boundary Files (EPSG:4326)
- District (ADM2): `DATA_DIR/districts_4326.geojson`
- Block (ADM3): `DATA_DIR/blocks_4326.geojson`

The loaders normalize key fields into:
- district: `state_name`, `district_name`, `geometry`
- block: `state_name`, `district_name`, `block_name`, `geometry`

### Processed Artifacts Layout (Parquet-first; CSV fallback)

For each index slug (e.g., `tas_gt32`):

```
DATA_DIR/{processed_subdir}/{index_slug}/{state}/
├── master_metrics_by_district.parquet
├── master_metrics_by_block.parquet
├── districts/
│   ├── raw/
│   │   ├── yearly/model={model}/scenario={scenario}/data.parquet
│   │   └── periods/model={model}/scenario={scenario}/data.parquet
│   └── ensembles/yearly/
│       └── scenario={scenario}/data.parquet
└── blocks/
    ├── raw/
    │   ├── yearly/model={model}/scenario={scenario}/data.parquet
    │   └── periods/model={model}/scenario={scenario}/data.parquet
    └── ensembles/yearly/
        └── scenario={scenario}/data.parquet
```

#### Master metrics schema convention
Master tables follow the naming convention:
`{metric}__{scenario}__{period}__{stat}`

Example:
- `days_gt_32C__ssp245__2020-2040__mean`

The master tables include identifier columns:
- District master: `state`, `district`
- Block master: `state`, `district`, `block`

---

## Module Reference (updated for block support + bundles)

### 1) Configuration (`india_resilience_tool/config/`)

#### `metrics_registry.py` (NEW: single source of truth)
**Purpose:** Unified metric definitions used by both dashboard and pipeline.

**Recent addition (Heat Risk): Wet-bulb temperature indices**
- `twb_annual_mean` (°C): Annual mean wet-bulb temperature
- `twb_annual_max` (°C): Annual maximum wet-bulb temperature
- `twb_days_ge_30` (days): Count of days with wet-bulb temperature ≥ 30°C

**Multi-variable metrics:** Some metrics depend on multiple raw variables and may specify `vars` (e.g., `["tas", "hurs"]`) instead of a single `var`. The compute pipeline schedules such metrics only for model/scenario combinations where *all required variables exist* and processes only years available across all required variables (intersection).


Key exports:
- `PIPELINE_METRICS_RAW`: list of metric definition dicts
- `METRICS_BY_SLUG`: dict of `slug → MetricSpec`
- `MetricSpec`: dataclass with all metric attributes
- `BUNDLES`: thematic bundle → slug mapping
- `BUNDLE_ORDER`: UI display order for bundles
- `BUNDLE_DESCRIPTIONS`: help text for each bundle
- `DEFAULT_BUNDLE`: default bundle for single-focus mode ("Heat Risk")

Bundle helper functions:
- `get_bundles()` → list of bundle names in display order
- `get_metrics_for_bundle(bundle)` → list of slugs
- `get_bundle_for_metric(slug)` → list of bundles containing metric
- `get_bundle_description(bundle)` → description string
- `get_default_bundle()` → default bundle name
- `get_metric_options_for_bundle(bundle)` → list of (slug, label) tuples
- `validate_bundles()` → validation issues list

When to modify:
- Add new metric: add to `PIPELINE_METRICS_RAW`
- Add/modify bundle: update `BUNDLES`, `BUNDLE_ORDER`, `BUNDLE_DESCRIPTIONS`
- Change default bundle: update `DEFAULT_BUNDLE`

#### `variables.py`
**Purpose:** Dashboard-facing variable registry (thin wrapper around metrics_registry).

Key exports:
- `VARIABLES`: index definitions (auto-generated from metrics_registry)
- `INDEX_GROUP_LABELS`: display labels for base groups (temperature/rain)
- `get_index_groups()`, `get_indices_for_group(...)`: legacy group-based access
- All bundle exports re-exported from metrics_registry

When to modify:
- Rarely; prefer modifying metrics_registry.py directly

#### `constants.py`
App styling and constants; no admin-level-specific logic.

---

### 2) Data layer (`india_resilience_tool/data/`)

#### `adm2_loader.py`
Loads district boundaries (ADM2), simplifies geometry, builds ADM1 boundaries by dissolving ADM2 if needed.

#### `adm3_loader.py`
Loads block boundaries (ADM3), standardizes naming, ensures key columns for merging and UI selection.

#### `master_loader.py`
Loads and normalizes master metrics tables (Parquet preferred, CSV fallback) for both:
- `master_metrics_by_district.parquet` (or `.csv`)
- `master_metrics_by_block.parquet` (or `.csv`)

#### `merge.py`
Merges master metrics with boundaries.
- district merges use ADM2
- block merges use ADM3 (plus district context)

---

### 3) Analysis layer (`india_resilience_tool/analysis/`)

#### `portfolio.py`
**Purpose:** Portfolio state + comparison table builder for:
- district portfolios
- block portfolios

Session state keys (typical):
- `portfolio_districts`: list of `{"state": ..., "district": ...}`
- `portfolio_blocks`: list of `{"state": ..., "district": ..., "block": ...}`

#### `metrics.py`
Risk classification + rank/percentile helpers.

#### `timeseries.py`
Loads yearly time series for district/block:
- district (new): from `districts/ensembles/yearly/scenario={scenario}/data.parquet`
- block (new): from `blocks/ensembles/yearly/scenario={scenario}/data.parquet`
- legacy CSV fallback paths are still supported (per-unit `*_yearly_ensemble.csv`)

Implementation detail:
- many ensemble-yearly CSVs do not include identifier columns
- loader injects missing identifiers (e.g., `state`, `district`, `block`, `scenario`) from context so the UI can filter consistently

---

### 4) Visualization layer (`india_resilience_tool/viz/`)
Unchanged structurally, but used by both district and block details/portfolio panels where applicable.
- `charts.py`: scenario comparison, comparison panels
- `exports.py`: PDF export (often district-first)
- `tables.py`: rankings and formatted tables

---

### 5) Application layer (`india_resilience_tool/app/`)

#### `legacy_dashboard_impl.py`
Main orchestrator. Responsibilities:
- admin level toggle (district/block)
- **Map View ribbon** (above the map) for selecting:
  - risk domain (bundle) → metric
  - scenario, period, statistic (mean/median)
  - map mode (absolute vs change from 1990–2010 baseline)
- placeholder-first ribbon UX (`— Select —`) with safe gating (avoid invalid/partial renders)
- state/district/block selection widgets (sidebar)
  - available states are discovered after metric selection (processed-root depends on metric slug)
- data root resolution (`PROCESSED_ROOT` per index slug)
- chooses correct master table by admin level:
  - district: `master_metrics_by_district.parquet` (CSV fallback)
  - block: `master_metrics_by_block.parquet` (CSV fallback)
- routes to map/rankings/details/portfolio panels

#### `perf.py`
Lightweight performance helpers used in the app (timing wrappers / counters) to support profiling and regressions.

#### `sidebar.py`
Renders:
- admin level (district/block) selection
- analysis mode (single vs portfolio), placeholder-first
- view selector (map vs rankings)
- portfolio quick stats display
- hover toggle

#### `portfolio_ui.py`
Renders portfolio panel with:
- portfolio badge and list management
- **bundle-first metric selection** for comparison (multi-select bundles → auto-expand to metrics)
- optional manual metric refinement
- comparison table with heatmap visualization
- coordinate-based unit lookup

Key widget keys:
- `portfolio_bundle_selection`: selected bundles for comparison
- `portfolio_manual_refinement`: whether manual metric selection is enabled
- `portfolio_multiindex_selection`: final list of metric slugs

#### `views/map_view.py`
Renders choropleth map and handles click payload.
- district click extracts district/state
- block click extracts block/district/state when available
- portfolio highlighting supported for districts and blocks

#### `views/rankings_view.py`
Rankings table renderer with portfolio integration.
- district and block rankings supported
- portfolio mode detection is level-agnostic (so "Multi-block portfolio" behaves like "Multi-district portfolio")

#### `point_selection_ui.py`
Coordinate-based lookup supports adding units to portfolio in both district and block mode (exact mapping depends on available geometry for the current admin level).

---


### 6) Compute layer (`india_resilience_tool/compute/`)

#### `spi_adapter.py`
**Purpose:** Adapter around the `climate-indices` package to compute SPI (and related drought indices) in a way that fits IRT's data conventions and file layout.

Typical responsibilities:
- Accept IRT-aligned precipitation time-series inputs (units and temporal aggregation are explicit in the calling code)
- Handle edge cases (e.g., long zero-precip periods) consistently
- Return SPI arrays/series aligned to the original time index so downstream aggregation works cleanly

See also:
- `spi_diagnostic.py` (repo root): sanity checks / distribution diagnostics for SPI outputs

## Thematic Bundles

Bundles organize metrics into risk-domain groupings for user-friendly selection. A metric may appear in multiple bundles.

### Available Bundles

| Bundle | Metrics | Description |
|--------|---------|-------------|
| Heat Risk | 24 | Heat extremes, heatwaves, thermal stress |
| Cold Risk | 10 | Cold thresholds, frost days, cold spells |
| Agriculture & Growing Conditions | 4 | Growing season, seasonal temperatures, DTR |
| Flood & Extreme Rainfall Risk | 12 | Peak intensity, heavy rain days, wet spells |
| Rainfall Totals & Typical Wetness | 3 | Annual totals, intensity, rainy days |
| Drought Risk | 8 | Dry spells, SPI, SPEI indices |
| Temperature Variability | 2 | DTR, ETR |

### Bundle Usage in UI

**Single-focus mode (Map View ribbon):**
1. Select **Risk domain** (bundle)
2. Select **Metric** (filtered to the bundle)
3. Complete **Scenario / Period / Statistic / Map mode** in the ribbon

**Portfolio mode (comparison panel):**
1. Select one or more bundles (multi-select)
2. Metrics auto-expand from selected bundles
3. Optional: enable "Manually refine" to add/remove individual metrics

### Adding a New Bundle

1. Edit `metrics_registry.py`:
   ```python
   BUNDLES["New Bundle Name"] = [
       "slug1",
       "slug2",
       # ...
   ]
   ```
2. Add to `BUNDLE_ORDER` for UI ordering
3. Add description to `BUNDLE_DESCRIPTIONS`
4. Run validation: `python -c "from india_resilience_tool.config.metrics_registry import validate_bundles; print(validate_bundles())"`

---

## Data flow (district and block)

High-level flow is the same; the admin level controls which geometry and master table are used:

1) Selection →
   - Sidebar: admin level, analysis focus, state/district/block
   - Ribbon (Map View): bundle, metric, scenario, period, statistic, map mode
2) Load boundaries (ADM2/ADM3) and master table (district/block)
3) Merge → `merged` GeoDataFrame
4) Render view (map / rankings / details / portfolio)
5) Time series (trend) loads ensemble yearly series if present

---

## Common tasks (district + block + bundles)

| Task | Primary Module(s) | Notes |
|------|-------------------|------|
| Add new metric | `config/metrics_registry.py` | Add to `PIPELINE_METRICS_RAW` + ensure processed data exists |
| Add metric to bundle | `config/metrics_registry.py` | Add slug to appropriate `BUNDLES[...]` list |
| Create new bundle | `config/metrics_registry.py` | Add to `BUNDLES`, `BUNDLE_ORDER`, `BUNDLE_DESCRIPTIONS` |
| Change default bundle | `config/metrics_registry.py` | Update `DEFAULT_BUNDLE` |
| Build block master metrics | `build_master_metrics.py` | produces `master_metrics_by_block.parquet` (see `--cleanup-raw` / `--prune-legacy-csv`) |
| Update block tooltip/map click | `app/views/map_view.py` | ensure block identifiers flow to state |
| Enable add-to-portfolio from block rankings | `app/views/rankings_view.py` | portfolio parity with district |
| Fix time series loading | `analysis/timeseries.py`, `data/discovery.py` | ensemble yearly discovery + id injection |
| Boundary merge changes | `data/merge.py` | ADM2 vs ADM3 merge logic |

---

## Session State (updated)

Core keys (typical):
- `admin_level`: `"district"` or `"block"`
- `analysis_mode`: one of:
  - `"Single district focus"` / `"Multi-district portfolio"`
  - `"Single block focus"` / `"Multi-block portfolio"`
- `selected_state`, `selected_district`, `selected_block`
- `selected_bundle`: currently selected risk domain (Map View ribbon)
- `selected_var`: currently selected metric slug (Map View ribbon)
- `sel_scenario`, `sel_period`, `sel_stat`: scenario/period/statistic (Map View ribbon)
- `map_mode`: map mode (Map View ribbon)
- `portfolio_districts`, `portfolio_blocks`
- `portfolio_bundle_selection`: bundles selected for portfolio comparison
- `portfolio_manual_refinement`: whether manual metric selection is enabled
- `portfolio_multiindex_selection`: final metric slugs for comparison

Other UI keys vary by panel (map markers, etc.).

---

## File locations (updated)

| Data Type | Location |
|-----------|----------|
| District boundaries | `DATA_DIR/districts_4326.geojson` |
| Block boundaries | `DATA_DIR/blocks_4326.geojson` |
| District master | `DATA_DIR/{processed_subdir}/{index}/{state}/master_metrics_by_district.parquet` |
| Block master | `DATA_DIR/{processed_subdir}/{index}/{state}/master_metrics_by_block.parquet` |
| District ensemble yearly | `.../districts/ensembles/yearly/scenario={scenario}/data.parquet` (CSV fallback supported) |
| Block ensemble yearly | `.../blocks/ensembles/yearly/scenario={scenario}/data.parquet` (CSV fallback supported) |
| Raw per-model intermediates | `.../{districts|blocks}/raw/{yearly|periods}/model={model}/scenario={scenario}/data.parquet` |
| PDF exports (if enabled) | `DATA_DIR/{processed_subdir}/{index}/{state}/pdf_plots/` |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.3 | 2026-01 | Thematic bundles for metric organization + bundle-first selection UI |
| 2.2 | 2026-01 | Block (ADM3) map + rankings + portfolio parity + trend support |
| 2.1 | 2024-12 | Portfolio UX improvements |
| 2.0 | 2024-12 | Modular refactor |
| 1.0 | 2024-Q4 | Initial monolithic dashboard |

---

## Contact

For questions about this codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** absthakur@resilience.org.in
