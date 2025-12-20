# India Resilience Tool (IRT) - Codebase Manifest

## Overview

The India Resilience Tool (IRT) is a Streamlit-based dashboard for exploring climate resilience metrics across Indian districts. It visualizes ensemble climate model outputs, enabling comparison of temperature and rainfall indices across scenarios (SSP2-4.5, SSP5-8.5) and time periods (1995-2100).

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in  
**Tech Stack:** Python 3.10+, Streamlit, Pandas, GeoPandas, Folium, Matplotlib

---

## Quick Reference

### Entry Points
| Command | Purpose |
|---------|---------|
| `streamlit run dashboard_unfactored.py` | Launch dashboard |
| `python -m india_resilience_tool.app.main` | Alternative entry |
| `python build_master_metrics.py` | Rebuild master CSV |

### Key Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default state to load |
| `IRT_PROCESSED_ROOT` | `DATA_DIR/processed/{index}` | Processed data location |
| `IRT_DEBUG` | `0` | Enable debug output |

---

## Project Structure

```
india_resilience_tool/
├── __init__.py
├── analysis/                    # Data analysis & computation
│   ├── __init__.py
│   ├── metrics.py              # Risk classification
│   ├── portfolio.py            # Multi-district portfolio logic
│   └── timeseries.py           # Time series data loading
├── app/                         # Streamlit application
│   ├── __init__.py
│   ├── dashboard.py            # Dashboard entry wrapper
│   ├── legacy_dashboard_impl.py # Main orchestrator (2,538 lines)
│   ├── main.py                 # CLI entry point
│   ├── orchestrator.py         # Module executor
│   ├── point_selection_ui.py   # Saved points UI panel
│   ├── portfolio_ui.py         # Portfolio management UI
│   ├── sidebar.py              # Sidebar utilities
│   └── views/                  # View renderers
│       ├── __init__.py
│       ├── details_panel.py    # District details view
│       ├── map_view.py         # Choropleth map view
│       ├── rankings_view.py    # Rankings table view
│       └── state_summary_view.py # State summary view
├── config/                      # Configuration
│   ├── __init__.py
│   ├── constants.py            # App constants & styling
│   └── variables.py            # Climate index registry
├── data/                        # Data loading & processing
│   ├── __init__.py
│   ├── adm2_loader.py          # GeoJSON district loading
│   ├── master_loader.py        # Master CSV loading
│   └── merge.py                # DataFrame merging utilities
├── utils/                       # Utilities
│   ├── __init__.py
│   └── naming.py               # Name normalization & aliases
└── viz/                         # Visualization
    ├── __init__.py
    ├── charts.py               # Chart/figure generation
    ├── colors.py               # Color scales & legends
    ├── exports.py              # PDF export generation
    └── tables.py               # Table formatting

Root files:
├── dashboard_unfactored.py     # Entry point shim
├── paths.py                    # DATA_DIR configuration
├── build_master_metrics.py     # Master CSV builder script
└── tests/                      # Test suite
```

---

## Module Reference

### 1. Configuration (`india_resilience_tool/config/`)

#### `variables.py`
**Purpose:** Central registry of all climate indices/metrics.

**Key Exports:**
- `VARIABLES: Dict[str, Dict]` — Index definitions with labels, groups, metric columns, file patterns
- `INDEX_GROUP_LABELS: Dict[str, str]` — Display names for index groups
- `get_index_groups() -> list[str]` — Ordered list of groups
- `get_indices_for_group(group: str) -> list[str]` — Indices in a group

**When to modify:**
- Adding new climate indices
- Changing metric column names
- Updating index descriptions

**Example entry:**
```python
"tas_gt32": {
    "label": "Summer Days",
    "group": "temperature",
    "periods_metric_col": "days_gt_32C",
    "description": "Number of days with max temp > 32°C",
    "district_yearly_candidates": [...],
    "state_yearly_candidates": [...],
}
```

#### `constants.py`
**Purpose:** Application-wide constants.

**Key Exports:**
- `SIMPLIFY_TOL_ADM2`, `SIMPLIFY_TOL_ADM1` — GeoJSON simplification tolerances
- `MIN_LON`, `MAX_LON`, `MIN_LAT`, `MAX_LAT` — India bounding box
- `FIG_SIZE_PANEL`, `FIG_DPI_PANEL` — Figure dimensions
- `FONT_SIZE_*` — Font sizing constants
- `LOGO_PATH` — Logo file path

---

### 2. Data Layer (`india_resilience_tool/data/`)

#### `adm2_loader.py`
**Purpose:** Load and process district (ADM2) GeoJSON boundaries.

**Key Functions:**
- `load_local_adm2(path, tolerance, bbox, min_area)` — Load & simplify GeoJSON
- `build_adm1_from_adm2(gdf)` — Dissolve districts to state boundaries
- `enrich_adm2_with_state_names(adm2, adm1)` — Add state names to districts
- `featurecollections_by_state(gdf)` — Split into per-state FeatureCollections
- `ensure_key_column(gdf)` — Add normalized `__key` column

**Dependencies:** GeoPandas, Shapely

#### `master_loader.py`
**Purpose:** Load and parse the master metrics CSV.

**Key Functions:**
- `load_master_csv(path) -> pd.DataFrame` — Read master CSV
- `normalize_master_columns(df) -> pd.DataFrame` — Standardize column names
- `parse_master_schema(columns) -> tuple` — Extract metric/scenario/period/stat structure

**Column naming convention:** `{metric}__{scenario}__{period}__{stat}`  
Example: `days_gt_32C__ssp245__2041-2060__mean`

#### `merge.py`
**Purpose:** Merge master CSV data with GeoDataFrame.

**Key Functions:**
- `get_or_build_merged_for_index_cached(adm2, df, slug, ...)` — Cached merge operation

---

### 3. Analysis Layer (`india_resilience_tool/analysis/`)

#### `portfolio.py`
**Purpose:** Multi-district portfolio management.

**Key Functions:**
- `portfolio_add(session_state, state, district, ...)` — Add district to portfolio
- `portfolio_remove(session_state, state, district, ...)` — Remove district
- `portfolio_contains(session_state, state, district, ...)` — Check membership
- `portfolio_clear(session_state)` — Clear all districts
- `portfolio_normalize(text, alias_fn)` — Normalize names for comparison
- `build_portfolio_multiindex_df(merged, districts, metric_col)` — Build comparison DataFrame

**Session state key:** `portfolio_districts` — List of `{"state": ..., "district": ...}` dicts

#### `metrics.py`
**Purpose:** Risk classification utilities.

**Key Functions:**
- `risk_class_from_percentile(percentile) -> str` — Map percentile to risk class (low/moderate/high)

#### `timeseries.py`
**Purpose:** Load yearly time series data.

**Key Functions:**
- `load_state_yearly(ts_root, state_dir, varcfg) -> pd.DataFrame` — Load state-level yearly ensemble stats

---

### 4. Visualization Layer (`india_resilience_tool/viz/`)

#### `charts.py`
**Purpose:** Generate matplotlib figures for the dashboard.

**Key Functions:**
- `make_scenario_comparison_figure(...)` — SSP245 vs SSP585 comparison chart
- `build_scenario_comparison_panel_for_row(...)` — Full comparison panel
- `canonical_period_label(period)` — Normalize period strings

**Key Exports:**
- `PERIOD_ORDER: list[str]` — Standard period ordering
- `SCENARIO_DISPLAY: dict` — Scenario display names
- `SCENARIO_ORDER: list[str]` — Standard scenario ordering

#### `colors.py`
**Purpose:** Color scale management.

**Key Functions:**
- `apply_fillcolor(gdf, metric_col, cmap, vmin, vmax)` — Add fill colors to GeoDataFrame
- `build_vertical_gradient_legend_html(...)` — Generate HTML legend
- `get_cmap_hex_list(cmap_name, n)` — Extract hex colors from colormap

#### `exports.py`
**Purpose:** PDF report generation.

**Key Functions:**
- `make_district_case_study_pdf(...)` — Generate district case study PDF
- `make_district_case_study_pdf_bytes(...)` — Return PDF as bytes

#### `tables.py`
**Purpose:** Build formatted tables.

**Key Functions:**
- `build_rankings_table_df(merged, metric_col, ...)` — Build rankings DataFrame

---

### 5. Application Layer (`india_resilience_tool/app/`)

#### `legacy_dashboard_impl.py` (Main Orchestrator)
**Purpose:** Primary dashboard logic — coordinates all components.

**Current size:** ~2,538 lines

**Key responsibilities:**
- Page configuration and layout
- Sidebar controls (metric, scenario, period, geography selection)
- Data loading and caching
- View routing (Map/Rankings/Details/Portfolio)
- Session state management

**Key internal functions:**
- `compute_state_metrics_from_merged(...)` — Calculate state-level statistics
- `make_state_boxplot_for_districts(...)` — Generate state boxplot
- `resolve_metric_column(...)` — Find actual column name for metric
- `find_baseline_column_for_stat(...)` — Find baseline period column
- `_portfolio_add/remove/contains/clear(...)` — Portfolio wrappers

**Session state keys used:**
- `analysis_mode` — "Single district focus" | "Multi-district portfolio"
- `portfolio_districts` — List of selected districts
- `selected_state`, `selected_district` — Current geography
- `selected_var` — Current index slug
- `sel_scenario`, `sel_period`, `sel_stat` — Current metric parameters
- `active_view` — "🗺 Map view" | "📊 Rankings view"
- `map_mode` — "Absolute value" | "Change from 1990-2010 baseline"

#### `views/map_view.py`
**Purpose:** Render the choropleth map with Folium.

**Key Functions:**
- `render_map_view(...)` — Main map renderer

**Features:**
- State/district boundaries
- Color-coded metric values
- Click handling for district selection
- Active point markers
- Portfolio district highlighting

#### `views/rankings_view.py`
**Purpose:** Render the rankings table.

**Key Functions:**
- `render_rankings_view(...)` — Main rankings renderer

**Features:**
- Sortable district rankings
- Risk class indicators
- Portfolio add/remove buttons
- Export functionality

#### `views/details_panel.py`
**Purpose:** Render district detail panel.

**Key Functions:**
- `render_details_panel(...)` — Main details renderer

**Features:**
- District metrics summary
- Scenario comparison charts
- Time series visualization
- PDF export

#### `views/state_summary_view.py`
**Purpose:** Render state summary (when no district selected).

**Key Functions:**
- `render_state_summary_view(...)` — Main state summary renderer

**Features:**
- State-level statistics
- District distribution boxplot
- Per-model averages
- State trend over time

#### `point_selection_ui.py`
**Purpose:** Saved points panel for portfolio mode.

**Key Functions:**
- `render_point_selection_panel(...)` — Main panel renderer

**Features:**
- Coordinate input (lat/lon)
- Map click selection
- Saved points list
- Add points' districts to portfolio

#### `portfolio_ui.py`
**Purpose:** Portfolio management panel.

**Key Functions:**
- `render_portfolio_panel(...)` — Main portfolio renderer

**Features:**
- Portfolio district list
- Comparison metrics table
- Export functionality

#### `sidebar.py`
**Purpose:** Sidebar utilities.

**Key Functions:**
- `render_analysis_mode_selector(...)` — Single/Multi-district toggle
- `render_view_selector(...)` — Map/Rankings toggle
- `render_hover_toggle_if_portfolio(...)` — Hover behavior toggle
- `apply_jump_once_flags()` — Handle view jump requests

---

### 6. Utilities (`india_resilience_tool/utils/`)

#### `naming.py`
**Purpose:** Name normalization for fuzzy matching.

**Key Functions:**
- `alias(name) -> str` — Normalize district/state name
- `normalize_name(name) -> str` — Basic normalization
- `normalize_compact(name) -> str` — Compact normalization

**Key Exports:**
- `NAME_ALIASES: dict` — Known name aliases/mappings

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Request                              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                 legacy_dashboard_impl.py                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Sidebar   │  │   Config    │  │    Session State        │  │
│  │   Controls  │  │  (variables,│  │  (selection, portfolio) │  │
│  │             │  │   constants)│  │                         │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                      │                │
│         └────────────────┼──────────────────────┘                │
│                          │                                       │
│                          ▼                                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    Data Loading                            │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐    │  │
│  │  │ adm2_loader │  │master_loader│  │     merge       │    │  │
│  │  │  (GeoJSON)  │  │   (CSV)     │  │ (merged GDF)    │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                          │                                       │
│                          ▼                                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                    View Router                             │  │
│  │  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌───────────┐   │  │
│  │  │ Map View│  │ Rankings │  │ Details │  │ Portfolio │   │  │
│  │  │         │  │   View   │  │  Panel  │  │   Panel   │   │  │
│  │  └─────────┘  └──────────┘  └─────────┘  └───────────┘   │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Visualization                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────────┐    │
│  │ charts  │  │ colors  │  │ tables  │  │     exports     │    │
│  │(figures)│  │(legends)│  │(ranking)│  │ (PDF reports)   │    │
│  └─────────┘  └─────────┘  └─────────┘  └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Common Tasks & Relevant Modules

| Task | Primary Module(s) | Secondary Module(s) |
|------|-------------------|---------------------|
| Add new climate index | `config/variables.py` | `build_master_metrics.py` |
| Modify map appearance | `app/views/map_view.py` | `viz/colors.py` |
| Change rankings table | `app/views/rankings_view.py` | `viz/tables.py` |
| Update district details | `app/views/details_panel.py` | `viz/charts.py`, `viz/exports.py` |
| Modify portfolio logic | `analysis/portfolio.py` | `app/portfolio_ui.py` |
| Add sidebar control | `app/legacy_dashboard_impl.py` | `app/sidebar.py` |
| Change data loading | `data/master_loader.py`, `data/adm2_loader.py` | `data/merge.py` |
| Modify PDF exports | `viz/exports.py` | `viz/charts.py` |
| Update state summary | `app/views/state_summary_view.py` | — |
| Fix name matching | `utils/naming.py` | — |
| Change constants/styling | `config/constants.py` | — |

---

## Testing

### Test Files
```
tests/
├── test_config.py                    # Config module tests
├── test_imports_smoke.py             # Import smoke tests
├── test_app_state_summary_view.py    # State summary tests
├── test_app_point_selection_ui.py    # Point selection tests
├── test_app_rankings_view.py         # Rankings view tests
├── test_viz_charts.py                # Chart generation tests
└── test_viz_exports.py               # Export tests
```

### Running Tests
```bash
# All tests
python -m pytest -q

# Specific module
python -m pytest tests/test_config.py -v

# With coverage
python -m pytest --cov=india_resilience_tool
```

---

## Session State Reference

| Key | Type | Purpose |
|-----|------|---------|
| `analysis_mode` | str | "Single district focus" or "Multi-district portfolio" |
| `portfolio_districts` | list[dict] | List of `{"state": ..., "district": ...}` |
| `selected_state` | str | Currently selected state ("All" or state name) |
| `selected_district` | str | Currently selected district ("All" or district name) |
| `selected_var` | str | Current index slug (e.g., "tas_gt32") |
| `selected_index_group` | str | Current group ("temperature" or "rain") |
| `sel_scenario` | str | Current scenario ("ssp245" or "ssp585") |
| `sel_period` | str | Current period (e.g., "2041-2060") |
| `sel_stat` | str | Current statistic ("mean", "median", "p05", "p95", "std") |
| `active_view` | str | "🗺 Map view" or "📊 Rankings view" |
| `map_mode` | str | "Absolute value" or "Change from 1990-2010 baseline" |
| `portfolio_build_route` | str | "rankings", "map", or "saved_points" |
| `point_query_lat/lon` | float | Current point coordinates |
| `point_query_points` | list[dict] | Saved points list |
| `jump_to_map/rankings` | bool | View jump flags |

---

## Widget Key Conventions

Widget keys follow patterns to ensure uniqueness:

- Buttons: `btn_{action}_{context}` — e.g., `btn_add_portfolio_{state}_{district}`
- Expanders: `exp_{section}_{context}`
- Selectboxes: `sel_{field}` — e.g., `sel_scenario`, `sel_period`
- State summary: `btn_state_boxplot_{slug}_{state}_{scenario}_{period}_{stat}`

---

## Dependency Injection Pattern

View renderers use dependency injection to avoid circular imports:

```python
def render_some_view(
    *,
    # Data
    merged: gpd.GeoDataFrame,
    metric_col: str,
    # Config
    variables: dict,
    # Callbacks (injected)
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_contains_fn: Callable[[str, str], bool],
) -> None:
    import streamlit as st  # Import inside function
    ...
```

This allows:
- Testing without Streamlit
- Avoiding circular imports
- Clear interface contracts

---

## File Locations

| Data Type | Location |
|-----------|----------|
| GeoJSON boundaries | `DATA_DIR/districts_4326.geojson` |
| Master CSV | `DATA_DIR/processed/{index}/{state}/master_metrics_by_district.csv` |
| District yearly data | `DATA_DIR/processed/{index}/{state}/{district}/ensembles/{scenario}/` |
| State yearly data | `DATA_DIR/processed/{index}/{state}/state_yearly_ensemble_stats.csv` |
| PDF exports | `DATA_DIR/processed/{index}/{state}/pdf_plots/` |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-Q4 | Initial monolithic dashboard |
| 2.0 | 2024-12 | Refactored to modular structure (~38% reduction) |

---

## Contact

For questions about this codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** abs.thakur@resilience.org.in