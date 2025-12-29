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
│   ├── portfolio.py            # Multi-district portfolio logic & state
│   └── timeseries.py           # Time series data loading
├── app/                         # Streamlit application
│   ├── __init__.py
│   ├── dashboard.py            # Dashboard entry wrapper
│   ├── legacy_dashboard_impl.py # Main orchestrator
│   ├── main.py                 # CLI entry point
│   ├── orchestrator.py         # Module executor
│   ├── point_selection_ui.py   # Coordinate input & batch support
│   ├── portfolio_ui.py         # Portfolio management panel
│   ├── sidebar.py              # Sidebar controls & navigation
│   ├── state.py                # Session state defaults & constants
│   └── views/                  # View renderers
│       ├── __init__.py
│       ├── details_panel.py    # District details view
│       ├── map_view.py         # Choropleth map with portfolio features
│       ├── rankings_view.py    # Rankings table with add buttons
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
**Purpose:** Multi-district portfolio management with unified state handling.

**Key Classes:**
- `PortfolioState` — Unified portfolio state manager wrapping session_state
  - `add_district(state, district)` — Add district, returns True if added
  - `remove_district(state, district)` — Remove district, returns True if removed
  - `contains_district(state, district)` — Check membership
  - `toggle_district(state, district)` — Toggle, returns True if now in portfolio
  - `clear_districts()` — Clear all, returns count removed
  - `get_district_keys()` — Get normalized keys set
  - `districts` — Property returning list of district dicts
  - `district_count` — Property returning count
  - `saved_points` — Property for coordinate-based saved points
  - `set_flash(message, level)` / `pop_flash()` — One-shot flash messages
  - `comparison_table` — Cached comparison DataFrame
  - `needs_table_rebuild(context)` — Check if rebuild needed

**Key Functions:**
- `portfolio_add(session_state, state, district, ...)` — Add district to portfolio
- `portfolio_remove(session_state, state, district, ...)` — Remove district
- `portfolio_contains(session_state, state, district, ...)` — Check membership
- `portfolio_clear(session_state)` — Clear all districts
- `portfolio_normalize(text, alias_fn)` — Normalize names for comparison
- `portfolio_key(state, district, normalize_fn)` — Create normalized key tuple
- `get_portfolio_district_keys(session_state, normalize_fn)` — Get all portfolio keys as set
- `build_portfolio_multiindex_df(...)` — Build multi-index comparison DataFrame

**Session state keys:**
- `portfolio_districts` — List of `{"state": ..., "district": ...}` dicts
- `point_query_points` — Saved coordinate points list
- `portfolio_multiindex_selection` — Selected indices for comparison
- `portfolio_multiindex_df` — Cached comparison table
- `portfolio_multiindex_context` — Cache invalidation context

#### `metrics.py`
**Purpose:** Risk classification utilities.

**Key Functions:**
- `risk_class_from_percentile(percentile) -> str` — Map percentile to risk class (low/moderate/high)
- `compute_rank_and_percentile(df, state, metric_col, value, ...)` — Compute rank and percentile within state

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

#### `state.py`
**Purpose:** Session state defaults and key registry.

**Key Functions:**
- `ensure_session_state(session_state, perf_default)` — Initialize all keys with defaults

**Key Exports:**
- `SESSION_DEFAULTS` — Dict of all default values
- `VIEW_MAP`, `VIEW_RANKINGS` — View constants
- `ANALYSIS_MODE_SINGLE`, `ANALYSIS_MODE_PORTFOLIO` — Mode constants

#### `sidebar.py`
**Purpose:** Sidebar controls and navigation.

**Key Functions:**
- `render_analysis_mode_selector(...)` — Single/Multi-district toggle
- `render_view_selector(...)` — Map/Rankings toggle
- `render_hover_toggle_if_portfolio(...)` — Hover behavior toggle
- `render_portfolio_quick_stats()` — Portfolio count in sidebar
- `apply_jump_once_flags()` — Handle view jump requests

#### `views/map_view.py`
**Purpose:** Render the choropleth map with Folium and portfolio features.

**Key Functions:**
- `render_map_view(...)` — Main map renderer with st_folium
- `extract_clicked_district_state(ret)` — Extract district/state from click payload
- `extract_click_coordinates(ret)` — Extract lat/lon from click payload
- `find_district_at_coordinates(merged, lat, lon)` — Reverse geocode to district
- `create_portfolio_style_function(portfolio_keys, normalize_fn)` — Style function for portfolio highlighting
- `add_portfolio_legend_to_map(m, portfolio_count)` — Add portfolio legend HTML
- `render_district_add_to_portfolio(...)` — Inline add/remove button for clicked district

**Features:**
- State/district boundaries with color-coded metrics
- Click handling with coordinate-based district lookup
- Portfolio district highlighting (blue borders)
- Preview markers (red star for single, green for batch)
- Saved point markers (blue)
- Portfolio legend showing count

**Marker Types:**
| Type | Color | Icon | Session Key |
|------|-------|------|-------------|
| Single preview | Red | Star | `map_preview_marker` |
| Batch preview | Green | Map marker | `map_preview_markers` |
| Saved points | Blue | Info sign | `point_query_points` |

#### `views/rankings_view.py`
**Purpose:** Render the rankings table with portfolio integration.

**Key Functions:**
- `render_rankings_view(...)` — Main rankings renderer (routes to mode-specific)
- `_render_simple_rankings(...)` — Single-district mode table
- `_render_portfolio_rankings(...)` — Portfolio mode with st.data_editor

**Features:**
- Sortable district rankings by value or change
- Full column display: Rank, District, State, Value, Δ, %Δ, Percentile, Risk class
- Portfolio mode: "In portfolio" status column, "Add to portfolio" checkbox column
- Batch add button: "Add checked districts to portfolio"
- Download as CSV

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
**Purpose:** Coordinate-based district lookup with batch input support.

**Key Functions:**
- `render_point_selection_panel(...)` — Full panel with tabs
- `find_district_at_point(merged, lat, lon)` — Find district at coordinates
- `parse_batch_coordinates(text)` — Parse multi-line coordinate input

**Features:**
- **Single Coordinate tab:**
  - Lat/lon number inputs
  - District preview
  - "Add to portfolio" button
  - "Show on map" button (places red star marker)
  - "Save point" button
- **Batch Input tab:**
  - Text area for pasting multiple coordinates
  - Supports formats: `lat, lon` / `lat, lon, label` / `lat lon`
  - Preview table with district lookup
  - "Add all to portfolio" button
  - "Show all on map" button (places green markers)
  - "Save all points" button
- **Saved Points section:**
  - Table of saved points with labels
  - "Add all to portfolio" button
  - "Show on map" button
  - "Clear all" button

#### `portfolio_ui.py`
**Purpose:** Portfolio management panel (right column in portfolio mode).

**Key Functions:**
- `render_portfolio_panel(...)` — Main panel orchestrator
- `render_portfolio_badge(portfolio_count)` — Compact count display
- `render_portfolio_list(...)` — District list with remove buttons
- `render_clear_portfolio_button(...)` — Clear all with confirmation
- `render_index_selector(...)` — Multi-select for comparison indices
- `render_comparison_table(...)` — Auto-rebuilding comparison table
- `render_coordinate_lookup(...)` — Wrapper for point_selection_ui

**Features:**
- Portfolio badge showing district count
- Expandable district list with per-item remove buttons
- Clear all with confirmation dialog
- Index multi-select for comparison
- Auto-rebuilding comparison table (no manual "Build" button needed)
- Integrated coordinate lookup panel with tabs

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
| Add district from map click | `app/views/map_view.py` | `analysis/portfolio.py` |
| Add districts from rankings | `app/views/rankings_view.py` | `analysis/portfolio.py` |
| Add districts by coordinates | `app/point_selection_ui.py` | `analysis/portfolio.py` |
| Show markers on map | `app/views/map_view.py` | `app/point_selection_ui.py` |
| Build comparison table | `app/portfolio_ui.py` | `analysis/portfolio.py` |
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
| `point_query_points` | list[dict] | Saved coordinate points with labels |
| `map_preview_marker` | dict | Single preview marker from "Show on map" |
| `map_preview_markers` | list[dict] | Batch preview markers from "Show all on map" |
| `portfolio_multiindex_selection` | list[str] | Selected index slugs for comparison |
| `portfolio_multiindex_df` | DataFrame | Cached comparison table |
| `portfolio_multiindex_context` | dict | Cache invalidation context |
| `selected_state` | str | Currently selected state ("All" or state name) |
| `selected_district` | str | Currently selected district ("All" or district name) |
| `selected_var` | str | Current index slug (e.g., "tas_gt32") |
| `selected_index_group` | str | Current group ("temperature" or "rain") |
| `sel_scenario` | str | Current scenario ("ssp245" or "ssp585") |
| `sel_period` | str | Current period (e.g., "2041-2060") |
| `sel_stat` | str | Current statistic ("mean", "median", "p05", "p95", "std") |
| `active_view` | str | "🗺 Map view" or "📊 Rankings view" |
| `map_mode` | str | "Absolute value" or "Change from 1990-2010 baseline" |
| `jump_to_map` | bool | Flag to switch to map view |
| `jump_to_rankings` | bool | Flag to switch to rankings view |

---

## Widget Key Conventions

Widget keys follow patterns to ensure uniqueness:

- Buttons: `btn_{action}_{context}` — e.g., `btn_add_portfolio_{state}_{district}`
- Expanders: `exp_{section}_{context}`
- Selectboxes: `sel_{field}` — e.g., `sel_scenario`, `sel_period`
- State summary: `btn_state_boxplot_{slug}_{state}_{scenario}_{period}_{stat}`
- Rankings editor: `rankings_portfolio_editor_{slug}_{scenario}_{period}_{stat}`
- Point selection: `_point_lat`, `_point_lon`, `_batch_coords_input`

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
| 2.1 | 2024-12 | Portfolio UX improvements: map click add, batch coordinates, auto-rebuild table |

---

## Contact

For questions about this codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** abs.thakur@resilience.org.in