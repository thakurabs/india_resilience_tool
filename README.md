# India Resilience Tool (IRT)

A Streamlit-based dashboard for exploring climate resilience metrics across **Indian administrative units at two levels**:

- **Districts (ADM2)**
- **Blocks / Sub-districts (ADM3)**

The tool visualizes **ensemble climate model outputs** and derived indices (temperature and rainfall), enabling comparison across **scenarios** and **time periods**, plus portfolio-style comparison of multiple districts **or blocks**.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.51.0-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

---

## Features

### Core exploration
- **Admin level toggle**: District ↔ Block
- **Risk domain selection (Map View ribbon)**: Choose from 8 thematic bundles (plus an optional advanced drought bundle)
- **Interactive Map View**: Choropleth visualization with hover highlight + tooltip
- **Rankings Table**:
  - District-wise rankings (ADM2)
  - Block-wise rankings (ADM3)
  - Risk classification + percentiles
- **Details panel (Climate Profile)**:
  - Risk summary
  - Scenario comparison (period-mean)
  - Trend over time (ensemble yearly series)
  - Case study export (single district, multi-index PDF/ZIP)

### Metric selection ribbon (above the map)

In **Map View**, the controls that determine what you see on the choropleth live in a compact **ribbon directly above the map** (replacing the old “selected metric / scenario / period / stat” summary text).

The ribbon includes:

- **Risk domain (bundle)**
- **Metric (index)**
- **Scenario**
- **Period**
- **Statistic** (mean/median only)
- **Map mode** (absolute vs change from 1990–2010 baseline)

All ribbon fields start with a placeholder (`— Select —`) to encourage deliberate selection. The map renders once the required ribbon fields are chosen. **Geography options** (available states/districts/blocks) are populated after you choose a metric, because the processed-root depends on the metric slug.

Metrics are organized into **thematic bundles** for easier navigation:

| Bundle | Metrics | Focus |
|--------|---------:|-------|
| 🔥 Heat Risk | 14 | Extreme heat, heatwaves, hot days/nights, persistence. |
| 🥵 Heat Stress | 5 | Wet-bulb temperature and wet-bulb day thresholds (heat stress). |
| ❄️ Cold Risk | 8 | Cold extremes: frost days, cold nights, cold spells. |
| 🌾 Agriculture & Growing Conditions | 4 | Growing season and temperature context for crops. |
| 🌊 Flood & Extreme Rainfall Risk | 6 | Heavy rainfall intensity, very wet days, wet spells. |
| 💧 Rainfall Totals & Typical Wetness | 3 | Annual totals, rainy days, typical wetness. |
| 🏜️ Drought Risk | 3 | Dry spell length and SPI-6 drought indicators. |
| 🌡️ Temperature Variability | 2 | Daily/annual temperature range and variability. |
| 🧪 Drought Risk (Advanced) | 9 | SPI-3/6/12 indices and severity counts (optional). |

> Note: **Drought Risk (Advanced)** is an optional bundle (not shown by default in the UI).

> **Wet-bulb (Heat Stress):** Wet-bulb indices (`twb_*`, `wbd_*`) are grouped under the **Heat Stress** bundle.

**Single-focus mode**: use the **ribbon** to select a risk domain, then choose a metric within that domain.

**Portfolio mode**: metric selection for portfolio comparison remains in the **Portfolio panel** (bundle multi-select → auto-expand to metrics, with optional manual refinement).

### Portfolio mode (districts and blocks)
Portfolio mode exists at **both** admin levels:

- **Multi-district portfolio**: build and compare sets of districts (portfolios can span multiple states)
- **Multi-block portfolio**: build and compare sets of blocks (portfolios can span multiple states; switch state to add units)

You can add units to your portfolio in three ways (same UX in district and block modes):

**From the Map**
- Click a district/block on the choropleth map
- Use the "+ Add to portfolio" control that appears below the map
- Portfolio units are highlighted (e.g., blue borders)

**From the Rankings Table**
- Switch to Rankings view
- Select rows to add (checkboxes / editor column)
- Click "Add checked … to portfolio"
- Units already in portfolio show ✓ in "In portfolio"

**By Coordinates (Point Selection)**
- Use the "Add by Location" panel (single + batch)
- Preview which district/block contains the point
- Add to portfolio or show markers on the map

In the right-side Portfolio panel, use:
- **Compare**: summary strip + comparison table (updates live)
- **Add units**: coordinate-based unit lookup and saved points

### Portfolio comparison
Once you have items in your portfolio:
- Select one or more **risk domains** to compare
- Metrics from selected domains are automatically included
- Optional: manually refine which metrics to include
- Table auto-rebuilds when portfolio or selection changes
- A summary strip shows unit/state/metric counts (and basic risk-class distribution)
- Download comparison as CSV
- Optional: enable visualizations (e.g., heatmap) for quick comparison (computed lazily)

---

## Quick Start

### Prerequisites
- Python 3.10+
- Processed climate data (see [Data Setup](#data-setup))

### Installation

```bash
# Clone the repository
git clone https://github.com/thakurabs/india_resilience_tool.git
cd india_resilience_tool
```

**Option 1: Conda (recommended)**

```bash
conda env create -f environment.yml
conda activate irt
```

**Option 2: pip**

> Note: `requirements.txt` may be UTF-16 encoded in this repo. If `pip install -r requirements.txt` fails with an encoding error, convert it first:
>
> ```bash
> iconv -f UTF-16 -t UTF-8 requirements.txt > requirements.utf8.txt
> pip install -r requirements.utf8.txt
> ```

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Running the dashboard

Recommended:

```bash
streamlit run dashboard_unfactored.py
```

Alternative (package entry):

```bash
streamlit run india_resilience_tool/app/main.py
```

Open in a browser: `http://localhost:8501`

---

## Data Setup

### Required boundary files (EPSG:4326)

Place these in `DATA_DIR` (configured in `paths.py`, or overridden via `IRT_DATA_DIR`):

1. **District boundaries**: `districts_4326.geojson` (ADM2)
2. **Block boundaries**: `blocks_4326.geojson` (ADM3)

> Block mode requires `blocks_4326.geojson`.

### Processed outputs directory structure

Processed artifacts are organized **by index slug** (e.g., `tas_gt32`) and state.

```
DATA_DIR/
├── districts_4326.geojson
├── blocks_4326.geojson
└── processed/
    └── {index_slug}/                 # e.g., tas_gt32
        └── {state}/                  # e.g., Telangana
            ├── master_metrics_by_district.csv
            ├── master_metrics_by_block.csv
            ├── districts/
            │   ├── {district}/{model}/{scenario}/
            │   │   ├── {district}_yearly.csv
            │   │   └── {district}_periods.csv
            │   └── ensembles/{district}/{scenario}/
            │       └── {district}_yearly_ensemble.csv
            └── blocks/
                ├── {district}/{block}/{model}/{scenario}/
                │   ├── {block}_yearly.csv
                │   └── {block}_periods.csv
                └── ensembles/{district}/{block}/{scenario}/
                    └── {block}_yearly_ensemble.csv
```

Notes:
- Windows may show `.csv` as "Microsoft Excel CSV"; they're normal CSVs.
- The dashboard uses **master metrics** for maps/rankings and **ensemble yearly** files for trends.

### Building master CSVs (district + block)

```bash
python build_master_metrics.py
```

Or use the dashboard's "Rebuild now" control if exposed in your branch.

---

## Usage Guide

### Admin level selection
Use the left sidebar toggle:
- **District**: explore districts and build district portfolios
- **Block**: explore blocks and build block portfolios

### Metric selection (Map View ribbon)

The **Map View ribbon** (above the map) controls what is visualized. The dashboard uses placeholder-first selection (`— Select —`) and will prompt you until the required fields are chosen.

Recommended order:

1. **Choose Analysis focus** in the left sidebar (required to render the map).
2. In the **ribbon above the map**, select:
   - **Risk domain** → **Metric**
3. Once a metric is selected, complete the remaining ribbon fields:
   - **Scenario**, **Period**, **Statistic** (mean/median), **Map mode**
4. In the left sidebar **Geography & analysis focus** panel, select:
   - **State** (and District/Block when applicable)

Changing any ribbon field triggers a rerun and the map updates accordingly. If you see an info message asking you to complete ribbon selections, it means one or more fields are still set to the placeholder.

### Analysis modes
Each admin level supports:
1. **Single focus**: explore one district/block at a time
2. **Portfolio focus**:
   - Multi-district portfolio
   - Multi-block portfolio

### Portfolio comparison (bundle-first selection)

In portfolio mode, you can select metrics by risk domain:

1. **Select risk domains**: Choose one or more bundles (e.g., "Heat Risk" + "Drought Risk")
2. **Auto-expansion**: All metrics from selected bundles are included automatically
3. **Optional refinement**: Check "Manually refine metric selection" to add/remove individual metrics
4. **View included metrics**: Expand the "View X included metrics" section to see what's selected

### Trend over time (yearly series)
The Trend panel looks for **ensemble yearly** time-series:

- District trend: `districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv`
- Block trend: `blocks/ensembles/{district}/{block}/{scenario}/{block}_yearly_ensemble.csv`

Some ensemble-yearly CSVs may not include identifier columns (e.g., `state`, `district`, `block`). The loader injects missing identifiers from the path context so filtering stays consistent in-memory.

If Trend shows "No yearly time-series available…":
- confirm the `*_yearly_ensemble.csv` exists under the `ensembles/` path
- confirm it contains `year` and at least one usable value column (commonly `ensemble_mean`, `mean`, or `value`)

---

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `IRT_PILOT_STATE` | `Telangana` | Default state to load |
| `IRT_DATA_DIR` | (from `paths.py`) | Base data directory for boundary + processed |
| `IRT_PROCESSED_ROOT` | `DATA_DIR/processed/{index}` | Processed data location override |
| `IRT_DEBUG` | `0` | Enable debug output (1=on) |

### Data directory
Edit `paths.py` to set `DATA_DIR`, or set `IRT_DATA_DIR`.

```python
DATA_DIR = Path("/path/to/your/data")
```

---

## Project Structure (high level)

```
india_resilience_tool/
├── analysis/
│   ├── portfolio.py             # Portfolio state & comparison builders (district + block)
│   ├── metrics.py               # Risk classification utilities
│   └── timeseries.py            # District/block time-series loaders
├── app/
│   ├── legacy_dashboard_impl.py # Main orchestrator (district + block + bundles)
│   ├── sidebar.py               # Sidebar controls (admin level, analysis focus, view navigation)
│   ├── portfolio_ui.py          # Portfolio panel with bundle-first selection
│   ├── point_selection_ui.py    # Coordinate input with batch support (district + block)
│   ├── perf.py                  # Perf helpers (timing / instrumentation)
│   └── views/
│       ├── map_view.py          # Map rendering for districts + blocks
│       ├── rankings_view.py     # Rankings with add-to-portfolio (district + block)
│       ├── details_panel.py     # Details panel (district + block; exports/case studies)
│       └── state_summary_view.py
├── compute/
│   └── spi_adapter.py           # SPI wrapper/adapter around climate-indices package
├── config/
│   ├── constants.py             # App constants
│   ├── metrics_registry.py      # Unified metrics + bundles (single source of truth)
│   └── variables.py             # Dashboard variable registry
├── data/
│   ├── adm2_loader.py           # District boundary loading
│   ├── adm3_loader.py           # Block boundary loading
│   ├── master_loader.py         # Master CSV loading
│   └── merge.py                 # Merge master ↔ boundaries (ADM2/ADM3)
└── viz/
    ├── charts.py                # Figure generation
    ├── colors.py                # Color scales
    ├── exports.py               # PDF export
    └── tables.py                # Table formatting

Root files:
├── dashboard_unfactored.py      # Streamlit entry point (shim)
├── paths.py                     # Data directory config (canonical)
├── build_master_metrics.py      # CSV builder
├── compute_indices_multiprocess.py  # Index compute pipeline (multi-process)
├── spi_diagnostic.py            # SPI diagnostic helper script
├── MANIFEST.md                  # Detailed codebase guide
└── README.md                    # This file
```

For detailed module documentation, see [MANIFEST.md](MANIFEST.md).


---

## Development

### Running tests

```bash
python -m pytest -q
python -m pytest --cov=india_resilience_tool
```

### Code style

```bash
black india_resilience_tool/
ruff check india_resilience_tool/
mypy india_resilience_tool/
```

### Adding a new metric

1. Add metric definition to `config/metrics_registry.py` in `PIPELINE_METRICS_RAW`
2. Add the slug to appropriate bundle(s) in `BUNDLES`
3. Run validation: `python config/metrics_registry.py`
4. Ensure processed data exists for the new metric

### Adding a new bundle

1. Add bundle to `BUNDLES` dict in `config/metrics_registry.py`
2. Add bundle name to `BUNDLE_ORDER` list
3. Add description to `BUNDLE_DESCRIPTIONS` dict
4. Run validation to ensure all slugs exist

---

## Changelog (high level)

### v2.4 — Map-top metric selection ribbon (2026-02)
- Moved Map View controls into a **ribbon above the map**: risk domain, metric, scenario, period, statistic, and map mode
- Added **placeholder-first** selection (`— Select —`) with safe gating (map renders only after required choices)
- Sidebar expanders are **user-controlled** (auto-collapse removed)

### v2.3 — Thematic bundles + bundle-first selection (2026-01)
- Added thematic bundles organizing metrics by risk domain (Heat Risk, Heat Stress, etc.)
- Sidebar now uses **bundle → metric** two-step selection
- Portfolio comparison supports **bundle multi-select** with auto-expansion
- Optional manual refinement for fine-grained metric selection
- New session state keys: `selected_bundle`, `portfolio_bundle_selection`, `portfolio_manual_refinement`

### v2.2 — Block-level visualization + portfolio parity (2026-01)
- Added **ADM3 Block** support across map, rankings, and portfolio comparison.
- Block rankings table now supports **add-to-portfolio** with the same UX as district mode.
- Trend over time supports **block ensemble yearly** series when present.
- Time-series loader injects missing identifiers (e.g., state) for consistent filtering.

### v2.1 — Portfolio UX improvements (2024-12)
- Add districts from map clicks, rankings table, and coordinates
- Batch coordinates + saved points
- Auto-rebuilding comparison table

### v2.0 — Modular refactor (2024-12)
- Refactored from monolithic dashboard to modular structure

---

## License
MIT License — see [LICENSE](LICENSE) for details.

---

## Contact

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in
