# India Resilience Tool (IRT)

A Streamlit-based dashboard for exploring climate resilience metrics across **Indian administrative units at two levels**:

- **Districts (ADM2)**
- **Blocks / Sub-districts (ADM3)**

The tool visualizes **ensemble climate model outputs** and derived indices (temperature and rainfall), enabling comparison across **scenarios** and **time periods**, plus portfolio-style comparison of multiple districts **or blocks**.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

---

## Features

### Core exploration
- **Admin level toggle**: District ↔ Block
- **Interactive Map View**: Choropleth visualization with hover highlight + tooltip
- **Rankings Table**:
  - District-wise rankings (ADM2)
  - Block-wise rankings (ADM3)
  - Risk classification + percentiles
- **Details panel (Climate Profile)**:
  - Risk summary
  - Scenario comparison (period-mean)
  - Trend over time (yearly series, when available)
  - Detailed statistics and exports (availability depends on branch)

### Portfolio mode (districts and blocks)
Portfolio mode exists at **both** admin levels:

- **Multi-district portfolio**: build and compare sets of districts
- **Multi-block portfolio**: build and compare sets of blocks (within the selected state)

You can add units to your portfolio in three ways (same UX in district and block modes):

**From the Map**
- Click a district/block on the choropleth map
- Use the "+ Add to portfolio" control that appears below the map
- Portfolio units are highlighted (e.g., blue borders)

**From the Rankings Table**
- Switch to Rankings view
- Select rows to add (checkboxes / editor column)
- Click “Add checked … to portfolio”
- Units already in portfolio show ✓ in “In portfolio”

**By Coordinates (Point Selection)**
- Use the “Add by Location” panel (single + batch)
- Preview which district/block contains the point
- Add to portfolio or show markers on the map

### Portfolio comparison
Once you have items in your portfolio:
- Select one or more indices to compare
- Table auto-rebuilds when portfolio or selection changes
- Download comparison as CSV
- Visualizations (e.g., heatmap) for quick comparison

---

## Quick Start

### Prerequisites
- Python 3.10+
- Processed climate data (see [Data Setup](#data-setup))

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/india-resilience-tool.git
cd india-resilience-tool
```

**Option 1: Conda (recommended)**

```bash
conda env create -f environment.yml
conda activate irt
```

**Option 2: pip**

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Running the dashboard

```bash
streamlit run dashboard_unfactored.py
```

Open in a browser: `http://localhost:8501`

---

## Data Setup

### Required boundary files (EPSG:4326)

Place these in `DATA_DIR` (configured in `paths.py`, or overridden via `IRT_DATA_DIR`):

1. **District boundaries**: `districts_4326.geojson` (ADM2)
2. **Block boundaries**: `block_4326.geojson` (ADM3)

> Block mode requires `block_4326.geojson`.

### Processed outputs directory structure

Processed artifacts are organized **by index slug** (e.g., `tas_gt32`) and state.

```
DATA_DIR/
├── districts_4326.geojson
├── block_4326.geojson
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
- Windows may show `.csv` as “Microsoft Excel CSV”; they’re normal CSVs.
- The dashboard uses **master metrics** for maps/rankings and **ensemble yearly** files for trends.

### Building master CSVs (district + block)

```bash
python build_master_metrics.py
```

Or use the dashboard’s “Rebuild now” control if exposed in your branch.

---

## Usage Guide

### Admin level selection
Use the left sidebar toggle:
- **District**: explore districts and build district portfolios
- **Block**: explore blocks and build block portfolios

### Analysis modes
Each admin level supports:
1. **Single focus**: explore one district/block at a time
2. **Portfolio focus**:
   - Multi-district portfolio
   - Multi-block portfolio

### Trend over time (yearly series)
The Trend panel looks for **ensemble yearly** time-series:

- District trend: `districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv`
- Block trend: `blocks/ensembles/{district}/{block}/{scenario}/{block}_yearly_ensemble.csv`

Some ensemble-yearly CSVs may not include identifier columns (e.g., `state`, `district`, `block`). The loader injects missing identifiers from the path context so filtering stays consistent in-memory.

If Trend shows “No yearly time-series available…”:
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
│   ├── portfolio.py            # Portfolio state & comparison builders (district + block)
│   ├── metrics.py              # Risk classification utilities
│   └── timeseries.py           # District/block time-series loaders
├── app/
│   ├── legacy_dashboard_impl.py # Main orchestrator (district + block flows)
│   ├── sidebar.py              # Sidebar controls (admin level, analysis mode, selection)
│   ├── portfolio_ui.py         # Portfolio panel for districts + blocks
│   ├── point_selection_ui.py   # Coordinate input with batch support (district + block)
│   └── views/
│       ├── map_view.py         # Map rendering for districts + blocks
│       ├── rankings_view.py    # Rankings with add-to-portfolio (district + block)
│       ├── details_panel.py    # Details panel (district + block; some panels may be district-first)
│       └── state_summary_view.py
├── config/
│   ├── constants.py            # App constants
│   └── variables.py            # Climate index registry
├── data/
│   ├── adm2_loader.py          # District boundary loading
│   ├── adm3_loader.py          # Block boundary loading
│   ├── master_loader.py        # Master CSV loading
│   └── merge.py                # Merge master ↔ boundaries (ADM2/ADM3)
└── viz/
    ├── charts.py               # Figure generation
    ├── colors.py               # Color scales
    ├── exports.py              # PDF export
    └── tables.py               # Table formatting

Root files:
├── dashboard_unfactored.py     # Entry point
├── paths.py                    # Data directory config
├── build_master_metrics.py     # CSV builder
├── MANIFEST.md                 # Detailed codebase guide
└── README.md                   # This file
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

---

## Changelog (high level)

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
