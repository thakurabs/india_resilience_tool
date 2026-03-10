"""
Application constants for IRT.

This module defines paths, styling, and other configuration constants
used throughout the dashboard.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

# ---- Geo simplification tolerances ----
SIMPLIFY_TOL_ADM2: float = 0.001
SIMPLIFY_TOL_ADM3: float = 0.001
SIMPLIFY_TOL_ADM1: float = 0.015
SIMPLIFY_TOL_BASIN_RENDER: float = 0.0035
SIMPLIFY_TOL_SUBBASIN_RENDER: float = 0.005

# ---- Bounding box for India ----
MIN_LON: float = 68.0
MAX_LON: float = 97.5
MIN_LAT: float = 5.0
MAX_LAT: float = 45.0

INDIA_BBOX: Tuple[float, float, float, float] = (MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

# ---- Figure / font styling for dashboard panels ----
# These are used for the main small-panel figures (trend + scenario comparison)
FIG_SIZE_PANEL: Tuple[float, float] = (4.8, 2.4)  # width, height in inches
FIG_DPI_PANEL: int = 150

FONT_SIZE_TITLE: int = 9
FONT_SIZE_LABEL: int = 8
FONT_SIZE_TICKS: int = 8
FONT_SIZE_LEGEND: int = 8

# ---- Period ordering for display ----
PERIOD_ORDER: list[str] = [
    "1995-2014",
    "2021-2040",
    "2041-2060",
    "2061-2080",
    "2081-2100",
]

# ---- Scenario display names ----
SCENARIO_DISPLAY: dict[str, str] = {
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
    "historical": "Historical",
}

# ---- Scenario UI labels (plain-English + scientific) ----
SCENARIO_UI_LABEL: dict[str, str] = {
    "ssp245": "Middle-of-the-road (SSP2-4.5)",
    "ssp585": "Fossil-fuelled development (SSP5-8.5)",
    "historical": "Historical",
}

# ---- Scenario help copy (shown in native Streamlit help tooltips) ----
SCENARIO_HELP_MD: dict[str, str] = {
    "ssp245": (
        "Middle-of-the-road (SSP2-4.5)\n"
        "A future where social, economic, and technological trends do not shift dramatically "
        "from historical patterns. Often used as a baseline planning scenario."
    ),
    "ssp585": (
        "Fossil-fuelled development (SSP5-8.5)\n"
        "A future with rapid economic growth and high energy demand met largely by fossil fuels. "
        "Often used to stress-test under higher emissions."
    ),
    "historical": "Historical\nObserved / historical reference period used for comparison (not a future projection).",
}

# ---- Risk classification thresholds ----
RISK_THRESHOLDS: dict[str, Tuple[float, float]] = {
    "low": (0.0, 33.3),
    "moderate": (33.3, 66.6),
    "high": (66.6, 100.0),
}

# ---- Logo path ----
LOGO_PATH: str = str(Path(__file__).parent.parent.parent / "resilience_actions_logo_transparent.png")
