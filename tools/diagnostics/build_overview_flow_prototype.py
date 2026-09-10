"""Overview flow prototype — the two-level national screening workflow as one
self-contained HTML page (CHG-0385).

This is a *workflow* prototype for vendor handoff, not a data product. It renders
the flow specified by ``recommended_target_workflow.md`` section 1-8 so the
interaction contract can be judged and implemented:

    India view   districts painted, State/UTs ranked, State/UT means binned
        -> select a State/UT
    State view   districts painted, districts ranked, district scores binned
        -> select a district   (inspection state; no breadcrumb level)

Everything about the score is frozen, exactly as in
``tools.diagnostics.build_heat_risk_frozen_map``: the ``cdf`` ruler, the
``composite_absolute_threshold`` headline, the ``0-100`` domain and the vendored
``WhiteBlueGreenYellowRed`` table. The ``linear`` ruler is not implemented and is
never exposed. This tool re-scores nothing; it reads the pilot's
``district_scores.csv``.

Two honest departures from the target spec, both surfaced in the page itself:

- **The State view paints districts, not blocks.** The spec's State view paints
  block composite scores. The pilot has never scored blocks, so there are no
  block values to paint. Rather than fabricate them, the State view runs in the
  spec's ``District fill`` mode and says so. Every interaction under test --
  drill-down, inspection, histogram binning, bin filtering, breadcrumb,
  Detailed Analysis handoff -- is unaffected.
- **Detailed Analysis is a stub.** The transition and the state it carries are
  real; the destination is a panel that displays that state and nothing more.

Only Heat Risk carries data. The other twelve eligible bundles appear in the
selector as disabled options so the intended screening surface is visible
without any unsupported combination being selectable.

Usage
-----
    python -m tools.diagnostics.build_overview_flow_prototype \
        --scores docs/diagnostics/heat_risk_pilot/district_scores.csv \
        --out docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.config.paths import get_paths_config
from tools.diagnostics.build_heat_risk_frozen_map import (
    CMAP_FLOOR,
    FROZEN_CMAP,
    FROZEN_FIELD,
    FROZEN_RULER,
    FROZEN_VMAX,
    FROZEN_VMIN,
    MISSING_COLOR,
    _geom_path,
    _project,
    ramp_hex,
)
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    BASELINE_REFERENCED_SLUGS,
    load_district_geometry,
)

DEFAULT_SCORES = Path("docs/diagnostics/heat_risk_pilot/district_scores.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html")

#: Douglas-Peucker tolerance in degrees, and SVG canvas width in user units.
#: Finer than the national-only viewer, because the State view zooms in.
DEFAULT_SIMPLIFY = 0.006
CANVAS_WIDTH = 1000.0

#: The six public forward slices. The pilot also holds ``historical|1990-2010``;
#: the workflow's screening surface is scenario-and-period, so the baseline is
#: not offered as a scenario here.
PUBLIC_SLICES: tuple[tuple[str, str], ...] = (
    ("ssp245", "2020-2040"),
    ("ssp245", "2040-2060"),
    ("ssp245", "2060-2080"),
    ("ssp585", "2020-2040"),
    ("ssp585", "2040-2060"),
    ("ssp585", "2060-2080"),
)

SCENARIO_LABELS: dict[str, str] = {
    "ssp245": "Middle-of-the-road (SSP2-4.5)",
    "ssp585": "Fossil-fuelled development (SSP5-8.5)",
}

PERIOD_LABELS: dict[str, str] = {
    "2020-2040": "Early century (2020–2040)",
    "2040-2060": "Mid-century (2040–2060)",
    "2060-2080": "End century (2060–2080)",
}

DEFAULT_SCENARIO = "ssp585"
DEFAULT_PERIOD = "2040-2060"

#: The thirteen eligible scenario-based bundles. Only Heat Risk is scored here.
ELIGIBLE_BUNDLES: tuple[tuple[str, str], ...] = (
    ("Thematic", "Heat Risk"),
    ("Thematic", "Drought Risk"),
    ("Thematic", "Extreme Rainfall | Flash Flood Risk"),
    ("Thematic", "Heat Stress"),
    ("Thematic", "Cold Risk"),
    ("Sector-wise", "Agricultural Risk"),
    ("Sector-wise", "Health Risk"),
    ("Sector-wise", "Industrial Risk"),
    ("Sector-wise", "Investment / Financial Risk"),
    ("Sector-wise", "Infrastructure Risk"),
    ("Sector-wise", "Asset Risk (Thermal Power Plants)"),
    ("Sector-wise", "Asset Risk (Hydropower Plants)"),
    ("Sector-wise", "Life & Livelihood Loss Risk"),
)

#: Display names for the nine absolute-threshold metrics that carry the headline.
METRIC_LABELS: dict[str, str] = {
    "tas_annual_mean": "Annual mean temperature",
    "tasmax_summer_mean": "Summer mean daily maximum",
    "tas_summer_mean": "Summer mean temperature",
    "txx_annual_max": "Hottest day of the year",
    "hwa_heatwave_amplitude": "Heatwave amplitude",
    "txge30_hot_days": "Days above 30°C",
    "txge35_extreme_heat_days": "Days above 35°C",
    "tasmin_tropical_nights_gt25": "Tropical nights above 25°C",
    "tnx_annual_max": "Warmest night of the year",
}

BANDS: tuple[tuple[float, str], ...] = (
    (20.0, "Very Low"),
    (40.0, "Low"),
    (60.0, "Moderate"),
    (80.0, "High"),
    (100.1, "Extreme"),
)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def load_frozen_scores(path: Path) -> pd.DataFrame:
    """Pilot rows for the frozen ruler over the six public slices.

    Raises rather than falling back when the frozen ruler, the frozen headline
    field, or any public slice is absent: a prototype that quietly paints a
    different quantity than its caption claims would mislead the vendor.
    """
    frame = pd.read_csv(path)
    required = ("ruler", "district_key", "state", "district", "scenario", "period", FROZEN_FIELD)
    for column in required:
        if column not in frame.columns:
            raise ValueError(f"{path} has no '{column}' column; not a pilot score table")

    frame = frame.loc[frame["ruler"] == FROZEN_RULER].copy()
    if frame.empty:
        raise ValueError(f"{path} carries no rows for ruler '{FROZEN_RULER}'")

    present = set(map(tuple, frame.loc[:, ["scenario", "period"]].drop_duplicates().values))
    missing = [s for s in PUBLIC_SLICES if s not in present]
    if missing:
        raise ValueError(
            "score table is missing public slices: "
            + ", ".join(f"{sc}/{pe}" for sc, pe in missing)
        )

    wanted = frame.set_index(["scenario", "period"]).index.isin(PUBLIC_SLICES)
    frame = frame.loc[wanted].copy()
    frame[FROZEN_FIELD] = pd.to_numeric(frame[FROZEN_FIELD], errors="coerce")

    metric_columns = [
        f"score__{slug}"
        for slug in METRIC_LABELS
        if f"score__{slug}" in frame.columns
    ]
    if len(metric_columns) != len(METRIC_LABELS):
        found = {c.removeprefix("score__") for c in metric_columns}
        raise ValueError(
            "score table is missing absolute-threshold metric columns: "
            + ", ".join(sorted(set(METRIC_LABELS) - found))
        )
    for column in metric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    keep = ["district_key", "state", "district", "scenario", "period", FROZEN_FIELD]
    return frame.loc[:, keep + metric_columns]


def _competition_ranks(values: Sequence[float]) -> list[int]:
    """Descending competition ranks over full-precision values.

    Highest score is rank 1. Exact equality shares a rank and the following rank
    reflects the number of preceding entries, per the workflow's ranking rule.
    """
    order = sorted(range(len(values)), key=lambda i: -values[i])
    ranks = [0] * len(values)
    previous: Optional[float] = None
    previous_rank = 0
    for position, index in enumerate(order, start=1):
        value = values[index]
        if previous is not None and value == previous:
            ranks[index] = previous_rank
        else:
            ranks[index] = position
            previous_rank = position
            previous = value
    return ranks


def band_of(score: float) -> str:
    """Five-band classification of a score on the frozen 0-100 domain."""
    for upper, label in BANDS:
        if score < upper:
            return label
    return BANDS[-1][1]


def build_tables(
    scores: pd.DataFrame,
    areas: dict[str, float],
) -> tuple[dict, dict, dict]:
    """Per-slice district values, State/UT aggregates, and driver orderings.

    The State/UT headline is the area-weighted mean of its valid district
    composite scores -- the only State statistic the workflow defines. Districts
    with no area are excluded from the weighting but still counted and ranked.
    """
    district_by_slice: dict[str, dict[str, float]] = {}
    state_by_slice: dict[str, dict[str, dict]] = {}
    drivers_by_slice: dict[str, dict[str, list[str]]] = {}

    metric_columns = [f"score__{slug}" for slug in METRIC_LABELS]

    for (scenario, period), block in scores.groupby(["scenario", "period"], sort=False):
        slice_id = f"{scenario}|{period}"
        valid = block.loc[block[FROZEN_FIELD].notna()]

        district_by_slice[slice_id] = {
            str(row.district_key): round(float(getattr(row, FROZEN_FIELD)), 4)
            for row in valid.itertuples(index=False)
        }

        drivers: dict[str, list[str]] = {}
        for row in valid.itertuples(index=False):
            ordered = sorted(
                (
                    (float(getattr(row, column)), column.removeprefix("score__"))
                    for column in metric_columns
                    if pd.notna(getattr(row, column))
                ),
                key=lambda pair: -pair[0],
            )
            drivers[str(row.district_key)] = [slug for _, slug in ordered[:3]]

        state_rows: list[dict] = []
        for state, group in valid.groupby("state", sort=True):
            weights = np.array(
                [areas.get(str(key), float("nan")) for key in group["district_key"]],
                dtype=float,
            )
            values = group[FROZEN_FIELD].to_numpy(dtype=float)
            usable = np.isfinite(weights) & (weights > 0)
            if usable.any():
                mean = float(np.average(values[usable], weights=weights[usable]))
            else:
                mean = float(values.mean())
            metric_means = {
                column.removeprefix("score__"): float(group[column].mean())
                for column in metric_columns
                if group[column].notna().any()
            }
            top = sorted(metric_means.items(), key=lambda pair: -pair[1])[:3]
            state_rows.append(
                {
                    "state": str(state),
                    "mean": round(mean, 4),
                    "n_valid": int(len(group)),
                    "drivers": [slug for slug, _ in top],
                }
            )

        ranks = _competition_ranks([row["mean"] for row in state_rows])
        for row, rank in zip(state_rows, ranks):
            row["rank"] = int(rank)
        state_by_slice[slice_id] = {row.pop("state"): row for row in state_rows}
        drivers_by_slice[slice_id] = drivers

    return district_by_slice, state_by_slice, drivers_by_slice


# ---------------------------------------------------------------------------
# Geometry -> SVG
# ---------------------------------------------------------------------------


def build_shapes(
    data_dir: Path,
    *,
    simplify: float,
    precision: int,
) -> tuple[list[dict], dict[str, str], float]:
    """District paths with bounding boxes, State/UT outline paths, canvas height.

    Every layer is projected against the same national bounds, so the State view
    zooms by moving the SVG viewBox rather than reprojecting: one geometry
    payload serves both views, and identical shapes stay registered across them.
    """
    gdf = load_district_geometry(data_dir)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)
    if simplify > 0:
        gdf["geometry"] = gdf.geometry.simplify(simplify, preserve_topology=True)

    bounds = tuple(float(v) for v in gdf.total_bounds)  # type: ignore[assignment]
    _, _, height = _project(np.array([bounds[0]]), np.array([bounds[1]]), bounds)

    districts: list[dict] = []
    for row in gdf.itertuples(index=False):
        path = _geom_path(row.geometry, bounds, precision)
        if not path:
            continue
        minx, miny, maxx, maxy = (float(v) for v in row.geometry.bounds)
        x0, y1, _ = _project(np.array([minx]), np.array([miny]), bounds)
        x1, y0, _ = _project(np.array([maxx]), np.array([maxy]), bounds)
        districts.append(
            {
                "k": str(getattr(row, "district_key", "")),
                "n": str(getattr(row, "district_name", "")),
                "s": str(getattr(row, "state_name", "")),
                "d": path,
                "b": [
                    round(float(x0[0]), 2),
                    round(float(y0[0]), 2),
                    round(float(x1[0]), 2),
                    round(float(y1[0]), 2),
                ],
            }
        )

    outlines = gdf.dissolve(by="state_name")
    state_paths: dict[str, str] = {}
    for state, geom in zip(outlines.index, outlines.geometry):
        path = _geom_path(geom, bounds, precision)
        if path:
            state_paths[str(state)] = path

    return districts, state_paths, float(height)


def district_areas(data_dir: Path) -> dict[str, float]:
    """Canonical district areas in m^2, read from the geometry property tables."""
    root = Path(data_dir) / "processed_optimised" / "geometry" / "admin" / "district"
    areas: dict[str, float] = {}
    for path in sorted(root.glob("state=*.geojson")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for feature in payload.get("features", []):
            properties = feature.get("properties") or {}
            key = properties.get("district_key")
            area = properties.get("area_m2")
            if key is not None and area is not None:
                areas[str(key)] = float(area)
    if not areas:
        raise FileNotFoundError(f"No district areas found under {root}")
    return areas


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

PAGE_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Overview flow prototype — India Resilience Tool</title>
<style>
  :root {
    --ink: #16202c; --ink-2: #46586c; --ink-3: #7a8794;
    --rule: #dfe4ea; --panel: #ffffff; --ground: #f4f6f8;
    --accent: #1c5fd6; --accent-soft: #e8effc;
    --warn-bg: #fff6e6; --warn-line: #e8c98a; --warn-ink: #6b4c12;
    --coarse: #7a8794; --fine: #7a8794;
    --font: "Segoe UI", -apple-system, BlinkMacSystemFont, Roboto, Helvetica, Arial, sans-serif;
  }
  * { box-sizing: border-box; }
  [hidden] { display: none !important; }
  body { margin: 0; background: var(--ground); color: var(--ink);
         font-family: var(--font); font-size: 14px; line-height: 1.5; }
  a { color: var(--accent); }
  .wrap { max-width: 1500px; margin: 0 auto; padding: 0 20px 40px; }

  header.top { background: var(--panel); border-bottom: 1px solid var(--rule); }
  .top-inner { max-width: 1500px; margin: 0 auto; padding: 14px 20px 0;
               display: flex; flex-wrap: wrap; gap: 18px; align-items: flex-end; }
  .brand { font-size: 17px; font-weight: 650; letter-spacing: -0.01em; }
  .brand span { display: block; font-size: 12px; font-weight: 400; color: var(--ink-3); }
  .controls { display: flex; gap: 14px; flex-wrap: wrap; margin-left: auto; }
  .ctl { display: flex; flex-direction: column; gap: 3px; }
  .ctl label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em;
               color: var(--ink-3); font-weight: 600; }
  .ctl select { font: inherit; font-size: 13px; padding: 6px 9px; border: 1px solid var(--rule);
                border-radius: 6px; background: #fff; color: var(--ink); min-width: 210px; }
  .ctl select:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
  .defaults-note { font-size: 11.5px; color: var(--ink-3); padding: 6px 20px 0;
                   max-width: 1500px; margin: 0 auto; }
  .defaults-note b { color: var(--ink-2); font-weight: 600; }

  nav.crumbs { max-width: 1500px; margin: 0 auto; padding: 10px 20px 12px;
               display: flex; align-items: center; gap: 8px; font-size: 13px; }
  .crumb { background: none; border: 0; font: inherit; color: var(--accent);
           cursor: pointer; padding: 2px 0; }
  .crumb[aria-current] { color: var(--ink); font-weight: 600; cursor: default; }
  .crumb-sep { color: var(--ink-3); }

  .banner { display: flex; gap: 10px; align-items: flex-start; background: var(--warn-bg);
            border: 1px solid var(--warn-line); color: var(--warn-ink);
            border-radius: 8px; padding: 10px 13px; font-size: 12.5px; margin: 0 0 16px; }
  .banner b { font-weight: 650; }

  .grid { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(360px, 1fr);
          gap: 18px; align-items: start; }
  @media (max-width: 1080px) { .grid { grid-template-columns: minmax(0, 1fr); } }

  .card { background: var(--panel); border: 1px solid var(--rule); border-radius: 10px;
          padding: 16px 18px; margin-bottom: 16px; }
  .card > h2 { margin: 0 0 3px; font-size: 13px; text-transform: uppercase;
               letter-spacing: .06em; color: var(--ink-3); font-weight: 650; }
  .card > .sub { margin: 0 0 12px; font-size: 12px; color: var(--ink-3); }

  /* ---- map ---- */
  .mapcard { padding: 12px; }
  .mapwrap { position: relative; background: #fbfcfd; border-radius: 8px; overflow: hidden; }
  svg.map { display: block; width: 100%; height: auto; }
  svg.map path { vector-effect: non-scaling-stroke; }
  .dist { stroke: var(--fine); stroke-opacity: .45; stroke-width: .45px; cursor: pointer; }
  .dist.out { fill: #eceff2 !important; stroke-opacity: .25; cursor: default; pointer-events: none; }
  .dist.muted { opacity: .28; }
  .dist.hl { stroke: #2b3947; stroke-opacity: .9; stroke-width: 1.1px; }
  .stroke-coarse { fill: none; stroke: var(--coarse); stroke-opacity: .95; stroke-width: 1.9px;
                   pointer-events: none; }
  .sel-stroke { fill: none; stroke: var(--accent); stroke-opacity: 1; stroke-width: 2.6px;
                pointer-events: none; }
  .maphead { display: flex; align-items: baseline; gap: 10px; padding: 4px 6px 10px; flex-wrap: wrap; }
  .maphead h2 { margin: 0; font-size: 13px; text-transform: uppercase; letter-spacing: .06em;
                color: var(--ink-3); font-weight: 650; }
  .maphead .sub { font-size: 12px; color: var(--ink-3); }

  /* ---- colourbar ---- */
  .cbar { padding: 12px 6px 2px; }
  .cbar-title { font-size: 12px; font-weight: 600; color: var(--ink-2); margin-bottom: 5px; }
  .cbar-strip { position: relative; height: 13px; border-radius: 3px; border: 1px solid #cfd6dd; }
  .cbar-bracket { position: absolute; top: -5px; height: 23px; border-left: 2px solid #16202c;
                  border-right: 2px solid #16202c; pointer-events: none; }
  .cbar-ticks { position: relative; height: 16px; font-size: 11px; color: var(--ink-3); }
  .cbar-ticks span { position: absolute; transform: translateX(-50%); top: 2px; }
  .cbar-foot { display: flex; gap: 16px; align-items: center; flex-wrap: wrap;
               font-size: 11.5px; color: var(--ink-3); margin-top: 6px; }
  .swatch { display: inline-block; width: 12px; height: 12px; border: 1px solid #c3c9cf;
            border-radius: 2px; vertical-align: -2px; margin-right: 5px; }

  /* ---- headline strip ---- */
  .headline { background: var(--panel); border: 1px solid var(--rule); border-radius: 10px;
              padding: 13px 18px; margin-bottom: 16px; display: flex; align-items: center;
              gap: 18px; flex-wrap: wrap; }
  .headline .hl-name { font-size: 19px; font-weight: 650; letter-spacing: -0.01em; }
  .headline .bigscore { font-size: 32px; font-weight: 680; letter-spacing: -0.02em;
                        line-height: 1; margin-left: 2px; }
  .headline .hl-meta { font-size: 12.5px; color: var(--ink-2); }
  .headline .hl-meta b { font-weight: 650; }
  .headline .hl-scope { display: block; font-size: 11.5px; color: var(--ink-3); }
  .headline .hl-drv { font-size: 11.5px; color: var(--ink-3); }
  .headline .hl-drv b { color: var(--ink-2); font-weight: 600; }
  .headline .spacer { margin-left: auto; }
  .place { font-size: 17px; font-weight: 650; letter-spacing: -0.01em; margin: 2px 0; }
  .place small { display: block; font-size: 12px; font-weight: 400; color: var(--ink-3);
                 letter-spacing: 0; }
  .scoreline { display: flex; align-items: baseline; gap: 10px; margin: 8px 0 4px; flex-wrap: wrap; }
  .bigscore { font-size: 26px; font-weight: 680; letter-spacing: -0.02em; line-height: 1; }
  .band { font-size: 12px; font-weight: 650; padding: 3px 9px; border-radius: 999px;
          border: 1px solid currentColor; white-space: nowrap; }
  .drivers { margin: 12px 0 0; padding: 0; list-style: none; }
  .drivers li { font-size: 13px; padding: 4px 0 4px 16px; position: relative; color: var(--ink-2); }
  .drivers li::before { content: "▸"; position: absolute; left: 0; color: var(--ink-3); }
  .boundary { margin: 12px 0 0; padding: 9px 11px; background: var(--ground);
              border-radius: 6px; font-size: 11.5px; color: var(--ink-2); }
  .btn { font: inherit; font-size: 13px; font-weight: 600; padding: 8px 14px; border-radius: 7px;
         border: 1px solid var(--accent); background: var(--accent); color: #fff; cursor: pointer; }
  .btn.ghost { background: #fff; color: var(--accent); }
  .btn:focus-visible { outline: 2px solid var(--ink); outline-offset: 2px; }
  .btn-row { display: flex; gap: 9px; margin-top: 14px; flex-wrap: wrap; }

  /* ---- histogram ---- */
  .hist { display: grid; grid-template-columns: repeat(10, 1fr); gap: 3px; height: 132px;
          align-items: end; border-bottom: 1px solid var(--rule); position: relative; }
  .hbin { display: flex; flex-direction: column; justify-content: flex-end; height: 100%;
          cursor: pointer; background: none; border: 0; padding: 0; font: inherit; }
  .hbin[data-empty="1"] { cursor: default; }
  .hbar { border-radius: 3px 3px 0 0; min-height: 2px; transition: opacity .12s; }
  .hbin[data-empty="1"] .hbar { background: #eef1f4 !important; height: 2px; }
  .hcount { font-size: 10.5px; color: var(--ink-3); text-align: center; padding-bottom: 3px; }
  .hbin.dim .hbar { opacity: .3; }
  .hbin.pinned .hbar { outline: 2px solid var(--accent); outline-offset: 1px; }
  .haxis { display: grid; grid-template-columns: repeat(10, 1fr); gap: 3px; font-size: 10px;
           color: var(--ink-3); text-align: center; padding-top: 4px; }
  .bandticks { display: grid; grid-template-columns: repeat(10, 1fr); gap: 3px;
               font-size: 9.5px; color: var(--ink-3); text-transform: uppercase;
               letter-spacing: .05em; padding-top: 2px; }
  .bandticks span { grid-column: span 2; text-align: center; border-top: 1px solid var(--rule);
                    padding-top: 3px; }
  .painted { font-size: 12px; color: var(--ink-2); margin-top: 12px; padding-top: 10px;
             border-top: 1px dashed var(--rule); }
  .filterbar { display: flex; align-items: center; gap: 10px; font-size: 12px;
               margin-top: 10px; color: var(--accent); }
  .linkbtn { background: none; border: 0; font: inherit; font-size: 12px; color: var(--accent);
             cursor: pointer; padding: 0; text-decoration: underline; }

  /* ---- ranking ---- */
  table.rank { width: 100%; border-collapse: collapse; font-size: 13px; }
  table.rank th { text-align: left; font-size: 10.5px; text-transform: uppercase;
                  letter-spacing: .06em; color: var(--ink-3); font-weight: 650;
                  padding: 0 6px 6px; border-bottom: 1px solid var(--rule); }
  table.rank th.num, table.rank td.num { text-align: right; }
  table.rank td { padding: 6px; border-bottom: 1px solid #f0f2f5; }
  table.rank tbody tr { cursor: pointer; }
  table.rank tbody tr:hover { background: var(--accent-soft); }
  table.rank tr.selected { background: var(--accent-soft); box-shadow: inset 3px 0 0 var(--accent); }
  .rk { color: var(--ink-3); font-variant-numeric: tabular-nums; width: 34px; }
  .sc { font-variant-numeric: tabular-nums; font-weight: 600; }
  .bandcell { font-size: 11px; color: var(--ink-3); }

  /* ---- inspection ---- */
  .insp { border-left: 3px solid var(--accent); }
  .insp .close { float: right; background: none; border: 0; font-size: 18px; line-height: 1;
                 color: var(--ink-3); cursor: pointer; }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; font-size: 13px;
        margin: 8px 0 0; }
  .kv dt { color: var(--ink-3); }
  .kv dd { margin: 0; }
  .stub { background: #f7f9fb; border: 1px dashed #c6cfd8; border-radius: 8px;
          padding: 12px 14px; font-size: 12.5px; color: var(--ink-2); }
  .stub h3 { margin: 0 0 8px; font-size: 13px; }

  /* ---- tooltip ---- */
  #tip { position: fixed; pointer-events: none; z-index: 60; background: #16202c; color: #fff;
         border-radius: 7px; padding: 9px 11px; font-size: 12.5px; max-width: 290px;
         box-shadow: 0 6px 20px rgba(0,0,0,.24); opacity: 0; transition: opacity .1s; }
  #tip b { display: block; font-size: 13px; margin-bottom: 3px; }
  #tip .t-row { color: #c3ced9; }
  #tip .t-row em { color: #fff; font-style: normal; font-weight: 600; }

  .method { font-size: 12px; color: var(--ink-2); }
  .method h3 { font-size: 12px; margin: 12px 0 4px; text-transform: uppercase;
               letter-spacing: .06em; color: var(--ink-3); }
  .method code { background: var(--ground); padding: 1px 4px; border-radius: 3px; font-size: 11.5px; }
  .method ul { margin: 4px 0; padding-left: 18px; }
  .method li { margin: 2px 0; }
  details.method-wrap > summary { cursor: pointer; font-size: 13px; font-weight: 600; }
</style>
</head>
<body>

<header class="top">
  <div class="top-inner">
    <div class="brand">India Resilience Tool — Overview
      <span>Workflow prototype for vendor implementation · not a data release</span>
    </div>
    <div class="controls">
      <div class="ctl">
        <label for="sel-bundle">Bundle</label>
        <select id="sel-bundle"></select>
      </div>
      <div class="ctl">
        <label for="sel-scenario">Scenario</label>
        <select id="sel-scenario"></select>
      </div>
      <div class="ctl">
        <label for="sel-period">Period</label>
        <select id="sel-period"></select>
      </div>
    </div>
  </div>
  <p class="defaults-note" id="defaults-note"></p>
  <nav class="crumbs" id="crumbs" aria-label="Geography"></nav>
</header>

<div class="wrap">
  <div class="banner">
    <div>
      <p style="margin:0 0 6px"><b>Two departures from the target spec, both deliberate.</b></p>
      <p style="margin:0">
      <b>1. The State view paints districts, not blocks.</b> The specification paints block
      composite scores here. No block has ever been scored on the frozen ruler, so there are no
      block values to paint and none have been invented. The State view therefore runs in the
      spec's <code>District fill</code> mode. Drill-down, inspection, binning, filtering, the
      breadcrumb and the Detailed Analysis handoff are unaffected.
      <br><b>2. Detailed Analysis is a stub.</b> The action and the state it carries are real;
      the destination only displays that state.
      </p>
    </div>
  </div>

  <div class="grid">
    <div>
      <div class="headline" id="headline" hidden></div>
      <div class="card mapcard">
        <div class="maphead">
          <h2 id="map-title">National view</h2>
          <span class="sub" id="map-sub"></span>
        </div>
        <div class="mapwrap">
          <svg class="map" id="map" preserveAspectRatio="xMidYMid meet" role="img"
               aria-label="Choropleth of district bundle scores"></svg>
        </div>
        <div class="cbar">
          <div class="cbar-title" id="cbar-title">Heat Risk score</div>
          <div class="cbar-strip" id="cbar-strip"><div class="cbar-bracket" id="cbar-bracket"></div></div>
          <div class="cbar-ticks" id="cbar-ticks"></div>
          <div class="cbar-foot">
            <span><span class="swatch" style="background:#d5d8dc"></span>No valid data</span>
            <span id="cbar-range"></span>
            <span>Domain fixed 0–100 · never rescaled by selection</span>
          </div>
        </div>
      </div>

    </div>

    <div>
      <div class="card">
        <h2 id="hist-title">Distribution</h2>
        <p class="sub" id="hist-sub"></p>
        <div class="hist" id="hist"></div>
        <div class="haxis" id="haxis"></div>
        <div class="bandticks">
          <span>Very Low</span><span>Low</span><span>Moderate</span><span>High</span><span>Extreme</span>
        </div>
        <div class="filterbar" id="filterbar" hidden>
          <span id="filter-label"></span>
          <button class="linkbtn" id="clear-filter">Clear filter</button>
        </div>
        <p class="painted" id="painted"></p>
      </div>
      <div class="card" id="ranking"></div>
      <div id="inspection"></div>
      <div class="card">
        <details class="method-wrap">
          <summary>Method note</summary>
          <div class="method" id="method"></div>
        </details>
      </div>
    </div>
  </div>
</div>

<div id="tip" aria-hidden="true"></div>

<script id="payload" type="application/json">__PAYLOAD__</script>
<script>
(function () {
  "use strict";
  var D = JSON.parse(document.getElementById("payload").textContent);

  var BANDS = [[20,"Very Low"],[40,"Low"],[60,"Moderate"],[80,"High"],[100.1,"Extreme"]];
  var BAND_INK = {"Very Low":"#2f6f4f","Low":"#3d7a55","Moderate":"#8a6a17",
                  "High":"#a4560f","Extreme":"#95261c"};

  var S = {
    bundle: D.default_bundle,
    scenario: D.default_scenario,
    period: D.default_period,
    view: "india",
    state: null,
    district: null,
    pinned: null,
    showAll: false,
    da: null
  };

  var byKey = {};
  D.districts.forEach(function (d) { byKey[d.k] = d; });
  var statesOf = {};
  D.districts.forEach(function (d) {
    (statesOf[d.s] = statesOf[d.s] || []).push(d);
  });
  var STATE_NAMES = Object.keys(statesOf).sort();

  function sliceId() { return S.scenario + "|" + S.period; }
  function dScores() { return D.district_scores[sliceId()] || {}; }
  function sStats() { return D.state_stats[sliceId()] || {}; }
  function dDrivers() { return D.drivers[sliceId()] || {}; }

  function band(v) {
    for (var i = 0; i < BANDS.length; i++) if (v < BANDS[i][0]) return BANDS[i][1];
    return "Extreme";
  }
  function colour(v) {
    if (v === undefined || v === null || isNaN(v)) return D.missing;
    var i = Math.round(v);
    if (i < 0) i = 0; if (i > 100) i = 100;
    return D.ramp[i];
  }
  function fmt(v) { return (v === undefined || v === null || isNaN(v)) ? "—" : v.toFixed(1); }

  /* Displayed score per row, widened only where 1dp would make two unequal
     scores look identical and so make the rank order unexplainable. Genuine
     full-precision ties keep 1dp and share a rank. */
  function displayScores(rows) {
    var out = {}, groups = {};
    rows.forEach(function (r) { (groups[fmt(r.score)] = groups[fmt(r.score)] || []).push(r); });
    Object.keys(groups).forEach(function (label) {
      var g = groups[label];
      var distinct = {};
      g.forEach(function (r) { distinct[r.score] = 1; });
      var dp = 1;
      if (Object.keys(distinct).length > 1) {
        for (dp = 2; dp <= 6; dp++) {
          var seen = {}, clash = false;
          g.forEach(function (r) {
            var t = r.score.toFixed(dp);
            if (seen[t] && seen[t] !== r.score) clash = true;
            seen[t] = r.score;
          });
          if (!clash) break;
        }
      }
      g.forEach(function (r) { out[r.key] = r.score.toFixed(dp); });
    });
    return out;
  }
  function esc(s) { return String(s).replace(/[&<>]/g, function (c) {
    return c === "&" ? "&amp;" : c === "<" ? "&lt;" : "&gt;"; }); }
  function metricLabel(slug) { return D.metric_labels[slug] || slug; }

  /* ---------- competition ranks over the current cohort ---------- */
  function rankedDistricts(state) {
    var sc = dScores();
    var rows = (statesOf[state] || []).map(function (d) {
      return { key: d.k, name: d.n, state: d.s, score: sc[d.k] };
    }).filter(function (r) { return r.score !== undefined; });
    rows.sort(function (a, b) { return b.score - a.score; });
    var prev = null, prevRank = 0;
    rows.forEach(function (r, i) {
      if (prev !== null && r.score === prev) { r.rank = prevRank; }
      else { r.rank = i + 1; prevRank = i + 1; prev = r.score; }
    });
    return rows;
  }
  function rankedStates() {
    var st = sStats();
    var rows = Object.keys(st).map(function (name) {
      return { key: name, name: name, score: st[name].mean, rank: st[name].rank,
               n: st[name].n_valid, drivers: st[name].drivers };
    });
    rows.sort(function (a, b) { return a.rank - b.rank || a.name.localeCompare(b.name); });
    return rows;
  }

  /* ---------- cohort the current view ranks and bins ---------- */
  function cohort() {
    return S.view === "india" ? rankedStates() : rankedDistricts(S.state);
  }

  function binIndex(v) { var i = Math.floor(v / 10); return i > 9 ? 9 : (i < 0 ? 0 : i); }

  /* ================= map ================= */
  var svg = document.getElementById("map");
  var built = false;
  var nodes = {};

  function buildMap() {
    var ns = "http://www.w3.org/2000/svg";
    svg.setAttribute("viewBox", "0 0 " + D.width + " " + D.height);
    var gd = document.createElementNS(ns, "g");
    gd.setAttribute("id", "g-dist");
    D.districts.forEach(function (d) {
      var p = document.createElementNS(ns, "path");
      p.setAttribute("d", d.d);
      p.setAttribute("class", "dist");
      p.dataset.key = d.k;
      gd.appendChild(p);
      nodes[d.k] = p;
    });
    svg.appendChild(gd);

    var gs = document.createElementNS(ns, "g");
    gs.setAttribute("id", "g-state");
    Object.keys(D.state_paths).forEach(function (name) {
      var p = document.createElementNS(ns, "path");
      p.setAttribute("d", D.state_paths[name]);
      p.setAttribute("class", "stroke-coarse");
      p.dataset.state = name;
      gs.appendChild(p);
    });
    svg.appendChild(gs);

    var sel = document.createElementNS(ns, "path");
    sel.setAttribute("id", "g-sel");
    sel.setAttribute("class", "sel-stroke");
    sel.setAttribute("d", "");
    svg.appendChild(sel);

    gd.addEventListener("mousemove", onHover);
    gd.addEventListener("mouseleave", hideTip);
    gd.addEventListener("click", onMapClick);
    built = true;
  }

  function viewBox() {
    if (S.view === "india") return [0, 0, D.width, D.height];
    var ds = statesOf[S.state] || [];
    var x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9;
    ds.forEach(function (d) {
      if (d.b[0] < x0) x0 = d.b[0]; if (d.b[1] < y0) y0 = d.b[1];
      if (d.b[2] > x1) x1 = d.b[2]; if (d.b[3] > y1) y1 = d.b[3];
    });
    var w = x1 - x0, h = y1 - y0, pad = Math.max(w, h) * 0.06;
    x0 -= pad; y0 -= pad; w += pad * 2; h += pad * 2;
    /* a fixed display aspect: shapes never distort, and the card height is
       stable across State/UTs rather than jumping with each one's extent */
    var ar = 1.3;
    if (w / h > ar) { var nh = w / ar; y0 -= (nh - h) / 2; h = nh; }
    else { var nw = h * ar; x0 -= (nw - w) / 2; w = nw; }
    return [x0, y0, w, h];
  }

  function paintMap() {
    if (!built) buildMap();
    var vb = viewBox();
    svg.setAttribute("viewBox", vb.join(" "));

    var sc = dScores();
    var pin = S.pinned;
    var inScope = S.view === "india" ? null : S.state;

    /* which painted units the pinned bin emphasises */
    var emph = null;
    if (pin !== null) {
      emph = {};
      if (S.view === "india") {
        var st = sStats();
        Object.keys(st).forEach(function (name) {
          if (binIndex(st[name].mean) === pin) {
            (statesOf[name] || []).forEach(function (d) { emph[d.k] = 1; });
          }
        });
      } else {
        rankedDistricts(S.state).forEach(function (r) {
          if (binIndex(r.score) === pin) emph[r.key] = 1;
        });
      }
    }

    D.districts.forEach(function (d) {
      var p = nodes[d.k];
      var out = inScope !== null && d.s !== inScope;
      p.classList.toggle("out", out);
      p.setAttribute("fill", out ? "#eceff2" : colour(sc[d.k]));
      p.classList.toggle("muted", !out && emph !== null && !emph[d.k]);
      p.classList.remove("hl");
    });

    /* boundary grammar: coarse stroke = the unit the previous view painted */
    var coarse = svg.querySelectorAll("#g-state path");
    for (var i = 0; i < coarse.length; i++) {
      var name = coarse[i].dataset.state;
      coarse[i].style.display = (inScope === null || name === inScope) ? "" : "none";
    }

    var selPath = document.getElementById("g-sel");
    selPath.setAttribute("d", S.district && byKey[S.district] ? byKey[S.district].d : "");
  }

  /* ---------- hover ---------- */
  var tip = document.getElementById("tip");
  function showTip(html, ev) {
    tip.innerHTML = html;
    tip.style.opacity = "1";
    var x = ev.clientX + 16, y = ev.clientY + 16;
    var r = tip.getBoundingClientRect();
    if (x + r.width > window.innerWidth - 10) x = ev.clientX - r.width - 16;
    if (y + r.height > window.innerHeight - 10) y = ev.clientY - r.height - 16;
    tip.style.left = x + "px"; tip.style.top = y + "px";
  }
  function hideTip() {
    tip.style.opacity = "0";
    for (var k in nodes) nodes[k].classList.remove("hl");
  }

  function onHover(ev) {
    var t = ev.target;
    if (!t.dataset || !t.dataset.key) { hideTip(); return; }
    var d = byKey[t.dataset.key];
    if (!d) { hideTip(); return; }
    for (var k in nodes) nodes[k].classList.remove("hl");

    if (S.view === "india") {
      /* the national tooltip is State-level only, though districts are painted */
      (statesOf[d.s] || []).forEach(function (x) { nodes[x.k].classList.add("hl"); });
      var st = sStats()[d.s];
      if (!st) { showTip("<b>" + esc(d.s) + "</b><div class='t-row'>No valid data</div>", ev); return; }
      var total = Object.keys(sStats()).length;
      showTip(
        "<b>" + esc(d.s) + "</b>" +
        "<div class='t-row'>Area-weighted mean district score <em>" + fmt(st.mean) + "</em> · " +
        esc(band(st.mean)) + "</div>" +
        "<div class='t-row'>Rank <em>" + st.rank + "</em> of " + total + " State/UTs</div>" +
        "<div class='t-row'>" + st.n_valid + " valid districts</div>", ev);
    } else {
      if (d.s !== S.state) { hideTip(); return; }
      nodes[d.k].classList.add("hl");
      var sc = dScores()[d.k];
      var rows = rankedDistricts(S.state);
      var me = null;
      rows.forEach(function (r) { if (r.key === d.k) me = r; });
      showTip(
        "<b>" + esc(d.n) + "</b>" +
        "<div class='t-row'>" + esc(d.s) + "</div>" +
        (me
          ? "<div class='t-row'>Score <em>" + fmt(sc) + "</em> · " + esc(band(sc)) + "</div>" +
            "<div class='t-row'>Rank <em>" + me.rank + "</em> of " + rows.length +
            " valid districts</div>"
          : "<div class='t-row'>No valid data</div>"), ev);
    }
  }

  function onMapClick(ev) {
    var t = ev.target;
    if (!t.dataset || !t.dataset.key) return;
    var d = byKey[t.dataset.key];
    if (!d) return;
    if (S.view === "india") { selectState(d.s); }
    else if (d.s === S.state) { selectDistrict(d.k); }
  }

  /* ================= state transitions ================= */
  function selectState(name) {
    S.view = "state"; S.state = name; S.district = null;
    S.pinned = null; S.showAll = false; S.da = null;
    render();
  }
  function goIndia() {
    S.view = "india"; S.state = null; S.district = null;
    S.pinned = null; S.showAll = false; S.da = null;
    render();
  }
  function selectDistrict(key) {
    var d = byKey[key];
    if (!d) return;
    /* a block outside the selected district would reselect its parent district;
       with no block layer, the district is the only inspection target here */
    if (d.s !== S.state) { S.view = "state"; S.state = d.s; S.pinned = null; }
    S.district = key; S.da = null;
    render();
  }

  /* ================= histogram ================= */
  function renderHistogram() {
    var rows = cohort();
    var counts = new Array(10).fill(0);
    var members = []; for (var i = 0; i < 10; i++) members.push([]);
    rows.forEach(function (r) {
      var b = binIndex(r.score);
      counts[b]++; members[b].push(r.name);
    });
    var max = Math.max.apply(null, counts) || 1;

    document.getElementById("hist-title").textContent =
      S.view === "india"
        ? "Distribution of State/UT mean scores · " + rows.length + " units"
        : "Distribution of district scores · " + rows.length + " districts";
    document.getElementById("hist-sub").textContent =
      S.view === "india"
        ? "Bins the units this view ranks — State/UTs — not the districts it paints."
        : "Bins the districts this view ranks within " + S.state + ".";

    var host = document.getElementById("hist");
    host.innerHTML = "";
    counts.forEach(function (c, i) {
      var b = document.createElement("button");
      b.className = "hbin" + (S.pinned !== null && S.pinned !== i ? " dim" : "") +
                    (S.pinned === i ? " pinned" : "");
      b.dataset.empty = c === 0 ? "1" : "0";
      b.type = "button";
      b.setAttribute("aria-label", (i * 10) + " to " + (i * 10 + 10) + ", " + c + " units");
      var n = document.createElement("span");
      n.className = "hcount"; n.textContent = c ? c : "";
      var bar = document.createElement("span");
      bar.className = "hbar";
      bar.style.height = c ? Math.max(4, (c / max) * 104) + "px" : "2px";
      bar.style.background = colour(i * 10 + 5);
      b.appendChild(n); b.appendChild(bar);
      if (c > 0) {
        b.addEventListener("click", function () {
          S.pinned = (S.pinned === i) ? null : i; S.showAll = false; render();
        });
        b.addEventListener("mousemove", function (ev) {
          var names = members[i];
          var shown = names.slice(0, 6).map(esc).join(", ");
          if (names.length > 6) shown += " +" + (names.length - 6) + " more";
          showTip("<b>" + (i * 10) + "–" + (i * 10 + 10) + "</b>" +
                  "<div class='t-row'><em>" + c + "</em> " +
                  (S.view === "india" ? "State/UTs" : "districts") + " · " +
                  (100 * c / rows.length).toFixed(0) + "%</div>" +
                  "<div class='t-row'>" + shown + "</div>", ev);
        });
        b.addEventListener("mouseleave", hideTip);
      }
      host.appendChild(b);
    });

    var ax = document.getElementById("haxis");
    ax.innerHTML = "";
    for (var j = 0; j < 10; j++) {
      var s = document.createElement("span");
      s.textContent = (j * 10) + "–" + (j * 10 + 10);
      ax.appendChild(s);
    }

    var fb = document.getElementById("filterbar");
    fb.hidden = S.pinned === null;
    if (S.pinned !== null) {
      document.getElementById("filter-label").textContent =
        "Ranking filtered to " + (S.pinned * 10) + "–" + (S.pinned * 10 + 10) +
        " · original ranks retained";
    }

    /* the painted units, as plain figures rather than a second chart */
    var sc = dScores();
    var vals = [];
    D.districts.forEach(function (d) {
      if (S.view === "state" && d.s !== S.state) return;
      var v = sc[d.k]; if (v !== undefined) vals.push(v);
    });
    vals.sort(function (a, b) { return a - b; });
    var med = vals.length ? (vals.length % 2 ? vals[(vals.length - 1) / 2]
              : (vals[vals.length / 2 - 1] + vals[vals.length / 2]) / 2) : NaN;
    document.getElementById("painted").textContent =
      "Painted: " + vals.length + " districts · median " + fmt(med) +
      " · min–max " + fmt(vals[0]) + "–" + fmt(vals[vals.length - 1]);
    return vals;
  }

  /* ================= colourbar ================= */
  function renderColourbar(vals) {
    var strip = document.getElementById("cbar-strip");
    strip.style.background = "linear-gradient(to right," + D.ramp.join(",") + ")";
    var ticks = document.getElementById("cbar-ticks");
    if (!ticks.childNodes.length) {
      [0, 20, 40, 60, 80, 100].forEach(function (t) {
        var s = document.createElement("span");
        s.textContent = t; s.style.left = t + "%";
        ticks.appendChild(s);
      });
    }
    var br = document.getElementById("cbar-bracket");
    if (vals.length) {
      br.style.display = "";
      br.style.left = vals[0] + "%";
      br.style.width = Math.max(0.6, vals[vals.length - 1] - vals[0]) + "%";
      document.getElementById("cbar-range").textContent =
        "Range in view " + fmt(vals[0]) + "–" + fmt(vals[vals.length - 1]);
    } else {
      br.style.display = "none";
      document.getElementById("cbar-range").textContent = "";
    }
    document.getElementById("cbar-title").textContent = S.bundle + " score";
  }

  /* ================= answer card ================= */
  function bandPill(v) {
    var b = band(v);
    return "<span class='band' style='color:" + BAND_INK[b] + "'>" + b + "</span>";
  }

  /* The State-view Headline the layout contract requires -- score, band, rank,
     valid count, drivers and the one Detailed Analysis action -- as a strip
     above the map rather than a card of prose. The national view has no
     selected unit and therefore no headline: the ranking and the distribution
     are the answer there. */
  function renderHeadline() {
    var host = document.getElementById("headline");
    if (S.view !== "state") { host.hidden = true; host.innerHTML = ""; return; }
    host.hidden = false;

    var st = sStats()[S.state];
    var rows = rankedDistricts(S.state);
    var total = Object.keys(sStats()).length;
    var drv = (st && st.drivers) || [];

    host.innerHTML =
      "<span class='hl-name'>" + esc(S.state) + "</span>" +
      "<span class='bigscore'>" + fmt(st ? st.mean : NaN) + "</span>" +
      (st ? bandPill(st.mean) : "") +
      "<span class='hl-meta'>Area-weighted mean district score · rank <b>" +
        (st ? st.rank : "—") + "</b> of " + total + " State/UTs · <b>" +
        rows.length + "</b> valid districts" +
        "<span class='hl-scope'>The area-weighted average of its districts' national " +
        "scores — not a percentile among States. Hazard-only.</span></span>" +
      (drv.length
        ? "<span class='hl-drv'><b>Drivers</b> " +
          drv.map(function (x) { return esc(metricLabel(x)); }).join(" · ") + "</span>"
        : "") +
      "<span class='spacer'></span>" +
      "<button class='btn' id='to-da'>Explore in Detailed Analysis</button>";

    document.getElementById("to-da").addEventListener("click", function () {
      S.da = { level: "State/UT", unit: S.state }; render();
    });
  }

  /* ================= ranking ================= */
  function renderRanking() {
    var host = document.getElementById("ranking");
    var rows = cohort();
    if (S.pinned !== null) {
      rows = rows.filter(function (r) { return binIndex(r.score) === S.pinned; });
    }
    var shown = S.showAll ? rows : rows.slice(0, 10);
    var isIndia = S.view === "india";
    var shownScore = displayScores(shown);

    var html =
      "<h2>" + (isIndia ? "State/UT ranking" : "District ranking — " + esc(S.state)) + "</h2>" +
      "<p class='sub'>" +
        (isIndia
          ? "Area-weighted mean district score, competition ranks over full precision. " +
            "Select a row to open that State/UT."
          : "Composite score on the frozen national scale, ranked within " + esc(S.state) +
            ". Select a row to inspect that district.") +
      "</p>" +
      "<table class='rank'><thead><tr><th class='rk'>#</th><th>" +
        (isIndia ? "State / UT" : "District") +
        "</th><th class='num'>Score</th><th>Band</th></tr></thead><tbody>";
    var tied = {};
    shown.forEach(function (r) { tied[r.rank] = (tied[r.rank] || 0) + 1; });
    shown.forEach(function (r) {
      var selected = isIndia ? false : (r.key === S.district);
      html += "<tr data-key='" + esc(r.key) + "'" + (selected ? " class='selected'" : "") + ">" +
        "<td class='rk'>" + r.rank + (tied[r.rank] > 1 ? "=" : "") + "</td>" +
        "<td>" + esc(r.name) + "</td>" +
        "<td class='num sc'>" + shownScore[r.key] + "</td>" +
        "<td class='bandcell'>" + band(r.score) + "</td></tr>";
    });
    html += "</tbody></table>";
    if (rows.length > 10) {
      html += "<div class='filterbar'><button class='linkbtn' id='toggle-all'>" +
        (S.showAll ? "Show top 10" : "View all " + rows.length) + "</button></div>";
    }
    if (!rows.length) html += "<p class='sub'>No units in this filter.</p>";
    host.innerHTML = html;

    var t = host.querySelector("tbody");
    if (t) t.addEventListener("click", function (ev) {
      var tr = ev.target.closest("tr");
      if (!tr) return;
      if (isIndia) selectState(tr.dataset.key); else selectDistrict(tr.dataset.key);
    });
    var ta = document.getElementById("toggle-all");
    if (ta) ta.addEventListener("click", function () { S.showAll = !S.showAll; renderRanking(); });
  }

  /* ================= inspection + DA stub ================= */
  function renderInspection() {
    var host = document.getElementById("inspection");
    var html = "";

    if (S.view === "state" && S.district) {
      var d = byKey[S.district];
      var rows = rankedDistricts(S.state);
      var me = null;
      rows.forEach(function (r) { if (r.key === S.district) me = r; });
      var drv = (dDrivers()[S.district] || []);
      html +=
        "<div class='card insp'>" +
        "<button class='close' id='close-insp' aria-label='Clear district selection'>×</button>" +
        "<h2>District inspection</h2>" +
        "<div class='place' style='font-size:17px'>" + esc(d.n) +
          "<small>" + esc(d.s) + "</small></div>" +
        "<div class='scoreline'><span class='bigscore' style='font-size:26px'>" +
          fmt(me ? me.score : NaN) + "</span>" + (me ? bandPill(me.score) : "") + "</div>" +
        "<dl class='kv'>" +
          "<dt>Rank</dt><dd>" + (me ? me.rank + " of " + rows.length + " valid districts in " +
            esc(S.state) : "—") + "</dd>" +
          "<dt>Coverage</dt><dd>Complete — all 9 headline metrics valid</dd>" +
          "<dt>Blocks</dt><dd><em>Not scored in this prototype.</em> The specified panel shows " +
            "the min–max score range of this district's blocks and their count.</dd>" +
        "</dl>" +
        (drv.length
          ? "<ul class='drivers'>" + drv.map(function (s) {
              return "<li>" + esc(metricLabel(s)) + "</li>"; }).join("") + "</ul>"
          : "") +
        "<div class='btn-row'><button class='btn' id='da-dist'>Explore in Detailed Analysis</button>" +
        "</div></div>";
    }

    if (S.da) {
      html +=
        "<div class='card'><h2>Detailed Analysis</h2>" +
        "<div class='stub'><h3>Stub destination</h3>" +
        "<p style='margin:0 0 8px'>Detailed Analysis is not designed in this prototype. What it " +
        "receives across the seam is real, and is exactly this:</p>" +
        "<dl class='kv'>" +
          "<dt>Bundle</dt><dd>" + esc(S.bundle) + "</dd>" +
          "<dt>Scenario</dt><dd>" + esc(D.scenario_labels[S.scenario]) + "</dd>" +
          "<dt>Period</dt><dd>" + esc(D.period_labels[S.period]) + "</dd>" +
          "<dt>Level</dt><dd>" + esc(S.da.level) + "</dd>" +
          "<dt>Unit</dt><dd>" + esc(S.da.unit) + "</dd>" +
          "<dt>Opens on</dt><dd>the composite metric for " + esc(S.bundle) +
            ", never <code>Metric = All</code></dd>" +
        "</dl>" +
        "<p style='margin:10px 0 0'>Hover, bin filter and temporary map emphasis are not " +
        "carried across.</p></div>" +
        "<div class='btn-row'><button class='btn ghost' id='back-ov'>Back to Overview</button></div>" +
        "</div>";
    }

    host.innerHTML = html;
    var c = document.getElementById("close-insp");
    if (c) c.addEventListener("click", function () { S.district = null; S.da = null; render(); });
    var dd = document.getElementById("da-dist");
    if (dd) dd.addEventListener("click", function () {
      S.da = { level: "District", unit: byKey[S.district].n + ", " + S.state }; render();
    });
    var bo = document.getElementById("back-ov");
    if (bo) bo.addEventListener("click", function () { S.da = null; render(); });
  }

  /* ================= chrome ================= */
  function renderCrumbs() {
    var host = document.getElementById("crumbs");
    host.innerHTML = "";
    function crumb(label, current, fn) {
      var b = document.createElement("button");
      b.className = "crumb"; b.textContent = label; b.type = "button";
      if (current) b.setAttribute("aria-current", "page");
      else b.addEventListener("click", fn);
      host.appendChild(b);
    }
    crumb("India", S.view === "india", goIndia);
    if (S.view === "state") {
      var sep = document.createElement("span");
      sep.className = "crumb-sep"; sep.textContent = "›";
      host.appendChild(sep);
      crumb(S.state, true, null);
    }
    if (S.district) {
      var note = document.createElement("span");
      note.className = "crumb-sep";
      note.style.marginLeft = "10px";
      note.style.fontSize = "12px";
      note.textContent = "· inspecting " + byKey[S.district].n +
        " (an inspection state — it adds no breadcrumb level)";
      host.appendChild(note);
    }
  }

  function renderMapHead() {
    document.getElementById("map-title").textContent =
      S.view === "india" ? "National view — districts painted" :
      S.state + " — districts painted";
    document.getElementById("map-sub").textContent =
      S.view === "india"
        ? "Thick State/UT boundary, thin district boundary. State/UT polygons are never filled."
        : "Block fill is the production default; blocks are unscored, so district fill stands in.";
  }

  function renderMethod() {
    document.getElementById("method").innerHTML = D.method_html;
  }

  function render() {
    renderCrumbs();
    renderMapHead();
    paintMap();
    var vals = renderHistogram();
    renderColourbar(vals);
    renderHeadline();
    renderRanking();
    renderInspection();
  }

  /* ---------- selectors ---------- */
  function buildSelectors() {
    var b = document.getElementById("sel-bundle");
    var groups = {};
    D.bundles.forEach(function (row) {
      var g = groups[row[0]];
      if (!g) { g = groups[row[0]] = document.createElement("optgroup"); g.label = row[0]; b.appendChild(g); }
      var o = document.createElement("option");
      o.value = row[1]; o.textContent = row[1];
      if (row[1] !== D.default_bundle) { o.disabled = true; o.textContent = row[1] + " — not scored in this prototype"; }
      g.appendChild(o);
    });
    b.value = D.default_bundle;

    var sc = document.getElementById("sel-scenario");
    Object.keys(D.scenario_labels).forEach(function (k) {
      var o = document.createElement("option");
      o.value = k; o.textContent = D.scenario_labels[k];
      sc.appendChild(o);
    });
    sc.value = S.scenario;

    var pe = document.getElementById("sel-period");
    Object.keys(D.period_labels).forEach(function (k) {
      var o = document.createElement("option");
      o.value = k; o.textContent = D.period_labels[k];
      pe.appendChild(o);
    });
    pe.value = S.period;

    function onSliceChange() {
      S.scenario = sc.value; S.period = pe.value;
      /* geography survives; transient emphasis and the DA stub do not */
      S.pinned = null; S.showAll = false; S.da = null;
      hideTip();
      renderDefaultsNote();
      render();
    }
    sc.addEventListener("change", onSliceChange);
    pe.addEventListener("change", onSliceChange);
  }

  function renderDefaultsNote() {
    var isDefault = S.scenario === D.default_scenario && S.period === D.default_period;
    document.getElementById("defaults-note").innerHTML = isDefault
      ? "Showing the public defaults — <b>Heat Risk</b>, <b>" +
        esc(D.scenario_labels[D.default_scenario]) + "</b>, <b>" +
        esc(D.period_labels[D.default_period]) + "</b>. These are defaults, not your selections."
      : "Selection changed from the defaults (<b>Heat Risk · " +
        esc(D.scenario_labels[D.default_scenario]) + " · " +
        esc(D.period_labels[D.default_period]) + "</b>).";
  }

  document.getElementById("clear-filter").addEventListener("click", function () {
    S.pinned = null; render();
  });

  buildSelectors();
  renderDefaultsNote();
  renderMethod();
  render();
})();
</script>
</body>
</html>
"""


METHOD_HTML = """
<h3>What the score is</h3>
<p>The <b>Heat Risk</b> bundle composite for a district, on a frozen national
<code>0-100</code> scale. It is hazard-only: it does not include exposure, vulnerability or
resilience, and scores are not comparable between bundles.</p>

<h3>The frozen ruler</h3>
<ul>
  <li><b>Ruler</b> <code>cdf</code> — the pooled mid-rank empirical CDF. A score answers
      &ldquo;how unusual is this value within the pooled national sample?&rdquo;, not
      &ldquo;how far up India's physical range is it?&rdquo;. The <code>linear</code> ruler is
      not implemented.</li>
  <li><b>Headline</b> <code>composite_absolute_threshold</code> — the nine metrics scored against
      absolute physical thresholds or levels, weights renormalized 0.633 &rarr; 1.000 within the
      half. The five baseline-referenced metrics are excluded from the headline by decision.</li>
  <li><b>Domain</b> fixed <code>0-100</code>. It does not rescale when bundle, scenario, period
      or selection changes, so identical colours mean identical scores everywhere.</li>
  <li><b>Ramp</b> the vendored NCL <code>WhiteBlueGreenYellowRed</code> table, sampled at 101 stops
      from fraction 0.045 so no valid score renders as pure white. Colour is
      <code>index = round(score)</code>, with no binning.</li>
</ul>

<h3>The State/UT statistic</h3>
<p>The <b>area-weighted mean of a State/UT's valid district composite scores</b> — the only State
statistic produced. There is no population-weighted mean. It is not a percentile among States: it
is the area-weighted average of that State's districts' national percentiles.</p>
<p>State/UT polygons are never filled. The national map paints districts; the State view paints
blocks in production.</p>

<h3>What each view paints, ranks and bins</h3>
<ul>
  <li><b>National</b> paints 784 districts, ranks 36 State/UT means, bins those 36 means.</li>
  <li><b>State</b> paints blocks in production (districts here), ranks that State's districts,
      bins those district scores.</li>
</ul>
<p>The histogram bins the units the view <i>ranks</i>, never the units it <i>paints</i>. A mean is
a summary, so a State's bin does not constrain its districts: hovering a bin can emphasise
districts painted in several different colours. The widget title is what stops that being read as
a claim about the districts.</p>

<h3>Bins and bands</h3>
<p>Ten fixed bins of width 10 across the full domain, all ten always drawn, with band ticks at
20 / 40 / 60 / 80. Bin edges are arithmetic divisions of a fixed domain, not a methodological cut
point. The five interpretive bands are Very Low, Low, Moderate, High, Extreme at those same cuts.
Under a rank-based ruler these cuts are close to definitional — <code>&ge; 80</code> reads as
&ldquo;worse than 80% of pooled national observations&rdquo;.</p>

<h3>Ranking</h3>
<p>Competition ranks over full-precision stored values; rounding is display-only. Every State/UT
with a valid mean is ranked and every valid district is ranked within its State/UT. There is no
cohort-size minimum, so a State/UT with three valid districts ranks against one with seventy-five.
Filtering by a histogram bin retains each unit's original rank and never recomputes rank inside the
filtered subset.</p>

<h3>Not implemented here</h3>
<ul>
  <li><b>Block scores and block fill.</b> No block has been scored on the frozen ruler.</li>
  <li><b>Detailed Analysis.</b> The transition and its carried state are real; the destination is a
      stub.</li>
  <li><b>Twelve of the thirteen eligible bundles</b>, Context and Evidence, coordinate entry,
      exports, and the provenance quartet.</li>
</ul>
<p>Scores come from the Heat Risk national pilot and predate the production frozen-scale change.
They are indicative of shape, not a release baseline.</p>
"""


def build_html(
    scores: pd.DataFrame,
    districts: list[dict],
    state_paths: dict[str, str],
    height: float,
    areas: dict[str, float],
) -> str:
    """One self-contained page: inline SVG, inline data, no network calls."""
    district_scores, state_stats, drivers = build_tables(scores, areas)

    payload = {
        "districts": districts,
        "state_paths": state_paths,
        "district_scores": district_scores,
        "state_stats": state_stats,
        "drivers": drivers,
        "metric_labels": METRIC_LABELS,
        "scenario_labels": SCENARIO_LABELS,
        "period_labels": PERIOD_LABELS,
        "bundles": [list(b) for b in ELIGIBLE_BUNDLES],
        "default_bundle": "Heat Risk",
        "default_scenario": DEFAULT_SCENARIO,
        "default_period": DEFAULT_PERIOD,
        "ramp": ramp_hex(),
        "missing": MISSING_COLOR,
        "vmin": FROZEN_VMIN,
        "vmax": FROZEN_VMAX,
        "width": CANVAS_WIDTH,
        "height": round(height, 2),
        "method_html": METHOD_HTML,
    }
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    # The payload lives in a <script type="application/json"> block, so only a
    # literal "</script>" can break out of it.
    blob = blob.replace("</", "<\\/")
    return PAGE_TEMPLATE.replace("__PAYLOAD__", blob)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the self-contained Overview flow prototype (CHG-0385).",
    )
    parser.add_argument("--scores", type=Path, default=DEFAULT_SCORES,
                        help="Pilot district_scores.csv (default: %(default)s).")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="Output HTML path (default: %(default)s).")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="Override the resolved IRT data directory.")
    parser.add_argument("--simplify", type=float, default=DEFAULT_SIMPLIFY,
                        help="Douglas-Peucker tolerance in degrees (default: %(default)s).")
    parser.add_argument("--precision", type=int, default=1,
                        help="Decimal places kept in SVG path data (default: %(default)s).")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else Path(get_paths_config().data_dir)

    scores = load_frozen_scores(args.scores)
    print(f"scores    : {len(scores):,} rows over {len(PUBLIC_SLICES)} public slices")

    areas = district_areas(data_dir)
    print(f"areas     : {len(areas):,} districts")

    districts, state_paths, height = build_shapes(
        data_dir, simplify=args.simplify, precision=args.precision
    )
    print(f"geometry  : {len(districts):,} district paths, {len(state_paths)} State/UT outlines")

    scored = set(scores["district_key"].unique())
    drawn = {d["k"] for d in districts}
    orphan_scores = sorted(scored - drawn)
    orphan_shapes = sorted(drawn - scored)
    if orphan_scores:
        print(f"WARNING   : {len(orphan_scores)} scored districts have no geometry, "
              f"e.g. {orphan_scores[:3]}", file=sys.stderr)
    if orphan_shapes:
        print(f"WARNING   : {len(orphan_shapes)} drawn districts have no score, "
              f"e.g. {orphan_shapes[:3]}", file=sys.stderr)

    html_text = build_html(scores, districts, state_paths, height, areas)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html_text, encoding="utf-8")
    print(f"wrote     : {args.out}  ({len(html_text.encode('utf-8')) / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
