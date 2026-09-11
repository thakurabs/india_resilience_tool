"""Overview flow prototype — the national screening workflow as one
self-contained HTML page (CHG-0385, extended by CHG-0392, CHG-0393 and
CHG-0399..0402).

This is a *workflow* prototype for vendor handoff, not a data product. It renders
the flow specified by ``recommended_target_workflow.md`` section 1-8 so the
interaction contract can be judged and implemented:

    India view      districts painted, State/UTs ranked, State/UT means binned
        -> select a State/UT
    State view      blocks painted, districts ranked, district scores binned
        -> select a district
    District view   that district's blocks painted and zoomed; ranking and
                    distribution inherited from the State, because blocks are
                    never ranked at any scope
        -> select a block  (inspection state; no breadcrumb level)

Everything about the score is frozen, exactly as in
``tools.diagnostics.build_heat_risk_frozen_map``: the ``cdf`` ruler, the
``composite_absolute_threshold`` headline, the ``0-100`` domain and the vendored
``WhiteBlueGreenYellowRed`` table. The ``linear`` ruler is not implemented and is
never exposed. This tool re-scores nothing; it reads the pilot's
``district_scores.csv`` and ``telangana_block_scores.csv``.

The colourbar carries both A10 mitigations for a fixed domain: an always-on
bracket marking the range present in the current view, and an opt-in
``Local contrast`` view that stretches the *map fill only* to the extent of the
painted units. That extent is recomputed on every render and never stored --
precomputing it per State/UT would be per-state min-max in a new hat.

Three honest departures from the target spec, all surfaced in the page itself:

- **Only Telangana opens below the national view.** Its 588 blocks are the only
  blocks scored on the frozen ruler, so every other State/UT hovers normally but
  is not selectable. One worked State/UT demonstrates the whole flow.
- **Context and Evidence carries no basin or river map overlay**, and no basin
  context at State/UT scope: a State/UT basin share needs a State-to-basin
  geometry intersection, and counting districts' dominant basins would be a
  different quantity under the same label. A population overlay (proportional
  circles) is implemented: it follows the unit the map paints, districts
  nationally and blocks once a State/UT is open, on one frozen national scale.
- **Detailed Analysis is a stub.** The transition and the state it carries are
  real, including the selected driver metric; the destination is a panel that
  displays that state and nothing more.

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
DEFAULT_BLOCK_SCORES = Path("docs/diagnostics/heat_risk_pilot/telangana_block_scores.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html")

#: The one State/UT whose blocks are scored and whose drill-down is live. Every
#: other State/UT hovers normally and is deliberately not selectable: the
#: prototype demonstrates the workflow, and one worked State/UT demonstrates it.
LIVE_STATE = "Telangana"

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


def load_block_scores(path: Path) -> pd.DataFrame:
    """Frozen-ruler block rows for the live State/UT, over the six public slices."""
    frame = pd.read_csv(path)
    required = ("ruler", "block_key", "state", "district", "block", "scenario", "period", FROZEN_FIELD)
    for column in required:
        if column not in frame.columns:
            raise ValueError(f"{path} has no '{column}' column; not a block score table")
    frame = frame.loc[frame["ruler"] == FROZEN_RULER].copy()
    wanted = frame.set_index(["scenario", "period"]).index.isin(PUBLIC_SLICES)
    frame = frame.loc[wanted].copy()
    if frame.empty:
        raise ValueError(f"{path} carries no frozen-ruler rows over the public slices")
    frame[FROZEN_FIELD] = pd.to_numeric(frame[FROZEN_FIELD], errors="coerce")

    metric_columns = [
        f"score__{slug}" for slug in METRIC_LABELS if f"score__{slug}" in frame.columns
    ]
    for column in metric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    keep = ["block_key", "state", "district", "block", "scenario", "period", FROZEN_FIELD]
    return frame.loc[:, keep + metric_columns]


def build_block_tables(blocks: pd.DataFrame) -> tuple[dict, dict]:
    """Per-slice block values and per-block driver orderings."""
    by_slice: dict[str, dict[str, float]] = {}
    drivers_by_slice: dict[str, dict[str, list[str]]] = {}
    metric_columns = [
        f"score__{slug}" for slug in METRIC_LABELS if f"score__{slug}" in blocks.columns
    ]
    for (scenario, period), block in blocks.groupby(["scenario", "period"], sort=False):
        slice_id = f"{scenario}|{period}"
        valid = block.loc[block[FROZEN_FIELD].notna()]
        by_slice[slice_id] = {
            str(row.block_key): round(float(getattr(row, FROZEN_FIELD)), 4)
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
            drivers[str(row.block_key)] = [slug for _, slug in ordered[:3]]
        drivers_by_slice[slice_id] = drivers
    return by_slice, drivers_by_slice


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
) -> tuple[list[dict], dict[str, str], list[dict], float]:
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
        rep = row.geometry.representative_point()
        cx, cy, _ = _project(np.array([rep.x]), np.array([rep.y]), bounds)
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
                "c": [round(float(cx[0]), 2), round(float(cy[0]), 2)],
            }
        )

    outlines = gdf.dissolve(by="state_name")
    state_paths: dict[str, str] = {}
    for state, geom in zip(outlines.index, outlines.geometry):
        path = _geom_path(geom, bounds, precision)
        if path:
            state_paths[str(state)] = path

    blocks = _build_block_shapes(
        data_dir, bounds=bounds, simplify=simplify, precision=precision
    )
    return districts, state_paths, blocks, float(height)


def _build_block_shapes(
    data_dir: Path,
    *,
    bounds: tuple[float, float, float, float],
    simplify: float,
    precision: int,
) -> list[dict]:
    """Block paths for the live State/UT, projected against the national bounds.

    Sharing the national bounds is what lets one SVG serve all three views: the
    block layer sits in the same coordinate space as the district layer, so
    zooming is a viewBox change and the two layers stay registered.
    """
    import geopandas as gpd

    root = Path(data_dir) / "processed_optimised" / "geometry" / "admin" / "block"
    path = root / f"state={LIVE_STATE}.geojson"
    if not path.exists():
        raise FileNotFoundError(f"No block geometry for {LIVE_STATE} at {path}")

    gdf = gpd.read_file(path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)
    if simplify > 0:
        gdf["geometry"] = gdf.geometry.simplify(simplify, preserve_topology=True)

    out: list[dict] = []
    for row in gdf.itertuples(index=False):
        svg_path = _geom_path(row.geometry, bounds, precision)
        if not svg_path:
            continue
        block_key = str(getattr(row, "block_key", ""))
        # The block geometry carries a district *name*, not a key. The parent key
        # is the first two segments of the block key, which is the canonical
        # district key by construction -- safer than re-deriving it from a name.
        parent = "|".join(block_key.split("|")[:2])
        rep = row.geometry.representative_point()
        cx, cy, _ = _project(np.array([rep.x]), np.array([rep.y]), bounds)
        out.append(
            {
                "k": block_key,
                "n": str(getattr(row, "block_name", "")),
                "dk": parent,
                "s": str(getattr(row, "state_name", "")),
                "d": svg_path,
                "c": [round(float(cx[0]), 2), round(float(cy[0]), 2)],
            }
        )
    return out


# ---------------------------------------------------------------------------
# Context and Evidence (CHG-0399, CHG-0400)
# ---------------------------------------------------------------------------

#: Exposure fields carried into the page, as ``(source column, payload key)``.
#: Payload keys are short because every one of them is repeated ~1,400 times.
EXPOSURE_FIELDS: tuple[tuple[str, str], ...] = (
    ("pop_2020", "pop"),
    ("population_share_parent_pct", "pshare"),
    ("rural_facilities_total_count", "rf"),
    ("rural_facilities_agro_count", "rf_agro"),
    ("rural_facilities_education_count", "rf_edu"),
    ("rural_facilities_health_count", "rf_health"),
    ("rural_facilities_service_count", "rf_service"),
    ("rural_facilities_total_count_per_100k", "rf_per100k"),
    ("built_up_area_km2", "bu"),
    ("built_up_area_share_pct", "bu_pct"),
    ("lulc_agri_area_km2", "ag"),
    ("lulc_agri_share_pct", "ag_pct"),
)

#: Hydrology fields carried into the page. ``drainage_area_km2`` and
#: ``runoff_coeff`` are omitted deliberately: both are null for every district
#: and block in scope, and the workflow says to omit an unavailable field rather
#: than render an empty row.
HYDRO_TEXT_FIELDS: tuple[tuple[str, str], ...] = (
    ("basin_name", "basin"),
    ("subbasin_name", "sub"),
    ("primary_river", "river"),
    ("hydro_type", "htype"),
)
HYDRO_NUM_FIELDS: tuple[tuple[str, str], ...] = (
    ("basin_frac", "bfrac"),
    ("subbasin_frac", "sfrac"),
)

#: Source, unit and vintage for each context subsection, shown in the page so a
#: reader never has to guess what a number is or when it was measured.
CONTEXT_PROVENANCE: dict[str, str] = {
    "population": "Population: WorldPop-derived admin master, 2025 snapshot",
    "facilities": "Rural facilities: Mission Antyodaya, 2019–2021 snapshot. The per-100k rate divides RURAL facilities by TOTAL population, so units with large urban populations read low by construction — compare it across rural geographies, never city against countryside.",
    "built_up": "Built-up area: LULC-derived admin master, current snapshot",
    "lulc": "Agricultural LULC: LULC-derived admin master, current snapshot",
    "hydro": "Basins and rivers: IRT hydrology crosswalk over the admin roster",
}


def _clean_number(value: object, places: int) -> Optional[float]:
    """A finite float rounded for transport, or ``None`` for anything else."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return round(number, places)


def _parse_also(raw: object) -> list[list]:
    """The two largest secondary basins as ``[name, percent]`` pairs."""
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    rows = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        name = str(item.get("basin_name") or "").strip()
        frac = _clean_number(item.get("basin_frac", item.get("overlap_frac")), 4)
        if name and frac is not None:
            rows.append([name, frac])
    rows.sort(key=lambda row: row[1], reverse=True)
    return rows[:2]


def _state_exposure_rows(
    exposure: pd.DataFrame,
    wanted_districts: set[str],
    areas: dict[str, float],
) -> dict[str, dict]:
    """Aggregate district exposure into one row per State/UT.

    Follows the workflow's State/UT aggregation rules literally: counts and areas
    are summed, and every share and rate is recomputed from the State/UT totals
    rather than averaged over district percentages. The share denominator is the
    canonical district area used for the score's own area weighting, so the two
    cannot drift apart.
    """
    frame = exposure.loc[exposure["admin_key"].isin(wanted_districts)].copy()
    if frame.empty:
        return {}

    frame["area_km2"] = [
        areas.get(str(key), float("nan")) / 1e6 for key in frame["admin_key"]
    ]
    national_pop = float(pd.to_numeric(frame["pop_2020"], errors="coerce").sum())

    out: dict[str, dict] = {}
    for state, group in frame.groupby("state_name", sort=True):
        totals = {
            column: float(pd.to_numeric(group[column], errors="coerce").sum())
            for column in (
                "pop_2020",
                "rural_facilities_total_count",
                "rural_facilities_agro_count",
                "rural_facilities_education_count",
                "rural_facilities_health_count",
                "rural_facilities_service_count",
                "built_up_area_km2",
                "lulc_agri_area_km2",
                "area_km2",
            )
        }
        area = totals["area_km2"]
        population = totals["pop_2020"]
        row: dict[str, object] = {
            "pop": _clean_number(population, 0),
            "rf": _clean_number(totals["rural_facilities_total_count"], 0),
            "rf_agro": _clean_number(totals["rural_facilities_agro_count"], 0),
            "rf_edu": _clean_number(totals["rural_facilities_education_count"], 0),
            "rf_health": _clean_number(totals["rural_facilities_health_count"], 0),
            "rf_service": _clean_number(totals["rural_facilities_service_count"], 0),
            "bu": _clean_number(totals["built_up_area_km2"], 1),
            "ag": _clean_number(totals["lulc_agri_area_km2"], 1),
            "n": int(len(group)),
        }
        if population > 0:
            row["rf_per100k"] = _clean_number(
                1e5 * totals["rural_facilities_total_count"] / population, 1
            )
        if national_pop > 0:
            row["pshare"] = _clean_number(100.0 * population / national_pop, 2)
            row["plevel"] = "India"
        if area > 0:
            row["bu_pct"] = _clean_number(100.0 * totals["built_up_area_km2"] / area, 2)
            row["ag_pct"] = _clean_number(100.0 * totals["lulc_agri_area_km2"] / area, 2)
        out[str(state)] = {k: v for k, v in row.items() if v is not None}
    return out


def load_context(
    data_dir: Path,
    district_keys: set[str],
    block_keys: set[str],
    areas: dict[str, float],
) -> dict:
    """Exposure and hydrology context for exactly the units this page can select.

    Both artifacts are optional. A missing or malformed one yields empty tables
    rather than an exception: the workflow requires that Context and Evidence be
    supplementary, so its absence must never invalidate a score.
    """
    root = Path(data_dir) / "processed_optimised" / "context"
    wanted = district_keys | block_keys

    def _read(name: str) -> pd.DataFrame:
        path = root / name
        if not path.exists():
            print(f"context   : {name} absent — that subsection will be omitted",
                  file=sys.stderr)
            return pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception as exc:  # pragma: no cover - depends on the local bundle
            print(f"context   : {name} unreadable ({exc}) — subsection omitted",
                  file=sys.stderr)
            return pd.DataFrame()

    exposure_raw = _read("admin_exposure_summary.parquet")
    hydro_raw = _read("admin_hydro_summary.parquet")

    exposure: dict[str, dict] = {}
    if not exposure_raw.empty and "admin_key" in exposure_raw.columns:
        frame = exposure_raw.loc[exposure_raw["admin_key"].isin(wanted)]
        for row in frame.itertuples(index=False):
            entry: dict[str, object] = {}
            for column, key in EXPOSURE_FIELDS:
                value = _clean_number(getattr(row, column, None),
                                      2 if key.endswith(("_pct", "share")) else
                                      (1 if key in {"bu", "ag", "rf_per100k"} else 0))
                if value is not None:
                    entry[key] = value
            parent = str(getattr(row, "parent_name", "") or "").strip()
            if parent:
                entry["plevel"] = parent
            if entry:
                exposure[str(row.admin_key)] = entry

    hydro: dict[str, dict] = {}
    if not hydro_raw.empty and "admin_key" in hydro_raw.columns:
        frame = hydro_raw.loc[hydro_raw["admin_key"].isin(wanted)]
        for row in frame.itertuples(index=False):
            entry: dict[str, object] = {}
            for column, key in HYDRO_TEXT_FIELDS:
                text = str(getattr(row, column, "") or "").strip()
                if text:
                    entry[key] = text
            for column, key in HYDRO_NUM_FIELDS:
                value = _clean_number(getattr(row, column, None), 4)
                if value is not None:
                    entry[key] = value
            also = _parse_also(getattr(row, "also_intersects_basin_json", None))
            if also:
                entry["also"] = also
            if entry:
                hydro[str(row.admin_key)] = entry

    states = _state_exposure_rows(exposure_raw, district_keys, areas) if not exposure_raw.empty else {}

    matched_districts = len(district_keys & set(exposure)) if exposure else 0
    matched_blocks = len(block_keys & set(exposure)) if exposure else 0
    print(f"context   : exposure {matched_districts}/{len(district_keys)} districts, "
          f"{matched_blocks}/{len(block_keys)} blocks, {len(states)} State/UT rows")
    unmatched = sorted(district_keys - set(exposure))
    if unmatched:
        print(f"context   : {len(unmatched)} districts carry no context row, "
              f"e.g. {unmatched[:3]} — they will read 'Not available'", file=sys.stderr)
    matched_hydro = len(district_keys & set(hydro)) if hydro else 0
    print(f"context   : hydrology {matched_hydro}/{len(district_keys)} districts, "
          f"{len(block_keys & set(hydro))}/{len(block_keys)} blocks")

    return {
        "exposure": exposure,
        "hydro": hydro,
        "states": states,
        "provenance": CONTEXT_PROVENANCE,
    }


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

  /* The breadcrumb rides in the map's top-right corner, on the thing it
     describes, rather than in the header above the selector row. */
  nav.crumbs { position: absolute; top: 10px; right: 10px; z-index: 4;
               max-width: min(58%, 400px);
               display: flex; align-items: center; flex-wrap: wrap; gap: 7px;
               font-size: 12.5px; background: rgba(255,255,255,.93);
               border: 1px solid var(--rule); border-radius: 8px;
               padding: 6px 11px; box-shadow: 0 1px 4px rgba(16,32,44,.11); }
  .crumb { background: none; border: 0; font: inherit; color: var(--accent);
           cursor: pointer; padding: 2px 0; }
  .crumb[aria-current] { color: var(--ink); font-weight: 600; cursor: default; }
  .crumb-sep { color: var(--ink-3); }
  .crumb-note { flex-basis: 100%; font-size: 11px; color: var(--ink-3); line-height: 1.35; }

  /* ---- local-contrast view (off by default, never the landing state) ---- */
  .lc { display: inline-flex; align-items: center; gap: 6px; font-size: 11.5px;
        color: var(--ink-2); cursor: pointer; }
  .lc input { margin: 0; cursor: pointer; }
  .lc input:disabled { cursor: not-allowed; }
  .lc.off { color: var(--ink-3); cursor: not-allowed; }
  .lc-on { color: var(--warn-ink); font-weight: 650; }
  .cbar-warn { margin: 7px 0 0; font-size: 11.5px; color: var(--warn-ink);
               background: var(--warn-bg); border: 1px solid var(--warn-line);
               border-radius: 6px; padding: 7px 10px; line-height: 1.45; }

  /* ---- Context layers (off by default, follows the painted unit) ---- */
  .ctxl-row { display: flex; gap: 14px; align-items: center; flex-wrap: wrap;
              margin: 8px 0 0; font-size: 11.5px; color: var(--ink-3); }
  .ctxl { display: inline-flex; align-items: center; gap: 6px; }
  .ctxl select { font: inherit; padding: 1px 4px; }
  .ctx-key { display: inline-flex; align-items: center; gap: 10px; }
  .ctx-key-lab { color: var(--ink-2); }
  .ctx-key-step { display: inline-flex; align-items: center; gap: 4px; }
  .ctx-key-step svg { overflow: visible; }
  .ctx-note { color: var(--warn-ink); }

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
  /* The map box never moves and never resizes: one fixed height for every
     view, so drilling in changes the map's content and not the page layout. */
  svg.map { display: block; width: 100%; height: 620px; }
  @media (max-width: 1080px) { svg.map { height: 460px; } }
  svg.map path { vector-effect: non-scaling-stroke; }
  .dist { stroke: var(--fine); stroke-opacity: .45; stroke-width: .45px; cursor: pointer; }
  .dist.out { fill: #eceff2 !important; stroke-opacity: .25; cursor: default; pointer-events: none; }
  .dist.muted, .blk.muted { opacity: .26; }
  .dist.hl, .blk.hl { stroke: #2b3947; stroke-opacity: .9; stroke-width: 1.1px; }
  .dist.locked { cursor: not-allowed; }
  .blk { stroke: var(--fine); stroke-opacity: .45; stroke-width: .4px; cursor: pointer; }
  .stroke-coarse { fill: none; stroke: var(--coarse); stroke-opacity: .95; stroke-width: 1.9px;
                   pointer-events: none; }
  .ctx-dot { fill: #2b3a67; fill-opacity: .30; stroke: #1b2545; stroke-opacity: .55;
             stroke-width: .6px; vector-effect: non-scaling-stroke; pointer-events: none; }
  .ctx-dot.ctx-muted { fill-opacity: .07; stroke-opacity: .12; }
  #g-ctx { pointer-events: none; }
  .cmp-stroke { fill: none; stroke: #7b3fa0; stroke-opacity: .95; stroke-width: 2.2px;
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

  /* ---- headline ---- */
  .headline .hl-top { display: flex; align-items: baseline; gap: 11px; flex-wrap: wrap; }
  .headline .hl-name { font-size: 19px; font-weight: 650; letter-spacing: -0.01em; }
  .headline .hl-parent { font-size: 12px; color: var(--ink-3); }
  .headline .bigscore { font-size: 32px; font-weight: 680; letter-spacing: -0.02em;
                        line-height: 1; margin-left: auto; }
  .headline .hl-meta { font-size: 12.5px; color: var(--ink-2); margin: 7px 0 0; }
  .headline .hl-meta b { font-weight: 650; }
  .headline .hl-scope { font-size: 11.5px; color: var(--ink-3); margin: 3px 0 0; }

  /* ---- driver list: each item routes into Detailed Analysis ---- */
  .drv-head { font-size: 10.5px; text-transform: uppercase; letter-spacing: .06em;
              color: var(--ink-3); font-weight: 650; margin: 13px 0 5px; }
  ul.drv { list-style: none; margin: 0; padding: 0; }
  ul.drv li { margin: 0 0 4px; }
  ul.drv button { display: flex; align-items: center; gap: 8px; width: 100%; text-align: left;
                  font: inherit; font-size: 13px; color: var(--ink); cursor: pointer;
                  background: #fff; border: 1px solid var(--rule); border-radius: 7px;
                  padding: 7px 11px; transition: border-color .12s, background .12s; }
  ul.drv button:hover { border-color: var(--accent); background: var(--accent-soft); }
  ul.drv button:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
  ul.drv .drv-go { margin-left: auto; color: var(--accent); font-size: 15px; line-height: 1; }
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
  .btn:disabled { border-color: #b7c0ca; background: #eef1f4; color: var(--ink-3);
                  cursor: not-allowed; }
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
  .cmp-cell { width: 1%; white-space: nowrap; text-align: right; }
  .cmp-mini { min-width: 27px; padding: 3px 7px; border: 1px solid #7b3fa0;
              border-radius: 5px; background: #fff; color: #7b3fa0; font: inherit;
              font-size: 11px; font-weight: 650; cursor: pointer; }
  .cmp-mini:disabled { border-color: #b7c0ca; background: #eef1f4; color: var(--ink-3);
                       cursor: not-allowed; }

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

  /* ---- Compare locations (CHG-0412..0416) ---- */
  .compare { border-top: 3px solid #7b3fa0; margin-top: 2px; }
  .cmp-head { display: flex; align-items: flex-start; justify-content: space-between;
              gap: 16px; flex-wrap: wrap; margin-bottom: 12px; }
  .cmp-head h2 { margin: 0 0 3px; font-size: 15px; }
  .cmp-head p { margin: 0; font-size: 12px; color: var(--ink-3); }
  .cmp-mode { display: inline-flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .cmp-mode label { display: flex; flex-direction: column; gap: 2px; font-size: 10px;
                    text-transform: uppercase; letter-spacing: .05em; color: var(--ink-3); }
  .cmp-mode select { font: inherit; font-size: 12px; padding: 5px 7px; border: 1px solid var(--rule);
                     border-radius: 6px; background: #fff; color: var(--ink); }
  .cmp-scroll { overflow-x: auto; padding-bottom: 3px; }
  .cmp-table { width: 100%; }
  .cmp-row { display: grid; grid-template-columns: repeat(var(--cmp-cols), minmax(0, 1fr)); }
  .cmp-row + .cmp-row { border-top: 1px solid var(--rule); }
  .cmp-row.cmp-headers { background: #f8f5fa; border: 1px solid #e1d4e8;
                         border-radius: 8px 8px 0 0; }
  .cmp-row.cmp-headers + .cmp-row { border-top: 0; }
  .cmp-slot { min-width: 0; padding: 10px 12px; border-left: 1px solid var(--rule); }
  .cmp-slot:first-child { border-left: 0; }
  .cmp-slot .lab { display: block; margin-bottom: 3px; font-size: 10px; font-weight: 650;
                   text-transform: uppercase; letter-spacing: .06em; color: var(--ink-3); }
  .cmp-slot .val { font-size: 13px; color: var(--ink); }
  .cmp-slot .val strong { font-size: 20px; letter-spacing: -.01em; }
  .cmp-place { display: block; font-weight: 650; font-size: 15px; }
  .cmp-parent { display: block; color: var(--ink-3); font-size: 11.5px; }
  .cmp-note { display: block; margin-top: 3px; color: var(--ink-3); font-size: 11px;
              line-height: 1.4; }
  .cmp-alert { margin: 0 0 10px; padding: 7px 10px; border-radius: 6px;
               border: 1px solid var(--warn-line); background: var(--warn-bg);
               color: var(--warn-ink); font-size: 11.5px; }
  .cmp-driver { display: inline-block; margin: 2px 5px 2px 0; padding: 3px 7px;
                border: 1px solid var(--rule); border-radius: 999px; font-size: 11.5px; }
  .cmp-driver.shared { border-color: #7b3fa0; background: #f4edf8; color: #663481;
                       font-weight: 650; }
  .cmp-driver.unique { background: #fff; color: var(--ink-2); }
  .cmp-actions { display: flex; gap: 7px; flex-wrap: wrap; }


  /* ---- Context and Evidence (CHG-0401) ---- */
  details.ctx > summary { cursor: pointer; font-size: 13px; font-weight: 600;
                          display: flex; align-items: baseline; gap: 8px; }
  details.ctx > summary .ctx-for { font-weight: 400; font-size: 12px; color: var(--ink-3); }
  .ctx-body { margin-top: 10px; }
  .ctx-sec { border-top: 1px solid var(--rule); padding-top: 10px; margin-top: 10px; }
  .ctx-sec:first-child { border-top: 0; padding-top: 0; margin-top: 0; }
  .ctx-sec h3 { margin: 0 0 8px; font-size: 12px; text-transform: uppercase;
                letter-spacing: .06em; color: var(--ink-3); }
  .ctx-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
              gap: 8px 14px; }
  .ctx-grid.four { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .ctx-cell .lab { font-size: 11px; color: var(--ink-3); display: block; }
  .ctx-cell .val { font-size: 14px; font-weight: 650; }
  .ctx-cell .val small { font-weight: 400; font-size: 11.5px; color: var(--ink-3); }
  .ctx-chip { display: inline-flex; align-items: baseline; gap: 6px; font-size: 12.5px;
              background: var(--ground); border: 1px solid var(--rule); border-radius: 999px;
              padding: 3px 10px; margin: 0 6px 6px 0; }
  .ctx-chip em { font-style: normal; color: var(--ink-3); font-size: 11.5px; }
  .ctx-none { font-size: 12px; color: var(--ink-3); font-style: italic; margin: 0; }
  .ctx-prov { margin: 10px 0 0; font-size: 11px; color: var(--ink-3); line-height: 1.5;
              border-top: 1px dashed var(--rule); padding-top: 8px; }
  /* ---- tooltip ---- */
  #tip { position: fixed; pointer-events: none; z-index: 60; background: #16202c; color: #fff;
         border-radius: 7px; padding: 9px 11px; font-size: 12.5px; max-width: 290px;
         box-shadow: 0 6px 20px rgba(0,0,0,.24); opacity: 0; transition: opacity .1s; }
  #tip b { display: block; font-size: 13px; margin-bottom: 3px; }
  #tip .t-row { color: #c3ced9; }
  #tip .t-row em { color: #fff; font-style: normal; font-weight: 600; }
  #tip .t-go { color: #8fc4ff; margin-top: 4px; }
  #tip .t-off { color: #9aa7b4; margin-top: 4px; font-style: italic; }

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
</header>

<div class="wrap">
  <div class="banner">
    <div>
      <p style="margin:0 0 6px"><b>Two departures in the flow itself, both deliberate.</b></p>
      <p style="margin:0">
      <b>1. Only Telangana opens below the national view.</b> Its 588 blocks are scored on the
      frozen ruler and painted for real. Every other State/UT hovers normally — whole-state
      highlight, State-level tooltip — but is not selectable, because no block outside Telangana
      has been scored. One worked State/UT demonstrates the workflow.
      <br><b>2. Detailed Analysis is a stub.</b> The action and the state it carries are real,
      including which driver metric was selected; the destination only displays that state.
      <br>Context and Evidence carries no basin or river map overlay and no basin context at
      State/UT scope; the card and the method note say why. A population overlay (proportional
      circles, off by default) is available.
      </p>
    </div>
  </div>

  <div class="grid">
    <div>
      <div class="card mapcard">
        <div class="maphead">
          <h2 id="map-title">National view</h2>
          <span class="sub" id="map-sub"></span>
        </div>
        <div class="mapwrap">
          <nav class="crumbs" id="crumbs" aria-label="Geography"></nav>
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
            <span id="cbar-domain">Domain fixed 0–100 · never rescaled by selection</span>
            <label class="lc" id="lc-label" for="lc-toggle">
              <input type="checkbox" id="lc-toggle"> Local contrast
            </label>
          </div>
          <div class="ctxl-row" id="ctxl-row" hidden>
            <label class="ctxl" for="ctx-layer">Context layer
              <select id="ctx-layer">
                <option value="">None</option>
                <option value="pop">Population</option>
              </select>
            </label>
            <span class="ctx-key" id="ctx-key" hidden></span>
            <span class="ctx-note" id="ctx-note"></span>
          </div>
          <p class="cbar-warn" id="cbar-warn" hidden></p>
        </div>
      </div>

    </div>

    <div>
      <div class="card headline" id="headline" hidden></div>
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
      <div class="card" id="context" hidden></div>
      <div class="card">
        <details class="method-wrap">
          <summary>Method note</summary>
          <div class="method" id="method"></div>
        </details>
      </div>
    </div>
  </div>
  <div id="compare"></div>
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
    view: "india",          /* "india" | "state" | "district" */
    state: null,
    district: null,
    block: null,
    pinned: null,
    showAll: false,
    local: false,          /* the local-contrast view; opt-in, never the landing state */
    ctxOpen: false,        /* Context and Evidence is collapsed on arrival, by contract */
    ctxLayer: null,        /* the active Context layer id, or null; off by default */
    cmp: { mode: "places", members: [], subject: null, slices: [] },
    da: null
  };

  var byKey = {};
  D.districts.forEach(function (d) { byKey[d.k] = d; });
  var statesOf = {};
  D.districts.forEach(function (d) { (statesOf[d.s] = statesOf[d.s] || []).push(d); });

  var blockByKey = {};
  var blocksOfDistrict = {};
  D.blocks.forEach(function (b) {
    blockByKey[b.k] = b;
    (blocksOfDistrict[b.dk] = blocksOfDistrict[b.dk] || []).push(b);
  });

  /* ---- Context layers -------------------------------------------------
     Every field here is already in the payload for the Context and Evidence
     card; an overlay costs no extra bytes. Population is a COUNT, so it is
     drawn as proportional circles rather than a fill: a fill would imply the
     value is spread evenly across the district, and would also have to evict
     the risk colour. */
  var CTX_LAYERS = {
    pop: { label: "Population", unit: "people",
           prov: "WorldPop-derived admin master, 2025 snapshot" }
  };

  var CTX_EXPOSURE = (D.context && D.context.exposure) || {};
  var POP_MAX = 0;
  D.districts.forEach(function (d) {
    var v = (CTX_EXPOSURE[d.k] || {}).pop;
    if (typeof v === "number" && v > 0 && v > POP_MAX) POP_MAX = v;
  });

  /* The circle scale is frozen to the national district maximum and is never
     renormalised per view, so one circle size means one population in every
     view -- the same contract the score ruler keeps. */
  var CTX_RMAX = 16;      /* SVG user units at the national viewBox (width 1000) */
  var CTX_RMIN_PX = 1.0;  /* SCREEN px: a legibility floor, so it must not zoom */

  /* The units this view paints, which are the units it overlays. */
  function ctxUnits() {
    if (S.view === "india") return D.districts;
    if (S.view === "district") return blocksOfDistrict[S.district] || [];
    return (D.blocks || []).filter(function (b) { return b.s === S.state; });
  }

  function ctxZoom() {
    var vb = viewBox();
    return vb[2] > 0 ? D.width / vb[2] : 1;
  }

  function popRadius(pop, zoom) {
    if (typeof pop !== "number" || !(pop > 0) || !POP_MAX) return 0;
    return Math.max(CTX_RMIN_PX / (zoom || 1), CTX_RMAX * Math.sqrt(pop / POP_MAX));
  }

  function popMissing() {
    var n = 0;
    ctxUnits().forEach(function (u) {
      var v = (CTX_EXPOSURE[u.k] || {}).pop;
      if (!(typeof v === "number" && v > 0) || !u.c) n++;
    });
    return n;
  }

  /* Three round steps derived from the maximum it should describe, so the key
     survives a data refresh rather than hard-coding today's maximum. */
  function popKeySteps(max) {
    var m = max > 0 ? max : POP_MAX;
    var mag = Math.pow(10, Math.floor(Math.log(m) / Math.LN10));
    var top = Math.floor(m / mag) * mag;
    return [top, top / 3, top / 10];
  }
  function popLabel(v) {
    if (v >= 1e6) return (v / 1e6).toFixed(v >= 1e7 ? 0 : 1) + "M";
    if (v >= 1e3) return Math.round(v / 1e3) + "k";
    return String(Math.round(v));
  }

  function sliceId() { return S.scenario + "|" + S.period; }
  function dScores() { return D.district_scores[sliceId()] || {}; }
  function sStats() { return D.state_stats[sliceId()] || {}; }
  function dDrivers() { return D.drivers[sliceId()] || {}; }
  function bScores() { return D.block_scores[sliceId()] || {}; }
  function bDrivers() { return D.block_drivers[sliceId()] || {}; }
  function isLive(state) { return state === D.live_state; }

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

  /* Local contrast is a *view extent*: recomputed from the visible scores on
     every render, never stored, never precomputed per State/UT into the
     artifact. See composite_scale_decisions.md A10 — the moment this becomes a
     stored parameter it is per-state min-max again, wearing a different hat. */
  var LX = { on: false, lo: 0, hi: 100 };
  function mapColour(v) {
    if (v === undefined || v === null || isNaN(v)) return D.missing;
    if (!LX.on) return colour(v);
    return colour(((v - LX.lo) / (LX.hi - LX.lo)) * 100);
  }

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
  function escAttr(s) { return esc(s).replace(/"/g, "&quot;").replace(/'/g, "&#39;"); }
  function metricLabel(slug) { return D.metric_labels[slug] || slug; }

  /* ---------- competition ranks over the current cohort ---------- */
  /* One district's place in its State/UT ranking, for the State-view hover. */
  function districtRank(key) {
    var d = byKey[key];
    if (!d) return null;
    var rows = rankedDistricts(d.s);
    for (var i = 0; i < rows.length; i++) {
      if (rows[i].key === key) {
        return { score: rows[i].score, rank: rows[i].rank, total: rows.length };
      }
    }
    return null;
  }

  function rankedDistrictsIn(state, sid) {
    var sc = D.district_scores[sid] || {};
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
  function rankedDistricts(state) { return rankedDistrictsIn(state, sliceId()); }
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
  /* The units the current view ranks. The District view introduces no ranking of
     its own -- blocks are painted but never ranked, because subdivision density
     reflects State administration rather than geography -- so it keeps ranking
     and binning its parent State/UT's districts. */
  function cohort() {
    return S.view === "india" ? rankedStates() : rankedDistricts(S.state);
  }

  /* ---------- persistent comparison tray ---------- */
  function cmpIdentity(member) { return member.level + ":" + member.key; }
  function cmpMemberIndex(member) {
    var id = cmpIdentity(member);
    for (var i = 0; i < S.cmp.members.length; i++) {
      if (cmpIdentity(S.cmp.members[i]) === id) return i;
    }
    return -1;
  }
  function makeCmpMember(level, key) {
    if (level === "State/UT") {
      return D.state_paths[key] === undefined ? null
        : { level: level, key: key, name: key, parent: "India" };
    }
    if (level === "District") {
      var d = byKey[key];
      return d ? { level: level, key: d.k, name: d.n, parent: d.s } : null;
    }
    if (level === "Block") {
      var b = blockByKey[key], p = b ? byKey[b.dk] : null;
      return b ? { level: level, key: b.k, name: b.n,
                   parent: (p ? p.n + " · " : "") + b.s } : null;
    }
    return null;
  }
  function cmpControl(member, compact) {
    var present = cmpMemberIndex(member) >= 0;
    var disabled = false, label, title;
    if (S.cmp.mode === "futures") {
      disabled = true;
      label = compact ? "Fixed" : "Place fixed";
      title = "Add-to-compare controls are disabled in futures mode because the place is fixed.";
    } else if (!present && S.cmp.members.length >= 4) {
      disabled = true;
      label = "Tray full (4)";
      title = "The comparison tray holds at most four places.";
    } else {
      label = present ? (compact ? "−" : "Remove") : (compact ? "+" : "Add");
      title = (present ? "Remove " : "Add ") + member.name +
        (present ? " from" : " to") + " the comparison tray";
    }
    return "<button type='button' class='" + (compact ? "cmp-mini" : "btn ghost") +
      " cmp-toggle' data-cmp-level='" + escAttr(member.level) + "' data-cmp-key='" +
      escAttr(member.key) + "' title='" + escAttr(title) + "' aria-label='" +
      escAttr(title) + "'" +
      (disabled ? " disabled" : "") + ">" + esc(label) + "</button>";
  }
  function wireCmpControls(host) {
    var buttons = host.querySelectorAll(".cmp-toggle");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function (ev) {
        ev.stopPropagation();
        var member = makeCmpMember(ev.currentTarget.dataset.cmpLevel,
                                   ev.currentTarget.dataset.cmpKey);
        if (!member || S.cmp.mode !== "places") return;
        var at = cmpMemberIndex(member);
        if (at >= 0) S.cmp.members.splice(at, 1);
        else if (S.cmp.members.length < 4) S.cmp.members.push(member);
        render();
      });
    }
  }
  function enterFutures(member) {
    S.cmp.mode = "futures";
    S.cmp.subject = member;
    S.cmp.members = [member];
    S.cmp.slices = Object.keys(D.period_labels).map(function (period) {
      return S.scenario + "|" + period;
    }).slice(0, 4);
    render();
  }
  function leaveFutures() {
    S.cmp.mode = "places";
    S.cmp.members = S.cmp.subject ? [S.cmp.subject] : [];
    S.cmp.subject = null;
    S.cmp.slices = [];
    render();
  }
  function cmpRange(blocks, scores) {
    var vals = [];
    blocks.forEach(function (b) {
      var v = scores[b.k];
      if (v !== undefined && v !== null && !isNaN(v)) vals.push(v);
    });
    vals.sort(function (a, b) { return a - b; });
    return { lo: vals.length ? vals[0] : null,
             hi: vals.length ? vals[vals.length - 1] : null,
             n: vals.length };
  }
  function cmpFacts(member, sid) {
    var score = null, drivers = [], rank = null, total = null, nvalid = null;
    var range = { lo: null, hi: null, n: null };
    if (member.level === "State/UT") {
      var st = (D.state_stats[sid] || {})[member.key];
      if (st) {
        score = st.mean; drivers = st.drivers || []; rank = st.rank;
        nvalid = st.n_valid; total = Object.keys(D.state_stats[sid] || {}).length;
      }
      range = cmpRange(D.blocks.filter(function (b) { return b.s === member.key; }),
                       D.block_scores[sid] || {});
    } else if (member.level === "District") {
      score = (D.district_scores[sid] || {})[member.key];
      drivers = (D.drivers[sid] || {})[member.key] || [];
      var d = byKey[member.key];
      var rows = rankedDistrictsIn(d ? d.s : member.parent, sid);
      total = rows.length; nvalid = rows.length;
      rows.forEach(function (r) { if (r.key === member.key) rank = r.rank; });
      range = cmpRange(blocksOfDistrict[member.key] || [], D.block_scores[sid] || {});
    } else if (member.level === "Block") {
      score = (D.block_scores[sid] || {})[member.key];
      drivers = (D.block_drivers[sid] || {})[member.key] || [];
    }
    var valid = score !== undefined && score !== null && !isNaN(score);
    return { score: valid ? score : null, band: valid ? band(score) : null,
             drivers: drivers, rank: rank, total: total, nvalid: nvalid,
             blockLo: range.lo, blockHi: range.hi, blockN: range.n };
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

    var gb = document.createElementNS(ns, "g");
    gb.setAttribute("id", "g-block");
    D.blocks.forEach(function (b) {
      var p = document.createElementNS(ns, "path");
      p.setAttribute("d", b.d);
      p.setAttribute("class", "blk");
      p.dataset.block = b.k;
      gb.appendChild(p);
      nodes["B:" + b.k] = p;
    });
    svg.appendChild(gb);

    /* the live State/UT's district outlines, drawn as the coarse stroke once
       blocks are the painted unit */
    var gdo = document.createElementNS(ns, "g");
    gdo.setAttribute("id", "g-distline");
    (statesOf[D.live_state] || []).forEach(function (d) {
      var p = document.createElementNS(ns, "path");
      p.setAttribute("d", d.d);
      p.setAttribute("class", "stroke-coarse");
      p.dataset.outline = d.k;
      gdo.appendChild(p);
    });
    svg.appendChild(gdo);

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

    /* Context overlay: above the administrative strokes so the circles read,
       below compare and selection so neither outline is ever buried. */
    var gx = document.createElementNS(ns, "g");
    gx.setAttribute("id", "g-ctx");
    svg.appendChild(gx);

    /* Comparison sits above all administrative strokes but below selection.
       Its purple 2.2px outline is intentionally neither another grey boundary
       weight nor strong enough to compete with the 2.6px blue selection. */
    var gc = document.createElementNS(ns, "g");
    gc.setAttribute("id", "g-cmp");
    svg.appendChild(gc);

    var sel = document.createElementNS(ns, "path");
    sel.setAttribute("id", "g-sel");
    sel.setAttribute("class", "sel-stroke");
    sel.setAttribute("d", "");
    svg.appendChild(sel);

    gd.addEventListener("mousemove", onHover);
    gd.addEventListener("mouseleave", hideTip);
    gd.addEventListener("click", onMapClick);
    gb.addEventListener("mousemove", onBlockHover);
    gb.addEventListener("mouseleave", hideTip);
    gb.addEventListener("click", onBlockClick);
    built = true;
  }

  function viewBox() {
    if (S.view === "india") return [0, 0, D.width, D.height];
    var ds = S.view === "district"
      ? (byKey[S.district] ? [byKey[S.district]] : [])
      : (statesOf[S.state] || []);
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
    svg.setAttribute("viewBox", viewBox().join(" "));

    var national = S.view === "india";
    var dsc = dScores(), bsc = bScores();
    var pin = S.pinned;

    /* the pinned bin always emphasises the units this view RANKS, and those are
       districts in every view except the national one */
    var emphDistricts = null;
    if (pin !== null) {
      emphDistricts = {};
      if (national) {
        var st = sStats();
        Object.keys(st).forEach(function (name) {
          if (binIndex(st[name].mean) === pin) {
            (statesOf[name] || []).forEach(function (d) { emphDistricts[d.k] = 1; });
          }
        });
      } else {
        rankedDistricts(S.state).forEach(function (r) {
          if (binIndex(r.score) === pin) emphDistricts[r.key] = 1;
        });
      }
    }

    /* ---- district layer: painted nationally, plain backdrop once drilled ---- */
    D.districts.forEach(function (d) {
      var p = nodes[d.k];
      p.style.display = "";
      if (national) {
        p.setAttribute("fill", mapColour(dsc[d.k]));
        p.classList.remove("out");
        p.classList.toggle("locked", !isLive(d.s));
        p.classList.toggle("muted", emphDistricts !== null && !emphDistricts[d.k]);
      } else {
        /* neighbours stay as a faint backdrop so the State reads in context */
        p.setAttribute("fill", "#eceff2");
        p.classList.add("out");
        p.classList.remove("muted", "locked");
      }
      p.classList.remove("hl");
    });

    /* ---- block layer: only once a State/UT is open ---- */
    D.blocks.forEach(function (b) {
      var p = nodes["B:" + b.k];
      if (national) { p.style.display = "none"; return; }
      p.style.display = "";
      p.setAttribute("fill", mapColour(bsc[b.k]));
      var parentEmph = emphDistricts === null || emphDistricts[b.dk];
      var inDistrict = S.view !== "district" || b.dk === S.district;
      p.classList.toggle("muted", !parentEmph || !inDistrict);
      p.classList.remove("hl");
    });

    /* ---- boundary grammar: coarse = the unit the previous view painted ---- */
    var stateLines = svg.querySelectorAll("#g-state path");
    for (var i = 0; i < stateLines.length; i++) {
      stateLines[i].style.display =
        national ? "" : (stateLines[i].dataset.state === S.state ? "" : "none");
    }
    var distLines = svg.querySelectorAll("#g-distline path");
    for (var j = 0; j < distLines.length; j++) {
      distLines[j].style.display = national ? "none" : "";
    }

    /* ---- persistent comparison emphasis ---- */
    var cmp = document.getElementById("g-cmp");
    cmp.replaceChildren();
    var selectedId = S.block ? "Block:" + S.block
      : S.view === "district" ? "District:" + S.district
      : S.view === "state" ? "State/UT:" + S.state : null;
    S.cmp.members.forEach(function (member) {
      if (cmpIdentity(member) === selectedId) return;
      var path = null, visible = false;
      if (member.level === "State/UT") {
        /* A drilled viewport contains only its active State/UT. Other compared
           States deliberately remain tray-only until the user returns to India. */
        path = D.state_paths[member.key]; visible = national;
      } else if (member.level === "District" && byKey[member.key]) {
        path = byKey[member.key].d;
        visible = national || (byKey[member.key].s === S.state &&
          (S.view === "state" || byKey[member.key].k === S.district));
      } else if (member.level === "Block" && blockByKey[member.key]) {
        var cb = blockByKey[member.key];
        path = cb.d; visible = !national && cb.s === S.state &&
          (S.view === "state" || cb.dk === S.district);
      }
      if (!path || !visible) return;
      var cp = document.createElementNS("http://www.w3.org/2000/svg", "path");
      cp.setAttribute("class", "cmp-stroke");
      cp.setAttribute("d", path);
      cmp.appendChild(cp);
    });

    /* ---- selection stroke ---- */
    var selPath = document.getElementById("g-sel");
    var selKey = S.block ? ("B:" + S.block) : null;
    if (selKey && blockByKey[S.block]) selPath.setAttribute("d", blockByKey[S.block].d);
    else if (S.view === "district" && byKey[S.district]) selPath.setAttribute("d", byKey[S.district].d);
    else selPath.setAttribute("d", "");

    paintContextLayer(emphDistricts);
  }

  /* Population circles: area proportional to population, so radius scales as
     the square root. The overlay follows the unit the map paints -- districts
     nationally, blocks once a State/UT is open -- so the symbol layer and the
     choropleth always describe the same geography. The scale is the national
     district scale in every view: circles magnify with the viewBox zoom, which
     keeps people-per-map-area honest, and only the legibility floor is divided
     back out so it stays one screen pixel rather than swelling with the zoom. */
  function paintContextLayer(emphDistricts) {
    var g = document.getElementById("g-ctx");
    if (!g) return;
    g.replaceChildren();
    var national = S.view === "india";
    if (S.ctxLayer !== "pop") {
      svg.setAttribute("aria-label", national
        ? "Choropleth of district bundle scores"
        : "Choropleth of block bundle scores");
      return;
    }

    svg.setAttribute("aria-label",
      "Choropleth of " + (national ? "district" : "block") +
      " bundle scores, with population shown as proportional circles");

    var ns = "http://www.w3.org/2000/svg";
    var zoom = ctxZoom();
    ctxUnits().forEach(function (u) {
      if (!u.c) return;
      var r = popRadius((CTX_EXPOSURE[u.k] || {}).pop, zoom);
      if (r <= 0) return;
      /* muting mirrors the choropleth exactly: the pinned bin emphasises
         districts, and a block inherits its parent district's emphasis */
      var ek = national ? u.k : u.dk;
      var c = document.createElementNS(ns, "circle");
      c.setAttribute("cx", u.c[0]);
      c.setAttribute("cy", u.c[1]);
      c.setAttribute("r", r.toFixed(3));
      c.setAttribute("class", "ctx-dot" +
        (emphDistricts !== null && !emphDistricts[ek] ? " ctx-muted" : ""));
      g.appendChild(c);
    });
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
    if (!t.dataset || !t.dataset.key || S.view !== "india") { hideTip(); return; }
    var d = byKey[t.dataset.key];
    if (!d) { hideTip(); return; }
    for (var k in nodes) nodes[k].classList.remove("hl");

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
      "<div class='t-row'>" + st.n_valid + " valid districts</div>" +
      (isLive(d.s)
        ? "<div class='t-row t-go'>Select to open the State view</div>"
        : "<div class='t-row t-off'>Not selectable — only " + esc(D.live_state) +
          " is scored below district level in this prototype</div>"), ev);
  }

  /* Hover always describes the unit this view can *select*, never the unit it
     paints. Nationally that means districts are painted but a hover reports the
     State/UT and highlights the whole State/UT. The State view inherits exactly
     that: blocks are painted, but a hover reports the parent district and
     highlights all of that district's blocks. Only in the District view, where
     the block is itself the selectable unit, does a hover report the block. */
  function onBlockHover(ev) {
    var t = ev.target;
    if (!t.dataset || !t.dataset.block || S.view === "india") { hideTip(); return; }
    var b = blockByKey[t.dataset.block];
    if (!b) { hideTip(); return; }
    if (S.view === "district" && b.dk !== S.district) { hideTip(); return; }
    for (var k in nodes) nodes[k].classList.remove("hl");

    if (S.view === "state") {
      var d = byKey[b.dk];
      var sibs = blocksOfDistrict[b.dk] || [];
      sibs.forEach(function (x) {
        var node = nodes["B:" + x.k];
        if (node) node.classList.add("hl");
      });
      if (!d) { hideTip(); return; }
      var info = districtRank(b.dk);
      showTip(
        "<b>" + esc(d.n) + "</b>" +
        "<div class='t-row'>" + esc(d.s) + "</div>" +
        (info === null
          ? "<div class='t-row'>No valid data</div>"
          : "<div class='t-row'>Composite score <em>" + fmt(info.score) + "</em> · " +
            esc(band(info.score)) + "</div>" +
            "<div class='t-row'>Rank <em>" + info.rank + "</em> of " + info.total +
            " valid districts in " + esc(d.s) + "</div>") +
        "<div class='t-row'>" + sibs.length + " blocks painted</div>" +
        "<div class='t-row t-go'>Select to open the District view</div>", ev);
      return;
    }

    nodes["B:" + b.k].classList.add("hl");
    var v = bScores()[b.k];
    var parent = byKey[b.dk];
    showTip(
      "<b>" + esc(b.n) + "</b>" +
      "<div class='t-row'>" + esc(parent ? parent.n : "") + " · " + esc(b.s) + "</div>" +
      (v === undefined
        ? "<div class='t-row'>No valid data</div>"
        : "<div class='t-row'>Score <em>" + fmt(v) + "</em> · " + esc(band(v)) + "</div>" +
          "<div class='t-row t-off'>Blocks are painted, never ranked</div>") +
      "<div class='t-row t-go'>Select to open Detailed Analysis</div>", ev);
  }

  function onMapClick(ev) {
    var t = ev.target;
    if (!t.dataset || !t.dataset.key || S.view !== "india") return;
    var d = byKey[t.dataset.key];
    if (!d || !isLive(d.s)) return;
    selectState(d.s);
  }

  function onBlockClick(ev) {
    var t = ev.target;
    if (!t.dataset || !t.dataset.block || S.view === "india") return;
    var b = blockByKey[t.dataset.block];
    if (!b) return;
    if (S.view === "state") { openDistrict(b.dk); return; }
    if (b.dk !== S.district) return;
    S.block = b.k;
    S.da = { level: "Block", unit: b.n + ", " + (byKey[b.dk] ? byKey[b.dk].n : "") };
    render();
  }

  /* ================= state transitions ================= */
  function selectState(name) {
    S.view = "state"; S.state = name;
    S.district = null; S.block = null;
    S.pinned = null; S.showAll = false; S.da = null;
    render();
  }
  function goIndia() {
    S.view = "india"; S.state = null; S.district = null; S.block = null;
    S.pinned = null; S.showAll = false; S.da = null;
    S.local = false;        /* the landing state is always the frozen domain */
    render();
  }
  function goState() {
    S.view = "state"; S.district = null; S.block = null;
    S.showAll = false; S.da = null;
    render();
  }
  /* The third navigation level: a district opens its blocks. */
  function openDistrict(key) {
    var d = byKey[key];
    if (!d || !isLive(d.s)) return;
    S.view = "district"; S.state = d.s; S.district = key;
    S.block = null; S.showAll = false; S.da = null;
    render();
  }

  /* The units this view actually paints, sorted. The colourbar bracket and the
     local-contrast extent both read exactly this set — the painted units, not
     the ranked ones. */
  function paintedValues() {
    var vals = [], noun;
    if (S.view === "india") {
      var sc = dScores();
      noun = "districts";
      D.districts.forEach(function (d) {
        var v = sc[d.k]; if (v !== undefined) vals.push(v);
      });
    } else {
      var bsc = bScores();
      noun = "blocks";
      D.blocks.forEach(function (b) {
        if (S.view === "district" && b.dk !== S.district) return;
        var v = bsc[b.k]; if (v !== undefined) vals.push(v);
      });
    }
    vals.sort(function (a, b) { return a - b; });
    return { vals: vals, noun: noun };
  }

  /* ================= histogram ================= */
  function renderHistogram(pv) {
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
        : S.view === "state"
          ? "Bins the districts this view ranks within " + S.state + ", not the blocks it paints."
          : "Still the districts of " + S.state + ": the District view ranks nothing of its own, " +
            "because blocks are never ranked.";

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
    var vals = pv.vals, noun = pv.noun;
    var med = vals.length ? (vals.length % 2 ? vals[(vals.length - 1) / 2]
              : (vals[vals.length / 2 - 1] + vals[vals.length / 2]) / 2) : NaN;
    document.getElementById("painted").textContent =
      "Painted: " + vals.length + " " + noun + " · median " + fmt(med) +
      " · min–max " + fmt(vals[0]) + "–" + fmt(vals[vals.length - 1]);
  }

  /* ================= colourbar ================= */
  function renderColourbar(vals) {
    var strip = document.getElementById("cbar-strip");
    strip.style.background = "linear-gradient(to right," + D.ramp.join(",") + ")";

    /* Ticks carry the domain. Frozen: 0-100. Local contrast: the visible
       extent, so the reader can see what the stretched ramp now means. */
    var ticks = document.getElementById("cbar-ticks");
    ticks.innerHTML = "";
    [0, 20, 40, 60, 80, 100].forEach(function (t) {
      var s = document.createElement("span");
      s.textContent = LX.on ? fmt(LX.lo + (LX.hi - LX.lo) * t / 100) : String(t);
      s.style.left = t + "%";
      ticks.appendChild(s);
    });

    /* The bracket exists to say "this view is narrow, not broken". Under local
       contrast the bracket is the whole bar, so it carries no information and
       is dropped. */
    var br = document.getElementById("cbar-bracket");
    if (vals.length && !LX.on) {
      br.style.display = "";
      br.style.left = vals[0] + "%";
      br.style.width = Math.max(0.6, vals[vals.length - 1] - vals[0]) + "%";
    } else {
      br.style.display = "none";
    }
    document.getElementById("cbar-range").textContent = vals.length
      ? "Range in view " + fmt(vals[0]) + "–" + fmt(vals[vals.length - 1]) +
        " · " + fmt(vals[vals.length - 1] - vals[0]) + " wide"
      : "";

    var dom = document.getElementById("cbar-domain");
    dom.textContent = LX.on
      ? "Domain stretched to this view — not comparable"
      : "Domain fixed 0–100 · never rescaled by selection";
    dom.className = LX.on ? "lc-on" : "";

    var span = vals.length ? vals[vals.length - 1] - vals[0] : 0;
    var lc = document.getElementById("lc-toggle");
    lc.checked = !!S.local;
    lc.disabled = !(vals.length > 1 && span > 0);
    document.getElementById("lc-label").className = "lc" + (lc.disabled ? " off" : "");

    var warn = document.getElementById("cbar-warn");
    warn.hidden = !LX.on;
    if (LX.on) {
      warn.innerHTML =
        "<b>Local contrast is on — colours are not comparable.</b> The ramp is " +
        "stretched to the " + vals.length + " painted " +
        (S.view === "india" ? "districts" : "blocks") + " in this view (" +
        fmt(LX.lo) + "–" + fmt(LX.hi) + "), so the same colour means a different " +
        "score here than in any other selection, period or scenario. The scores " +
        "are unchanged: this is a view extent, not a normalization. The " +
        "distribution and the ranking stay on the frozen 0–100 ruler.";
    }
    document.getElementById("cbar-title").textContent =
      S.bundle + " score" + (LX.on ? " — local contrast (not comparable)" : "");
  }

  function renderContextLayer() {
    var row = document.getElementById("ctxl-row");
    /* Section 5: omit an unavailable overlay rather than showing it disabled.
       Population is carried for districts AND blocks, so it is available in
       every view; only a payload with no population at all removes the row. */
    var available = POP_MAX > 0;
    row.hidden = !available;
    if (!available) return;

    document.getElementById("ctx-layer").value = S.ctxLayer || "";

    var key = document.getElementById("ctx-key");
    var note = document.getElementById("ctx-note");
    if (S.ctxLayer !== "pop") {
      key.hidden = true; key.innerHTML = ""; note.textContent = "";
      return;
    }

    /* The scale is frozen nationally, but the KEY labels sizes that actually
       occur in this view -- a 10M swatch is useless over blocks. The key is a
       reading aid; it never changes what a circle means. */
    var zoom = ctxZoom();
    var inView = 0;
    ctxUnits().forEach(function (u) {
      var v = (CTX_EXPOSURE[u.k] || {}).pop;
      if (typeof v === "number" && v > inView) inView = v;
    });
    var steps = popKeySteps(inView);
    /* popRadius returns USER units and the map magnifies them by the viewBox
       zoom, so the swatch must do the same -- otherwise the key understates
       every circle it labels in every view except the national one. */
    var swatch = steps.map(function (v) { return popRadius(v, zoom) * zoom; });
    var box = Math.ceil(swatch[0] * 2) + 2;

    key.hidden = false;
    key.innerHTML = "<span class='ctx-key-lab'>Circle area = population</span>" +
      steps.map(function (v, i) {
        return "<span class='ctx-key-step'>" +
               "<svg width='" + box + "' height='" + box +
               "' viewBox='0 0 " + box + " " + box +
               "'><circle class='ctx-dot' cx='" + (box / 2) + "' cy='" + (box / 2) +
               "' r='" + swatch[i].toFixed(2) + "'></circle></svg>" +
               "<span>" + popLabel(v) + "</span></span>";
      }).join("");

    var miss = popMissing(), unit = S.view === "india" ? "district" : "block";
    note.textContent = miss
      ? miss + " " + unit + (miss === 1 ? " carries" : "s carry") +
        " no circle — no population figure, or zero."
      : "";
  }

  /* ================= answer card ================= */
  function bandPill(v) {
    var b = band(v);
    return "<span class='band' style='color:" + BAND_INK[b] + "'>" + b + "</span>";
  }

  /* A driver list whose every item routes into Detailed Analysis with the
     current geography, level, bundle, scenario and period preserved and that
     metric selected -- the driver-to-Detailed-Analysis route of section 7. */
  function driverList(slugs, level, unit) {
    if (!slugs || !slugs.length) {
      return "<p class='sub' style='margin:12px 0 0'>Driver information is not " +
             "available for this geography.</p>";
    }
    return "<p class='drv-head'>" +
      (D.bundle_kind === "sector" ? "Top rule signals" : "Metric drivers") +
      "</p><ul class='drv'>" +
      slugs.map(function (slug) {
        return "<li><button type='button' data-drv='" + esc(slug) +
          "' data-lvl='" + esc(level) + "' data-unit='" + esc(unit) + "'>" +
          esc(metricLabel(slug)) + "<span class='drv-go'>&rsaquo;</span></button></li>";
      }).join("") + "</ul>";
  }

  function wireDrivers(host) {
    var buttons = host.querySelectorAll("ul.drv button");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener("click", function (ev) {
        var b = ev.currentTarget;
        S.da = { level: b.dataset.lvl, unit: b.dataset.unit, driver: b.dataset.drv };
        render();
      });
    }
  }

  /* The State-view Headline the layout contract requires -- score, band, rank,
     valid count, drivers and the one Detailed Analysis action -- as a strip
     above the map rather than a card of prose. The national view has no
     selected unit and therefore no headline: the ranking and the distribution
     are the answer there. */
  function renderHeadline() {
    var host = document.getElementById("headline");
    if (S.view === "india") { host.hidden = true; host.innerHTML = ""; return; }
    host.hidden = false;

    var total = Object.keys(sStats()).length;
    var rows = rankedDistricts(S.state);

    if (S.view === "state") {
      var st = sStats()[S.state];
      var drv = (st && st.drivers) || [];
      var stateMember = makeCmpMember("State/UT", S.state);
      host.innerHTML =
        "<h2>State/UT headline</h2>" +
        "<div class='hl-top'><span class='hl-name'>" + esc(S.state) + "</span>" +
          (st ? bandPill(st.mean) : "") +
          cmpControl(stateMember, false) +
          "<span class='bigscore'>" + fmt(st ? st.mean : NaN) + "</span></div>" +
        "<p class='hl-meta'>Area-weighted mean district score · rank <b>" +
          (st ? st.rank : "—") + "</b> of " + total + " State/UTs · <b>" +
          rows.length + "</b> valid districts · <b>" + D.blocks.length +
          "</b> blocks painted</p>" +
        "<p class='hl-scope'>The area-weighted average of its districts' national scores — " +
          "not a percentile among States. Hazard-only: no exposure, vulnerability or " +
          "resilience.</p>" +
        driverList(drv, "State/UT", S.state);
      wireDrivers(host);
      wireCmpControls(host);
      return;
    }

    /* District view */
    var d = byKey[S.district];
    var districtMember = makeCmpMember("District", S.district);
    var me = null;
    rows.forEach(function (r) { if (r.key === S.district) me = r; });
    var blocks = blocksOfDistrict[S.district] || [];
    var bsc = bScores();
    var vals = [];
    blocks.forEach(function (b) { if (bsc[b.k] !== undefined) vals.push(bsc[b.k]); });
    vals.sort(function (a, b) { return a - b; });

    host.innerHTML =
      "<h2>District headline</h2>" +
      "<div class='hl-top'><span class='hl-name'>" + esc(d.n) + "</span>" +
        (me ? bandPill(me.score) : "") +
        cmpControl(districtMember, false) +
        "<span class='bigscore'>" + fmt(me ? me.score : NaN) + "</span></div>" +
      "<p class='hl-parent'>" + esc(S.state) + "</p>" +
      "<p class='hl-meta'>Composite score · rank <b>" + (me ? me.rank : "—") +
        "</b> of " + rows.length + " valid districts in " + esc(S.state) + "</p>" +
      "<p class='hl-scope'>" + vals.length + " blocks painted, scoring <b>" +
        fmt(vals[0]) + "</b> to <b>" + fmt(vals[vals.length - 1]) + "</b>. Blocks are " +
        "scored from their own physical values on the same frozen ruler, so this " +
        "district's score is not their average.</p>" +
      driverList(dDrivers()[S.district] || [], "District", d.n + ", " + S.state);
    wireDrivers(host);
    wireCmpControls(host);
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
            ". Select a row to open that district's blocks.") +
      "</p>" +
      "<table class='rank'><thead><tr><th class='rk'>#</th><th>" +
        (isIndia ? "State / UT" : "District") +
        "</th><th class='num'>Score</th><th>Band</th><th class='cmp-cell'>Compare</th>" +
        "</tr></thead><tbody>";
    var tied = {};
    shown.forEach(function (r) { tied[r.rank] = (tied[r.rank] || 0) + 1; });
    shown.forEach(function (r) {
      var selected = isIndia ? false : (r.key === S.district);
      var member = makeCmpMember(isIndia ? "State/UT" : "District", r.key);
      html += "<tr data-key='" + esc(r.key) + "'" + (selected ? " class='selected'" : "") + ">" +
        "<td class='rk'>" + r.rank + (tied[r.rank] > 1 ? "=" : "") + "</td>" +
        "<td>" + esc(r.name) + "</td>" +
        "<td class='num sc'>" + shownScore[r.key] + "</td>" +
        "<td class='bandcell'>" + band(r.score) + "</td>" +
        "<td class='cmp-cell'>" + cmpControl(member, true) + "</td></tr>";
    });
    html += "</tbody></table>";
    if (rows.length > 10) {
      html += "<div class='filterbar'><button class='linkbtn' id='toggle-all'>" +
        (S.showAll ? "Show top 10" : "View all " + rows.length) + "</button></div>";
    }
    if (!rows.length) html += "<p class='sub'>No units in this filter.</p>";
    host.innerHTML = html;
    wireCmpControls(host);

    var t = host.querySelector("tbody");
    if (t) t.addEventListener("click", function (ev) {
      if (ev.target.closest(".cmp-toggle")) return;
      var tr = ev.target.closest("tr");
      if (!tr) return;
      if (isIndia) selectState(tr.dataset.key); else openDistrict(tr.dataset.key);
    });
    var ta = document.getElementById("toggle-all");
    if (ta) ta.addEventListener("click", function () { S.showAll = !S.showAll; renderRanking(); });
  }

  /* ================= block inspection + DA stub ================= */
  function renderInspection() {
    var host = document.getElementById("inspection");
    var html = "";

    if (S.view === "district" && S.block && blockByKey[S.block]) {
      var b = blockByKey[S.block];
      var v = bScores()[b.k];
      var parent = byKey[b.dk];
      var blockMember = makeCmpMember("Block", b.k);
      html +=
        "<div class='card insp'>" +
        "<button class='close' id='close-insp' aria-label='Clear block selection'>×</button>" +
        "<h2>Block inspection</h2>" +
        "<div class='place'>" + esc(b.n) + "<small>" + esc(parent ? parent.n : "") +
          " · " + esc(b.s) + "</small></div>" +
        "<div class='scoreline'><span class='bigscore'>" + fmt(v) + "</span>" +
          (v === undefined ? "" : bandPill(v)) + "</div>" +
        "<div class='btn-row'>" + cmpControl(blockMember, false) + "</div>" +
        "<dl class='kv'>" +
          "<dt>Rank</dt><dd>Blocks are not ranked at any scope — subdivision density " +
            "reflects State administration rather than geography</dd>" +
          "<dt>Coverage</dt><dd>Complete — all 9 headline metrics valid</dd>" +
        "</dl>" +
        driverList(bDrivers()[b.k] || [], "Block", b.n + ", " + (parent ? parent.n : "")) +
        "</div>";
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
          "<dt>Comparison context</dt><dd>" + esc(cmpContextText()) + "</dd>" +
          "<dt>Opens on</dt><dd>" +
            (S.da.driver
              ? "the metric <b>" + esc(metricLabel(S.da.driver)) + "</b>, the driver that was " +
                "selected"
              : "the composite metric for " + esc(S.bundle) + ", never <code>Metric = All</code>") +
          "</dd>" +
        "</dl>" +
        "<p style='margin:10px 0 0'>Hover, bin filter and temporary map emphasis are not " +
        "carried across.</p></div>" +
        "<div class='btn-row'><button class='btn ghost' id='back-ov'>Back to Overview</button></div>" +
        "</div>";
    }

    host.innerHTML = html;
    wireDrivers(host);
    wireCmpControls(host);
    var c = document.getElementById("close-insp");
    if (c) c.addEventListener("click", function () { S.block = null; S.da = null; render(); });
    var bo = document.getElementById("back-ov");
    if (bo) bo.addEventListener("click", function () { S.da = null; render(); });
  }

  /* ================= Compare locations ================= */
  function cmpSliceLabel(sid) {
    var parts = sid.split("|");
    return { scenario: D.scenario_labels[parts[0]] || parts[0],
             period: D.period_labels[parts[1]] || parts[1] };
  }
  function cmpRankText(member, facts, baseFacts, compareToBase) {
    if (member.level === "Block") return "Blocks are not ranked at any scope";
    if (facts.rank === null || facts.rank === undefined) return "Rank not available";
    var scope = member.level === "State/UT"
      ? facts.rank + " of " + facts.total + " in India"
      : facts.rank + " of " + facts.total + " in " + member.parent;
    if (!compareToBase || !baseFacts || baseFacts.rank === null || baseFacts.rank === undefined) {
      return scope;
    }
    var move = facts.rank - baseFacts.rank;
    return scope + (move < 0 ? " · up " + Math.abs(move)
      : move > 0 ? " · down " + move : " · unchanged from column 1");
  }
  function cmpBlocksText(member, facts) {
    if (member.level === "Block") return "Not applicable for a Block";
    if (!facts.blockN) return "No scored blocks in this prototype · 0 valid";
    return fmt(facts.blockLo) + "–" + fmt(facts.blockHi) +
      " on the frozen scale · " + facts.blockN + " valid blocks";
  }
  function cmpContextText() {
    if (S.cmp.mode === "futures" && S.cmp.subject) {
      var labels = S.cmp.slices.map(function (sid) {
        var x = cmpSliceLabel(sid); return x.scenario + " " + x.period;
      });
      return "Futures for " + S.cmp.subject.name + " (" + S.cmp.subject.level + "): " +
        (labels.length ? labels.join("; ") : "no slices selected");
    }
    if (S.cmp.members.length) {
      return "Places at " + D.scenario_labels[S.scenario] + " " + D.period_labels[S.period] +
        ": " + S.cmp.members.map(function (m) {
          return m.name + " (" + m.level + ")";
        }).join("; ");
    }
    return "No active comparison tray";
  }
  function renderCompare() {
    var host = document.getElementById("compare");
    if (S.cmp.mode === "places" && !S.cmp.members.length) {
      host.innerHTML = "";
      return;
    }

    var futures = S.cmp.mode === "futures";
    var slots = futures
      ? S.cmp.slices.map(function (sid) { return { member: S.cmp.subject, sid: sid }; })
      : S.cmp.members.map(function (member) { return { member: member, sid: sliceId() }; });
    var facts = slots.map(function (slot) { return cmpFacts(slot.member, slot.sid); });
    var driverCounts = {};
    facts.forEach(function (f) {
      var seen = {};
      f.drivers.forEach(function (slug) {
        if (!seen[slug]) driverCounts[slug] = (driverCounts[slug] || 0) + 1;
        seen[slug] = 1;
      });
    });

    var modeControls = "";
    if (futures) {
      var scenarioOptions = Object.keys(D.scenario_labels).map(function (key) {
        return "<option value='" + escAttr(key) + "'>" + esc(D.scenario_labels[key]) + "</option>";
      }).join("");
      var periodOptions = Object.keys(D.period_labels).map(function (key) {
        return "<option value='" + escAttr(key) + "'>" + esc(D.period_labels[key]) + "</option>";
      }).join("");
      modeControls =
        "<div class='cmp-mode'>" +
          "<label>Scenario<select id='cmp-add-scenario'>" + scenarioOptions + "</select></label>" +
          "<label>Period<select id='cmp-add-period'>" + periodOptions + "</select></label>" +
          "<button type='button' class='btn ghost' id='cmp-add-slice'>Add future</button>" +
          "<button type='button' class='btn ghost' id='cmp-back-places'>Compare places</button>" +
        "</div>";
    }

    var html =
      "<section class='card compare' aria-label='Compare locations'>" +
      "<div class='cmp-head'><div><h2>Compare locations · " + esc(S.bundle) + "</h2>" +
      "<p><b>Active bundle:</b> " + esc(S.bundle) + " · " +
        (futures
          ? "Futures mode fixes " + esc(S.cmp.subject.name) + " (" +
            esc(S.cmp.subject.level) + "). Each column keeps its named scenario-period slice; " +
            "the page header selectors update the rest of Overview without rewriting this tray."
          : "Places mode fixes " + esc(D.scenario_labels[S.scenario]) + " · " +
            esc(D.period_labels[S.period]) + " while locations vary.") +
        " Members are retained when the bundle changes; every figure is replaced by the active bundle." +
        " Map outlines appear only for tray members inside the current geography; off-view members " +
        "remain listed here." +
      "</p></div>" + modeControls + "</div>";

    var nvalid = facts.map(function (f) { return f.nvalid; })
      .filter(function (n) { return n !== null && n !== undefined; });
    var nvalidKinds = {};
    nvalid.forEach(function (n) { nvalidKinds[n] = 1; });
    if (futures && Object.keys(nvalidKinds).length > 1) {
      var coverageUnit = S.cmp.subject.level === "State/UT"
        ? "districts in the State/UT mean" : "members in the rank cohort";
      html += "<p class='cmp-alert'><b>Coverage warning:</b> <code>n_valid</code> differs across " +
        "slices (" + nvalid.join(", ") + " " + coverageUnit + "). Rank movement is shown with " +
        "this denominator difference exposed; do not read it as clean movement.</p>";
    }

    if (!slots.length) {
      html += "<p class='sub'>No future slices are in the tray. Add a scenario and period above.</p>" +
        "</section>";
      host.innerHTML = html;
    } else {
      function row(className, renderCell) {
        return "<div class='cmp-row " + className + "'>" +
          slots.map(function (slot, i) {
            return "<div class='cmp-slot'>" + renderCell(slot, facts[i], i) + "</div>";
          }).join("") + "</div>";
      }
      html += "<div class='cmp-scroll'><div class='cmp-table' style='--cmp-cols:" +
        slots.length + ";min-width:" + (slots.length * 220) + "px'>";
      html += row("cmp-headers", function (slot, f, i) {
        if (futures) {
          var label = cmpSliceLabel(slot.sid);
          return "<span class='lab'>Column " + (i + 1) + "</span><span class='cmp-place'>" +
            esc(label.scenario) + "</span><span class='cmp-parent'>" + esc(label.period) + "</span>";
        }
        return "<span class='lab'>Column " + (i + 1) + "</span><span class='cmp-place'>" +
          esc(slot.member.name) + "</span><span class='cmp-parent'>" + esc(slot.member.parent) + "</span>";
      });
      html += row("", function (slot) {
        return "<span class='lab'>Level</span><span class='val'>" + esc(slot.member.level) + "</span>";
      });
      html += row("", function (slot, f) {
        return "<span class='lab'>Score + band</span><span class='val'><strong>" +
          fmt(f.score) + "</strong> " + (f.score === null ? "" : bandPill(f.score)) + "</span>";
      });
      html += row("", function (slot, f, i) {
        if (i === 0) {
          return "<span class='lab'>Δ vs column 1</span><span class='val'>Baseline</span>";
        }
        var delta = facts[0].score === null || f.score === null ? null : f.score - facts[0].score;
        var deltaText = delta === null ? "—" : (Math.abs(delta) < 0.0000001 ? "0.0" :
          (delta > 0 ? "+" : "") + delta.toFixed(1)) + " scale points";
        return "<span class='lab'>Δ vs column 1</span><span class='val'>" + deltaText + "</span>" +
          "<span class='cmp-note'>A difference in national percentile position, never a physical " +
          "difference, percentage, or multiple.</span>";
      });
      html += row("", function (slot, f, i) {
        var coverage = f.nvalid === null || f.nvalid === undefined ? ""
          : slot.member.level === "State/UT"
            ? f.nvalid + " districts in the mean"
            : "cohort n_valid " + f.nvalid;
        return "<span class='lab'>Rank · scoped cohort</span><span class='val'>" +
          esc(cmpRankText(slot.member, f, facts[0], futures && i > 0)) + "</span>" +
          (coverage ? "<span class='cmp-note'>" + esc(coverage) + "</span>" : "");
      });
      html += row("", function (slot, f) {
        return "<span class='lab'>Blocks · range + count</span><span class='val'>" +
          esc(cmpBlocksText(slot.member, f)) + "</span>";
      });
      html += row("", function (slot, f) {
        var chips = f.drivers.length ? f.drivers.map(function (slug) {
          var kind = driverCounts[slug] > 1 ? "shared" : "unique";
          return "<span class='cmp-driver " + kind + "'>" + esc(metricLabel(slug)) +
            " · " + kind + "</span>";
        }).join("") : "<span class='cmp-note'>Driver information not available.</span>";
        return "<span class='lab'>Drivers · shared vs unique</span><span class='val'>" + chips + "</span>";
      });
      html += row("", function (slot) {
        if (futures) {
          return "<span class='lab'>Actions</span><span class='cmp-actions'>" +
            "<button type='button' class='btn ghost cmp-remove-slice' data-cmp-slice='" +
            escAttr(slot.sid) + "'>Remove</button></span>";
        }
        return "<span class='lab'>Actions</span><span class='cmp-actions'>" +
          "<button type='button' class='btn ghost cmp-remove-member' data-cmp-id='" +
          escAttr(cmpIdentity(slot.member)) + "'>Remove</button>" +
          "<button type='button' class='btn ghost cmp-futures' data-cmp-id='" +
          escAttr(cmpIdentity(slot.member)) + "'>Compare futures &rsaquo;</button></span>";
      });
      html += "</div></div></section>";
      host.innerHTML = html;
    }

    var removeMembers = host.querySelectorAll(".cmp-remove-member");
    for (var i = 0; i < removeMembers.length; i++) {
      removeMembers[i].addEventListener("click", function (ev) {
        var id = ev.currentTarget.dataset.cmpId;
        S.cmp.members = S.cmp.members.filter(function (m) { return cmpIdentity(m) !== id; });
        render();
      });
    }
    var futuresButtons = host.querySelectorAll(".cmp-futures");
    for (var j = 0; j < futuresButtons.length; j++) {
      futuresButtons[j].addEventListener("click", function (ev) {
        var id = ev.currentTarget.dataset.cmpId, member = null;
        S.cmp.members.forEach(function (m) { if (cmpIdentity(m) === id) member = m; });
        if (member) enterFutures(member);
      });
    }
    var removeSlices = host.querySelectorAll(".cmp-remove-slice");
    for (var k = 0; k < removeSlices.length; k++) {
      removeSlices[k].addEventListener("click", function (ev) {
        var sid = ev.currentTarget.dataset.cmpSlice;
        S.cmp.slices = S.cmp.slices.filter(function (x) { return x !== sid; });
        render();
      });
    }
    var back = document.getElementById("cmp-back-places");
    if (back) back.addEventListener("click", leaveFutures);
    var add = document.getElementById("cmp-add-slice");
    var addScenario = document.getElementById("cmp-add-scenario");
    var addPeriod = document.getElementById("cmp-add-period");
    function updateFutureAdd() {
      if (!add) return;
      var sid = addScenario.value + "|" + addPeriod.value;
      var full = S.cmp.slices.length >= 4;
      var present = S.cmp.slices.indexOf(sid) >= 0;
      add.disabled = full || present;
      add.textContent = full ? "Tray full (4)" : present ? "Already added" : "Add future";
      add.title = full ? "The comparison tray holds at most four futures."
        : present ? "That scenario and period are already in the tray." : "Add this future.";
    }
    if (add) {
      addScenario.value = S.scenario;
      addPeriod.value = S.period;
      addScenario.addEventListener("change", updateFutureAdd);
      addPeriod.addEventListener("change", updateFutureAdd);
      add.addEventListener("click", function () {
        var sid = addScenario.value + "|" + addPeriod.value;
        if (S.cmp.slices.length < 4 && S.cmp.slices.indexOf(sid) < 0) {
          S.cmp.slices.push(sid); render();
        }
      });
      updateFutureAdd();
    }
  }

  /* ================= Context and Evidence ================= */
  /* Supplementary by contract: this section may be entirely absent without
     invalidating a score, a band, a rank or a driver. It is collapsed by
     default, appears from the State view onward, and describes the most local
     unit currently selected. Fields absent from the artifact are omitted rather
     than rendered empty, and nothing is ever inferred from another level. */

  function cxPop(v) {
    if (v === undefined || v === null || isNaN(v)) return null;
    if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(2).replace(/\.00$/, "") + " million";
    return Math.round(v).toLocaleString("en-IN");
  }
  function cxCount(v) {
    if (v === undefined || v === null || isNaN(v)) return null;
    return Math.round(v).toLocaleString("en-IN");
  }
  function cxRate(v) {
    if (v === undefined || v === null || isNaN(v)) return null;
    return v.toFixed(1).replace(/\.0$/, "");
  }
  function cxPct(v, isFraction) {
    if (v === undefined || v === null || isNaN(v)) return null;
    var x = isFraction ? v * 100 : v;
    return (Math.abs(x - Math.round(x)) < 0.05 ? x.toFixed(0) : x.toFixed(1)) + "%";
  }
  function cxArea(v) {
    if (v === undefined || v === null || isNaN(v)) return null;
    return v.toLocaleString("en-IN", { maximumFractionDigits: 1 }) + " km²";
  }
  function cxCell(label, value, note) {
    if (value === null) return "";
    return "<div class='ctx-cell'><span class='lab'>" + esc(label) + "</span>" +
           "<span class='val'>" + value + (note ? " <small>" + esc(note) + "</small>" : "") +
           "</span></div>";
  }

  /* The most local selected unit, and the row that describes it. */
  function contextScope() {
    if (S.view === "district" && S.block && blockByKey[S.block]) {
      var b = blockByKey[S.block];
      var parent = byKey[b.dk];
      return { key: b.k, level: "block", name: b.n,
               where: (parent ? parent.n + " · " : "") + b.s };
    }
    if (S.view === "district" && byKey[S.district]) {
      return { key: S.district, level: "district", name: byKey[S.district].n, where: S.state };
    }
    if (S.view === "state" || S.view === "district") {
      return { key: null, level: "state", name: S.state, where: "India" };
    }
    return null;
  }

  function ctxExposureHtml(scope, cx) {
    var row = scope.level === "state" ? (cx.states || {})[scope.name]
                                      : (cx.exposure || {})[scope.key];
    if (!row) {
      return "<div class='ctx-sec'><h3>Exposure snapshot</h3>" +
             "<p class='ctx-none'>No exposure summary is published for this " +
             scope.level + ".</p></div>";
    }
    var html = "<div class='ctx-sec'><h3>Exposure snapshot</h3>";

    var pop = cxPop(row.pop);
    if (pop !== null) {
      html += "<div class='ctx-grid'>" +
        cxCell("Population", pop) +
        cxCell("Share of " + (scope.level === "state" ? "India" : (row.plevel || "parent")),
               cxPct(row.pshare, false)) +
        "</div>";
    }

    var rf = cxCount(row.rf);
    if (rf !== null) {
      html += "<div class='ctx-grid' style='margin-top:10px'>" +
        cxCell("Rural facilities", rf) +
        cxCell("Per 100k population (rural facilities)", cxRate(row.rf_per100k)) +
        "</div>" +
        "<div class='ctx-grid four' style='margin-top:8px'>" +
        cxCell("Agro", cxCount(row.rf_agro)) +
        cxCell("Education", cxCount(row.rf_edu)) +
        cxCell("Health", cxCount(row.rf_health)) +
        cxCell("Service", cxCount(row.rf_service)) +
        "</div>";
    }

    var bu = cxArea(row.bu), ag = cxArea(row.ag);
    if (bu !== null || ag !== null) {
      html += "<div class='ctx-grid' style='margin-top:10px'>" +
        cxCell("Built-up area", bu) +
        cxCell("Built-up share", cxPct(row.bu_pct, false)) +
        cxCell("Agricultural LULC", ag) +
        cxCell("Agricultural share", cxPct(row.ag_pct, false)) +
        "</div>";
    }

    if (scope.level === "state" && row.n) {
      html += "<p class='ctx-none' style='margin-top:9px'>Summed over " + row.n +
              " districts; every share and rate is recomputed from the State/UT totals, " +
              "never averaged over district percentages.</p>";
    }
    return html + "</div>";
  }

  function ctxHydroHtml(scope, cx) {
    var html = "<div class='ctx-sec'><h3>Hydrological context</h3>";
    /* Deliberately absent at State/UT scope. The workflow forbids counting the
       dominant basins of districts, and a correct State/UT basin share needs a
       State-to-basin geometry intersection this prototype does not carry. An
       honest gap beats a wrong number inferred from another level. */
    if (scope.level === "state") {
      return html + "<p class='ctx-none'>Not available at State/UT level. A State/UT " +
             "basin share must come from a State-to-basin geometry intersection, not from " +
             "counting its districts' dominant basins — so it is omitted rather than " +
             "approximated. Select a district to see basin context.</p></div>";
    }
    var row = (cx.hydro || {})[scope.key];
    if (!row) {
      return html + "<p class='ctx-none'>No hydrological summary is published for this " +
             scope.level + ".</p></div>";
    }
    if (row.basin) {
      html += "<span class='lab' style='font-size:11px;color:var(--ink-3)'>Dominant basin</span><br>" +
        "<span class='ctx-chip'>" + esc(row.basin) +
        (cxPct(row.bfrac, true) ? "<em>" + cxPct(row.bfrac, true) + "</em>" : "") + "</span>";
    }
    if (row.sub) {
      html += "<br><span class='lab' style='font-size:11px;color:var(--ink-3)'>Dominant sub-basin</span><br>" +
        "<span class='ctx-chip'>" + esc(row.sub) +
        (cxPct(row.sfrac, true) ? "<em>" + cxPct(row.sfrac, true) + "</em>" : "") + "</span>";
    }
    if (row.also && row.also.length) {
      html += "<br><span class='lab' style='font-size:11px;color:var(--ink-3)'>Also intersects</span><br>";
      row.also.forEach(function (pair) {
        html += "<span class='ctx-chip'>" + esc(pair[0]) +
                "<em>" + cxPct(pair[1], true) + "</em></span>";
      });
    }
    html += "<div class='ctx-grid' style='margin-top:8px'>" +
      cxCell("Hydro type", row.htype ? esc(row.htype) : null) +
      cxCell("Primary river", row.river ? esc(row.river) : null) +
      "</div>";
    return html + "</div>";
  }

  function renderContext() {
    var host = document.getElementById("context");
    var scope = contextScope();
    var cx = D.context;
    if (!scope || !cx) { host.hidden = true; host.innerHTML = ""; return; }
    host.hidden = false;

    var prov = cx.provenance || {};
    var lines = Object.keys(prov).map(function (k) { return prov[k]; });

    host.innerHTML =
      "<details class='ctx'" + (S.ctxOpen ? " open" : "") + ">" +
      "<summary>Context and Evidence" +
        "<span class='ctx-for'>" + esc(scope.name) + " · " + scope.level +
        " · " + esc(scope.where) + "</span></summary>" +
      "<div class='ctx-body'>" +
        ctxExposureHtml(scope, cx) +
        ctxHydroHtml(scope, cx) +
        "<p class='ctx-prov'>Contextual only — it does not enter the bundle score, " +
        "which stays hazard-only. " + lines.map(esc).join(" · ") + ". Population is " +
        "available as a map overlay (WorldPop-derived admin " +
        "master, 2025 snapshot); basin boundaries and the river network are not " +
        "implemented in this prototype and are omitted from the controls rather than " +
        "shown disabled.</p>" +
      "</div></details>";

    var det = host.querySelector("details.ctx");
    det.addEventListener("toggle", function () { S.ctxOpen = det.open; });
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
    function sep() {
      var s = document.createElement("span");
      s.className = "crumb-sep"; s.textContent = "›";
      host.appendChild(s);
    }
    crumb("India", S.view === "india", goIndia);
    if (S.view !== "india") { sep(); crumb(S.state, S.view === "state", goState); }
    if (S.view === "district") { sep(); crumb(byKey[S.district].n, true, null); }
    if (S.block && blockByKey[S.block]) {
      var note = document.createElement("span");
      note.className = "crumb-note";
      note.textContent = "Inspecting " + blockByKey[S.block].n +
        " — an inspection state; it adds no breadcrumb level.";
      host.appendChild(note);
    }
  }

  function renderMapHead() {
    var title, sub;
    if (S.view === "india") {
      title = "National view — districts painted";
      sub = "Thick State/UT boundary, thin district boundary. State/UT polygons are never " +
            "filled. Hover reports the State/UT; only " + D.live_state + " opens.";
    } else if (S.view === "state") {
      title = S.state + " — blocks painted";
      sub = "Thick district boundary, thin block boundary. Blocks are painted; districts are " +
            "what this view ranks, hovers report and clicks open.";
    } else {
      title = byKey[S.district].n + " — its blocks painted";
      sub = "The same blocks on the same ruler against the same colourbar, at this district's " +
            "extent. Select a block to open Detailed Analysis.";
    }
    document.getElementById("map-title").textContent = title;
    document.getElementById("map-sub").textContent = sub;
  }

  function renderMethod() {
    document.getElementById("method").innerHTML = D.method_html;
  }

  function render() {
    /* extent first: paintMap() reads it, so it cannot be a by-product of the
       histogram the way the bracket range used to be */
    var pv = paintedValues();
    LX.lo = pv.vals.length ? pv.vals[0] : 0;
    LX.hi = pv.vals.length ? pv.vals[pv.vals.length - 1] : 100;
    LX.on = !!S.local && pv.vals.length > 1 && LX.hi > LX.lo;

    renderCrumbs();
    renderMapHead();
    paintMap();
    renderContextLayer();
    renderHistogram(pv);
    renderColourbar(pv.vals);
    renderHeadline();
    renderRanking();
    renderInspection();
    renderContext();
    renderCompare();
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
    b.addEventListener("change", function () {
      S.bundle = b.value;
      /* Locations and futures are the durable shortlist. A bundle change only
         clears transient UI state and recomputes every visible figure. */
      S.pinned = null; S.showAll = false; S.da = null; S.block = null;
      hideTip();
      renderDefaultsNote();
      render();
    });

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
      S.pinned = null; S.showAll = false; S.da = null; S.block = null;
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

  document.getElementById("lc-toggle").addEventListener("change", function (ev) {
    S.local = ev.target.checked; render();
  });

  document.getElementById("ctx-layer").addEventListener("change", function (ev) {
    S.ctxLayer = ev.target.value || null;
    render();
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
      or selection changes, so identical colours mean identical scores everywhere — outside the
      labelled local-contrast view below.</li>
  <li><b>Ramp</b> the vendored NCL <code>WhiteBlueGreenYellowRed</code> table, sampled at 101 stops
      from fraction 0.045 so no valid score renders as pure white. Colour is
      <code>index = round(score)</code>, with no binning.</li>
</ul>

<h3>The range bracket and the local-contrast view</h3>
<p>A fixed domain means a genuinely narrow view renders nearly monochrome, which is correct but
reads as broken. Two things address that, and <b>neither touches the score</b>:</p>
<ul>
  <li><b>The bracket</b> on the colourbar, always on, marking the score range present in the
      current view with a numeric readout. A State/UT whose blocks span two points should read as
      narrow, not as broken.</li>
  <li><b>Local contrast</b>, off by default and never the landing state: the ramp is stretched to
      the extent of the units this view paints, the colourbar ticks change to that extent, and the
      view is labelled as not comparable. Returning to India clears it.</li>
</ul>
<p>Local contrast is a <b>view extent, not a normalization parameter</b>. It is recomputed from the
visible scores on every render and is never stored or precomputed per State/UT — the moment it
were, it would be per-state min&ndash;max again under a different name, which is the thing the
frozen ruler exists to remove.</p>
<p>Rescaling the <i>default</i> colourbar to the on-screen range was considered and rejected: it
reintroduces per-state min&ndash;max at the legend instead of at the score. At SSP5-8.5 2040-2060
a score of 66.2 would be deep red in Goa and yellow in Uttar Pradesh, and Ladakh's 1.8-point spread
would be painted across the whole ramp. Only the map fill responds to local contrast; the
distribution and the ranking stay on the frozen ruler.</p>

<h3>The State/UT statistic</h3>
<p>The <b>area-weighted mean of a State/UT's valid district composite scores</b> — the only State
statistic produced. There is no population-weighted mean. It is not a percentile among States: it
is the area-weighted average of that State's districts' national percentiles.</p>
<p>State/UT polygons are never filled. The national map paints districts; the State view paints
blocks in production.</p>

<h3>What each view paints, ranks and bins</h3>
<ul>
  <li><b>National</b> paints 784 districts, ranks 36 State/UT means, bins those 36 means.</li>
  <li><b>State</b> paints that State/UT's blocks, ranks its districts, bins those district
      scores.</li>
  <li><b>District</b> paints that district's blocks, and ranks and bins nothing of its own —
      it keeps its parent State/UT's district ranking and distribution, because blocks are
      never ranked.</li>
</ul>
<p>Hover follows the same rule as ranking, not painting: it reports the unit the view can
<i>select</i>. Nationally a hover over a painted district reports its State/UT and highlights the
whole State/UT; in the State view a hover over a painted block reports its parent district and
highlights all of that district's blocks. Only in the District view, where the block is itself the
selectable unit, does a hover report the block.</p>
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

<h3>Compare locations</h3>
<p>The comparison tray uses the same frozen national ruler and varies one axis at a time. Places
mode fixes the header scenario and period while locations vary; futures mode fixes one place while
independently pinned scenario-period slices vary. In futures mode the page header selectors continue
to update the rest of Overview but do not rewrite those pinned columns. The tray holds at most four
columns and survives geography, view, selector and bundle changes. Its face always names the active
bundle, because scores from different bundles are never compared. Map outlines appear only for tray
members inside the current geography; off-view members remain in the tray without an outline.</p>
<p>Ranks remain scoped to their real cohort. Places mode shows each rank only as a scoped string,
never as an orderable cross-State number. Futures mode may show movement for the fixed cohort and
exposes any change in <code>n_valid</code>. Blocks are never ranked. Every score difference is in
scale points: a difference in national percentile position, not a physical or percentage change.</p>

<h3>Context and Evidence</h3>
<p>Collapsed by default and available from the State view onward, describing the most local unit
selected: the block if one is selected, otherwise the district, otherwise the State/UT. It is
supplementary by contract — its absence never invalidates a score, band, rank or driver, and it
never enters the score, which stays hazard-only.</p>
<p>State/UT exposure is aggregated from its districts under the workflow's rules: counts and areas
are summed, and every share and per-capita rate is recomputed from the State/UT totals rather than
averaged over district percentages. The share denominator is the same canonical district area that
weights the State/UT headline, so the two cannot drift apart.</p>
<p><b>Hydrological context is deliberately absent at State/UT scope.</b> A State/UT basin share
must come from a State-to-basin geometry intersection; counting the dominant basins of districts
would be a different quantity wearing the same label. It is omitted rather than approximated, which
is the same rule that governs every other field here: an unavailable field is dropped, an
unavailable subsection says so plainly, and nothing is inferred from another geography level.</p>
<p>Map overlays — basin boundaries and the river network — are not implemented, so they are omitted
from the controls rather than shown disabled.</p>
<p><b>A population overlay is implemented</b> as a Context layer: proportional circles, one per
painted unit, centred on each unit's representative point (not its bounding-box or geometric
centroid, which can fall outside a crescent-shaped or coastal district). Circle <b>area</b> is
proportional to population — radius scales as the square root — so it reads honestly at a glance
rather than overstating large districts by the square of a linear radius. The overlay follows the
unit the map paints — districts nationally, blocks once a State/UT is open — and the circle scale
is frozen to the national district maximum in every view, so circle sizes stay comparable across
views. It is off by default and contextual: it does not enter the bundle score, which stays
hazard-only, and it never touches the risk colour ramp beneath it.</p>

<h3>Not implemented here</h3>
<ul>
  <li><b>Blocks outside the live State/UT.</b> Only Telangana's 588 blocks are scored, so only
      Telangana opens below the national view. Every other State/UT hovers normally.</li>
  <li><b>Detailed Analysis.</b> The transition and its carried state are real; the destination is a
      stub.</li>
  <li><b>Twelve of the thirteen eligible bundles</b>, coordinate entry, exports,
      basin and river map overlays, State/UT-level basin context, and the provenance quartet.</li>
</ul>
<p>Scores come from the Heat Risk national pilot and predate the production frozen-scale change.
They are indicative of shape, not a release baseline.</p>
"""


def build_html(
    scores: pd.DataFrame,
    blocks: pd.DataFrame,
    districts: list[dict],
    state_paths: dict[str, str],
    block_shapes: list[dict],
    height: float,
    areas: dict[str, float],
    context: dict,
) -> str:
    """One self-contained page: inline SVG, inline data, no network calls."""
    district_scores, state_stats, drivers = build_tables(scores, areas)
    block_scores, block_drivers = build_block_tables(blocks)

    payload = {
        "districts": districts,
        "state_paths": state_paths,
        "blocks": block_shapes,
        "block_scores": block_scores,
        "block_drivers": block_drivers,
        "live_state": LIVE_STATE,
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
        "context": context,
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
    parser.add_argument("--block-scores", type=Path, default=DEFAULT_BLOCK_SCORES,
                        help="Frozen-ruler block scores for the live State/UT "
                             "(default: %(default)s).")
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
    print(f"scores    : {len(scores):,} district rows over {len(PUBLIC_SLICES)} public slices")

    blocks = load_block_scores(args.block_scores)
    print(f"blocks    : {len(blocks):,} block rows for {LIVE_STATE} "
          f"({blocks['block_key'].nunique()} blocks)")

    areas = district_areas(data_dir)
    print(f"areas     : {len(areas):,} districts")

    districts, state_paths, block_shapes, height = build_shapes(
        data_dir, simplify=args.simplify, precision=args.precision
    )
    print(f"geometry  : {len(districts):,} district paths, {len(state_paths)} State/UT outlines, "
          f"{len(block_shapes):,} {LIVE_STATE} block paths")

    scored_blocks = set(blocks["block_key"].unique())
    drawn_blocks = {b["k"] for b in block_shapes}
    if scored_blocks != drawn_blocks:
        print(f"WARNING   : block score/geometry mismatch — "
              f"{len(scored_blocks - drawn_blocks)} scored without geometry, "
              f"{len(drawn_blocks - scored_blocks)} drawn without a score", file=sys.stderr)

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

    context = load_context(
        data_dir,
        district_keys={d["k"] for d in districts},
        block_keys={b["k"] for b in block_shapes},
        areas=areas,
    )

    html_text = build_html(
        scores, blocks, districts, state_paths, block_shapes, height, areas, context
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html_text, encoding="utf-8")
    print(f"wrote     : {args.out}  ({len(html_text.encode('utf-8')) / 1e6:.2f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
