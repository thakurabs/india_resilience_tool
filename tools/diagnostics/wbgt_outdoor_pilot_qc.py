#!/usr/bin/env python
"""Outdoor-WBGT pilot follow-up: input quality, elevation, coverage and geometry.

Milestone 4b.  Milestone 4 (``wbgt_outdoor_pilot``, verdict ``ENGINEERING CONDITIONAL``) closed
with two named conditions: ``hurs`` above 100 % in the NEX inputs destroyed whole cell-years
under the frozen complete-365 rule, and no elevation field existed locally so every cell ran at
sea level.  This module closes both, and adds the coverage classification and the independent
geometry checks that milestone 4 did not have.

It **reuses** the milestone-4 runner rather than reimplementing it: the frozen W1 physics, the
grid-first order, the aggregation contract and the parity checks all come from
``wbgt_outdoor_pilot``, which gained the input-quality policy and the elevation field as
parameters whose defaults are milestone 4's behaviour.  What lives here is only what is new.

Every output remains DIAGNOSTIC-ONLY, including temperature levels, threshold counts and every
ranking.  See ``docs/diagnostics/wbgt_outdoor_pilot_qc/SPEC.md``, frozen before any WBGT score of
this follow-up existed.  No production module imports this tool and no production helper was
modified to accommodate it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import tools.diagnostics.wbgt_outdoor_feasibility as m1
import tools.diagnostics.wbgt_outdoor_pilot as P

# ==========================================================================
# Frozen identities (pilot_qc SPEC.md 3, 4, 6, 7)
# ==========================================================================

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_outdoor_pilot_qc")
DEFAULT_WORK_DIR = Path("scratch/wbgt_outdoor_pilot_qc")

#: Milestone 4's own evidence and cache, immutable to this milestone (pilot_qc SPEC.md 8).
PREDECESSOR_DIRS = (
    Path("docs/diagnostics/wbgt_outdoor_pilot"),
    Path("scratch/wbgt_outdoor_pilot"),
)

#: Elevation source, frozen in pilot_qc SPEC.md 3.2.  GMTED2010 ``mea`` is the *mean* aggregate,
#: which is the right one to average over a climate cell.  30 arc-second gives about 784 samples
#: per 0.25 degree cell; the finer GMTED variants are a far larger download for no gain in a
#: cell mean.
GMTED_URL_ROOT = (
    "https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/topo/downloads/GMTED/"
    "Global_tiles_GMTED/300darcsec/mea/E060/"
)
GMTED_TILES = ("10S060E", "10N060E", "30N060E")
GMTED_STAMP = "20101117"
GMTED_SUFFIX = "gmted_mea300"
GMTED_NODATA = -32768.0
GMTED_PRODUCT = "GMTED2010 30 arc-second mean (USGS/NGA, public domain)"
GMTED_VERTICAL_REFERENCE = "metres above the EGM96 geoid"
GMTED_HORIZONTAL_REFERENCE = "WGS84 geographic, decimal degrees"
GMTED_SAMPLING = "cos-latitude-weighted mean of GMTED pixel centres inside the 0.25 deg cell box"

#: Acquisition budget, measured separately from computation (pilot_qc SPEC.md 8).
ACQUIRE_BUDGET_SECONDS = 600.0
ACQUIRE_BUDGET_BYTES = 250 * 1024**2

#: The coverage screen and its denominator, defined in pilot_qc SPEC.md 6.1.
COVERAGE_SCREEN = 0.99
COVERAGE_SCREEN_DENOMINATOR = "valid_fraction_of_represented"

#: Geometry tolerances, declared before any geometry was evaluated (pilot_qc SPEC.md 7).
GEOMETRY_GAP_TOLERANCE = 0.001
GEOMETRY_OUTSIDE_TOLERANCE = 0.001
GEOMETRY_OVERLAP_TOLERANCE = 0.001
GEOMETRY_SLIVER_FLOOR_M2 = 10_000.0
ANALYSIS_CRS = "EPSG:6933"

#: The four comparison runs of pilot_qc SPEC.md 4.  ``baseline`` is milestone 4's configuration.
RUN_MATRIX = (
    ("baseline", "strict", False),
    ("rh", "clip100", False),
    ("elev", "strict", True),
    ("combined", "clip100", True),
)
#: MRI-ESM2-0 repeats only the two ends of the matrix, on Himachal Pradesh only.
SECOND_MODEL_RUNS = ("baseline", "combined")

#: Acceptance tolerance for reproducing the frozen milestone-4 tables (pilot_qc SPEC.md 5, A1).
REPRODUCTION_TOL_C = 1e-9


# ==========================================================================
# Write isolation (pilot_qc SPEC.md 8)
# ==========================================================================


def qc_guard(path: Path, label: str) -> Path:
    """Resolve ``path`` and refuse it if it lands anywhere this milestone must not write.

    Delegates to milestone 4's guard and extends it with milestone 4's own evidence and cache,
    so this follow-up cannot overwrite the results it is supposed to reproduce.  Resolution
    happens inside the guard, before any comparison, so a symlink is refused on its destination.
    """

    return P.guard_write_target(path, label, extra_protected=PREDECESSOR_DIRS)


# ==========================================================================
# Elevation acquisition and sampling (pilot_qc SPEC.md 3)
# ==========================================================================


def tile_url(tile: str) -> str:
    return f"{GMTED_URL_ROOT}{tile}_{GMTED_STAMP}_{GMTED_SUFFIX}.tif"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def acquire_elevation_tiles(
    dest: Path, *, tiles: Sequence[str] = GMTED_TILES, verbose: bool = True
) -> list[dict[str, object]]:
    """Download the GMTED tiles once and record a reproducible identity for each.

    An already-present tile is not re-downloaded; its checksum is recomputed either way so the
    recorded identity always describes the bytes actually used.  Acquisition time and bytes are
    returned per tile so they can be reported separately from computation
    (pilot_qc SPEC.md 8).
    """

    dest = qc_guard(dest, "elevation tile directory")
    dest.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, object]] = []
    for tile in tiles:
        url = tile_url(tile)
        path = dest / Path(url).name
        started = time.perf_counter()
        downloaded = False
        if not path.is_file():
            with urllib.request.urlopen(url, timeout=120) as response, open(path, "wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            downloaded = True
        seconds = time.perf_counter() - started
        records.append({
            "tile": tile,
            "url": url,
            "path": str(path),
            "bytes": int(path.stat().st_size),
            "sha256": _sha256(path),
            "downloaded_now": downloaded,
            "seconds": seconds,
        })
        if verbose:
            state = "downloaded" if downloaded else "already present"
            print(f"  {tile}: {state}, {path.stat().st_size / 1024**2:.1f} MB, {seconds:.1f} s",
                  flush=True)
    return records


def tile_set_digest(records: Sequence[Mapping[str, object]]) -> str:
    """One short digest over the whole tile set, for the elevation identity."""

    digest = hashlib.sha256()
    for record in sorted(records, key=lambda r: str(r["tile"])):
        digest.update(f"{record['tile']}:{record['sha256']}".encode())
    return digest.hexdigest()[:16]


def elevation_convention_name(records: Sequence[Mapping[str, object]]) -> str:
    """The declared elevation identity that enters the method signature and the cache key."""

    return f"elev-gmted2010-mea300-cellmean-{tile_set_digest(records)}"


def _coord_edges(centres: Sequence[float], *, step: float | None = None) -> np.ndarray:
    """Cell edges for a uniformly spaced coordinate of cell centres.

    ``step`` must be supplied when only one centre is given, because a single centre carries no
    spacing to infer.  It is never guessed.
    """

    values = np.asarray(centres, dtype=float)
    if step is None:
        if values.size < 2:
            raise ValueError("need at least two cell centres to infer the spacing, or an "
                             "explicit step")
        step = float(np.median(np.diff(values)))
    step = float(step)
    return np.concatenate([values - step / 2.0, [values[-1] + step / 2.0]])


def cell_elevation_field(
    lat: Sequence[float],
    lon: Sequence[float],
    tile_paths: Sequence[Path],
    *,
    cell_size_deg: float | None = None,
) -> tuple[np.ndarray, dict[str, object]]:
    """Area-average GMTED elevation onto the climate cells of one grid subset.

    Each GMTED pixel is assigned to the climate cell containing its centre and averaged with a
    ``cos(latitude)`` weight, which makes the result an area mean on the sphere.  Nodata pixels
    are excluded from the mean, never counted as zero.  A cell with no valid pixel stays NaN;
    the caller decides whether that is a failure or a flagged fallback, which keeps the
    substitution out of this function (pilot_qc SPEC.md 3.3).

    Negative elevations are **retained**: GMTED carries genuine below-geoid land, and clipping
    would be a silent edit.
    """

    import rasterio
    from rasterio.windows import Window, from_bounds

    lat_edges = _coord_edges(lat, step=cell_size_deg)
    lon_edges = _coord_edges(lon, step=cell_size_deg)
    n_lat, n_lon = len(lat), len(lon)
    sum_wz = np.zeros(n_lat * n_lon, dtype=float)
    sum_w = np.zeros(n_lat * n_lon, dtype=float)
    pixels_used = 0
    pixels_nodata = 0

    for path in tile_paths:
        with rasterio.open(path) as src:
            left, bottom, right, top = src.bounds
            x0 = max(float(lon_edges[0]), left)
            x1 = min(float(lon_edges[-1]), right)
            y0 = max(float(lat_edges[0]), bottom)
            y1 = min(float(lat_edges[-1]), top)
            if x1 <= x0 or y1 <= y0:
                continue
            window = from_bounds(x0, y0, x1, y1, src.transform).round_offsets().round_lengths()
            window = Window(max(0, int(window.col_off)), max(0, int(window.row_off)),
                            int(window.width), int(window.height))
            if window.width <= 0 or window.height <= 0:
                continue
            data = src.read(1, window=window).astype(float)
            transform = src.window_transform(window)
            cols = np.arange(window.width) + 0.5
            rows = np.arange(window.height) + 0.5
            xs = transform.c + transform.a * cols
            ys = transform.f + transform.e * rows
            nodata = src.nodata if src.nodata is not None else GMTED_NODATA
            bad = ~np.isfinite(data) | (data == nodata)
            pixels_nodata += int(bad.sum())

            iy = np.searchsorted(lat_edges, ys, side="right") - 1
            ix = np.searchsorted(lon_edges, xs, side="right") - 1
            row_ok = (iy >= 0) & (iy < n_lat)
            col_ok = (ix >= 0) & (ix < n_lon)
            if not row_ok.any() or not col_ok.any():
                continue
            weight_rows = np.cos(np.deg2rad(ys))
            keep = (~bad) & row_ok[:, None] & col_ok[None, :]
            if not keep.any():
                continue
            flat_index = (iy[:, None] * n_lon + ix[None, :])
            weights = np.broadcast_to(weight_rows[:, None], data.shape)
            idx = flat_index[keep]
            w = weights[keep]
            z = data[keep]
            sum_w += np.bincount(idx, weights=w, minlength=sum_w.size)
            sum_wz += np.bincount(idx, weights=w * z, minlength=sum_wz.size)
            pixels_used += int(keep.sum())

    with np.errstate(invalid="ignore", divide="ignore"):
        field = np.where(sum_w > 0, sum_wz / np.where(sum_w > 0, sum_w, 1.0), np.nan)
    field = field.reshape(n_lat, n_lon)
    finite = np.isfinite(field)
    diagnostics = {
        "pixels_used": pixels_used,
        "pixels_nodata": pixels_nodata,
        "cells_total": int(field.size),
        "cells_finite": int(finite.sum()),
        "cells_missing": int((~finite).sum()),
        "elevation_min_m": float(np.nanmin(field)) if finite.any() else np.nan,
        "elevation_median_m": float(np.nanmedian(field)) if finite.any() else np.nan,
        "elevation_max_m": float(np.nanmax(field)) if finite.any() else np.nan,
        "cells_negative": int(np.sum(finite & (field < 0.0))),
        "units": "m",
        "vertical_reference": GMTED_VERTICAL_REFERENCE,
        "horizontal_reference": GMTED_HORIZONTAL_REFERENCE,
        "sampling": GMTED_SAMPLING,
    }
    return field, diagnostics


def save_elevation(path: Path, field: np.ndarray, convention: str,
                   meta: Mapping[str, object]) -> Path:
    """Persist one grid subset's elevation field with the convention name it must be used under."""

    path = qc_guard(path, "elevation field")
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, elevation_m=np.asarray(field, dtype=float),
             convention=np.array(convention), meta=np.array(json.dumps(dict(meta), default=str)))
    return path


# ==========================================================================
# Input-quality investigation (pilot_qc SPEC.md 2)
# ==========================================================================


def rh_source_record(path: Path) -> dict[str, object]:
    """Read-only facts about one ``hurs`` file: identity, decoding, fill and extent.

    Both the undecoded and the decoded array are inspected, because the question the
    specification asks first is whether decoding could be the cause.
    """

    with xr.open_dataset(path, decode_cf=False) as raw:
        attrs = dict(raw["hurs"].attrs)
        scaled = ("scale_factor" in attrs) or ("add_offset" in attrs)
        raw_dtype = str(raw["hurs"].dtype)
    with xr.open_dataset(path) as ds:
        arr = np.asarray(ds["hurs"].values, dtype=float)
    finite = np.isfinite(arr)
    over = finite & (arr > P.RH_PHYSICAL_MAX_PCT)
    return {
        "path": str(path),
        "standard_name": attrs.get("standard_name"),
        "units": attrs.get("units"),
        "cell_methods": attrs.get("cell_methods"),
        "fill_value": str(attrs.get("_FillValue")),
        "missing_value": str(attrs.get("missing_value")),
        "has_scale_or_offset": bool(scaled),
        "valid_range_published": bool(
            {"valid_range", "valid_min", "valid_max"} & set(attrs)),
        "raw_dtype": raw_dtype,
        "finite_cell_days": int(finite.sum()),
        "nonfinite_cell_days": int((~finite).sum()),
        "observed_min_pct": float(arr[finite].min()) if finite.any() else np.nan,
        "observed_max_pct": float(arr[finite].max()) if finite.any() else np.nan,
        "cell_days_above_100": int(over.sum()),
        "cell_days_above_101": int(np.sum(finite & (arr > 101.0))),
        "cell_days_above_102": int(np.sum(finite & (arr > 102.0))),
        "cell_days_above_105": int(np.sum(finite & (arr > 105.0))),
        "cell_days_below_0": int(np.sum(finite & (arr < 0.0))),
        "fraction_of_finite_above_100": float(over.sum() / finite.sum()) if finite.any() else np.nan,
        "max_excess_pp": float((arr[over] - P.RH_PHYSICAL_MAX_PCT).max()) if over.any() else 0.0,
    }


def temperature_order_record(
    *, source_root: Path, wbgt_root: Path, model: str, year: int, lat: float, lon: float
) -> dict[str, object]:
    """Read-only facts about one cell's ``tasmin``/``tasmax`` ordering and its provenance.

    This answers whether an inversion is an ingestion or alignment error on our side, or a
    defect in the delivered values.  Every variable's grid, calendar, units and member are
    compared on the same cell (pilot_qc SPEC.md 2.3).
    """

    def path(var: str) -> Path:
        return P.variable_path(var, source_root=source_root, wbgt_root=wbgt_root,
                               model=model, scenario=P.PILOT_SCENARIO, year=year)

    identities: dict[str, dict[str, object]] = {}
    series: dict[str, np.ndarray] = {}
    times: dict[str, pd.DatetimeIndex] = {}
    for var in m1.REQUIRED_NEX_VARIABLES:
        with xr.open_dataset(path(var)) as ds:
            cell = ds[var].sel(lat=lat, lon=lon, method="nearest")
            identities[var] = {
                "units": ds[var].attrs.get("units"),
                "calendar": str(ds.time.encoding.get("calendar")),
                "lat_actual": float(cell.lat),
                "lon_actual": float(cell.lon),
                "variant_label": ds.attrs.get("variant_label"),
                "source_id": ds.attrs.get("cmip6_source_id") or ds.attrs.get("source_id"),
                "scenario": ds.attrs.get("scenario"),
            }
            series[var] = np.asarray(cell.values, dtype=float)
            times[var] = pd.DatetimeIndex(ds.time.values).normalize()

    reference = times[m1.REQUIRED_NEX_VARIABLES[0]]
    aligned = all(times[v].equals(reference) for v in m1.REQUIRED_NEX_VARIABLES)
    same_cell = len({(round(i["lat_actual"], 6), round(i["lon_actual"], 6))
                     for i in identities.values()}) == 1
    same_member = len({i["variant_label"] for i in identities.values()}) == 1
    same_model = len({i["source_id"] for i in identities.values()}) == 1
    dtr = (series["tasmax"] - series["tasmin"])
    worst = int(np.nanargmin(dtr))
    return {
        "lat": float(lat),
        "lon": float(lon),
        "model": model,
        "year": int(year),
        "dates_aligned_across_variables": bool(aligned),
        "same_cell_across_variables": bool(same_cell),
        "same_member_across_variables": bool(same_member),
        "same_model_across_variables": bool(same_model),
        "units_tasmin": identities["tasmin"]["units"],
        "units_tasmax": identities["tasmax"]["units"],
        "worst_date": str(reference[worst].date()),
        "tasmin_c": float(series["tasmin"][worst] - 273.15),
        "tasmax_c": float(series["tasmax"][worst] - 273.15),
        "tas_c": float(series["tas"][worst] - 273.15),
        "tas_between_min_and_max": bool(
            min(series["tasmin"][worst], series["tasmax"][worst])
            <= series["tas"][worst]
            <= max(series["tasmin"][worst], series["tasmax"][worst])),
        "inverted_days_at_cell": int(np.nansum(dtr < 0.0)),
        "worst_inversion_c": float(np.nanmin(dtr)),
        "classification": (
            "unresolved source defect: dates, coordinates, units, model and member all agree, "
            "so the inversion is present in the delivered values"
            if aligned and same_cell and same_member and same_model
            else "processing defect: alignment or provenance mismatch detected"
        ),
    }


# ==========================================================================
# Coverage classification (pilot_qc SPEC.md 6)
# ==========================================================================


def classify_coverage(valid_fraction_of_represented: float, n_cells_valid: int) -> str:
    """Assign the declared coverage class from the declared screen and its denominator."""

    if n_cells_valid <= 0 or not np.isfinite(valid_fraction_of_represented):
        return "no_valid_coverage"
    if valid_fraction_of_represented >= COVERAGE_SCREEN:
        return "meets_screen"
    return "partial_coverage"


def coverage_table(
    gdf,
    weights: pd.DataFrame,
    grid_ds: xr.Dataset,
    *,
    level: str,
    state: str,
    model: str,
    year: int,
    run: str,
) -> pd.DataFrame:
    """Full polygon area, represented area, valid area and the three fractions, per unit.

    All three areas are computed in EPSG:6933, the same equal-area projection the production
    weight builder uses, so the fractions are mutually consistent (pilot_qc SPEC.md 6).
    """

    from india_resilience_tool.compute.gridfirst_spatial import _unit_key

    units = gdf if gdf.crs is not None else gdf.set_crs("EPSG:4326")
    units = units.to_crs(ANALYSIS_CRS)
    full_area = {}
    for _, row in units.iterrows():
        key = _unit_key(row, level)
        if key:
            full_area[key] = full_area.get(key, 0.0) + float(row.geometry.area)

    finite = np.isfinite(grid_ds["wbgt_annual_mean_c"].transpose("lat", "lon").values)
    flat_valid = np.asarray(finite).reshape(-1)
    rows: list[dict[str, object]] = []
    grouped = (weights.groupby("unit_key", sort=False) if not weights.empty
               else [])
    seen: set[str] = set()
    for unit_key, group in grouped:
        key = str(unit_key)
        seen.add(key)
        idx = group["cell_index"].to_numpy(dtype=int)
        area = group["area_m2"].to_numpy(dtype=float)
        good = flat_valid[idx]
        represented = float(area.sum())
        valid = float(area[good].sum())
        polygon = float(full_area.get(key, np.nan))
        frac_of_rep = valid / represented if represented > 0 else np.nan
        rows.append({
            "state_name": state, "level": level, "unit_key": key,
            "model": model, "year": int(year), "run": run,
            "full_polygon_area_m2": polygon,
            "represented_area_m2": represented,
            "valid_area_m2": valid,
            "represented_fraction": represented / polygon if polygon > 0 else np.nan,
            "valid_fraction_of_represented": frac_of_rep,
            "valid_fraction_of_full": valid / polygon if polygon > 0 else np.nan,
            "n_cells_total": int(len(idx)),
            "n_cells_valid": int(good.sum()),
            "n_cells_invalid": int((~good).sum()),
            "coverage_class": classify_coverage(frac_of_rep, int(good.sum())),
            "coverage_screen": COVERAGE_SCREEN,
            "coverage_screen_denominator": COVERAGE_SCREEN_DENOMINATOR,
        })
    for key, polygon in full_area.items():
        if key in seen:
            continue
        rows.append({
            "state_name": state, "level": level, "unit_key": key,
            "model": model, "year": int(year), "run": run,
            "full_polygon_area_m2": float(polygon),
            "represented_area_m2": 0.0, "valid_area_m2": 0.0,
            "represented_fraction": 0.0,
            "valid_fraction_of_represented": np.nan,
            "valid_fraction_of_full": 0.0,
            "n_cells_total": 0, "n_cells_valid": 0, "n_cells_invalid": 0,
            "coverage_class": "no_valid_coverage",
            "coverage_screen": COVERAGE_SCREEN,
            "coverage_screen_denominator": COVERAGE_SCREEN_DENOMINATOR,
        })
    return pd.DataFrame(rows)


# ==========================================================================
# Independent geometry checks (pilot_qc SPEC.md 7)
# ==========================================================================


def geometry_checks(districts, blocks, *, state: str) -> pd.DataFrame:
    """Test the district/block relationship directly, in EPSG:6933, against declared tolerances.

    This is the check that milestone 4 lacked.  Numerical aggregation agreement cannot answer
    it: aggregation equality verifies consistency under the tested support, not that the child
    polygons tile the parent.  Production boundaries are never repaired here, only measured.
    """

    from shapely.ops import unary_union

    d = (districts if districts.crs is not None else districts.set_crs("EPSG:4326"))
    b = (blocks if blocks.crs is not None else blocks.set_crs("EPSG:4326"))
    d = d.to_crs(ANALYSIS_CRS)
    b = b.to_crs(ANALYSIS_CRS)

    rows: list[dict[str, object]] = []

    def record(check: str, unit: str, ok: bool, area_m2: float = 0.0,
               fraction: float = 0.0, detail: str = "") -> None:
        rows.append({"state_name": state, "check": check, "unit": unit, "ok": bool(ok),
                     "discrepancy_area_m2": float(area_m2),
                     "discrepancy_fraction": float(fraction), "detail": detail})

    dup_d = int(d["district_name"].duplicated().sum())
    record("district_names_unique", state, dup_d == 0, detail=f"{dup_d} duplicate district names")
    dup_b = int(b.groupby("district_name")["block_name"].apply(
        lambda s: int(s.duplicated().sum())).sum())
    record("block_names_unique_within_district", state, dup_b == 0,
           detail=f"{dup_b} duplicate block names")

    orphans = sorted(set(b["district_name"]) - set(d["district_name"]))
    record("every_block_parent_present", state, not orphans,
           detail=f"{len(orphans)} orphan parents: {', '.join(orphans[:5])}")

    invalid_d = int((~d.geometry.is_valid | d.geometry.is_empty).sum())
    invalid_b = int((~b.geometry.is_valid | b.geometry.is_empty).sum())
    record("district_geometries_valid", state, invalid_d == 0, detail=f"{invalid_d} invalid")
    record("block_geometries_valid", state, invalid_b == 0, detail=f"{invalid_b} invalid")

    dup_geom_b = int(b.geometry.apply(lambda g: g.wkb).duplicated().sum())
    record("block_geometries_distinct", state, dup_geom_b == 0,
           detail=f"{dup_geom_b} duplicate geometries")

    for name, parent in zip(d["district_name"], d.geometry):
        children = b.loc[b["district_name"].eq(name), "geometry"]
        parent_area = float(parent.area)
        if children.empty:
            record("children_present", str(name), False, detail="no blocks for this district")
            continue
        union = unary_union(list(children))
        try:
            gap = float(parent.difference(union).area)
            outside = float(union.difference(parent).area)
        except Exception as exc:  # pragma: no cover - degenerate geometry
            record("set_operations", str(name), False, detail=f"{type(exc).__name__}: {exc}")
            continue
        child_area = float(union.area)
        gap_fraction = gap / parent_area if parent_area > 0 else np.nan
        out_fraction = outside / child_area if child_area > 0 else np.nan
        record("gap_blocks_cover_district", str(name),
               gap <= GEOMETRY_SLIVER_FLOOR_M2 or gap_fraction <= GEOMETRY_GAP_TOLERANCE,
               gap, gap_fraction, f"tolerance {GEOMETRY_GAP_TOLERANCE}")
        record("child_inside_parent", str(name),
               outside <= GEOMETRY_SLIVER_FLOOR_M2 or out_fraction <= GEOMETRY_OUTSIDE_TOLERANCE,
               outside, out_fraction, f"tolerance {GEOMETRY_OUTSIDE_TOLERANCE}")

        geoms = list(children)
        names = list(b.loc[b["district_name"].eq(name), "block_name"])
        worst_area, worst_fraction, worst_pair = 0.0, 0.0, ""
        for p in range(len(geoms)):
            for q in range(p + 1, len(geoms)):
                if not geoms[p].intersects(geoms[q]):
                    continue
                inter = float(geoms[p].intersection(geoms[q]).area)
                if inter <= 0:
                    continue
                smaller = min(float(geoms[p].area), float(geoms[q].area))
                fraction = inter / smaller if smaller > 0 else np.nan
                if inter > worst_area:
                    worst_area, worst_fraction = inter, fraction
                    worst_pair = f"{names[p]} / {names[q]}"
        record("blocks_do_not_overlap", str(name),
               worst_area <= GEOMETRY_SLIVER_FLOOR_M2
               or worst_fraction <= GEOMETRY_OVERLAP_TOLERANCE,
               worst_area, worst_fraction,
               f"worst pair {worst_pair}" if worst_pair else "no overlapping pair")
    return pd.DataFrame(rows)


# ==========================================================================
# Run comparison (pilot_qc SPEC.md 4)
# ==========================================================================


def cell_comparison(
    left: xr.Dataset, right: xr.Dataset, *, state: str, model: str,
    left_run: str, right_run: str
) -> list[dict[str, object]]:
    """Compare two runs' per-cell annual statistics, on each support and on the common support.

    Validity change and value change are reported as separate quantities, because a difference
    in an administrative value can come from either and the two must not be conflated.
    """

    rows: list[dict[str, object]] = []
    fields = ["wbgt_annual_mean_c"] + [f"days_ge_{int(t)}" for t in P.THRESHOLDS_C]
    a_valid = np.isfinite(left["wbgt_annual_mean_c"].values)
    b_valid = np.isfinite(right["wbgt_annual_mean_c"].values)
    common = a_valid & b_valid
    for field in fields:
        a = left[field].values
        b = right[field].values
        delta = (b - a)[common]
        rows.append({
            "state_name": state, "model": model,
            "left_run": left_run, "right_run": right_run, "field": field,
            "n_cells_valid_left": int(a_valid.sum()),
            "n_cells_valid_right": int(b_valid.sum()),
            "n_cells_common_valid": int(common.sum()),
            "n_cells_gained": int((b_valid & ~a_valid).sum()),
            "n_cells_lost": int((a_valid & ~b_valid).sum()),
            "mean_delta_common": float(np.nanmean(delta)) if delta.size else np.nan,
            "min_delta_common": float(np.nanmin(delta)) if delta.size else np.nan,
            "max_delta_common": float(np.nanmax(delta)) if delta.size else np.nan,
            "max_abs_delta_common": float(np.nanmax(np.abs(delta))) if delta.size else np.nan,
            "n_cells_changed_common": int(np.sum(np.abs(delta) > 0)) if delta.size else 0,
        })
    return rows


def unit_comparison(
    left: pd.DataFrame, right: pd.DataFrame, *, left_run: str, right_run: str,
    coverage_left: pd.DataFrame, coverage_right: pd.DataFrame
) -> pd.DataFrame:
    """Per-unit value delta alongside the valid-area delta, so neither is read as the other.

    A unit whose value moved *and* whose contributing area moved is marked
    ``support_changed``: its change must not be attributed wholly to the treatment under test.
    """

    keys = ["state_name", "level", "unit_key"]
    fields = ["wbgt_annual_mean_c"] + [f"days_ge_{int(t)}" for t in P.THRESHOLDS_C]
    a = left.set_index(keys)
    b = right.set_index(keys)
    cov_a = coverage_left.set_index(keys)
    cov_b = coverage_right.set_index(keys)
    common = a.index.intersection(b.index)
    out = pd.DataFrame(index=common).reset_index()
    out["left_run"], out["right_run"] = left_run, right_run
    for field in fields:
        out[f"{field}_left"] = a.loc[common, field].to_numpy()
        out[f"{field}_right"] = b.loc[common, field].to_numpy()
        out[f"{field}_delta"] = out[f"{field}_right"] - out[f"{field}_left"]
    for frame, suffix in ((cov_a, "left"), (cov_b, "right")):
        aligned = frame.reindex(common)
        out[f"valid_area_m2_{suffix}"] = aligned["valid_area_m2"].to_numpy()
        out[f"valid_fraction_of_represented_{suffix}"] = (
            aligned["valid_fraction_of_represented"].to_numpy())
        out[f"coverage_class_{suffix}"] = aligned["coverage_class"].to_numpy()
    out["valid_area_delta_m2"] = out["valid_area_m2_right"] - out["valid_area_m2_left"]
    out["support_changed"] = np.abs(out["valid_area_delta_m2"]) > 1.0
    out["attribution"] = np.where(
        out["support_changed"],
        "value and support both changed; not attributable to the treatment alone",
        "support unchanged; value change attributable to the treatment")
    return out


def reproduction_check(
    new_units: pd.DataFrame | Path, frozen_path: Path, *, level: str,
    run: str = "baseline", model: str | None = None,
) -> dict[str, object]:
    """Acceptance criterion A1: the strict, sea-level run must reproduce milestone 4's table.

    Milestone 4's artifacts are read **read-only** and compared numerically.  They are not
    reused through the cache: the signature bumped to ``v2``, so a cache hit is impossible by
    construction, which is exactly why this comparison has to be explicit.

    ``new_units`` may be the in-memory table or the path of the table this run published.  The
    frozen side is necessarily a published CSV, so passing the published path puts **the same
    serialisation on both sides**, which is what A1 asks about: whether the two published tables
    agree.  Comparing an in-memory float against a published decimal instead compares two
    different representations of the same number, and disagrees in the last bit on a handful of
    area-weighted values (measured: 3.6e-15 degC on the annual mean, 5.7e-14 on a fractional
    exceedance count, 1.1e-16 on an area fraction) without any computation having changed.  The
    declared tolerance is **not** relaxed to absorb that; the representation is made to match.
    """

    if not frozen_path.is_file():
        return {"check": f"reproduces_frozen_{level}", "ok": False,
                "comparison_basis": "none", "detail": f"{frozen_path} missing"}
    if isinstance(new_units, (str, Path)):
        path = Path(new_units)
        if not path.is_file():
            return {"check": f"reproduces_frozen_{level}", "ok": False,
                    "comparison_basis": "none", "detail": f"{path} missing"}
        new_units = pd.read_csv(path)
        basis = "published CSV on both sides"
    else:
        basis = "in-memory values against the published frozen CSV"
    frozen = pd.read_csv(frozen_path)
    frozen = frozen[frozen.candidate.eq(P.PILOT_CANDIDATE)]
    keys = ["state_name", "unit_key"]
    a = frozen.set_index(keys).sort_index()
    selected = new_units[new_units.level.eq(level)]
    # The published table carries every run and model, so the baseline slice must be taken
    # explicitly; without it the index would carry one key per run and nothing would align.
    if "run" in selected.columns:
        selected = selected[selected["run"].eq(run)]
    if model is not None and "model" in selected.columns:
        selected = selected[selected["model"].eq(model)]
    b = selected.set_index(keys).sort_index()
    common = a.index.intersection(b.index)
    missing = len(a.index.difference(b.index)) + len(b.index.difference(a.index))
    mean_a = a.loc[common, "wbgt_annual_mean_c"].to_numpy(dtype=float)
    mean_b = b.loc[common, "wbgt_annual_mean_c"].to_numpy(dtype=float)
    both = np.isfinite(mean_a) & np.isfinite(mean_b)
    nan_mismatch = int((np.isfinite(mean_a) != np.isfinite(mean_b)).sum())
    worst = float(np.max(np.abs(mean_a[both] - mean_b[both]))) if both.any() else 0.0
    count_mismatch = 0
    worst_count = 0.0
    for field in [f"days_ge_{int(t)}" for t in P.THRESHOLDS_C]:
        ca = a.loc[common, field].to_numpy(dtype=float)
        cb = b.loc[common, field].to_numpy(dtype=float)
        ok = (np.isfinite(ca) == np.isfinite(cb))
        both_c = np.isfinite(ca) & np.isfinite(cb)
        count_mismatch += int((~ok).sum()) + int((ca[both_c] != cb[both_c]).sum())
        if both_c.any():
            worst_count = max(worst_count, float(np.max(np.abs(ca[both_c] - cb[both_c]))))
    area_a = a.loc[common, "valid_area_fraction"].to_numpy(dtype=float)
    area_b = b.loc[common, "valid_area_fraction"].to_numpy(dtype=float)
    area_mismatch = int(np.sum(np.abs(area_a - area_b) > 0))
    ok = (missing == 0 and nan_mismatch == 0 and worst <= REPRODUCTION_TOL_C
          and count_mismatch == 0 and area_mismatch == 0)
    return {
        "check": f"reproduces_frozen_{level}", "ok": bool(ok), "n_units": int(len(common)),
        "unmatched_units": int(missing), "nan_pattern_mismatches": nan_mismatch,
        "max_abs_mean_diff_c": worst, "count_mismatches": count_mismatch,
        "max_abs_count_diff_days": worst_count,
        "valid_area_fraction_mismatches": area_mismatch,
        "max_abs_area_fraction_diff": float(np.max(np.abs(area_a - area_b)))
        if len(common) else 0.0,
        "tolerance_c": REPRODUCTION_TOL_C,
        "comparison_basis": basis,
        "detail": f"frozen table {frozen_path}",
    }


# ==========================================================================
# Runner
# ==========================================================================


def _write_csv(frame: pd.DataFrame, path: Path, *, verbose: bool = True) -> None:
    frame.to_csv(path, index=False)
    if verbose:
        print(f"  wrote {path} ({len(frame)} rows)", flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    """Acquire elevation, investigate the inputs, run the four comparisons and report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=P.DEFAULT_SOURCE_ROOT)
    parser.add_argument("--wbgt-root", type=Path, default=P.DEFAULT_WBGT_ROOT)
    parser.add_argument("--boundary-root", type=Path, default=P.DEFAULT_BOUNDARY_ROOT)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--states", nargs="+", default=list(P.PILOT_STATES))
    parser.add_argument("--levels", nargs="+", default=list(P.PILOT_LEVELS))
    parser.add_argument("--model", default=P.PILOT_MODEL)
    parser.add_argument("--year", type=int, default=P.PILOT_YEAR)
    parser.add_argument("--runs", nargs="+", default=[r[0] for r in RUN_MATRIX])
    parser.add_argument("--sample-cells", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--allow-sea-level-fallback", action="store_true")
    parser.add_argument("--skip-second-model", action="store_true")
    parser.add_argument("--skip-geometry", action="store_true")
    parser.add_argument("--acquire-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    verbose = not args.quiet

    out_dir = qc_guard(args.out_dir, "--out-dir")
    work_dir = qc_guard(args.work_dir, "--work-dir")
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print("Outdoor-WBGT pilot follow-up: input quality, elevation, coverage, geometry")
        print("  spec       docs/diagnostics/wbgt_outdoor_pilot_qc/SPEC.md")
        print(f"  scope      {', '.join(args.states)} | {args.model} | {args.year}")
        print(f"  runs       {', '.join(args.runs)}")
        print(f"  out-dir    {out_dir}")
        print(f"  work-dir   {work_dir}")
        print(f"  workers    {args.workers}")
        print("  DIAGNOSTIC-ONLY; no output enters production.\n")

    # ---------------- Stage 1: elevation acquisition, measured separately ----------------
    acquire_started = time.perf_counter()
    tiles = acquire_elevation_tiles(work_dir / "elevation", verbose=verbose)
    acquire_seconds = time.perf_counter() - acquire_started
    acquire_bytes = sum(int(t["bytes"]) for t in tiles)
    convention = elevation_convention_name(tiles)
    if acquire_bytes > ACQUIRE_BUDGET_BYTES:
        raise SystemExit(f"Acquisition of {acquire_bytes / 1024**2:.0f} MB exceeds the declared "
                         f"{ACQUIRE_BUDGET_BYTES / 1024**2:.0f} MB (pilot_qc SPEC.md 8).")
    _write_csv(pd.DataFrame(tiles), out_dir / "elevation_source.csv", verbose=verbose)
    if verbose:
        print(f"  elevation identity {convention}")
        print(f"  acquisition {acquire_seconds:.1f} s, {acquire_bytes / 1024**2:.1f} MB "
              f"(measured separately from computation)\n")
    if args.acquire_only:
        return 0

    # ---------------- Stage 2: input-quality investigation ----------------
    rh_rows = [
        {"model": model, **rh_source_record(P.variable_path(
            "hurs", source_root=args.source_root, wbgt_root=args.wbgt_root,
            model=model, scenario=P.PILOT_SCENARIO, year=args.year))}
        for model in dict.fromkeys([args.model, P.PILOT_SECOND_MODEL])
    ]
    _write_csv(pd.DataFrame(rh_rows), out_dir / "input_quality_hurs.csv", verbose=verbose)
    temp_row = temperature_order_record(
        source_root=args.source_root, wbgt_root=args.wbgt_root, model=args.model,
        year=args.year, lat=12.375, lon=74.875)
    _write_csv(pd.DataFrame([temp_row]), out_dir / "input_quality_temperature.csv",
               verbose=verbose)
    if verbose:
        print(f"  temperature-order classification: {temp_row['classification']}\n")

    if args.dry_run:
        (out_dir / "run_manifest_dry_run.json").write_text(json.dumps({
            "stage": "dry-run", "elevation_identity": convention,
            "acquire_seconds": acquire_seconds, "acquire_bytes": acquire_bytes,
            "runs": list(args.runs), "states": list(args.states),
            "out_dir": str(out_dir), "work_dir": str(work_dir),
        }, indent=2), encoding="utf-8")
        print("Dry run complete; nothing computed.")
        return 0

    # ---------------- Stage 3: the comparison runs ----------------
    wanted = {name: (policy, use_elev) for name, policy, use_elev in RUN_MATRIX
              if name in set(args.runs)}
    jobs: list[tuple[str, Sequence[str], Sequence[str]]] = [
        (args.model, args.states, list(wanted))]
    if not args.skip_second_model:
        jobs.append((P.PILOT_SECOND_MODEL, list(P.PILOT_SECOND_MODEL_STATES),
                     [r for r in SECOND_MODEL_RUNS if r in wanted]))

    parity_context: tuple | None = None
    units_all: list[pd.DataFrame] = []
    coverage_all: list[pd.DataFrame] = []
    admin_checks: list[dict[str, object]] = []
    elevation_rows: list[dict[str, object]] = []
    cell_rows: list[dict[str, object]] = []
    timings: list[dict[str, object]] = []
    geometry_all: list[pd.DataFrame] = []
    grids: dict[tuple[str, str, str], xr.Dataset] = {}
    rh_effect: list[dict[str, object]] = []

    compute_started = time.perf_counter()
    with P._PeakMemory() as memory:
        for model, states, runs in jobs:
            for state in states:
                started = time.perf_counter()
                frames, hashes, index_range, grid, weights, cells = P.state_setup(
                    state, boundary_root=args.boundary_root, source_root=args.source_root,
                    wbgt_root=args.wbgt_root, model=model, year=args.year)
                setup_seconds = time.perf_counter() - started
                if args.sample_cells:
                    cells = cells[: args.sample_cells]
                if not args.skip_geometry and model == args.model:
                    geometry_all.append(geometry_checks(frames["district"], frames["block"],
                                                        state=state))

                elevation_path = work_dir / "elevation" / (
                    f"cells_{state.replace(' ', '_')}_{grid.grid_id[:12]}.npz")
                if elevation_path.is_file():
                    with np.load(elevation_path, allow_pickle=False) as archive:
                        elevation_field = np.asarray(archive["elevation_m"], dtype=float)
                        elevation_meta = json.loads(str(archive["meta"].item()))
                else:
                    elevation_field, elevation_meta = cell_elevation_field(
                        grid.lat, grid.lon, [Path(t["path"]) for t in tiles])
                    save_elevation(elevation_path, elevation_field, convention, elevation_meta)
                elevation_rows.append({"state_name": state, "model": model,
                                       "grid_id": grid.grid_id,
                                       "convention": convention, **elevation_meta})

                started = time.perf_counter()
                raw_cube, days = P.load_daily_cube(
                    source_root=args.source_root, wbgt_root=args.wbgt_root, model=model,
                    scenario=P.PILOT_SCENARIO, year=args.year, index_range=index_range)
                load_seconds = time.perf_counter() - started
                if verbose:
                    print(f"{state} | {model}: grid {grid.shape} | cells {len(cells)} | "
                          f"elevation {elevation_meta['elevation_min_m']:.0f}-"
                          f"{elevation_meta['elevation_max_m']:.0f} m | setup {setup_seconds:.1f} s"
                          f" | load {load_seconds:.1f} s", flush=True)

                if model == args.model and state == args.states[0]:
                    parity_context = (grid, index_range, cells, elevation_field)
                input_paths = [P.variable_path(v, source_root=args.source_root,
                                               wbgt_root=args.wbgt_root, model=model,
                                               scenario=P.PILOT_SCENARIO, year=y)
                               for y in P.required_years(args.year)
                               for v in m1.REQUIRED_NEX_VARIABLES]

                for run in runs:
                    policy, use_elev = wanted[run]
                    elevation_identity = convention if use_elev else P.ELEVATION_CONVENTION
                    cube, rh_flags, rh_record = P.apply_rh_policy(raw_cube, rh_policy=policy)
                    # The adjustment magnitude per cell-day, derived from the raw values the
                    # policy did not modify, so the correction can be reported per cell-year.
                    rh_excess = np.where(
                        rh_flags,
                        np.asarray(raw_cube["hurs"], dtype=float)
                        - np.asarray(cube["hurs"], dtype=float), 0.0)
                    valid_mask, invalid_counts = P.input_validity_mask(cube)
                    rh_effect.append({"state_name": state, "model": model, "run": run,
                                      **invalid_counts, **rh_record})

                    sidecar = P.cell_cache_sidecar(
                        state=state, model=model, year=args.year, candidate=P.PILOT_CANDIDATE,
                        grid_id=grid.grid_id, input_paths=input_paths,
                        boundary_hash=hashes["district"], rh_policy=policy,
                        elevation_identity=elevation_identity)
                    cache_path = work_dir / (
                        f"cells_{state.replace(' ', '_')}_{model}_{args.year}_{run}.nc")
                    grid_ds = P.read_cell_cache(cache_path, expected=sidecar)
                    if grid_ds is None:
                        started = time.perf_counter()
                        grid_ds = P.compute_cell_grid(
                            cube, days, lat=grid.lat, lon=grid.lon, cell_indices=cells,
                            year=args.year, candidate=P.PILOT_CANDIDATE, valid_mask=valid_mask,
                            elevation_m=(elevation_field if use_elev else P.ELEVATION_M),
                            elevation_convention=elevation_identity, rh_policy=policy,
                            rh_flags=rh_flags, rh_excess_pct=rh_excess,
                            allow_sea_level_fallback=args.allow_sea_level_fallback)
                        compute_seconds = time.perf_counter() - started
                        P.write_cell_cache(grid_ds, cache_path, sidecar=sidecar)
                        timings.append({
                            "state": state, "model": model, "run": run, "cells": len(cells),
                            "setup_seconds": setup_seconds, "load_seconds": load_seconds,
                            "compute_seconds": compute_seconds,
                            "seconds_per_cell": compute_seconds / max(1, len(cells))})
                        if verbose:
                            print(f"  {run}: {len(cells)} cells in {compute_seconds:.1f} s "
                                  f"({compute_seconds / max(1, len(cells)):.2f} s/cell)",
                                  flush=True)
                    elif verbose:
                        print(f"  {run}: reused cached cell grid", flush=True)
                    grids[(model, state, run)] = grid_ds

                    for level in args.levels:
                        frame = P.aggregate_units(
                            grid_ds, weights[level], level=level, grid=grid, state=state,
                            model=model, year=args.year, candidate=P.PILOT_CANDIDATE,
                            rh_policy=policy, elevation_convention=elevation_identity)
                        frame["run"] = run
                        cov = coverage_table(frames[level], weights[level], grid_ds, level=level,
                                             state=state, model=model, year=args.year, run=run)
                        frame = frame.merge(
                            cov[["state_name", "level", "unit_key", "coverage_class",
                                 "represented_fraction", "valid_fraction_of_full"]],
                            on=["state_name", "level", "unit_key"], how="left")
                        units_all.append(frame)
                        coverage_all.append(cov)
                        for check in P.check_admin_correctness(frame, level=level):
                            admin_checks.append({"state": state, "model": model, "run": run,
                                                 **check})
                        admin_checks.append({
                            "state": state, "model": model, "run": run, "level": level,
                            **P.weighted_mean_within_cell_range(grid_ds, weights[level], frame)})

                for run in runs:
                    ds = grids[(model, state, run)]
                    mask = np.isfinite(ds["cell_elevation_m"].values)
                    cell_rows.append({
                        "state_name": state, "model": model, "run": run,
                        "cells_computed": int(mask.sum()),
                        "cells_complete_year": int(np.nansum(ds["complete_year"].values == 1.0)),
                        "cells_invalid_year": int(np.nansum(ds["complete_year"].values == 0.0)),
                        "rh_clipped_cell_days": float(np.nansum(ds["rh_clipped_days"].values)),
                        "cells_with_clipped_days": int(
                            np.nansum(ds["rh_clipped_days"].values > 0)),
                        "elevation_fallback_cells": int(
                            np.nansum(ds["elevation_fallback"].values == 1.0)),
                    })
    compute_seconds_total = time.perf_counter() - compute_started

    units = pd.concat(units_all, ignore_index=True)
    coverage = pd.concat(coverage_all, ignore_index=True)
    _write_csv(units[units.level.eq("district")], out_dir / "district_values.csv", verbose=verbose)
    _write_csv(units[units.level.eq("block")], out_dir / "block_values.csv", verbose=verbose)
    _write_csv(coverage, out_dir / "coverage_classification.csv", verbose=verbose)
    _write_csv(pd.DataFrame(admin_checks), out_dir / "admin_checks.csv", verbose=verbose)
    _write_csv(pd.DataFrame(elevation_rows), out_dir / "elevation_cells.csv", verbose=verbose)
    _write_csv(pd.DataFrame(rh_effect), out_dir / "input_validity_by_run.csv", verbose=verbose)
    _write_csv(pd.DataFrame(cell_rows), out_dir / "cell_summary_by_run.csv", verbose=verbose)
    # Per cell-year adjustment ledger: corrected-day count and maximum correction, with the
    # raw values preserved alongside, so a flagged correction is always auditable.
    adjustment_rows: list[dict[str, object]] = []
    for (model_key, state_key, run_key), ds in grids.items():
        clipped = ds["rh_clipped_days"].values
        lat_i, lon_i = np.where(np.nan_to_num(clipped) > 0)
        for i, j in zip(lat_i, lon_i):
            adjustment_rows.append({
                "state_name": state_key, "model": model_key, "run": run_key,
                "lat": float(ds.lat.values[i]), "lon": float(ds.lon.values[j]),
                "rh_clipped_days": float(clipped[i, j]),
                "rh_max_correction_pct": float(ds["rh_max_correction_pct"].values[i, j]),
                "cell_elevation_m": float(ds["cell_elevation_m"].values[i, j]),
                "complete_year": float(ds["complete_year"].values[i, j]),
                "input_invalid_days": float(ds["input_invalid_days"].values[i, j]),
            })
    if adjustment_rows:
        _write_csv(pd.DataFrame(adjustment_rows), out_dir / "rh_adjustments.csv", verbose=verbose)

    # Block-to-district reconciliation, retained from milestone 4 but described correctly:
    # numerical agreement verifies aggregation consistency under the tested support.  It does
    # NOT prove that blocks tile districts -- that is what geometry_checks.csv tests.
    rollups: list[pd.DataFrame] = []
    for (model_key, run_key), group in units.groupby(["model", "run"], sort=False):
        blocks = group[group.level.eq("block")]
        districts = group[group.level.eq("district")]
        if blocks.empty or districts.empty:
            continue
        rollup = P.block_to_district(blocks)
        merged = districts.merge(rollup, on=["state_name", "district_name"], how="outer")
        merged["difference_c"] = (merged["block_to_district_wbgt_annual_mean_c"]
                                  - merged["wbgt_annual_mean_c"])
        merged["model"], merged["run"] = model_key, run_key
        merged["verifies"] = "aggregation consistency under the tested support, NOT tiling"
        rollups.append(merged[["model", "run", "state_name", "district_name", "n_blocks",
                               "wbgt_annual_mean_c", "block_to_district_wbgt_annual_mean_c",
                               "difference_c", "valid_area_fraction", "coverage_class",
                               "verifies"]])
    if rollups:
        _write_csv(pd.concat(rollups, ignore_index=True),
                   out_dir / "district_vs_block_rollup.csv", verbose=verbose)

    # Ranking summary, restricted to units that meet the declared coverage screen.  A partial
    # unit keeps its diagnostic value in the value tables but is excluded here, because an
    # estimate formed over part of a polygon must not be ranked against whole ones.
    ranked = units[units.coverage_class.eq("meets_screen")
                   & units.wbgt_annual_mean_c.notna()].copy()
    ranking_rows: list[dict[str, object]] = []
    for (model_key, run_key, level), group in ranked.groupby(["model", "run", "level"],
                                                             sort=False):
        total = int(units[(units.model.eq(model_key)) & (units.run.eq(run_key))
                          & (units.level.eq(level))].shape[0])
        for field in ["wbgt_annual_mean_c"] + [f"days_ge_{int(t)}" for t in P.THRESHOLDS_C]:
            top = group.nlargest(3, field)
            ranking_rows.append({
                "model": model_key, "run": run_key, "level": level, "field": field,
                "quantity": (P.QUANTITY_NAME if field == "wbgt_annual_mean_c"
                             else P.EXCEEDANCE_NAME),
                "n_units_ranked": int(len(group)), "n_units_total": total,
                "n_units_excluded_partial_or_invalid": total - int(len(group)),
                "min": float(group[field].min()), "max": float(group[field].max()),
                "top_3": "; ".join(f"{k} {v:.2f}" for k, v in
                                   zip(top.unit_key, top[field])),
                "ranking_scope": "units meeting the declared coverage screen only",
            })
    if ranking_rows:
        _write_csv(pd.DataFrame(ranking_rows), out_dir / "ranking_summary.csv", verbose=verbose)

    if timings:
        _write_csv(pd.DataFrame(timings), out_dir / "timings.csv", verbose=verbose)
    if geometry_all:
        _write_csv(pd.concat(geometry_all, ignore_index=True), out_dir / "geometry_checks.csv",
                   verbose=verbose)

    # ---------------- Stage 4: acceptance and comparison ----------------
    acceptance: list[dict[str, object]] = []
    baseline_units = units[(units.run.eq("baseline")) & (units.model.eq(args.model))]
    frozen_dir = Path("docs/diagnostics/wbgt_outdoor_pilot")
    if not baseline_units.empty and set(args.states) == set(P.PILOT_STATES):
        for level, name in (("district", "district_values.csv"), ("block", "block_values.csv")):
            if level in args.levels:
                # Both sides are the published CSV, so the comparison is between the two
                # published tables rather than between two representations of one number.
                published = (out_dir / name)
                acceptance.append(reproduction_check(published, frozen_dir / name, level=level,
                                                     run="baseline", model=args.model))

    pairs = [("baseline", "rh"), ("baseline", "elev"), ("baseline", "combined"),
             ("rh", "combined"), ("elev", "combined")]
    comparisons: list[dict[str, object]] = []
    unit_deltas: list[pd.DataFrame] = []
    for model, states, runs in jobs:
        for left_run, right_run in pairs:
            if left_run not in runs or right_run not in runs:
                continue
            for state in states:
                if (model, state, left_run) not in grids or (model, state, right_run) not in grids:
                    continue
                comparisons.extend(cell_comparison(
                    grids[(model, state, left_run)], grids[(model, state, right_run)],
                    state=state, model=model, left_run=left_run, right_run=right_run))
            left = units[(units.model.eq(model)) & (units.run.eq(left_run))
                         & (units.state_name.isin(states))]
            right = units[(units.model.eq(model)) & (units.run.eq(right_run))
                          & (units.state_name.isin(states))]
            cov_l = coverage[(coverage.model.eq(model)) & (coverage.run.eq(left_run))]
            cov_r = coverage[(coverage.model.eq(model)) & (coverage.run.eq(right_run))]
            if left.empty or right.empty:
                continue
            delta = unit_comparison(left, right, left_run=left_run, right_run=right_run,
                                    coverage_left=cov_l, coverage_right=cov_r)
            delta["model"] = model
            unit_deltas.append(delta)
    if comparisons:
        _write_csv(pd.DataFrame(comparisons), out_dir / "cell_comparisons.csv", verbose=verbose)
    if unit_deltas:
        _write_csv(pd.concat(unit_deltas, ignore_index=True), out_dir / "unit_comparisons.csv",
                   verbose=verbose)

    # A3/A4/A5: validity behaviour of each treatment, scored against the frozen criteria.
    summary = pd.DataFrame(cell_rows)
    primary = summary[summary.model.eq(args.model)]
    if {"baseline", "rh"} <= set(primary.run):
        gained = (primary[primary.run.eq("rh")].cells_complete_year.sum()
                  - primary[primary.run.eq("baseline")].cells_complete_year.sum())
        acceptance.append({"check": "A3_rh_restores_cell_years", "ok": bool(gained > 0),
                           "detail": f"{int(gained)} cell-years restored by the RH treatment"})
    if {"baseline", "elev"} <= set(primary.run):
        same = (primary[primary.run.eq("elev")].cells_complete_year.sum()
                == primary[primary.run.eq("baseline")].cells_complete_year.sum())
        acceptance.append({"check": "A4_elevation_changes_no_validity", "ok": bool(same),
                           "detail": "valid cell-year count identical to baseline"})
    nan_with_number = int(((units.n_cells_valid.eq(0))
                           & units.wbgt_annual_mean_c.notna()).sum())
    acceptance.append({"check": "A5_no_valid_coverage_never_numeric",
                       "ok": nan_with_number == 0,
                       "detail": f"{nan_with_number} unsupported units carry a number"})
    failed_admin = int(sum(1 for c in admin_checks if not c["ok"]))
    acceptance.append({"check": "A6_admin_checks_pass", "ok": failed_admin == 0,
                       "detail": f"{failed_admin} admin check failures"})
    if geometry_all:
        geom = pd.concat(geometry_all, ignore_index=True)
        failed_geom = int((~geom.ok).sum())
        acceptance.append({"check": "A7_geometry_within_tolerance", "ok": failed_geom == 0,
                           "detail": f"{failed_geom} of {len(geom)} geometry checks outside "
                                     "the declared tolerances; all are reported"})
    # A2: milestone 4's own per-cell parity checks, re-run on the *treated* path so the
    # guarantee covers the configuration this milestone actually adds, not only the baseline.
    parity_rows: list[dict[str, object]] = []
    if parity_context is not None:
        from tools.diagnostics.wbgt_method_validation import Site

        grid, index_range, cells, elevation_field = parity_context
        cube, days = P.load_daily_cube(
            source_root=args.source_root, wbgt_root=args.wbgt_root, model=args.model,
            scenario=P.PILOT_SCENARIO, year=args.year, index_range=index_range)
        cube, _, _ = P.apply_rh_policy(cube, rh_policy="clip100")
        i, j = divmod(int(cells[0]), len(grid.lon))
        daily = pd.DataFrame({v: cube[v][:, i, j] for v in m1.REQUIRED_NEX_VARIABLES}, index=days)
        elevation = float(elevation_field[i, j])
        site = Site("parity_cell", float(grid.lat[i]), float(grid.lon[j]), elevation, "pilot_qc")
        for candidate in (P.PILOT_CANDIDATE, P.COMPARISON_CANDIDATE):
            row = P.parity_against_frozen_path(daily, site, candidate=candidate)
            row["configuration"] = f"rh=clip100, elevation={elevation:.1f} m"
            parity_rows.append(row)
        parity_rows.append({**P.parity_hour_span(daily, site, args.year),
                            "configuration": f"rh=clip100, elevation={elevation:.1f} m"})
        parity_rows.extend(
            {**row, "configuration": f"rh=clip100, elevation field"}
            for row in P.parity_batch_and_chunks(
                cube, days, lat=grid.lat, lon=grid.lon, cell_indices=cells[:4], year=args.year))
        _write_csv(pd.DataFrame(parity_rows), out_dir / "parity_checks.csv", verbose=verbose)
        failed_parity = int(sum(1 for r in parity_rows if not r.get("ok", True)))
        acceptance.append({"check": "A2_cell_parity_under_the_treated_path",
                           "ok": failed_parity == 0,
                           "detail": f"{len(parity_rows) - failed_parity} of {len(parity_rows)} "
                                     "parity checks ok, run with the RH treatment and the "
                                     "acquired elevation applied"})

    _write_csv(pd.DataFrame(acceptance), out_dir / "acceptance.csv", verbose=verbose)

    # ---------------- Manifest ----------------
    wall = time.perf_counter() - acquire_started
    artifact_bytes = P._directory_bytes(work_dir) + P._directory_bytes(out_dir)
    # The manifest's own signature fields describe milestone 4's configuration, which is this
    # follow-up's baseline; every run's actual signature is listed separately below, because one
    # invocation now covers four of them.
    args.rh_policy = "strict"
    args.elevation_convention = P.ELEVATION_CONVENTION
    run_signatures = {
        name: P.method_signature(
            P.PILOT_CANDIDATE, rh_policy=policy,
            elevation_convention=(convention if use_elev else P.ELEVATION_CONVENTION))
        for name, policy, use_elev in RUN_MATRIX if name in wanted
    }
    manifest = P.run_manifest(args, {
        "run_signatures": run_signatures,
        "tool": "tools/diagnostics/wbgt_outdoor_pilot_qc.py",
        "spec": "docs/diagnostics/wbgt_outdoor_pilot_qc/SPEC.md",
        "milestone": "4b",
        "predecessor": "docs/diagnostics/wbgt_outdoor_pilot (milestone 4, commit 459d5c7)",
        "stage": "all",
        "runs": list(args.runs),
        "run_matrix": [{"run": n, "rh_policy": p, "elevation": "gmted" if e else "sea_level"}
                       for n, p, e in RUN_MATRIX if n in wanted],
        "elevation_source": {
            "product": GMTED_PRODUCT, "sampling": GMTED_SAMPLING,
            "vertical_reference": GMTED_VERTICAL_REFERENCE,
            "horizontal_reference": GMTED_HORIZONTAL_REFERENCE,
            "identity": convention, "tiles": tiles,
            "acquire_seconds": acquire_seconds, "acquire_bytes": acquire_bytes,
        },
        "coverage_screen": COVERAGE_SCREEN,
        "coverage_screen_denominator": COVERAGE_SCREEN_DENOMINATOR,
        "geometry_tolerances": {
            "gap_fraction": GEOMETRY_GAP_TOLERANCE,
            "child_outside_fraction": GEOMETRY_OUTSIDE_TOLERANCE,
            "block_overlap_fraction": GEOMETRY_OVERLAP_TOLERANCE,
            "sliver_floor_m2": GEOMETRY_SLIVER_FLOOR_M2,
            "crs": ANALYSIS_CRS,
        },
        "wall_seconds": wall,
        "compute_seconds": compute_seconds_total,
        "peak_rss_bytes": int(memory.peak),
        "artifact_bytes": int(artifact_bytes),
        "budget_respected": {
            "compute_wall": compute_seconds_total <= P.BUDGET_WALL_SECONDS,
            "acquire_wall": acquire_seconds <= ACQUIRE_BUDGET_SECONDS,
            "acquire_bytes": acquire_bytes <= ACQUIRE_BUDGET_BYTES,
            "peak_rss": memory.peak <= P.BUDGET_PEAK_RSS_BYTES,
            "artifacts": artifact_bytes <= P.BUDGET_ARTIFACT_BYTES,
        },
        "acceptance": acceptance,
        "parity": parity_rows,
        "admin_checks_failed": failed_admin,
        "aggregation_equality_caveat": (
            "Numerical block-to-district agreement verifies aggregation consistency under the "
            "tested support. It does NOT prove that blocks tile districts; see "
            "geometry_checks.csv."),
    })
    (out_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    if verbose:
        print(f"\nwall {wall:.1f} s (acquire {acquire_seconds:.1f} s, "
              f"compute {compute_seconds_total:.1f} s) | peak RSS "
              f"{memory.peak / 1024**3:.2f} GB | artifacts {artifact_bytes / 1024**2:.1f} MB")
        for row in acceptance:
            print(f"  {'ok  ' if row['ok'] else 'FAIL'} {row['check']}: {row.get('detail', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
