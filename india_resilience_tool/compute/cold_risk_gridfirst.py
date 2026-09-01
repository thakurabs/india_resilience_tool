"""Grid-first Cold Risk v2 compute helpers.

Mirrors the Heat Risk v2 architecture (see ``heat_risk_gridfirst``): compute
each metric per climate grid cell with cached thresholds and per-cell annual
fields, then area-weight to polygons via the shared sparse spatial weights.

Cold-specific concerns handled here:

* DJF cross-year semantics for ``seasonal_mean`` / ``seasonal_min`` with the
  same SSP historical-Dec fallback the polygon path uses
  (``tools/pipeline/compute_indices_multiprocess.py`` dispatcher).
* Strict-< (``exceed_ge=False``) lower-tail percentile metrics: TX10p, TN10p,
  CSDI. The shared cellwise helpers honor ``direction="below"`` natively.
* Registry-driven baselines (no hard-coded literals).

No Streamlit imports and no ``tools/`` imports per the compute-layer rules in
``india_resilience_tool/compute/CLAUDE.md``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.gridfirst_spatial import (
    _cellwise_annual_min_temperature,
    _cellwise_djf_cross_year_mean,
    _cellwise_djf_cross_year_min,
    _cellwise_longest_consecutive_run_le,
    _cellwise_percent_days,
    _cellwise_spell_days,
    aggregate_cell_values,
    aggregate_percent_days,
    assert_grid_matches,
    compute_doy_thresholds,
    concat_years,
    grid_metric_cache_path,
    input_file_signature,
    open_year_dataarray,
    read_grid_metric_cache,
    read_threshold_cache,
    subcell_idw_fill,
    threshold_cache_path,
    write_grid_metric_cache,
    write_threshold_cache,
)


GRIDFIRST_METHOD_VERSION = "cold-risk-v2-gridfirst-1"

COLD_RISK_GRIDFIRST_SLUGS = frozenset(
    {
        # Background cold (DJF cross-year mean).
        "tas_winter_mean",
        "tasmin_winter_mean",
        # Absolute extremes.
        "tnn_annual_min",
        "tasmin_winter_min",
        # Threshold cold days/nights.
        "tnle10_cold_nights",
        "tnle5_severe_cold_nights",
        "txle15_cold_days",
        # Persistence: longest consecutive cold-night run.
        "tnle10_consecutive_cold_nights",
        # Lower-tail percentile (strict <).
        "tx10p_cool_days_pct",
        "tn10p_cool_nights_pct",
        # Cold spell duration index.
        "csdi_cold_spell_days",
    }
)

# Computes that require a per-cell DOY threshold field built from the
# historical baseline window.
COLD_RISK_GRIDFIRST_BASELINE_THRESHOLD_COMPUTES = frozenset(
    {
        "tx90p_etccdi",
        "cold_spell_duration_index",
    }
)

# Computes that require Dec(year-1) data to form the cross-year DJF window.
COLD_RISK_GRIDFIRST_DJF_COMPUTES = frozenset(
    {
        "seasonal_mean",
        "seasonal_min",
    }
)


def _jsonable(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _grid_metric_sidecar(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year: int,
    eval_input_signature: str,
    prev_input_signature: str | None,
    baseline_input_signature: str | None,
    baseline_years: tuple[int, int] | None,
    percentile: int | None,
    window_days: int | None,
    quantile_method: str | None,
    value_col: str,
    grid_id: str | None = None,
) -> dict[str, object]:
    return {
        "method_version": GRIDFIRST_METHOD_VERSION,
        "artifact_type": "annual-grid-first-metric",
        "slug": str(metric.get("slug") or ""),
        "model": model,
        "scenario": scenario,
        "year": int(year),
        "var": str(metric.get("var") or ""),
        "value_col": value_col,
        "compute": str(metric.get("compute") or ""),
        "params": _jsonable(dict(metric.get("params") or {})),
        "eval_input_hash": eval_input_signature,
        "prev_input_hash": prev_input_signature,
        "baseline_input_hash": baseline_input_signature,
        "baseline_years": list(baseline_years) if baseline_years is not None else None,
        "percentile": int(percentile) if percentile is not None else None,
        "window_days": int(window_days) if window_days is not None else None,
        "quantile_method": quantile_method,
        "grid_id": grid_id,
        "methodology_note": "Cold Risk v2 annual per-cell metric field before polygon aggregation",
    }


def _cold_risk_cell_values(
    *,
    metric: Mapping[str, object],
    eval_da: xr.DataArray,
    prev_da: xr.DataArray | None,
    threshold: xr.DataArray | None,
) -> xr.DataArray | tuple[xr.DataArray, xr.DataArray]:
    """Dispatch a Cold Risk metric to its cellwise compute, returning per-cell values.

    Returns a single ``xr.DataArray`` for scalar metrics (mean/min/count/longest-run)
    or a ``(exceed_days, valid_days)`` tuple for percent-day metrics so the caller
    can area-weight numerator and denominator separately.
    """
    slug = str(metric.get("slug") or "")
    compute = str(metric.get("compute") or "")
    params = dict(metric.get("params") or {})

    if compute == "annual_min_temperature":
        return _cellwise_annual_min_temperature(eval_da)

    if compute == "annual_max_temperature":
        # Not currently used by Cold Risk, but matches Heat semantics if a
        # future metric needs it.
        return eval_da.max(dim="time", skipna=True) - 273.15

    if compute == "seasonal_mean":
        months = set((params.get("months") or []))
        if months != {12, 1, 2}:
            raise ValueError(f"[{slug}] Cold Risk grid-first seasonal_mean only supports DJF; got months={sorted(months)}")
        return _cellwise_djf_cross_year_mean(prev_da=prev_da, cur_da=eval_da)

    if compute == "seasonal_min":
        months = set((params.get("months") or []))
        if months != {12, 1, 2}:
            raise ValueError(f"[{slug}] Cold Risk grid-first seasonal_min only supports DJF; got months={sorted(months)}")
        return _cellwise_djf_cross_year_min(prev_da=prev_da, cur_da=eval_da)

    if compute in {"count_days_le_threshold", "count_days_below_threshold"}:
        thresh_k = float(params["thresh_k"])
        comparator = eval_da <= thresh_k if compute == "count_days_le_threshold" else eval_da < thresh_k
        return comparator.where(eval_da.notnull()).sum(dim="time", skipna=True)

    if compute == "longest_consecutive_run_le_threshold":
        thresh_k = float(params["thresh_k"])
        return _cellwise_longest_consecutive_run_le(eval_da, thresh_k=thresh_k)

    if compute == "tx90p_etccdi":
        if threshold is None:
            raise ValueError(f"[{slug}] tx90p_etccdi requires a per-cell DOY threshold field")
        direction = str(params.get("direction") or "below").strip().lower()
        exceed_ge = bool(params.get("exceed_ge", False))
        return _cellwise_percent_days(eval_da, threshold, exceed_ge=exceed_ge, direction=direction)

    if compute == "cold_spell_duration_index":
        if threshold is None:
            raise ValueError(f"[{slug}] cold_spell_duration_index requires a per-cell DOY threshold field")
        min_spell_days = int(params.get("min_spell_days", 6))
        direction = str(params.get("direction") or "below").strip().lower()
        exceed_ge = bool(params.get("exceed_ge", False))
        return _cellwise_spell_days(
            eval_da,
            threshold,
            min_spell_days=min_spell_days,
            exceed_ge=exceed_ge,
            direction=direction,
        )

    raise ValueError(f"[{slug}] Unsupported Cold Risk grid-first compute: {compute!r}")


def _wrap_cell_payload(
    payload: xr.DataArray | tuple[xr.DataArray, xr.DataArray],
    *,
    value_col: str,
) -> xr.Dataset:
    """Coerce a cellwise payload into a cache-friendly ``xr.Dataset``."""
    if isinstance(payload, tuple):
        exceed_days, valid_days = payload
        values = xr.where(valid_days > 0, 100.0 * exceed_days / valid_days, np.nan)
        return xr.Dataset(
            {
                value_col: values.rename(value_col),
                "exceed_days": exceed_days.rename("exceed_days"),
                "valid_days": valid_days.rename("valid_days"),
            }
        )
    return xr.Dataset({value_col: payload.rename(value_col)})


def _aggregate_cell_dataset(
    ds: xr.Dataset, *, value_col: str, weights: pd.DataFrame, grid: "GridSpec | None" = None
) -> dict[str, float]:
    if "exceed_days" in ds and "valid_days" in ds:
        return aggregate_percent_days(ds["exceed_days"], ds["valid_days"], weights, grid=grid)
    return aggregate_cell_values(ds[value_col], weights, grid=grid)


def _resolve_prev_year_da(
    *,
    var: str,
    year: int,
    year_to_paths: Mapping[int, Mapping[str, Path]],
    historical_year_to_paths: Mapping[int, Mapping[str, Path]] | None,
    index_range: tuple[int, int, int, int] | None = None,
) -> tuple[xr.DataArray | None, Path | None]:
    """Resolve the ``Dec(year-1)`` DataArray, falling back to history for SSP first years."""
    prev_path: Path | None = year_to_paths.get(year - 1, {}).get(var)
    if prev_path is None and historical_year_to_paths is not None:
        prev_path = historical_year_to_paths.get(year - 1, {}).get(var)
    if prev_path is None:
        return None, None
    return open_year_dataarray(prev_path, var, index_range=index_range), prev_path


def compute_cold_risk_rows_for_metric(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year_to_paths: Mapping[int, Mapping[str, Path]],
    baseline_year_to_paths: Mapping[int, Mapping[str, Path]],
    weights: pd.DataFrame,
    level: str = "district",
    cache_root: Path | None = None,
    historical_year_to_paths: Mapping[int, Mapping[str, Path]] | None = None,
    index_range: tuple[int, int, int, int] | None = None,
    grid: "GridSpec | None" = None,
) -> list[dict[str, object]]:
    """Compute yearly Cold Risk rows from per-cell indicators and area weights.

    Args:
        historical_year_to_paths: Optional historical-scenario inventory for the
            same model. Used to source ``Dec(year-1)`` for the first SSP year
            (matches the polygon-mean dispatcher behavior shipped in Phase 2).
        index_range: Positional ``(lat0, lat1, lon0, lon1)`` bbox subset (the
            per-state memory fix), forwarded to every yearly load — current,
            baseline, and the cross-year ``Dec(year-1)`` — so only the subset is
            read into RAM.
        grid: Matching subset ``GridSpec``. When supplied its ``grid_id`` keys
            the threshold and annual caches and its coordinates gate
            aggregation. ``index_range``/``grid`` both ``None`` reproduce the
            exact full-grid behavior.

    Unknown registry params are ignored intentionally; only baseline /
    percentile / window / direction / exceed_ge / threshold settings relevant
    to v2 Cold Risk are consumed.
    """
    grid_id = grid.grid_id if grid is not None else None
    slug = str(metric.get("slug") or "")
    if slug and slug not in COLD_RISK_GRIDFIRST_SLUGS:
        raise ValueError(f"Cold Risk grid-first invoked for unsupported slug: {slug!r}")

    var = str(metric.get("var") or "")
    if not var:
        raise ValueError(f"[{slug}] Cold Risk grid-first metric has no variable")
    value_col = str(metric.get("value_col") or "value")
    compute = str(metric.get("compute") or "")
    params = dict(metric.get("params") or {})

    needs_threshold = compute in COLD_RISK_GRIDFIRST_BASELINE_THRESHOLD_COMPUTES
    needs_prev_year = compute in COLD_RISK_GRIDFIRST_DJF_COMPUTES

    baseline_input_sig: str | None = None
    threshold: xr.DataArray | None = None
    baseline_years_tuple: tuple[int, int] | None = None
    percentile: int | None = None
    window_days: int | None = None
    quantile_method: str | None = None
    if needs_threshold:
        baseline_years_tuple = tuple(int(v) for v in params.get("baseline_years", (1990, 2010)))  # type: ignore[assignment]
        percentile = int(params.get("percentile", 10))
        window_days = int(params.get("window_days", 5))
        quantile_method = str(params.get("quantile_method", "nearest"))
        smooth = params.get("smooth")
        smooth_int = int(smooth) if smooth is not None else None
        baseline_years_available = [
            y for y in sorted(baseline_year_to_paths) if baseline_years_tuple[0] <= int(y) <= baseline_years_tuple[1]
        ]
        if not baseline_years_available:
            raise ValueError(
                f"[{slug}] No baseline yearly files inside {baseline_years_tuple} for model={model}"
            )
        baseline_da = concat_years(baseline_year_to_paths, var, baseline_years_available, index_range=index_range)
        assert_grid_matches(baseline_da, grid, name=f"[{slug}] baseline")
        baseline_input_sig = input_file_signature(
            [baseline_year_to_paths[y][var] for y in baseline_years_available]
        )
        cache_path: Path | None = None
        if cache_root is not None:
            cache_path = threshold_cache_path(
                Path(cache_root),
                model=model,
                var=var,
                baseline_label=f"{baseline_years_tuple[0]}-{baseline_years_tuple[1]}",
                grid_id=grid_id,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                smooth=smooth_int,
            )
            threshold = read_threshold_cache(
                cache_path,
                input_signature=baseline_input_sig,
                baseline_years=baseline_years_tuple,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                grid_id=grid_id,
            )
        if threshold is None:
            threshold = compute_doy_thresholds(
                baseline_da,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                smooth=smooth_int,
            )
            assert_grid_matches(threshold, grid, name=f"[{slug}] threshold")
            if cache_path is not None:
                write_threshold_cache(
                    threshold,
                    cache_path,
                    input_signature=baseline_input_sig,
                    baseline_years=baseline_years_tuple,
                    percentile=percentile,
                    window_days=window_days,
                    quantile_method=quantile_method,
                    grid_id=grid_id,
                    smooth=smooth_int,
                )

    rows: list[dict[str, object]] = []
    for year in sorted(int(y) for y in year_to_paths):
        cur_path = year_to_paths[year].get(var)
        if cur_path is None:
            continue
        eval_input_sig = input_file_signature([cur_path])
        prev_da: xr.DataArray | None = None
        prev_input_sig: str | None = None
        if needs_prev_year:
            prev_da, prev_path = _resolve_prev_year_da(
                var=var,
                year=year,
                year_to_paths=year_to_paths,
                historical_year_to_paths=historical_year_to_paths,
                index_range=index_range,
            )
            if prev_da is not None:
                assert_grid_matches(prev_da, grid, name=f"[{slug}] prev-year {year - 1}")
            if prev_path is not None:
                prev_input_sig = input_file_signature([prev_path])

        grid_ds: xr.Dataset | None = None
        grid_cache_path: Path | None = None
        grid_sidecar: dict[str, object] | None = None
        if cache_root is not None:
            grid_cache_path = grid_metric_cache_path(
                Path(cache_root), slug=slug, model=model, scenario=scenario, year=year, grid_id=grid_id,
            )
            grid_sidecar = _grid_metric_sidecar(
                metric=metric,
                model=model,
                scenario=scenario,
                year=year,
                eval_input_signature=eval_input_sig,
                prev_input_signature=prev_input_sig,
                baseline_input_signature=baseline_input_sig,
                baseline_years=baseline_years_tuple,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                value_col=value_col,
                grid_id=grid_id,
            )
            grid_ds = read_grid_metric_cache(grid_cache_path, expected_sidecar=grid_sidecar)
        if grid_ds is None:
            eval_da = open_year_dataarray(cur_path, var, index_range=index_range)
            assert_grid_matches(eval_da, grid, name=f"[{slug}] eval {year}")
            cell_payload = _cold_risk_cell_values(
                metric=metric,
                eval_da=eval_da,
                prev_da=prev_da,
                threshold=threshold,
            )
            grid_ds = _wrap_cell_payload(cell_payload, value_col=value_col)
            if grid_cache_path is not None and grid_sidecar is not None:
                write_grid_metric_cache(grid_ds, grid_cache_path, sidecar=grid_sidecar)

        values = _aggregate_cell_dataset(grid_ds, value_col=value_col, weights=weights, grid=grid)
        fills = subcell_idw_fill(grid_ds[value_col], weights, grid=grid)
        source_file = str(cur_path)
        for unit_key in list(values.keys()) + [u for u in fills if u not in values]:
            if unit_key in fills:
                value, fill_method = fills[unit_key], "idw"
            else:
                value, fill_method = values[unit_key], "native"
            row: dict[str, object] = {
                "year": int(year),
                "value": float(value) if np.isfinite(value) else np.nan,
                value_col: float(value) if np.isfinite(value) else np.nan,
                "source_file": source_file,
                "climate_fill_method": fill_method,
            }
            if level == "block" and "||" in unit_key:
                left, right = unit_key.split("||", 1)
                row["district"] = left
                row["block"] = right
            else:
                row["district"] = unit_key
            if scenario:
                row["scenario"] = scenario
            rows.append(row)
    return rows
