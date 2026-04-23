from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from tools.pipeline import compute_indices_multiprocess as CMP


def _precip_series(values_mm: list[float]) -> tuple[xr.DataArray, xr.DataArray]:
    data = xr.DataArray(
        np.asarray(values_mm, dtype=float).reshape(len(values_mm), 1, 1) / 86400.0,
        dims=("time", "lat", "lon"),
        coords={
            "time": pd.date_range("2020-01-01", periods=len(values_mm), freq="D"),
            "lat": [17.0],
            "lon": [78.0],
        },
        attrs={"units": "kg m-2 s-1"},
    )
    mask = xr.DataArray(np.ones((1, 1), dtype=bool), dims=("lat", "lon"), coords={"lat": [17.0], "lon": [78.0]})
    return data, mask


def test_consecutive_heavy_rainfall_events_counts_one_long_run_as_one_event() -> None:
    data, mask = _precip_series([0.0, 150.0, 160.0, 170.0, 0.0, 151.0, 152.0])
    assert CMP.consecutive_heavy_rainfall_events(data, mask, daily_thresh_mm=150.0, min_event_days=2) == 2


def test_percentile_precipitation_total_honors_exceed_ge() -> None:
    data, mask = _precip_series([1.0] * 95 + [100.0] * 5)
    inclusive = CMP.percentile_precipitation_total(
        data,
        mask,
        percentile=95,
        baseline_years=(1981, 2010),
        quantile_method="nearest",
        exceed_ge=True,
    )
    strict = CMP.percentile_precipitation_total(
        data,
        mask,
        percentile=95,
        baseline_years=(1981, 2010),
        quantile_method="nearest",
        exceed_ge=False,
    )
    assert inclusive == 595.0
    assert strict == 500.0
