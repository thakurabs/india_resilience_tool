import subprocess
import sys

import numpy as np
import pytest
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import daily_to_monthly_totals, pr_to_mm_per_day_strict
from india_resilience_tool.compute.spi_adapter import CLIMATE_INDICES_AVAILABLE, Distribution, compute_spi_climate_indices


@pytest.mark.parametrize(
    ("units", "expected"),
    [
        ("kg m-2 s-1", 86400.0),
        ("kg m**-2 s**-1", 86400.0),
        ("kg/m2/s", 86400.0),
        ("kg/m^2/s", 86400.0),
        ("mm/day", 1.0),
        ("mm d-1", 1.0),
        ("mm/d", 1.0),
        ("mm", 1.0),
    ],
)
def test_pr_units_flux_and_daily_depth_are_classified(units, expected):
    da = xr.DataArray([1.0], attrs={"units": units})
    assert float(pr_to_mm_per_day_strict(da).item()) == expected


def test_climate_indices_nan_behavior_is_not_zero_fill():
    if not CLIMATE_INDICES_AVAILABLE:
        pytest.skip("climate-indices is not installed in this test environment")
    rng = np.random.default_rng(42)
    vals = rng.gamma(shape=2.0, scale=25.0, size=360).astype(float)
    vals[100] = np.nan
    out = compute_spi_climate_indices(
        monthly_precip=vals,
        data_start_year=1981,
        calibration_start_year=1981,
        calibration_end_year=2010,
        scale_months=3,
        distribution=Distribution.GAMMA,
    )

    assert out.shape == (360,)
    assert np.isnan(out[100]) or np.isnan(out[101]) or np.isnan(out[102])


def test_pr_units_unknown_raise_value_error():
    with pytest.raises(ValueError, match="Unsupported precipitation units"):
        pr_to_mm_per_day_strict(xr.DataArray([1.0], attrs={"units": ""}))


def test_monthly_total_requires_90_percent_daily_coverage():
    time = np.arange("2000-01-01", "2000-02-01", dtype="datetime64[D]")
    vals = np.ones(len(time), dtype=float)
    vals[:4] = np.nan
    da = xr.DataArray(vals, coords={"time": time}, dims=("time",), attrs={"units": "mm"})
    monthly = daily_to_monthly_totals(da)
    assert np.isnan(float(monthly.item()))

    vals[3] = 1.0
    da = xr.DataArray(vals, coords={"time": time}, dims=("time",), attrs={"units": "mm"})
    monthly = daily_to_monthly_totals(da)
    assert float(monthly.item()) == 28.0


def test_spi_legacy_flag_exits_code_2():
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.pipeline.compute_indices_multiprocess",
            "--spi-legacy",
            "--list-metrics",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 2
    assert "Legacy SPI z-score is non-conformant" in proc.stderr
