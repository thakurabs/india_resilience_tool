import subprocess
import sys

import numpy as np
import pytest
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import daily_to_monthly_totals, pr_to_mm_per_day_strict


def test_pr_units_flux_and_daily_depth_are_classified():
    da = xr.DataArray([1.0], attrs={"units": "kg m-2 s-1"})
    assert float(pr_to_mm_per_day_strict(da).item()) == 86400.0

    da = xr.DataArray([2.0], attrs={"units": "mm/day"})
    assert float(pr_to_mm_per_day_strict(da).item()) == 2.0


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
