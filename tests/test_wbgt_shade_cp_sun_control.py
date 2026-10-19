"""Small end-to-end control of cached shade plus a pinned solar delta."""
from __future__ import annotations

import numpy as np
import pandas as pd

from tools.diagnostics import wbgt_shade_cp_sun_control as control


def test_sun_control_reproduces_cell_first_annual_csv(tmp_path, monkeypatch):
    monkeypatch.setitem(control.PERIODS, "historical", (2001, 2001))
    monkeypatch.setattr(control, "sun_adjustments",
                        lambda mod, rsds, wind, dates, calendar, cells, upstream:
                        (np.zeros_like(rsds, dtype=float), []))
    dates = pd.date_range("2001-01-01", "2001-12-31")
    driver = tmp_path / "drivers" / "ACCESS-CM2" / "historical" / "2001.npz"
    driver.parent.mkdir(parents=True)
    np.savez(driver, dates=dates.strftime("%Y-%m-%d").to_numpy(dtype=str),
             rsds=np.zeros((365, 2), dtype="float32"),
             sfcWind=np.ones((365, 2), dtype="float32"))
    cache_dir = tmp_path / "qdm" / "ACCESS-CM2" / "cell"
    cache_dir.mkdir(parents=True)
    for index, value in enumerate((27., 29.)):
        np.savez(cache_dir / f"{index}.npz", signature=np.array("signed"),
                 historical=np.full(365, value))
    validated = pd.DataFrame({"unit_index": [7], "scenario": ["historical"],
                              "year": [2001], "method": ["grid_first_cell_metrics"],
                              "mean_c": [28.5], "max_c": [28.5],
                              "days_ge_28": [273.75], "days_ge_30": [0.],
                              "days_ge_32": [0.]})
    report = control.scenario_control(tmp_path, tmp_path / "evidence", "ACCESS-CM2",
        "historical", np.array([True, True]), np.array([7]), np.array([[.25, .75]]),
        pd.DataFrame({"lat": [20., 21.], "elevation_m": [0., 0.]}),
        dates, "proleptic_gregorian", "signed", None, tmp_path, validated,
        chunk_size=1)
    assert report["verdict"] == "PASS"
    assert report["values_checked"] == 5


def test_sun_control_accepts_only_jointly_missing_unit_years(tmp_path, monkeypatch):
    monkeypatch.setitem(control.PERIODS, "historical", (2001, 2001))
    monkeypatch.setattr(control, "sun_adjustments",
                        lambda mod, rsds, wind, dates, calendar, cells, upstream:
                        (np.zeros_like(rsds, dtype=float), []))
    dates = pd.date_range("2001-01-01", "2001-12-31")
    driver = tmp_path / "drivers" / "ACCESS-CM2" / "historical" / "2001.npz"
    driver.parent.mkdir(parents=True)
    np.savez(driver, dates=dates.strftime("%Y-%m-%d").to_numpy(dtype=str),
             rsds=np.zeros((365, 1), dtype="float32"),
             sfcWind=np.ones((365, 1), dtype="float32"))
    cache_dir = tmp_path / "qdm" / "ACCESS-CM2" / "cell"
    cache_dir.mkdir(parents=True)
    shade = np.full(365, 29.)
    shade[10] = np.nan
    np.savez(cache_dir / "0.npz", signature=np.array("signed"), historical=shade)
    row = {"unit_index": [7], "scenario": ["historical"], "year": [2001],
           "method": ["grid_first_cell_metrics"]}
    missing = pd.DataFrame({**row, **{m: [np.nan] for m in control.OUTDOOR_METRICS}})
    args = ("ACCESS-CM2", "historical", np.array([True]), np.array([7]), np.array([[1.]]),
            pd.DataFrame({"lat": [20.], "elevation_m": [0.]}), dates,
            "proleptic_gregorian", "signed", None, tmp_path)
    report = control.scenario_control(tmp_path, tmp_path / "evidence", *args, missing)
    assert report["verdict"] == "PASS"
    assert report["jointly_missing_values"] == 5

    finite = pd.DataFrame({**row, **{m: [29.] for m in control.OUTDOOR_METRICS}})
    import pytest
    with pytest.raises(ValueError, match="One-sided missing"):
        control.scenario_control(tmp_path, tmp_path / "evidence2", *args, finite)
