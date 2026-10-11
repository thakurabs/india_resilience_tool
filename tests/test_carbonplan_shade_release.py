"""Contracts for reducing signed QDM shade caches without recomputing shade."""
from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd
import pytest

from tools.pipeline.build_carbonplan_shade_release import (
    driver_dates, reduce_all_caches, reduce_cache, seam_frame,
)


def test_reduce_cache_thresholds_before_fixed_area_transfer(tmp_path):
    dates = pd.date_range("2000-01-01", "2000-12-31")
    root = tmp_path
    cache_dir = root / "qdm" / "ACCESS-CM2" / "cell"
    cache_dir.mkdir(parents=True)
    hot = np.full(len(dates), 30.0)
    cool = np.full(len(dates), 26.0)
    for index, values in enumerate((hot, cool)):
        np.savez(cache_dir / f"{index}.npz", signature=np.array("signed"),
                 historical=values)
    weights = np.array([[.25, .75]])
    result = reduce_cache(root, "ACCESS-CM2", "historical", dates, "signed", 2,
                          weights, chunk_size=1)
    assert result["years"].tolist() == [2000]
    assert result["mean_c"][0, 0] == pytest.approx(27.0)
    assert result["days_ge_28"][0, 0] == pytest.approx(365 * .25)
    assert result["days_ge_30"][0, 0] == pytest.approx(365 * .25)
    assert result["days_ge_32"][0, 0] == 0

    cool[0] = np.nan
    np.savez(cache_dir / "1.npz", signature=np.array("signed"), historical=cool)
    missing = reduce_cache(root, "ACCESS-CM2", "historical", dates, "signed", 2,
                           weights, chunk_size=2)
    assert all(np.isnan(missing[name][0, 0]) for name in
               ("mean_c", "days_ge_28", "days_ge_30", "days_ge_32"))


def test_one_pass_cache_reduction_uses_each_window_and_holdout(tmp_path):
    axis = pd.date_range("2001-01-01", "2001-12-31")
    directory = tmp_path / "qdm" / "ACCESS-CM2" / "cell"
    directory.mkdir(parents=True)
    np.savez(directory / "0.npz", signature=np.array("signed"),
             historical=np.full(365, 27.), ssp245=np.full(365, 28.),
             ssp585=np.full(365, 30.), holdout=np.full(365, 32.))
    result = reduce_all_caches(tmp_path, "ACCESS-CM2",
                               {"historical": axis, "ssp245": axis, "ssp585": axis},
                               "signed", 1, np.array([[1.]]), chunk_size=1)
    assert [result[key]["days_ge_28"][0, 0] for key in
            ("historical", "ssp245", "ssp585", "holdout")] == [0., 365., 365., 365.]
    assert result["holdout"]["days_ge_32"][0, 0] == 365.


def test_driver_dates_reconstructs_noleap_and_rejects_missing_day(tmp_path, monkeypatch):
    from tools.pipeline import build_carbonplan_shade_release as release

    monkeypatch.setitem(release.PERIODS, "historical", (2000, 2000))
    path = tmp_path / "drivers" / "ACCESS-CM2" / "historical" / "2000.npz"
    path.parent.mkdir(parents=True)
    dates = pd.date_range("2000-01-01", "2000-12-31")
    dates = dates[~((dates.month == 2) & (dates.day == 29))]
    np.savez(path, dates=np.asarray(dates.strftime("%Y-%m-%d"), dtype=str),
             calendar=np.array("noleap"), signature=np.array("support"))
    expected = hashlib.sha256(hashlib.sha256(path.read_bytes()).hexdigest().encode()).hexdigest()
    actual, calendar = driver_dates(tmp_path, "ACCESS-CM2", "historical", "support", expected)
    assert actual.equals(dates)
    assert calendar == "noleap"

    np.savez(path, dates=np.asarray(dates[:-1].strftime("%Y-%m-%d"), dtype=str),
             calendar=np.array("noleap"), signature=np.array("support"))
    with pytest.raises(ValueError, match="Incomplete or duplicate driver dates"):
        driver_dates(tmp_path, "ACCESS-CM2", "historical", "support", expected)


def test_cache_length_and_signature_fail_closed(tmp_path):
    dates = pd.date_range("2001-01-01", "2001-12-31")
    path = tmp_path / "qdm" / "ACCESS-CM2" / "cell" / "0.npz"
    path.parent.mkdir(parents=True)
    np.savez(path, signature=np.array("wrong"), historical=np.ones(len(dates)))
    with pytest.raises(ValueError, match="signature mismatch"):
        reduce_cache(tmp_path, "ACCESS-CM2", "historical", dates, "signed", 1,
                     np.array([[1.]]))
    np.savez(path, signature=np.array("signed"), historical=np.ones(len(dates) - 1))
    with pytest.raises(ValueError, match="length mismatch"):
        reduce_cache(tmp_path, "ACCESS-CM2", "historical", dates, "signed", 1,
                     np.array([[1.]]))


def test_seam_steps_are_unit_specific_and_keep_missing():
    frame = pd.DataFrame({
        "unit_index": [0, 0, 0, 0, 1, 1, 1, 1] * 2,
        "scenario": ["ssp245"] * 8 + ["ssp585"] * 8,
        "year": [2039, 2040, 2059, 2060] * 4,
        "mean_c": [1., 2., 4., 6., np.nan, np.nan, 3., 4.] * 2,
        "days_ge_28": [0., 1., 2., 3., np.nan, np.nan, 0., 0.] * 2,
        "days_ge_30": [0.] * 16,
        "days_ge_32": [0.] * 16,
    })
    result = seam_frame(frame)
    selected = result[(result.scenario == "ssp245") & (result.unit_index == 0)
                      & (result.metric == "mean_c")]
    assert selected.step.tolist() == [1., 2.]
    assert np.isnan(result[(result.scenario == "ssp245") & (result.unit_index == 1)
                           & (result.year_before == 2039) &
                           (result.metric == "mean_c")].step.iloc[0])
