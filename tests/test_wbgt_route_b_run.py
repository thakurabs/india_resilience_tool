"""Protect QDM holdout separation, application windows and resumable signatures."""
import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import wbgt_route_b_run as run


def test_fit_separates_holdout_and_covers_future_once(tmp_path, monkeypatch):
    """A spy trainer must never see held-out dates in the validation fit."""
    calls, applications = [], []

    def array(values, dates, calendar="proleptic_gregorian"):
        return (np.asarray(values), pd.DatetimeIndex(dates))

    def train(reference, historical, model):
        calls.append((reference[1], historical[1]))
        return len(calls)

    def adjust(model, values, dates, calendar):
        applications.append((model, pd.DatetimeIndex(dates)))
        return values + model

    monkeypatch.setattr(run, "data_array", array)
    monkeypatch.setattr(run, "adjust", adjust)
    raw = {}
    for scenario, (first, last) in run.PERIODS.items():
        dates = pd.date_range(f"{first}-01-01", f"{last}-12-31")
        raw[scenario] = {"dates": dates, "values": np.full(len(dates), 25.)}
    dates = raw["historical"]["dates"]
    path = tmp_path / "series.npz"
    result, _ = run.fit_series(train, np.full(len(dates), 26.), dates, raw,
                               "gregorian", "signature-a", path, True)
    assert len(calls) == 2
    assert calls[0][0].min().year == 1985 and calls[0][0].max().year == 2014
    assert calls[1][0].max().year == 1999 and calls[1][1].max().year == 1999
    assert applications[-1][1].min().year == 2000
    assert applications[-1][1].max().year == 2014
    future = [axis for _, axis in applications if axis.min().year >= 2020]
    assert [(axis.min().year, axis.max().year) for axis in future[:3]] == list(run.FUTURE_WINDOWS)
    assert sum(len(axis) for axis in future[:3]) == len(raw["ssp245"]["dates"])
    assert np.all(result["ssp585"] == 26.)
    assert np.all(result["holdout"] == 27.)
    _, seconds = run.fit_series(train, np.full(len(dates), 26.), dates, raw,
                                "gregorian", "signature-a", path, True)
    assert seconds == 0 and len(calls) == 2
    with pytest.raises(ValueError, match="Stale QDM"):
        run.fit_series(train, np.full(len(dates), 26.), dates, raw,
                       "gregorian", "signature-b", path)


def test_missing_adjustment_window_never_becomes_zero():
    class NeverCalled:
        def adjust(self, values):
            pytest.fail("QDM must not receive incomplete weather")

    actual = run.adjust(NeverCalled(), np.array([25., np.nan]),
                        pd.date_range("2001-01-01", periods=2), "gregorian")
    assert np.isnan(actual).all()


def test_distribution_score_is_not_synchronized_weather_error():
    dates = pd.date_range("2001-01-01", "2001-12-31")
    values = 29 + np.sin(np.arange(365))
    # Permuting within each month preserves the scored seasonal distributions.
    permutation = np.concatenate([np.flatnonzero(dates.month == m)[::-1] for m in range(1, 13)])
    result = run.distribution_errors(dates, values[permutation], dates, values)
    assert result["annual_mean_bias_c"] == pytest.approx(0, abs=1e-12)
    assert result["monthly_climatology_mae_c"] == pytest.approx(0, abs=1e-12)
    assert result["days_ge_30_bias"] == 0
    assert run.distribution_errors(dates, np.full(365, np.nan), dates, values) == {"complete": False}
