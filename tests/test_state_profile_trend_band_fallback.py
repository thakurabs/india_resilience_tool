"""
Tests for state climate profile trend data prep.

Focus: deterministic dataframe transforms feeding the shared Plotly trend chart.
"""

from __future__ import annotations

import pytest


pd = pytest.importorskip("pandas")


def test_merge_missing_trend_band_fills_p05_p95_from_models() -> None:
    from india_resilience_tool.app.views.state_summary_view import _merge_missing_trend_band

    # Ensemble file missing p05/p95 (regression safety for older artifacts).
    ensemble_df = pd.DataFrame(
        {
            "scenario": ["historical", "historical", "ssp245", "ssp245"],
            "year": [1990, 1991, 2020, 2021],
            "mean": [10.0, 11.0, 12.0, 13.0],
        }
    )

    model_df = pd.DataFrame(
        {
            "scenario": ["historical", "historical", "historical", "historical", "ssp245", "ssp245", "ssp245", "ssp245"],
            "year": [1990, 1990, 1991, 1991, 2020, 2020, 2021, 2021],
            "model": ["m1", "m2", "m1", "m2", "m1", "m2", "m1", "m2"],
            "value": [9.0, 11.0, 10.0, 12.0, 11.5, 12.5, 12.0, 14.0],
        }
    )

    out = _merge_missing_trend_band(ensemble_df=ensemble_df, model_df=model_df)

    assert not out.empty
    assert set(out.columns).issuperset({"p05", "p95"})
    assert pd.to_numeric(out["p05"], errors="coerce").notna().all()
    assert pd.to_numeric(out["p95"], errors="coerce").notna().all()


def test_coerce_trend_central_tendency_mean_drops_median() -> None:
    from india_resilience_tool.app.views.state_summary_view import _coerce_trend_central_tendency

    df = pd.DataFrame(
        {
            "scenario": ["historical"],
            "year": [1990],
            "mean": [10.0],
            "median": [9.5],
        }
    )

    out = _coerce_trend_central_tendency(df, "mean")
    assert "median" not in out.columns
    assert float(out["mean"].iloc[0]) == 10.0

