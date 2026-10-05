"""Keep incomplete periods missing and diagnostic comparisons honestly labelled."""
import numpy as np
import pandas as pd
import pytest

from tools.diagnostics.wbgt_route_b_report import period_metrics, paired_gaps, w1_comparison, matched_w1
from tools.diagnostics import wbgt_route_b_finish as finish


def test_resume_costs_retain_measured_work_and_reject_tampering(tmp_path):
    import hashlib
    import json
    from tools.diagnostics.wbgt_route_b_report import measured_timings

    root = tmp_path / "model"
    root.mkdir()
    frame = pd.DataFrame({"kind": ["cell", "unit"], "series": [0, 0], "seconds": [20., 25.]})
    initial = root / "initial_qdm_timings.csv"
    frame.to_csv(initial, index=False)
    frame.assign(seconds=0.).to_csv(root / "timings.csv", index=False)
    frame.iloc[:1].to_csv(root / "probe_timings.csv", index=False)
    (tmp_path / "solar_precision_cache_audit.json").write_text(json.dumps(
        {"initial_timings_sha256": hashlib.sha256(initial.read_bytes()).hexdigest()}))
    measured = measured_timings(root)
    assert len(measured) == 2 and measured.seconds.sum() == 45.
    initial.write_text(initial.read_text() + "\n")
    with pytest.raises(ValueError, match="Changed original QDM timing"):
        measured_timings(root)


def test_period_mean_requires_all_years_and_values():
    frame = pd.DataFrame({"unit_index": 0, "scenario": "historical", "method": "grid_first_series",
                          "year": range(1990, 2011), "mean_c": 30., "max_c": 35.,
                          "days_ge_28": 365., "days_ge_30": 180., "days_ge_32": 0.})
    result = period_metrics(frame).iloc[0]
    assert result.days_ge_32 == 0 and result.expected_years == 21
    assert np.isnan(period_metrics(frame.iloc[1:]).iloc[0].mean_c)
    frame.loc[0, "mean_c"] = np.nan
    result = period_metrics(frame).iloc[0]
    assert np.isnan(result.mean_c) and result.mean_c_valid_years == 20
    with pytest.raises(ValueError, match="Duplicate"):
        period_metrics(pd.concat([frame, frame.iloc[:1]]))


def test_gap_types_keep_semantics_separate():
    rows = []
    for method, value in (("region_first_series", 1.), ("grid_first_series", 3.), ("grid_first_cell_metrics", 7.)):
        rows.append({"unit_index": 0, "scenario": "historical", "period": "1990-2010", "method": method,
                     **{k: value for k in ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32")}})
    result = paired_gaps(pd.DataFrame(rows)).set_index("comparison")
    assert result.loc["aggregation_order", "days_ge_32"] == 2
    assert result.loc["cell_metric_vs_regional_series", "days_ge_32"] == 4


def test_cost_gate_requires_measurement_in_both_kinds():
    probe = {"total_cell_series": 1000, "total_unit_series": 500,
             "timings": [{"kind": "cell", "seconds": 2}, {"kind": "cell", "seconds": 4},
                         {"kind": "unit", "seconds": 3}]}
    assert finish.probe_hours(probe) == pytest.approx(5500 / 3600)
    probe["timings"][-1]["seconds"] = 0
    with pytest.raises(ValueError, match="No uncached unit"):
        finish.probe_hours(probe)


def test_input_inventory_contains_every_date_and_both_models(tmp_path):
    refs, drivers = finish.expected_inputs(tmp_path / "reference", tmp_path / "work")
    assert len(refs) == 10957 and len(set(refs)) == 10957
    assert len(drivers) == 304 and len(set(drivers)) == 304
    assert refs[0].name == "wbgtmax.1985-01-01.npz"
    assert refs[-1].name == "wbgtmax.2014-12-31.npz"


def test_w1_comparison_never_implies_matched_support(tmp_path):
    units = pd.DataFrame({"unit_index": [0], "state_name": ["Kerala"], "level": ["district"],
                          "district_name": ["Example"], "block_name": [""], "retained_fraction": [.99]})
    yearly = pd.DataFrame({"unit_index": [0], "year": [2005], "scenario": ["historical"],
                           "method": ["grid_first_cell_metrics"], "mean_c": [30.],
                           "days_ge_28": [300.], "days_ge_30": [100.], "days_ge_32": [10.]})
    w1 = units.drop(columns=["unit_index", "retained_fraction"]).assign(
        model="ACCESS-CM2", year=2005, scenario="historical", candidate="W1", run="baseline",
        wbgt_annual_mean_c=31., days_ge_28=310., days_ge_30=110., days_ge_32=20.,
        valid_fraction_of_full=1., method_signature="w1-original")
    w1.to_csv(tmp_path / "district_values.csv", index=False)
    w1.iloc[:0].to_csv(tmp_path / "block_values.csv", index=False)
    result = w1_comparison(yearly, units, "ACCESS-CM2", tmp_path).iloc[0]
    assert result.comparison_status == "DESCRIPTIVE_ONLY_SUPPORT_NOT_PROVEN_IDENTICAL"
    assert result.mean_delta_c == -1 and result.days_ge_32_delta == -10


def test_matched_w1_uses_identical_cells_and_gates_partial_polygon(tmp_path):
    import xarray as xr

    cells = pd.DataFrame({"cell": [0, 1], "lat": [10., 10.], "lon": [70., 70.25],
                          "mean_c": [31., 35.], "days_ge_28": [365., 365.],
                          "days_ge_30": [100., 200.], "days_ge_32": [10., 20.]})
    units = pd.DataFrame({"unit_index": [0, 1], "state_name": ["Kerala"] * 2,
                          "level": ["district"] * 2, "district_name": ["a", "b"],
                          "block_name": [""] * 2, "whole_area_m2": [1., 2.]})
    ds = xr.Dataset({k: (("lat", "lon"), [v]) for k, v in {
        "wbgt_annual_mean_c": [30., np.nan], "days_ge_28": [360., np.nan],
        "days_ge_30": [90., np.nan], "days_ge_32": [5., np.nan],
        "complete_year": [1., 0.], "valid_days": [365., 100.], "expected_days": [365., 365.]}.items()},
        coords={"lat": [10.], "lon": [70., 70.25]}, attrs={"method_signature": "synthetic-w1"})
    ds.to_netcdf(tmp_path / "cells_Kerala_ACCESS-CM2_2005_baseline.nc")
    result = matched_w1(cells, units, np.array([[1., 0.], [1., 1.]]), "ACCESS-CM2", tmp_path)
    baseline = result[result.variant == "baseline"].set_index("unit_index")
    assert baseline.loc[0, "status"] == "MATCHED_SPATIAL_SUPPORT_METHOD_COMPARISON"
    assert baseline.loc[0, "mean_c_delta"] == 1 and baseline.loc[0, "days_ge_32_delta"] == 5
    assert baseline.loc[1, "common_fraction_of_whole_polygon"] == .5
    assert baseline.loc[1, "status"] == "INSUFFICIENT_COMMON_SUPPORT"
    assert np.isnan(baseline.loc[1, "mean_c_delta"])
