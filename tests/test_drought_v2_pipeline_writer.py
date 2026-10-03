import pandas as pd

from tools.pipeline.compute_indices_multiprocess import _write_metric_rows_outputs


def test_writer_accepts_precomputed_period_rows(tmp_path, monkeypatch):
    monkeypatch.setattr("tools.pipeline.compute_indices_multiprocess.MIN_YEARS_ABSOLUTE", 5)
    rows = [{"district": "Demo", "year": 2000, "value": 3.0, "demo_value": 3.0}]
    period_rows = [
        {
            "district": "Demo",
            "period": "2000-2000",
            "value": 9.0,
            "demo_value": 9.0,
            "years_used_count": 1,
            "years_requested": 1,
            "retained_weight_fraction": 1.0,
        }
    ]
    result = _write_metric_rows_outputs(
        rows=rows,
        period_rows=period_rows,
        coverage_df=pd.DataFrame([{"unit_key": "Demo", "coverage_ok": True}]),
        metric_root_path=tmp_path,
        state_name="State",
        level="district",
        slug="demo",
        model="m1",
        scenario="historical",
        scenario_conf={"periods": {"2000-2000": (2000, 2000)}},
        value_col="demo_value",
        year_to_paths={2000: {"pr": tmp_path / "pr_2000.nc"}},
    )

    assert result == {"yearly_file_count": 1, "period_file_count": 1}
    out = pd.read_csv(tmp_path / "State" / "districts" / "Demo" / "m1" / "historical" / "Demo_periods.csv")
    assert float(out.loc[0, "value"]) == 9.0
    assert "retained_weight_fraction" in out.columns
