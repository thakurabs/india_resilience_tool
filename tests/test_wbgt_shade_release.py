"""Scientific and release-contract regression checks for shade peak correction."""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from india_resilience_tool.compute.wbgt import (
    shade_daily,
    shade_annual,
    saturation_pressure,
    require_shade_signature,
)


def test_national_release_roster_retains_missing_values():
    from tools.pipeline.build_shade_release import validate_yearly
    from india_resilience_tool.data.wbgt_contract import SHADE_METHOD_SIGNATURE

    frame = pd.DataFrame({"district_key": ["a", "a", "b", "b"],
                          "model": ["m"] * 4, "scenario": ["historical"] * 4,
                          "year": [2000, 2001, 2000, 2001], "value": [1, np.nan, 2, 3],
                          "shade_method_signature": [SHADE_METHOD_SIGNATURE] * 4})
    kwargs = dict(models=["m"], years={"historical": [2000, 2001]},
                  unit_column="district_key", units={"a", "b"})
    validate_yearly(frame, **kwargs)
    with pytest.raises(ValueError, match="Incomplete"):
        validate_yearly(frame.iloc[:-1], **kwargs)
    with pytest.raises(ValueError, match="Missing/duplicate"):
        validate_yearly(pd.concat([frame, frame.iloc[:1]]), **kwargs)


def inputs(n=365):
    time = pd.date_range("2001-01-01", periods=n)

    def a(v, u):
        return xr.DataArray(
            np.tile(v, (n, 1, 1)),
            dims=("time", "lat", "lon"),
            coords={"time": time, "lat": [10.0], "lon": [70.0, 71.0]},
            attrs={"units": u},
        )

    return a([25.0, 35.0], "degC"), a([30.0, 42.0], "degC"), a([90.0, 35.0], "%")


def test_formula_and_vapour_roundtrip():
    t, tx, rh = inputs(1)
    d = shade_daily(t, tx, rh)
    np.testing.assert_allclose(
        d.rh_at_tasmax * saturation_pressure(tx), rh * saturation_pressure(t)
    )
    r = float(d.rh_at_tasmax[0, 0, 0])
    temp = 30
    tw = (
        temp * np.arctan(0.151977 * np.sqrt(r + 8.313659))
        + np.arctan(temp + r)
        - np.arctan(r - 1.676331)
        + 0.00391838 * r**1.5 * np.arctan(0.023101 * r)
        - 4.686035
    )
    assert float(d.peak[0, 0, 0]) == pytest.approx(0.7 * tw + 0.3 * temp)
    kelvin = t + 273.15
    kelvin.attrs["units"] = "K"
    xr.testing.assert_allclose(shade_daily(kelvin, tx, rh).peak, d.peak)
    fraction = rh / 100
    fraction.attrs["units"] = "1"
    xr.testing.assert_allclose(shade_daily(t, tx, fraction).peak, d.peak)


def test_completeness_monotonicity_and_nan():
    t, tx, rh = inputs()
    d = shade_daily(t, tx, rh)
    a = shade_annual(d, 2001)
    assert bool((a.days_ge_28 >= a.days_ge_30).all())
    assert bool((a.days_ge_30 >= a.days_ge_32).all())
    d["peak"][0, 0, 0] = np.nan
    a = shade_annual(d, 2001)
    assert np.isnan(a.annual_mean[0, 0]) and np.isnan(a.days_ge_28[0, 0])
    assert a.valid_days[0, 0] == 364
    for n in (0, 1):
        a = shade_annual(shade_daily(*inputs(n)), 2001)
        assert bool(a.annual_mean.isnull().all())
    t[:] = np.nan
    assert bool(shade_annual(shade_daily(t, tx, rh), 2001).days_ge_28.isnull().all())


def test_alignment_physics_and_calendar():
    t, tx, rh = inputs(1)
    with pytest.raises(ValueError):
        shade_daily(t, tx.assign_coords(lon=[72.0, 73.0]), rh)
    tx[:] = t - 1
    assert bool(shade_daily(t, tx, rh).peak.isnull().all())
    with pytest.raises(ValueError, match="tasmax"):
        shade_daily(t, None, rh)
    t, tx, rh = inputs(366)
    for a in (t, tx, rh):
        a["time"] = pd.date_range("2000-01-01", periods=366)
    d = shade_daily(t, tx, rh)
    d["peak"].loc[dict(time="2000-02-29")] = np.nan
    assert bool((shade_annual(d, 2000).valid_days == 365).all())
    d["time"] = xr.date_range(
        "2000-01-01", periods=366, calendar="360_day", use_cftime=True
    )
    with pytest.raises(ValueError, match="calendar"):
        shade_annual(d, 2000)


def test_cross_path_parity_and_retention(tmp_path):
    from tools.pipeline import compute_indices_multiprocess as c
    from india_resilience_tool.compute.heat_stress_gridfirst import (
        compute_heat_stress_rows_for_metric,
    )

    t, tx, rh = inputs()
    paths = {}
    for key, a in zip(("tas", "tasmax", "hurs"), (t, tx, rh)):
        paths[key] = tmp_path / f"{key}.nc"
        a.rename(key).to_netcdf(paths[key])
    weights = pd.DataFrame(
        {"unit_key": ["A", "A"], "cell_index": [0, 1], "area_m2": [1.0, 3.0]}
    )
    mask = xr.DataArray(
        [[1.0, 3.0]], dims=("lat", "lon"), coords={"lat": t.lat, "lon": t.lon}
    )
    for key in ("annual_mean", "days_ge_28", "days_ge_30", "days_ge_32"):
        slug = "wbgt_shade_stull_" + key
        rows = compute_heat_stress_rows_for_metric(
            metric={"slug": slug, "value_col": "value"},
            model="M",
            scenario="historical",
            year_to_paths={2001: paths},
            weights=weights,
        )
        value = (
            c.wbgt_shade_stull_annual_mean(t, tx, rh, mask)
            if key == "annual_mean"
            else c.wbgt_shade_stull_days_ge_threshold(
                t, tx, rh, mask, thresh_c=int(key[-2:])
            )
        )
        assert rows[0]["value"] == pytest.approx(value)
        assert rows[0]["valid_area_fraction"] == 1


def test_stale_provenance_rejected():
    with pytest.raises(ValueError, match="signature"):
        require_shade_signature(pd.DataFrame({"value": [1]}), context="test")


def test_threshold_equality_and_no_humidity_floor():
    t, tx, rh = inputs()
    rh[:] = 1
    d = shade_daily(t, tx, rh)
    assert bool((d.rh_at_tasmax < 1).all())
    assert bool(d.stull_excursion.all())
    d["peak"][:] = 30
    a = shade_annual(d, 2001)
    assert bool((a.days_ge_30 == 365).all())
    assert bool((a.days_ge_32 == 0).all())


def test_temporal_failure_never_enters_idw(tmp_path, monkeypatch):
    from india_resilience_tool.compute import heat_stress_gridfirst as g

    t, tx, rh = inputs()
    t[0, 0, 0] = np.nan
    paths = {}
    for name, a in zip(("tas", "tasmax", "hurs"), (t, tx, rh)):
        path = tmp_path / f"{name}.nc"
        a.rename(name).to_netcdf(path)
        paths[name] = path
    w = pd.DataFrame(
        {"unit_key": ["failed", "valid"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]}
    )
    seen = []

    def fill(field, weights, **kwargs):
        seen.extend(weights.unit_key.tolist())
        return {"failed": 123.0}

    monkeypatch.setattr(g, "subcell_idw_fill", fill)
    rows = g.compute_heat_stress_rows_for_metric(
        metric={"slug": "wbgt_shade_stull_days_ge_28"},
        model="M",
        scenario="historical",
        year_to_paths={2001: paths},
        weights=w,
    )
    assert seen == ["failed", "valid"]
    assert next(r["idw_blocked_temporal"] for r in rows if r["district"] == "failed")
    assert {r["district"] for r in rows} == {"failed", "valid"}
    assert np.isnan(next(r["value"] for r in rows if r["district"] == "failed"))


def test_old_shade_cache_signature_and_markers(monkeypatch):
    from tools.pipeline import compute_indices_multiprocess as c
    from india_resilience_tool.compute.heat_stress_gridfirst import (
        _grid_sidecar,
        HEAT_STRESS_GRIDFIRST_METHOD_VERSION,
    )
    from india_resilience_tool.compute.wbgt import SHADE_METHOD_SIGNATURE

    kwargs = dict(
        model="M",
        scenario="historical",
        year=2001,
        grid_id="G",
        input_paths=[],
        value_col="value",
    )
    assert (
        _grid_sidecar(metric={"slug": "wbgt_shade_stull_annual_mean"}, **kwargs)[
            "method_version"
        ]
        == SHADE_METHOD_SIGNATURE
    )
    assert (
        _grid_sidecar(metric={"slug": "twb_annual_mean"}, **kwargs)["method_version"]
        == HEAT_STRESS_GRIDFIRST_METHOD_VERSION
    )
    task = c.ProcessingTask(
        metric_idx=0,
        slug="wbgt_shade_stull_annual_mean",
        model="M",
        scenario="historical",
        scenario_conf={},
        task_id=0,
        total_tasks=1,
    )
    monkeypatch.setattr(c, "_load_marker_json", lambda _: {"schema_version": 1})
    assert (
        c.task_completion_marker_status(task).reason
        == "compute_marker_shade_method_mismatch"
    )
    assert (
        c.ensemble_completion_marker_status(
            slug=task.slug, level="district", scope_name="Kerala"
        ).reason
        == "ensemble_marker_shade_method_mismatch"
    )


def test_missing_shade_survives_master_optimized_and_ensemble(tmp_path):
    from india_resilience_tool.compute.wbgt import SHADE_METHOD_SIGNATURE
    from tools.pipeline import compute_indices_multiprocess as c
    from tools.optimized.build_processed_optimised import _select_master_columns
    from india_resilience_tool.compute.master_builder import _build_wide_master

    source = pd.DataFrame(
        {
            "state": ["Kerala"],
            "district": ["D"],
            "scenario": ["historical"],
            "period": ["1990-2010"],
            "model": ["M"],
            "year": [2001],
            "value": [np.nan],
            "shade_method_signature": [SHADE_METHOD_SIGNATURE],
        }
    )
    master = _build_wide_master(
        source, "wbgt_shade_stull_annual_mean_C", "district", verbose=False
    )
    assert len(master) == 1
    assert (
        master["wbgt_shade_stull_annual_mean_C__historical__1990-2010__mean"]
        .isna()
        .all()
    )
    selected = _select_master_columns(
        master,
        slug="wbgt_shade_stull_annual_mean",
        level="district",
        supported_stats=["mean"],
    )
    assert (
        len(selected) == 1
        and selected.shade_method_signature.iloc[0] == SHADE_METHOD_SIGNATURE
    )
    clean, _ = c._clean_ensemble_yearly_frame(
        source, metadata_columns=set(), model_name="M"
    )
    assert clean is not None and np.isnan(clean.value.iloc[0])
    assert c._write_ensemble_stats([clean], tmp_path, "D") == 1
    out = pd.read_csv(tmp_path / "D_yearly_ensemble.csv")
    assert np.isnan(out.ensemble_mean.iloc[0]) and out.n_models.iloc[0] == 0


@pytest.mark.parametrize(
    "module",
    [
        "wbgt_shade_release",
        "wbgt_shade_inventory",
        "wbgt_shade_pilot",
        "wbgt_shade_nex",
    ],
)
def test_diagnostic_help_is_read_only(module):
    from importlib import import_module

    with pytest.raises(SystemExit) as exc:
        import_module("tools.diagnostics." + module).main(["--help"])
    assert exc.value.code == 0


def test_fixed_pressure_fixture():
    assert saturation_pressure(0.0) == pytest.approx(6.112)
    assert saturation_pressure(30.0) == pytest.approx(42.337239159, rel=1e-9)


def test_unrelated_grid_sidecar_remains_byte_equivalent():
    from india_resilience_tool.compute.heat_stress_gridfirst import _grid_sidecar

    sidecar = _grid_sidecar(
        metric={"slug": "twb_annual_mean"},
        model="M",
        scenario="historical",
        year=2001,
        grid_id="G",
        input_paths=[],
        value_col="value",
    )
    assert sidecar["methodology_note"] == (
        "Heat Stress v2 annual per-cell metric field before polygon aggregation; "
        "covers Twb, Shaded WBGT, and Outdoor sWBGT cell fields under method version "
        "heat-stress-v2-gridfirst-2."
    )
    assert "shade_method_signature" not in sidecar


def test_release_manifest_rejects_mixed_shade_versions(tmp_path):
    from tools.optimized.build_processed_optimised import _shade_release_signature
    from india_resilience_tool.data.wbgt_contract import (
        SHADE_SLUGS,
        SHADE_METHOD_SIGNATURE,
    )

    roots = []
    for slug in SHADE_SLUGS:
        path = (
            tmp_path
            / "processed_optimised"
            / "metrics"
            / slug
            / "masters"
            / "admin"
            / "district"
            / "state=Kerala.parquet"
        )
        path.parent.mkdir(parents=True)
        pd.DataFrame({"shade_method_signature": [SHADE_METHOD_SIGNATURE]}).to_parquet(
            path
        )
        roots.append(path)
    assert _shade_release_signature(tmp_path) == SHADE_METHOD_SIGNATURE
    pd.DataFrame({"value": [1]}).to_parquet(roots[0])
    with pytest.raises(ValueError, match="Mixed shade release"):
        _shade_release_signature(tmp_path)


def test_tree_size_streams_without_materializing_sizes(tmp_path):
    from tools.pipeline.build_shade_release import tree_size

    (tmp_path / "a/b").mkdir(parents=True)
    (tmp_path / "a/one.txt").write_bytes(b"x" * 10)
    (tmp_path / "a/b/two.txt").write_bytes(b"y" * 5)
    measured = tree_size(tmp_path, block_bytes=100)
    assert measured == {"bytes": 15, "files": 2, "reserved_bytes": 215,
                        "read_errors": 0, "root_absent": False, "complete": True}
    # A path that does not exist must measure as empty rather than raise mid-budget.
    assert tree_size(tmp_path / "absent")["files"] == 0


def test_rollback_measurement_is_cached_and_resumable(tmp_path, monkeypatch):
    import json
    from tools.pipeline import build_shade_release as runner
    from india_resilience_tool.data.wbgt_contract import SHADE_SLUGS

    data_dir = tmp_path / "data"
    for base in runner.ROLLBACK_BASES:
        for slug in SHADE_SLUGS:
            target = data_dir / base / slug
            target.mkdir(parents=True)
            (target / "f.parquet").write_bytes(b"z" * 7)
    cache = tmp_path / "rollback_sizes.json"
    first = runner.measure_rollback(data_dir, cache, remeasure=False)
    assert len(first["trees"]) == 2 * len(SHADE_SLUGS)
    assert cache.exists()

    calls = []
    original = runner.tree_size
    monkeypatch.setattr(runner, "tree_size",
                        lambda *a, **k: calls.append(a) or original(*a, **k))
    second = runner.measure_rollback(data_dir, cache, remeasure=False)
    assert not calls, "cached trees must not be walked again"
    assert second["reserved_bytes"] == first["reserved_bytes"]
    runner.measure_rollback(data_dir, cache, remeasure=True)
    assert len(calls) == 2 * len(SHADE_SLUGS), "--remeasure-rollback must re-walk every tree"

    # A cache truncated by an interruption keeps its completed trees and finishes the rest.
    partial = json.loads(cache.read_text())
    kept = dict(list(partial["trees"].items())[:3])
    cache.write_text(json.dumps({"data_root": partial["data_root"], "trees": kept}))
    calls.clear()
    resumed = runner.measure_rollback(data_dir, cache, remeasure=False)
    assert len(calls) == 2 * len(SHADE_SLUGS) - 3
    assert resumed["reserved_bytes"] == first["reserved_bytes"]


def test_stale_lock_is_broken_only_when_the_holder_is_gone(tmp_path, monkeypatch):
    import json
    import pytest
    from tools.pipeline import build_shade_release as runner

    lock = tmp_path / ".build.lock"
    lock.write_text(json.dumps({"pid": 4242, "started_unix": 0.0}))

    monkeypatch.setattr(runner, "process_is_running", lambda pid: True)
    with pytest.raises(RuntimeError, match="still running"):
        runner.acquire_lock(tmp_path, force=False)

    monkeypatch.setattr(runner, "process_is_running", lambda pid: None)
    with pytest.raises(RuntimeError, match="--force"):
        runner.acquire_lock(tmp_path, force=False)
    assert runner.acquire_lock(tmp_path, force=True) == lock

    lock.write_text(json.dumps({"pid": 4242, "started_unix": 0.0}))
    monkeypatch.setattr(runner, "process_is_running", lambda pid: False)
    assert json.loads(runner.acquire_lock(tmp_path, force=False).read_text())["pid"] == __import__("os").getpid()

    # An unreadable lock must not be treated as a live holder.
    lock.write_text("{not json")
    assert runner.acquire_lock(tmp_path, force=False) == lock


def test_resume_refuses_a_stage_built_from_another_specification(tmp_path):
    import pytest
    from tools.pipeline.build_shade_release import guard_build_spec
    from india_resilience_tool.data.wbgt_contract import SHADE_METHOD_SIGNATURE

    specification = {"signature": SHADE_METHOD_SIGNATURE, "states": ["Kerala"], "workers": 1,
                     "data_root": "/published/irt_data",
                     "boundary_source": {"districts": {"bytes": 10, "mtime_ns": 1}},
                     "roster": {"models": ["m1"], "required_years": {"historical": [2000]}}}
    guard_build_spec(tmp_path, specification)
    guard_build_spec(tmp_path, specification)  # unchanged resume is allowed

    for mutation in ({"states": ["Kerala", "Rajasthan"]},
                     {"roster": {"models": ["m1", "m2"], "required_years": {"historical": [2000]}}},
                     {"roster": {"models": ["m1"], "required_years": {"historical": [2000, 2001]}}},
                     {"data_root": "/published/other_data"},
                     {"boundary_source": {"districts": {"bytes": 99, "mtime_ns": 1}}}):
        with pytest.raises(ValueError, match="different specification"):
            guard_build_spec(tmp_path, {**specification, **mutation})


def test_input_preflight_names_absent_rostered_model_years(tmp_path):
    import pytest
    from tools.pipeline.build_shade_release import RAW_VARIABLES, preflight_inputs

    roster = {"models": ["m1", "m2"], "model_years": 4,
              "required_years": {"historical": [2000, 2001]}}
    for variable in RAW_VARIABLES:
        for model in roster["models"]:
            directory = tmp_path / "historical" / variable / model
            directory.mkdir(parents=True)
            for year in (2000, 2001):
                (directory / f"{year}.nc").write_bytes(b"")
    assert preflight_inputs(tmp_path, roster)["missing_count"] == 0

    (tmp_path / "historical" / RAW_VARIABLES[1] / "m2" / "2001.nc").unlink()
    with pytest.raises(ValueError, match="2001.nc"):
        preflight_inputs(tmp_path, roster)


def test_release_validation_rejects_empty_and_short_coverage():
    import pytest
    from tools.pipeline.build_shade_release import validate_yearly

    kwargs = dict(models=["m"], years={"historical": [2000]},
                  unit_column="district_key", units={"a"})
    empty = pd.DataFrame({"district_key": [], "model": [], "scenario": [], "year": [],
                          "shade_method_signature": []})
    with pytest.raises(ValueError, match="Empty yearly output"):
        validate_yearly(empty, **kwargs)


@pytest.fixture
def native_shade_stage(tmp_path):
    """Write compute's native layout, deliberately without master summaries."""
    import json
    from india_resilience_tool.data.wbgt_contract import SHADE_METHOD_SIGNATURE, SHADE_SLUGS

    data = tmp_path / "data"
    data.mkdir()
    (data / "states_4326.geojson").write_text(json.dumps({
        "type": "FeatureCollection", "features": []}))
    stage = tmp_path / "stage"
    roster = {"models": ["m1"], "required_years": {"historical": [2000, 2001]}}
    for level in ("district", "block"):
        properties = {"STATE_UT": "Kerala", "DISTRICT": "Test District"}
        if level == "block":
            properties["Sub_dist"] = "Test Block"
        boundary = {"type": "FeatureCollection", "features": [
            {"type": "Feature", "properties": properties,
             "geometry": {"type": "Point", "coordinates": [76, 10]}}]}
        (data / f"{level}s_4326.geojson").write_text(json.dumps(boundary))
        for slug in SHADE_SLUGS:
            parts = ["Test_District"] + (["Test_Block"] if level == "block" else [])
            directory = (stage / "processed" / slug / "Kerala" / f"{level}s").joinpath(*parts) / "m1" / "historical"
            directory.mkdir(parents=True)
            rows = pd.DataFrame({"year": [2000, 2001], "model": ["m1"] * 2,
                                 "scenario": ["historical"] * 2, "value": [np.nan, np.nan],
                                 "district": ["Test District"] * 2,
                                 "shade_method_signature": [SHADE_METHOD_SIGNATURE] * 2})
            if level == "block":
                rows["block"] = "Test Block"
            rows.to_csv(directory / f"{parts[-1]}_yearly.csv", index=False)
    return stage, data, roster


def test_state_compute_validation_matches_the_release_roster(native_shade_stage):
    from tools.pipeline.build_shade_release import validate_state_compute

    stage, data, roster = native_shade_stage
    assert not list(stage.rglob("state_yearly_model_averages_*.csv"))
    validate_state_compute(stage, "Kerala", roster, data_dir=data)


@pytest.mark.parametrize("level", ["district", "block"])
@pytest.mark.parametrize("defect,match", [
    ("missing_file", "Missing staged yearly output"),
    ("missing_unit", "Missing staged unit directory"),
    ("missing_model", "Model roster mismatch"),
    ("missing_scenario", "Scenario roster mismatch"),
    ("short", "Incomplete/duplicate"),
    ("empty", "Incomplete/duplicate"),
    ("duplicate", "Incomplete/duplicate"),
    ("stale", "stale shade signature"),
    ("identity", "identity mismatch"),
    ("columns", "Missing yearly columns"),
])
def test_native_compute_validation_rejects_invalid_outputs(native_shade_stage, level, defect, match):
    import shutil
    from tools.pipeline.build_shade_release import validate_state_compute

    stage, data, roster = native_shade_stage
    root = stage / "processed" / "wbgt_shade_stull_annual_mean" / "Kerala" / f"{level}s"
    path = next(root.rglob("*_yearly.csv"))
    if defect == "missing_file":
        path.unlink()
    elif defect == "missing_unit":
        shutil.rmtree(path.parents[2])
    elif defect == "missing_model":
        shutil.rmtree(path.parents[1])
    elif defect == "missing_scenario":
        shutil.rmtree(path.parent)
    else:
        frame = pd.read_csv(path)
        if defect == "short":
            frame = frame.iloc[:1]
        elif defect == "empty":
            frame = frame.iloc[:0]
        elif defect == "duplicate":
            frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
        elif defect == "stale":
            frame["shade_method_signature"] = "shade-peak-v0"
        elif defect == "identity":
            frame[level] = "Other unit"
        elif defect == "columns":
            frame = frame.drop(columns=["value"])
        frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match=match):
        validate_state_compute(stage, "Kerala", roster, data_dir=data)


def test_build_records_validation_failure_and_revalidates_on_resume(native_shade_stage, monkeypatch):
    import json
    from types import SimpleNamespace
    import tools.pipeline.build_shade_release as runner

    stage, data, roster = native_shade_stage
    specification = {"space_passed": True, "signature": runner.SHADE_METHOD_SIGNATURE,
                     "states": ["Kerala"], "workers": 1, "data_root": str(data),
                     "boundary_source": {}, "roster": roster}
    monkeypatch.setattr(runner, "preflight_inputs", lambda *a: {})
    calls = []

    def subprocess_run(arguments, **kwargs):
        calls.append(arguments)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(runner.subprocess, "run", subprocess_run)
    yearly = next(stage.rglob("*_yearly.csv"))
    original = yearly.read_bytes()
    yearly.unlink()
    (stage / "release_ready.json").write_text('{}')
    with pytest.raises(ValueError, match="Missing staged yearly output"):
        runner.build(data, stage, specification)
    status = json.loads((stage / "status.json").read_text())
    assert status["failed"] is True
    assert status["current_stage"] == "validate_compute_Kerala"
    assert "Missing staged yearly output" in status["error"]
    assert len(calls) == 1  # no downstream work after validation failure
    assert not (stage / "release_ready.json").exists()
    history = status["completed"]
    yearly.write_bytes(original)
    monkeypatch.setattr(runner, "validate_release", lambda *a: {"status": "validated"})
    runner.build(data, stage, specification)
    status = json.loads((stage / "status.json").read_text())
    assert status["failed"] is False
    assert status["completed"][:len(history)] == history
    assert any(e["stage"] == "validate_compute_Kerala" for e in status["completed"])
    for args in calls[:2]:
        assert "--skip-existing" in args
        assert args[args.index("--yearly-cleanup-policy") + 1] == "preserve"
    assert (stage / "release_ready.json").exists()


def test_dry_run_cannot_authorize_a_build():
    import pytest
    from tools.pipeline.build_shade_release import main

    with pytest.raises(SystemExit):
        main(["--data-dir", "d", "--stage", "s", "--dry-run", "--build"])


def shade_budget_evidence(tmp_path):
    """Write the minimum evidence and pilot tree that a budget run reads."""
    import json
    from india_resilience_tool.data.wbgt_contract import SHADE_METHOD_SIGNATURE, SHADE_SLUGS

    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "release_roster.json").write_text(json.dumps(
        {"models": ["m1"], "required_years": {"historical": [2000, 2001]}, "model_years": 2}))
    (evidence / "pilot_report.json").write_text(json.dumps(
        {"timings": [{"units": 7, "cached": False}], "model_years": 2,
         "estimated_national_seconds": 1000.0}))
    (evidence / "staged_downstream_timings.json").write_text(json.dumps(
        [{"stage": "Kerala_compute_district", "seconds": 10.0},
         {"stage": "masters", "seconds": 1.0}, {"stage": "optimized", "seconds": 1.0},
         {"stage": "state_values", "seconds": 1.0}, {"stage": "parity", "seconds": 1.0}]))
    pd.DataFrame({"state": ["Kerala"], "level": ["district"], "units": [14]}).to_csv(
        evidence / "published_baseline.csv", index=False)

    pilot = tmp_path / "pilot"
    yearly = (pilot / "processed_optimised/metrics" / sorted(SHADE_SLUGS)[0] /
              "yearly_models/admin/district")
    yearly.mkdir(parents=True)
    pd.DataFrame({"model": ["m1", "m1"], "scenario": ["historical"] * 2, "year": [2000, 2001],
                  "district_key": ["kerala|x"] * 2,
                  "shade_method_signature": [SHADE_METHOD_SIGNATURE] * 2}).to_parquet(
        yearly / "state=Kerala.parquet")
    (pilot / "boundaries.geojson").write_bytes(b"{}")
    return evidence, pilot


def test_budget_records_a_measuring_status_before_the_slow_rollback_walk(tmp_path, monkeypatch):
    """The published-tree walk takes minutes per tree; interrupting it must leave evidence."""
    import json
    import pytest
    from tools.pipeline import build_shade_release as runner

    evidence, pilot = shade_budget_evidence(tmp_path)
    stage = tmp_path / "stage"

    def explode(*args, **kwargs):
        raise KeyboardInterrupt("interrupted mid-walk")

    monkeypatch.setattr(runner, "measure_rollback", explode)
    with pytest.raises(KeyboardInterrupt):
        runner.budget(tmp_path / "data", stage, evidence, pilot)
    recorded = json.loads((stage / "budget.json").read_text())
    assert recorded["status"] == "measuring"
    assert recorded["signature"] == runner.SHADE_METHOD_SIGNATURE


def test_budget_derives_pilot_model_years_and_defers_space_on_dry_run(tmp_path):
    from tools.pipeline import build_shade_release as runner

    evidence, pilot = shade_budget_evidence(tmp_path)
    (tmp_path / "data").mkdir()
    result = runner.budget(tmp_path / "data", tmp_path / "stage", evidence, pilot, dry_run=True)
    assert result["pilot_model_years"] == 2, "measured from staged output, not a literal"
    assert result["measured_pilot_compute_seconds"] == 10.0
    assert result["measured_pilot_downstream_seconds"] == 4.0
    assert result["space_passed"] is None
    assert result["existing_release_rollback_reserved_bytes"] is None
    assert "DRY RUN" in result["limitations"]
    # units/pilot_units * model_years/pilot_model_years = 14/7 * 2/2
    assert result["model_year_unit_scale"] == pytest.approx(2.0)


def test_force_never_breaks_a_lock_whose_holder_is_alive(tmp_path, monkeypatch):
    """Two builds in one stage would interleave markers and delete each other's lock."""
    import json
    from tools.pipeline import build_shade_release as runner

    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / ".build.lock").write_text(json.dumps({"pid": 4242, "started_unix": 0.0}))
    monkeypatch.setattr(runner, "process_is_running", lambda pid: True)
    for force in (False, True):
        with pytest.raises(RuntimeError, match="still running"):
            runner.acquire_lock(stage, force=force)
    # The live holder's record survives both attempts.
    assert json.loads((stage / ".build.lock").read_text())["pid"] == 4242


def test_force_applies_only_where_liveness_cannot_be_probed(tmp_path, monkeypatch):
    import json
    from tools.pipeline import build_shade_release as runner

    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / ".build.lock").write_text(json.dumps({"pid": 4242, "started_unix": 0.0}))
    monkeypatch.setattr(runner, "process_is_running", lambda pid: None)
    with pytest.raises(RuntimeError, match="cannot be probed"):
        runner.acquire_lock(stage, force=False)
    lock = runner.acquire_lock(stage, force=True)
    assert json.loads(lock.read_text())["pid"] == os.getpid()


def test_lock_release_leaves_another_process_record_alone(tmp_path):
    """An unconditional unlink would release a lock this process no longer owns."""
    import json
    from tools.pipeline import build_shade_release as runner

    stage = tmp_path / "stage"
    stage.mkdir()
    lock = runner.acquire_lock(stage, force=False)
    lock.write_text(json.dumps({"pid": 4242, "started_unix": 0.0}))
    runner.release_lock(lock)
    assert lock.exists(), "released a lock belonging to another process"
    lock.write_text(json.dumps({"pid": os.getpid(), "started_unix": 0.0}))
    runner.release_lock(lock)
    assert not lock.exists()


def test_rollback_cache_is_not_reused_across_published_roots(tmp_path):
    """Cached sizes from another data directory would understate the rollback requirement."""
    import json
    from india_resilience_tool.data.wbgt_contract import SHADE_SLUGS
    from tools.pipeline import build_shade_release as runner

    def published(root: Path, payload: bytes) -> Path:
        for base in runner.ROLLBACK_BASES:
            for slug in sorted(SHADE_SLUGS):
                tree = root / base / slug
                tree.mkdir(parents=True)
                (tree / "part.csv").write_bytes(payload)
        return root

    small = published(tmp_path / "small", b"x" * 10)
    large = published(tmp_path / "large", b"x" * 1000)
    cache = tmp_path / "rollback_sizes.json"

    first = runner.measure_rollback(small, cache, remeasure=False)
    assert first["data_root"] == str(small.resolve())
    second = runner.measure_rollback(large, cache, remeasure=False)
    assert second["data_root"] == str(large.resolve())
    assert second["reserved_bytes"] > first["reserved_bytes"], "reused the other root's sizes"
    # A cache written before roots were recorded is unbound, so it is discarded too.
    cache.write_text(json.dumps({"trees": {"processed/x": {
        "reserved_bytes": 1, "measured_unix": 0.0, "complete": True}}}))
    assert runner.measure_rollback(large, cache, remeasure=False)["data_root"] == str(large.resolve())
    assert "processed/x" not in json.loads(cache.read_text())["trees"]


def test_tree_size_reports_an_unreadable_subtree_as_incomplete(tmp_path, monkeypatch):
    """An absent tree is legitimately zero; an unreadable one must not look small."""
    from tools.pipeline import build_shade_release as runner

    (tmp_path / "tree/blocked").mkdir(parents=True)
    (tmp_path / "tree/kept.csv").write_bytes(b"x" * 8)
    (tmp_path / "tree/blocked/hidden.csv").write_bytes(b"y" * 4096)

    real = os.scandir

    def blocked(path):
        if str(path).endswith("blocked"):
            raise PermissionError(13, "denied")
        return real(path)

    monkeypatch.setattr(runner.os, "scandir", blocked)
    measured = runner.tree_size(tmp_path / "tree")
    assert measured["complete"] is False and measured["read_errors"] == 1
    assert measured["files"] == 1, "the unreadable subtree is simply absent from the total"

    monkeypatch.undo()
    absent = runner.tree_size(tmp_path / "nothing-here")
    assert absent["complete"] is True and absent["root_absent"] is True
    assert runner.tree_size(tmp_path / "tree")["complete"] is True


def test_incomplete_rollback_measurement_withholds_the_space_verdict(tmp_path, monkeypatch):
    """A partial size sum must never clear a check that --build depends on."""
    from tools.pipeline import build_shade_release as runner

    evidence, pilot = shade_budget_evidence(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    real_tree_size = runner.tree_size

    def unreadable(root, **kwargs):
        if "wbgt_shade" in str(root):
            return {"bytes": 0, "files": 0, "reserved_bytes": 0,
                    "read_errors": 3, "root_absent": False, "complete": False}
        return real_tree_size(root, **kwargs)

    monkeypatch.setattr(runner, "tree_size", unreadable)
    result = runner.budget(data_dir, tmp_path / "stage", evidence, pilot)
    assert result["space_passed"] is None, "cleared a check on an understated requirement"
    assert result["rollback_measurement_complete"] is False
    assert result["rollback_incomplete_trees"], "must name the trees that failed to read"
    assert "INCOMPLETE ROLLBACK MEASUREMENT" in result["limitations"]


def test_preflight_inspects_the_archive_compute_will_read(tmp_path):
    """--source-root asserts the derived archive; it cannot substitute another one."""
    from india_resilience_tool.config.paths import get_paths_config
    from tools.pipeline import build_shade_release as runner

    data_dir = tmp_path / "data"
    derived = runner.compute_source_root(data_dir)
    assert derived.parent == data_dir
    assert derived.name == get_paths_config().data_root.name

    specification = {"space_passed": True, "signature": runner.SHADE_METHOD_SIGNATURE,
                     "states": ["Kerala"], "workers": 1,
                     "data_root": str(data_dir), "boundary_source": {},
                     "roster": {"models": ["m1"], "required_years": {"historical": [2000]},
                                "model_years": 1}}
    with pytest.raises(ValueError, match="is not the archive this build will read"):
        runner.build(data_dir, tmp_path / "stage", specification,
                     source_root=tmp_path / "somewhere-else")
