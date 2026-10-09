"""Offline contract tests for the national Route B outdoor-WBGT release.

CHG-0663/CHG-0664. No climate data, no network and no pinned environment: every
fixture is synthetic, so these run in the ordinary IRT environment and guard the
contract rather than the physics.

What they protect, in the words of the release specification:

* excluded units stay in the roster carrying missing values, never zero;
* count-first aggregation is not replaced by thresholding a spatial mean;
* threshold counts stay monotonic and inside the calendar;
* the declared model/scenario/year roster is complete, once each;
* the master periods are inclusive at both ends and count years used honestly;
* a partial roster is refused rather than published as an ensemble.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import wbgt_route_b_validate as validate
from tools.diagnostics import wbgt_route_b_campaign as campaign
from tools.pipeline import publish_route_b_outdoor as publish

METRIC_COLUMNS = ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32")


def _units(count: int = 3) -> pd.DataFrame:
    """A tiny roster: one district and two blocks beneath it."""
    return pd.DataFrame({
        "unit_index": list(range(count)),
        "state_name": ["Testland"] * count,
        "level": ["district"] + ["block"] * (count - 1),
        "district_name": ["Testpur"] * count,
        "block_name": [np.nan] + [f"Block{i}" for i in range(1, count)],
        "lgd_code": list(range(100, 100 + count)),
        "whole_area_m2": [3e9] + [1e9] * (count - 1),
        "grid_area_fraction": [1.0] * count,
    })


def _yearly(units: pd.DataFrame, excluded: set[int] = frozenset(),
            identical_estimands: bool = False) -> pd.DataFrame:
    """Build a complete, declared-envelope yearly table for both grid-first methods."""
    rows = []
    for unit in units.unit_index:
        for scenario, (first, last) in validate.PERIODS.items():
            for year in range(first, last + 1):
                for method in validate.GRID_FIRST_METHODS:
                    if unit in excluded:
                        values = {c: np.nan for c in METRIC_COLUMNS}
                    else:
                        offset = 0.0 if (identical_estimands
                                         or method == "grid_first_cell_metrics") else -3.0
                        values = {"mean_c": 27.0 + unit, "max_c": 36.0 + unit,
                                  "days_ge_28": 120.5 + offset, "days_ge_30": 80.25 + offset,
                                  "days_ge_32": 40.0 + offset}
                    rows.append({"unit_index": unit, "scenario": scenario, "year": year,
                                 "method": method, **values})
    return pd.DataFrame(rows)


def _write_model(root: Path, work: Path, units: pd.DataFrame, yearly: pd.DataFrame,
                 excluded: set[int] = frozenset(), model: str = "TEST-MODEL") -> Path:
    """Lay out one assembled model directory and its campaign work root."""
    work.mkdir(parents=True, exist_ok=True)
    units.to_csv(work / "units.csv", index=False)
    (work / "support_manifest.json").write_text(json.dumps({"signature_sha256": "sig0"}))
    model_dir = root / model
    model_dir.mkdir(parents=True, exist_ok=True)
    support = units.copy()
    support["retained_fraction"] = [0.2 if i in excluded else 0.99 for i in support.unit_index]
    support["retained_fraction_strict_reference"] = [
        0.1 if i in excluded else 0.98 for i in support.unit_index]
    support["eligible"] = ~support.unit_index.isin(excluded)
    support.to_csv(model_dir / "unit_support.csv", index=False)
    yearly.to_csv(model_dir / "yearly_metrics.csv", index=False)
    eligible = sorted(set(units.unit_index) - set(excluded))
    pd.DataFrame({"unit_index": eligible, "kind": ["grid_first"] * len(eligible),
                  "bias_c": [0.4, -0.2, 0.9][:len(eligible)],
                  "p95_error_c": [1.1, 0.8, 1.6][:len(eligible)]}
                 ).to_csv(model_dir / "holdout_scores.csv", index=False)
    (model_dir / "run_manifest.json").write_text(json.dumps({
        "signature_sha256": "fit0", "code_sha256": {"wbgt_route_b_run.py": "a"},
        "source_lock": {"carbonplan_commit": "f662b37"},
        "support": {"signature_sha256": "sig0"},
        "status": "PILOT_COMPLETE_DIAGNOSTIC_ONLY", "aggregation": "grid_first_only",
        "units_eligible": len(eligible), "units_total": len(units), "cells_eligible": 7,
        "warnings": ["Strange results could be returned when using dayofyear grouping "
                     "on data defined in the proleptic_gregorian calendar "],
    }))
    return model_dir


def test_complete_release_passes_every_structural_check(tmp_path: Path) -> None:
    units = _units()
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units,
                             _yearly(units, excluded={2}), excluded={2})
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "PASS", report["failed_checks"]
    names = {c["check"]: c for c in report["checks"]}
    assert names["missing_not_zero"]["excluded_units"] == 1
    assert names["leap_grouping"]["dayofyear_warnings_recorded"], "the warning must be surfaced"
    assert names["holdout"]["geographic_outliers"], "outliers must be named, not just summarised"


def test_zero_filled_exclusion_is_rejected(tmp_path: Path) -> None:
    """A missing unit that arrives as zero is the defect this release must never ship."""
    units = _units()
    yearly = _yearly(units, excluded={2})
    zeroed = (yearly.unit_index == 2)
    for column in METRIC_COLUMNS:
        yearly.loc[zeroed, column] = 0.0
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units, yearly,
                             excluded={2})
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "FAIL"
    assert "missing_not_zero" in report["failed_checks"]


def test_count_first_replaced_by_series_threshold_is_rejected(tmp_path: Path) -> None:
    """Thresholding a spatial mean is a different estimand and must not pass as counts."""
    units = _units()
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units,
                             _yearly(units, identical_estimands=True))
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "FAIL"
    assert "estimands_distinct" in report["failed_checks"]


def test_non_monotonic_thresholds_are_rejected(tmp_path: Path) -> None:
    units = _units()
    yearly = _yearly(units)
    # Target the released estimand: a defect in the retained comparator is not
    # what the release gate is protecting.
    released = yearly.index[yearly.method == validate.RELEASED_METHOD][0]
    yearly.loc[released, "days_ge_32"] = 999.0
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units, yearly)
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "FAIL"
    assert "values" in report["failed_checks"]


def test_short_year_envelope_is_rejected(tmp_path: Path) -> None:
    """A truncated scenario must fail the roster check, not be averaged over quietly."""
    units = _units()
    yearly = _yearly(units)
    yearly = yearly[~((yearly.scenario == "ssp585") & (yearly.year > 2070))]
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units, yearly)
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "FAIL"
    assert "roster" in report["failed_checks"]


def test_duplicated_rows_are_rejected(tmp_path: Path) -> None:
    units = _units()
    yearly = _yearly(units)
    model_dir = _write_model(tmp_path / "assembly", tmp_path / "work", units,
                             pd.concat([yearly, yearly.head(1)], ignore_index=True))
    report = validate.validate(model_dir, tmp_path / "work")
    assert report["verdict"] == "FAIL"
    assert "roster" in report["failed_checks"]


def test_absent_assembly_fails_rather_than_passing_silently(tmp_path: Path) -> None:
    work = tmp_path / "work"
    work.mkdir()
    _units().to_csv(work / "units.csv", index=False)
    (work / "support_manifest.json").write_text(json.dumps({"signature_sha256": "sig0"}))
    report = validate.validate(tmp_path / "assembly" / "ABSENT", work)
    assert report["verdict"] == "FAIL"
    assert "manifest" in report["failed_checks"]


def test_campaign_preflight_requires_radiation_only_inside_its_window(tmp_path: Path) -> None:
    """Radiation and wind exist for 1990-2010 alone in the historical period."""
    paths = campaign.required_inputs(tmp_path, "TEST-MODEL")
    names = {(p.parts[-4], p.parts[-3], p.stem) for p in paths}
    assert ("historical", "rsds", "1990") in names
    assert ("historical", "rsds", "2010") in names
    assert ("historical", "rsds", "1989") not in names
    assert ("historical", "rsds", "2011") not in names
    assert ("historical", "tas", "1985") in names
    assert ("ssp585", "sfcWind", "2080") in names
    radiation_path = campaign.driver_path(tmp_path, "M", "ssp245", 2050, "rsds")
    assert "nex_gddp_cmip6_v2_wbgt" in radiation_path.parts
    shade_path = campaign.driver_path(tmp_path, "M", "ssp245", 2050, "huss")
    assert "nex_gddp_cmip6_v2_wbgt" not in shade_path.parts


def test_campaign_lock_refuses_a_second_concurrent_run(tmp_path: Path) -> None:
    lock = campaign.Lock(tmp_path / "campaign.lock")
    with lock:
        with pytest.raises(SystemExit, match="already locked"):
            with campaign.Lock(tmp_path / "campaign.lock"):
                pass
    assert not (tmp_path / "campaign.lock").exists()


def test_campaign_state_survives_and_resumes(tmp_path: Path) -> None:
    path = tmp_path / "campaign_state.json"
    state = campaign.State(path)
    assert not state.done("M", "fit")
    state.record("M", "fit", verdict="PASS", series_fitted=10)
    assert campaign.State(path).done("M", "fit")
    state.record("M", "assemble", verdict="FAIL")
    assert not campaign.State(path).done("M", "assemble")


def test_published_periods_are_inclusive_and_count_years_used(tmp_path: Path) -> None:
    group = pd.DataFrame({"year": range(2020, 2081), "days_ge_30": 10.0})
    group.loc[group.year.isin([2021, 2022]), "days_ge_30"] = np.nan
    rows = publish._period_values(group, "days_ge_30", "ssp245")
    first = next(r for r in rows if r["period"] == "2020-2040")
    assert first["years_requested"] == 21, "inclusive at both ends"
    assert first["years_used_count"] == 19, "missing years are not counted as used"
    assert first["value"] == pytest.approx(10.0)
    empty = pd.DataFrame({"year": range(1990, 2011), "days_ge_30": np.nan})
    assert np.isnan(publish._period_values(empty, "days_ge_30", "historical")[0]["value"])


def test_publish_refuses_a_partial_roster(tmp_path: Path) -> None:
    """A nominal 19-model ensemble must never be built from a smaller available subset."""
    units = _units()
    assembly = tmp_path / "assembly"
    work = tmp_path / "work"
    _write_model(assembly, work, units, _yearly(units), model="MODEL-A")
    with pytest.raises(SystemExit, match="partial roster"):
        publish.main(["--run", "--assembly-dir", str(assembly), "--work-dir", str(work),
                      "--processed-root", str(tmp_path / "processed"),
                      "--models", "MODEL-A", "MODEL-B"])


def test_publish_refuses_a_failing_validation_report(tmp_path: Path) -> None:
    units = _units()
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    model_dir = _write_model(assembly, work, units, _yearly(units), model="MODEL-A")
    (model_dir / "validation_report.json").write_text(
        json.dumps({"verdict": "FAIL", "failed_checks": ["values"]}))
    with pytest.raises(ValueError, match="failing validation report"):
        publish.load_assembly(assembly, work, ["MODEL-A"])


def test_publish_refuses_mixed_eligibility_between_models(tmp_path: Path) -> None:
    """One ensemble must not average different spatial supports together."""
    units = _units()
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    _write_model(assembly, work, units, _yearly(units), model="MODEL-A")
    _write_model(assembly, work, units, _yearly(units, excluded={2}), excluded={2},
                 model="MODEL-B")
    with pytest.raises(ValueError, match="eligibility differs"):
        publish.load_assembly(assembly, work, ["MODEL-A", "MODEL-B"])


def test_published_tree_keeps_counts_fractional_and_exclusions_missing(tmp_path: Path) -> None:
    units = _units()
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    _write_model(assembly, work, units, _yearly(units, excluded={2}), excluded={2},
                 model="MODEL-A")
    root = tmp_path / "processed"
    job = ("Testland", "wbgt_outdoor_route_b_days_ge_30", "days_ge_30",
           "wbgt_outdoor_route_b_days_ge_30_days", str(assembly), str(work), str(root),
           ["MODEL-A"])
    result = publish._state_task(job)
    assert result["files"] > 0
    state_root = root / "wbgt_outdoor_route_b_days_ge_30" / "Testland"
    yearly = pd.read_csv(state_root / "districts" / "Testpur" / "MODEL-A" / "historical"
                         / "Testpur_yearly.csv")
    assert yearly.value.iloc[0] == pytest.approx(80.25), "counts stay fractional"
    assert set(yearly.year) == set(range(1990, 2011))
    assert (yearly.method_version == publish.METHOD_VERSION).all()
    assert (yearly.aggregation_method == publish.AGGREGATION_METHOD).all()

    master = pd.read_csv(state_root / "master_metrics_by_block.csv")
    column = "wbgt_outdoor_route_b_days_ge_30_days__historical__1990-2010__mean"
    excluded_row = master[master.block == "Block2"]
    assert len(excluded_row) == 1, "the excluded unit stays in the master roster"
    assert excluded_row[column].isna().all(), "and carries a missing value, not a zero"
    kept = master[master.block == "Block1"]
    assert kept[column].iloc[0] == pytest.approx(80.25)
    n_models = "wbgt_outdoor_route_b_days_ge_30_days__historical__1990-2010__n_models"
    assert excluded_row[n_models].iloc[0] == 0
    assert kept[n_models].iloc[0] == 1
    per_model = json.loads(kept[
        "wbgt_outdoor_route_b_days_ge_30_days__historical__1990-2010__values_per_model"].iloc[0])
    assert per_model == {"MODEL-A": pytest.approx(80.25)}
    assert json.loads(excluded_row[
        "wbgt_outdoor_route_b_days_ge_30_days__historical__1990-2010__values_per_model"].iloc[0]) == {}

    # The coverage and exclusion table must ship with the metric, naming a reason.
    coverage = pd.read_csv(state_root / "support_coverage.csv")
    assert len(coverage) == 3, "the complete roster travels with the metric"
    assert coverage.unit_coverage_gate.eq(0.95).all()
    excluded_cov = coverage[coverage.unit_index == 2]
    assert not excluded_cov.eligible.iloc[0]
    assert excluded_cov.exclusion_reason.iloc[0].startswith("coverage_below_gate_")
    assert (coverage.loc[coverage.eligible, "exclusion_reason"] == "eligible").all()
    assert {"grid_gap_fraction", "reference_gap_fraction",
            "retained_fraction_strict_reference"} <= set(coverage.columns)


def test_published_slugs_match_the_registry(tmp_path: Path) -> None:
    """The converter and the dashboard must agree on slug and value-column spelling."""
    from india_resilience_tool.config.metrics_registry import get_metric_spec

    for slug, (_, value_col, units_label) in publish.SLUGS.items():
        spec = get_metric_spec(slug)
        assert spec is not None, f"{slug} is not registered"
        assert spec.value_col == value_col, slug
        assert spec.units == units_label, slug


def test_count_tolerance_admits_rounding_but_not_a_real_overcount(tmp_path: Path) -> None:
    """The ULP tolerance must not become a loophole for a genuine over-count.

    Area weights sum to 1.0 only to floating-point precision, so a fully saturated
    unit-year lands a few ULP above 365. That must pass. A count a single day above
    the calendar must still fail.
    """
    units = _units()
    rounding = _yearly(units)
    released = rounding.index[rounding.method == validate.RELEASED_METHOD]
    rounding.loc[released, ["days_ge_28", "days_ge_30", "days_ge_32"]] = 365.0 + 1.1e-13
    model_dir = _write_model(tmp_path / "a1", tmp_path / "w1", units, rounding)
    report = validate.validate(model_dir, tmp_path / "w1")
    assert report["verdict"] == "PASS", report["failed_checks"]
    values = next(c for c in report["checks"] if c["check"] == "values")
    assert values["saturated_at_calendar_ceiling"]["days_ge_28"] > 0, \
        "saturation must be reported, not absorbed into the tolerance"

    overcount = _yearly(units)
    released = overcount.index[overcount.method == validate.RELEASED_METHOD]
    overcount.loc[released, "days_ge_28"] = 366.0
    model_dir = _write_model(tmp_path / "a2", tmp_path / "w2", units, overcount)
    report = validate.validate(model_dir, tmp_path / "w2")
    assert report["verdict"] == "FAIL"
    assert "values" in report["failed_checks"]


def test_partial_period_is_missing_not_a_one_year_climatology(tmp_path: Path) -> None:
    """A master period mean must require every year of its period.

    Window-wise QDM can blank 20 of a 21-year period for one model. Averaging the
    single survivor would put a one-year value in the master, where there is no
    years-used column to distinguish it from a real 21-year climatology.
    """
    units = _units()
    yearly = _yearly(units)
    # Blank 2020-2039 for unit 1, mirroring the loss of QDM window 1.
    blanked = ((yearly.unit_index == 1) & (yearly.scenario == "ssp245")
               & (yearly.year >= 2020) & (yearly.year <= 2039))
    yearly.loc[blanked, list(METRIC_COLUMNS)] = np.nan
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    _write_model(assembly, work, units, yearly, model="MODEL-A")
    root = tmp_path / "processed"
    publish._state_task(("Testland", "wbgt_outdoor_route_b_days_ge_30", "days_ge_30",
                         "wbgt_outdoor_route_b_days_ge_30_days", str(assembly), str(work),
                         str(root), ["MODEL-A"]))
    master = pd.read_csv(root / "wbgt_outdoor_route_b_days_ge_30" / "Testland"
                         / "master_metrics_by_block.csv")
    row = master[master.block == "Block1"]
    prefix = "wbgt_outdoor_route_b_days_ge_30_days__ssp245__"
    # 2020-2040 keeps only 2040, so it must be missing rather than a one-year mean.
    assert row[prefix + "2020-2040__mean"].isna().all()
    assert row[prefix + "2020-2040__n_models"].iloc[0] == 0
    # 2040-2060 and 2060-2080 are untouched and must still be published.
    assert row[prefix + "2040-2060__mean"].iloc[0] == pytest.approx(80.25)
    assert row[prefix + "2060-2080__mean"].iloc[0] == pytest.approx(80.25)
    # The period CSV still records the honest years-used count for the blanked window.
    periods = pd.read_csv(root / "wbgt_outdoor_route_b_days_ge_30" / "Testland" / "blocks"
                          / "Testpur" / "Block1" / "MODEL-A" / "ssp245" / "Block1_periods.csv")
    first = periods[periods.period == "2020-2040"]
    assert first.years_requested.iloc[0] == 21
    assert first.years_used_count.iloc[0] == 1


def test_admin_names_with_slashes_and_spaces_are_path_safe(tmp_path: Path) -> None:
    """Andhra Pradesh's `Pulicherla H/O Reddivaripalle` must not split into two directories.

    The incumbent processed tree sanitises path components with
    ``replace(" ", "_").replace("/", "_")`` while keeping the original name in the
    data columns, and the optimised builder globs that tree by position. A slash
    left in a path component creates a spurious directory level and a corrupt
    filename, which is how this surfaced on the real national roster.
    """
    units = _units(2)
    units.loc[0, "district_name"] = "Chittoor"
    units.loc[1, "district_name"] = "Chittoor"
    units.loc[1, "block_name"] = "Pulicherla H/O Reddivaripalle"
    units.loc[:, "state_name"] = "Andhra Pradesh"
    yearly = _yearly(units)
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    _write_model(assembly, work, units, yearly, model="MODEL-A")
    # A short root on purpose: pytest's own temp path plus these names exceeds
    # Windows' 260-character limit, which would mask the sanitisation this checks.
    root = Path(tempfile.mkdtemp(prefix="rb"))
    publish._state_task(("Andhra Pradesh", "wbgt_outdoor_route_b_days_ge_30", "days_ge_30",
                         "wbgt_outdoor_route_b_days_ge_30_days", str(assembly), str(work),
                         str(root), ["MODEL-A"]))
    # The state directory keeps its space; only district and block are sanitised.
    folder = (root / "wbgt_outdoor_route_b_days_ge_30" / "Andhra Pradesh" / "blocks"
              / "Chittoor" / "Pulicherla_H_O_Reddivaripalle" / "MODEL-A" / "historical")
    target = folder / "Pulicherla_H_O_Reddivaripalle_yearly.csv"
    assert target.is_file(), sorted(p.name for p in folder.parent.rglob("*"))
    assert not (root / "wbgt_outdoor_route_b_days_ge_30" / "Andhra Pradesh" / "blocks"
                / "Chittoor" / "Pulicherla H").exists(), "the slash must not create a level"
    assert not (root / "wbgt_outdoor_route_b_days_ge_30" / "Andhra_Pradesh").exists(), \
        "the state directory must keep its space, or the published state= key changes"
    # The data columns keep the original name, exactly as the incumbent tree does.
    rows = pd.read_csv(target)
    assert rows.block.iloc[0] == "Pulicherla H/O Reddivaripalle"
    master = pd.read_csv(root / "wbgt_outdoor_route_b_days_ge_30" / "Andhra Pradesh"
                         / "master_metrics_by_block.csv")
    assert "Pulicherla H/O Reddivaripalle" in set(master.block)
    assert publish.path_safe("A B/C") == "A_B_C"
    shutil.rmtree(root, ignore_errors=True)


# --- Yearly-ensemble tier (CHG-0669) ---------------------------------------
#
# The optimised builder sources its ``yearly_ensemble`` artifacts from per-unit
# CSVs under ``<state>/districts/ensembles/...``. A metric that omits them
# publishes no ensemble trend, which the strict parity audit does not catch
# because no ensemble job is ever queued. These guard the layout the builder
# discovers, and guard the honesty of the contributing-model count.


def _ensemble_unit(level: str = "district") -> pd.Series:
    return pd.Series({"level": level, "district_name": "Testpur",
                      "block_name": "Testblock" if level == "block" else ""})


def _ensemble_frame(values_by_model: dict[str, list[float]], years=(1990, 1991)) -> pd.DataFrame:
    rows = []
    for model, values in values_by_model.items():
        for year, value in zip(years, values):
            rows.append({"model": model, "scenario": "historical", "year": year, "mean_c": value})
    return pd.DataFrame(rows)


def test_ensemble_files_land_where_the_builder_discovers_them(tmp_path: Path) -> None:
    from india_resilience_tool.data.discovery import (
        iter_block_yearly_ensemble_files,
        iter_district_yearly_ensemble_files,
    )

    root = tmp_path / "Testland"
    frame = _ensemble_frame({"ModelA": [27.0, 27.5], "ModelB": [28.0, 28.5]})
    publish._write_unit_ensemble_files(root, _ensemble_unit("district"), frame, "mean_c")
    publish._write_unit_ensemble_files(root, _ensemble_unit("block"), frame, "mean_c")

    found_d = iter_district_yearly_ensemble_files(ts_root=tmp_path, state_dir="Testland")
    found_b = iter_block_yearly_ensemble_files(ts_root=tmp_path, state_dir="Testland")
    assert [(d, s) for d, s, _ in found_d] == [("Testpur", "historical")]
    assert [(d, b, s) for d, b, s, _ in found_b] == [("Testpur", "Testblock", "historical")]


def test_ensemble_statistics_match_numpy_and_count_real_models(tmp_path: Path) -> None:
    root = tmp_path / "Testland"
    frame = _ensemble_frame({"ModelA": [27.0, 27.5], "ModelB": [28.0, np.nan],
                             "ModelC": [29.0, 28.5]})
    publish._write_unit_ensemble_files(root, _ensemble_unit(), frame, "mean_c")
    out = pd.read_csv(root / "districts" / "ensembles" / "Testpur" / "historical"
                      / "Testpur_yearly_ensemble.csv").set_index("year")

    assert out.loc[1990, "n_models"] == 3
    # The model that is missing in 1991 must not be counted, and must not be
    # read back as a zero that would drag the mean down.
    assert out.loc[1991, "n_models"] == 2
    assert out.loc[1990, "ensemble_mean"] == pytest.approx(28.0)
    assert out.loc[1991, "ensemble_mean"] == pytest.approx(28.0)
    assert out.loc[1990, "ensemble_std"] == pytest.approx(np.std([27.0, 28.0, 29.0], ddof=1))
    assert out.loc[1990, "ensemble_median"] == pytest.approx(28.0)


def test_ensemble_of_an_excluded_unit_is_blank_not_zero(tmp_path: Path) -> None:
    root = tmp_path / "Testland"
    frame = _ensemble_frame({"ModelA": [np.nan, np.nan], "ModelB": [np.nan, np.nan]})
    publish._write_unit_ensemble_files(root, _ensemble_unit(), frame, "mean_c")
    path = root / "districts" / "ensembles" / "Testpur" / "historical" / "Testpur_yearly_ensemble.csv"
    text = path.read_text()
    out = pd.read_csv(path)

    assert list(out.n_models) == [0, 0]
    assert out.ensemble_mean.isna().all()
    assert ",0.0," not in text and not text.rstrip().endswith("0.0")


def test_single_model_ensemble_reports_blank_spread_not_zero(tmp_path: Path) -> None:
    root = tmp_path / "Testland"
    frame = _ensemble_frame({"ModelA": [27.0, 27.5]})
    publish._write_unit_ensemble_files(root, _ensemble_unit(), frame, "mean_c")
    out = pd.read_csv(root / "districts" / "ensembles" / "Testpur" / "historical"
                      / "Testpur_yearly_ensemble.csv")

    assert list(out.n_models) == [1, 1]
    assert out.ensemble_std.isna().all()
    assert out.ensemble_mean.tolist() == [27.0, 27.5]


def test_path_preflight_accounts_for_the_ensemble_tier(tmp_path: Path) -> None:
    units = _units(3)
    report = publish.longest_path_preflight(tmp_path, units, list(publish.SLUGS),
                                            ["EC-Earth3-Veg-LR"])
    assert report["longest_path_chars"] > 0
    assert report["headroom_chars"] == publish.WINDOWS_MAX_PATH - report["longest_path_chars"]


def test_master_carries_the_per_unit_reason_a_value_is_missing(tmp_path: Path) -> None:
    """An excluded unit must be able to say *why* it is blank, not only that it is.

    The exclusion reason previously existed only in ``support_coverage.csv``, which
    ships inside the metric tree but which no part of the application reads. It now
    travels in the master, where the dashboard already looks (CHG-0674).
    """
    from india_resilience_tool.analysis.map_enrichment import format_exclusion_reason
    from tools.optimized.build_processed_optimised import _select_master_columns

    units = _units()
    assembly, work = tmp_path / "assembly", tmp_path / "work"
    _write_model(assembly, work, units, _yearly(units, excluded={2}), excluded={2},
                 model="MODEL-A")
    root = tmp_path / "processed"
    slug = "wbgt_outdoor_route_b_annual_mean"
    job = ("Testland", slug, "mean_c", "wbgt_outdoor_route_b_annual_mean_C",
           str(assembly), str(work), str(root), ["MODEL-A"])
    publish._state_task(job)
    state_root = root / slug / "Testland"

    master = pd.read_csv(state_root / "master_metrics_by_block.csv")
    for column in publish.COVERAGE_MASTER_COLUMNS:
        assert column in master.columns, column

    excluded = master[master.block == "Block2"]
    assert excluded.exclusion_reason.iloc[0].startswith("coverage_below_gate_")
    assert excluded.support_gap_kind.iloc[0] in {"grid", "reference"}
    assert 0.0 <= float(excluded.support_coverage.iloc[0]) < 0.95
    # It renders as a sentence a reader can act on, with the retained share.
    text = format_exclusion_reason(
        excluded.exclusion_reason.iloc[0], coverage=excluded.support_coverage.iloc[0]
    )
    assert text.startswith("No data:") and "%" in text

    kept = master[master.block == "Block1"]
    assert kept.exclusion_reason.iloc[0] == "eligible"
    assert kept.support_gap_kind.iloc[0] == "none"
    # An eligible unit produces no reason text, so the tooltip shows no row for it.
    assert format_exclusion_reason(kept.exclusion_reason.iloc[0]) == ""

    # And the columns survive the optimized builder's column selection, which is
    # where every previous attempt at carrying provenance was silently dropped.
    out = _select_master_columns(
        master,
        slug=slug,
        level="block",
        supported_stats=["mean", "median"],
        retained_stats=["n_models"],
        retained_columns=publish.COVERAGE_MASTER_COLUMNS,
    )
    for column in publish.COVERAGE_MASTER_COLUMNS:
        assert column in out.columns, column
