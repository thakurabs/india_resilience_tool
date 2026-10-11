"""CarbonPlan shade publication: staged processed tree, builder guard, cutover journal."""
from __future__ import annotations

import gzip
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from india_resilience_tool.data.wbgt_contract import (
    CARBONPLAN_SHADE_SIGNATURE,
    LEGACY_SHADE_IDENTITY,
    SHADE_IDENTITY_SIDECAR,
    SHADE_SLUGS,
    shade_artifact_identity,
    shade_identity_description,
    shade_sources_identity,
)
from tools.pipeline import cutover_carbonplan_shade as cutover
from tools.pipeline import publish_carbonplan_shade as publisher

SLUGS = sorted(SHADE_SLUGS)


# --------------------------------------------------------------------------- publisher


def _stage_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Two district units, one eligible and one excluded, one model, full envelope."""
    work = tmp_path / "work"
    stage = tmp_path / "stage"
    units = pd.DataFrame({"unit_index": [0, 1], "state_name": ["Goa", "Goa"],
                          "level": ["district", "district"],
                          "district_name": ["North Goa", "South Goa"], "block_name": [np.nan, np.nan]})
    work.mkdir()
    units.to_csv(work / "units.csv", index=False)
    support = units.assign(lgd_code=[1, 2], grid_area_fraction=[1.0, 1.0],
                           retained_fraction=[0.99, 0.6], retained_fraction_strict_reference=[0.99, 0.6],
                           eligible=[True, False])
    (work / "assembly" / "M1").mkdir(parents=True)
    support.to_csv(work / "assembly" / "M1" / "unit_support.csv", index=False)
    rows = []
    for scenario, (first, last) in publisher.ENVELOPE.items():
        for year in range(first, last + 1):
            for unit in (0, 1):
                value = np.nan if unit == 1 else 30.0 + (year - 1985) * 0.01
                rows.append({"unit_index": unit, "scenario": scenario, "year": year,
                             "mean_c": value, "days_ge_28": value, "days_ge_30": value,
                             "days_ge_32": np.nan if unit == 1 else 0.5})
    (stage / "M1").mkdir(parents=True)
    yearly = stage / "M1" / "yearly_metrics.csv.gz"
    pd.DataFrame(rows).to_csv(yearly, index=False, compression="gzip")
    status = {"status": "PASS", "identity": {"method": publisher.STAGE_METHOD, "qdm_signature": "q"},
              "outputs": {"yearly_metrics.csv.gz": publisher._digest(yearly)}}
    (stage / "M1" / "status.json").write_text(json.dumps(status))
    return stage, work


def test_publisher_signs_every_processed_file_and_keeps_excluded_units_blank(tmp_path):
    stage, work = _stage_fixture(tmp_path)
    root = tmp_path / "processed"
    args = type("Args", (), {"stage": stage, "work_dir": work, "processed_root": root,
                             "models": ["M1"], "workers": 1, "states": None})()
    manifest = publisher.publish(args)
    assert manifest["units_excluded"] == 1
    assert manifest["shade_method_signature"] == CARBONPLAN_SHADE_SIGNATURE
    for slug in SLUGS:
        yearly = pd.read_csv(root / slug / "Goa" / "districts" / "North_Goa" / "M1" / "historical"
                             / "North_Goa_yearly.csv")
        assert yearly.year.min() == 1985 and yearly.year.max() == 2014
        assert yearly.shade_method_signature.eq(CARBONPLAN_SHADE_SIGNATURE).all()
        ensemble = pd.read_csv(root / slug / "Goa" / "districts" / "ensembles" / "North_Goa"
                               / "ssp585" / "North_Goa_yearly_ensemble.csv")
        assert ensemble.shade_method_signature.eq(CARBONPLAN_SHADE_SIGNATURE).all()
        master = pd.read_csv(root / slug / "Goa" / "master_metrics_by_district.csv")
        assert shade_artifact_identity(root / slug / "Goa" / "master_metrics_by_district.csv") == CARBONPLAN_SHADE_SIGNATURE
        south = master.set_index("district").loc["South Goa"]
        value_cols = [c for c in master.columns if c.endswith("__mean")]
        assert south[value_cols].isna().all()
        assert south["exclusion_reason"].startswith("coverage_below_gate")


def test_publisher_refuses_a_stage_whose_output_changed(tmp_path):
    stage, _ = _stage_fixture(tmp_path)
    with gzip.open(stage / "M1" / "yearly_metrics.csv.gz", "at") as stream:
        stream.write("0,historical,1984,1,1,1,1\n")
    with pytest.raises(ValueError, match="hash changed"):
        publisher.verify_stage(stage, ["M1"])


# --------------------------------------------------------------------------- builder guard


def test_shade_release_build_refuses_a_root_with_the_legacy_sidecar(tmp_path):
    from tools.optimized.build_processed_optimised import _validate_shade_release_target

    _validate_shade_release_target(tmp_path, metrics=SLUGS, full_rebuild=False)
    with pytest.raises(ValueError, match="exactly the four"):
        _validate_shade_release_target(tmp_path, metrics=SLUGS[:2], full_rebuild=False)
    master = tmp_path / "metrics" / SLUGS[0] / "masters" / "admin" / "district" / "state=Goa.parquet"
    master.parent.mkdir(parents=True)
    pd.DataFrame({"district": ["x"]}).to_parquet(master)
    with pytest.raises(ValueError, match="not CarbonPlan-signed"):
        _validate_shade_release_target(tmp_path, metrics=SLUGS, full_rebuild=False)
    (tmp_path / SHADE_IDENTITY_SIDECAR).write_text("{}")
    with pytest.raises(ValueError, match="legacy shade identity sidecar"):
        _validate_shade_release_target(tmp_path, metrics=SLUGS, full_rebuild=False)


def test_identity_description_follows_the_verified_artifact():
    target = "registry text"
    assert shade_identity_description(CARBONPLAN_SHADE_SIGNATURE, target) == target
    assert "0.7 × Stull" in shade_identity_description(LEGACY_SHADE_IDENTITY, target)
    assert shade_identity_description(None, target) == target


# --------------------------------------------------------------------------- cutover


def _signed(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"shade_method_signature": [CARBONPLAN_SHADE_SIGNATURE]}).to_parquet(path)


def _cutover_fixture(tmp_path: Path) -> tuple[Path, Path]:
    data = tmp_path / "irt_data"
    bundle = data / "processed_optimised"
    staged = tmp_path / "staged_bundle"
    optimized_files = {}
    for slug in SLUGS:
        for tier in cutover.TIERS:
            legacy = bundle / "metrics" / slug / tier / "admin" / "district" / "state=Goa.parquet"
            legacy.parent.mkdir(parents=True)
            legacy.write_bytes(f"legacy {slug} {tier}".encode())
            optimized_files[legacy.relative_to(bundle).as_posix()] = cutover.sha256(legacy)
            _signed(staged / "metrics" / slug / tier / "admin" / "district" / "state=Goa.parquet")
        (data / "processed" / slug / "Goa").mkdir(parents=True)
        (data / "processed" / slug / "Goa" / "master_metrics_by_district.csv").write_text("legacy")
    (bundle / "bundle_manifest.json").write_text('{"old": true}')
    (bundle / SHADE_IDENTITY_SIDECAR).write_text(json.dumps(
        {"schema": 1, "method": LEGACY_SHADE_IDENTITY, "optimized_files": optimized_files,
         "processed_master_files": {}}))
    return data, staged


def _args(**kwargs):
    base = {"staged_bundle": None, "staged_processed": None, "processed_mode": "aside",
            "max_downtime_minutes": 30.0, "audit_minutes": 1.0, "run": True, "signed_off_by": None}
    base.update(kwargs)
    return type("Args", (), base)()


def _fake_manifest(data_dir: Path) -> dict:
    path = data_dir / "processed_optimised" / "bundle_manifest.json"
    path.write_text(json.dumps({"shade_method_signature": CARBONPLAN_SHADE_SIGNATURE}))
    return {"path": str(path)}


def test_cutover_swaps_journals_and_rolls_back_to_verified_legacy(tmp_path, monkeypatch):
    import tools.optimized.build_processed_optimised as builder

    data, staged = _cutover_fixture(tmp_path)
    monkeypatch.setattr(cutover, "time_manifest_rescan", lambda: 1.0)
    monkeypatch.setattr(builder, "write_manifest_only", _fake_manifest)
    release = cutover.Release(data, "r1")
    prepared = cutover.prepare(_args(staged_bundle=staged), release)
    assert prepared["status"] == "PASS"
    # Prepare touched no live path.
    assert shade_sources_identity(sorted((data / "processed_optimised" / "metrics").rglob("*.parquet"))) == LEGACY_SHADE_IDENTITY

    journal = cutover.swap(_args(), release)
    assert journal["status"] == "PUBLISHED"
    live = sorted((data / "processed_optimised" / "metrics").rglob("*.parquet"))
    assert shade_sources_identity(live) == CARBONPLAN_SHADE_SIGNATURE
    assert not (data / "processed" / SLUGS[0]).exists()
    with pytest.raises(ValueError, match="cannot close before"):
        cutover.close(_args(signed_off_by="owner"), release)

    restored = cutover.rollback(_args(), release)
    assert restored["status"] == "ROLLED_BACK"
    assert (data / "processed_optimised" / "bundle_manifest.json").read_text() == '{"old": true}'
    assert (data / "processed" / SLUGS[0] / "Goa" / "master_metrics_by_district.csv").read_text() == "legacy"
    live = sorted((data / "processed_optimised" / "metrics").rglob("*.parquet"))
    assert shade_sources_identity(live) == LEGACY_SHADE_IDENTITY


def test_interrupted_swap_rolls_back_from_the_journal(tmp_path, monkeypatch):
    data, staged = _cutover_fixture(tmp_path)
    monkeypatch.setattr(cutover, "time_manifest_rescan", lambda: 1.0)
    release = cutover.Release(data, "r2")
    cutover.prepare(_args(staged_bundle=staged), release)
    real_rename = os.rename
    calls = {"n": 0}

    def flaky(source, target):
        calls["n"] += 1
        if calls["n"] == 4:  # second slug's live tree is "open" in the dashboard
            raise PermissionError("in use")
        real_rename(source, target)

    monkeypatch.setattr(cutover.os, "rename", flaky)
    with pytest.raises(PermissionError):
        cutover.swap(_args(), release)
    monkeypatch.setattr(cutover.os, "rename", real_rename)
    journal = json.loads(release.journal.read_text())
    assert [entry["state"] for entry in journal["renames"]][-1] == "intent"
    cutover.rollback(_args(), release)
    live = sorted((data / "processed_optimised" / "metrics").rglob("*.parquet"))
    assert shade_sources_identity(live) == LEGACY_SHADE_IDENTITY


def test_prepare_aborts_when_downtime_estimate_is_too_long(tmp_path, monkeypatch):
    data, staged = _cutover_fixture(tmp_path)
    monkeypatch.setattr(cutover, "time_manifest_rescan", lambda: 3600.0)
    release = cutover.Release(data, "r3")
    record = cutover.prepare(_args(staged_bundle=staged), release)
    assert record["status"] == "ABORT_DOWNTIME"
    with pytest.raises(ValueError, match="replan"):
        cutover.swap(_args(), release)


def test_prepare_refuses_live_trees_that_drifted_from_the_sidecar(tmp_path, monkeypatch):
    data, staged = _cutover_fixture(tmp_path)
    drifted = next((data / "processed_optimised" / "metrics").rglob("*.parquet"))
    drifted.write_bytes(b"rebuilt by a routine run")
    with pytest.raises(ValueError, match="differ from the identity sidecar"):
        cutover.prepare(_args(staged_bundle=staged), cutover.Release(data, "r4"))


# --------------------------------------------------------------------------- state headline


def test_state_coverage_note_reports_contributing_units_and_area():
    from india_resilience_tool.analysis.area_weighting import AREA_WEIGHT_COL
    from india_resilience_tool.app.views.state_summary_view import state_coverage_note

    frame = pd.DataFrame({"m__ssp245__2020-2040__mean": [1.0, np.nan, 3.0],
                          AREA_WEIGHT_COL: [30.0, 10.0, 60.0],
                          "exclusion_reason": ["eligible", "coverage_below_gate_grid_gap_dominant", "eligible"]})
    note = state_coverage_note(frame, "m__ssp245__2020-2040__mean")
    assert "2 of 3 units" in note and "90%" in note
    assert state_coverage_note(frame.drop(columns="exclusion_reason"), "m__ssp245__2020-2040__mean") == ""
