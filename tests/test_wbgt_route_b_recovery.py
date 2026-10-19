"""Prevent solar recovery from accepting unrelated code or stale pilot outputs."""
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


def module(name):
    path = Path(__file__).resolve().parents[1] / "docs/diagnostics/wbgt_outdoor_route_b_pilot/scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def test_cache_recovery_allows_only_solar_function_change():
    recovery = module("recover_solar_precision")
    before = b"def fit_series():\n return 1\ndef sun_adjustments():\n return 2\n"
    solar_change = before.replace(b"return 2", b"return 3")
    fit_change = before.replace(b"return 1", b"return 3")
    assert recovery.without_solar(before) == recovery.without_solar(solar_change)
    assert recovery.without_solar(before) != recovery.without_solar(fit_change)


@pytest.mark.skipif(__import__("sys").platform == "win32", reason="recovery coordinator uses Linux flock")
def test_recovery_checks_access_before_running_second_model(tmp_path, monkeypatch):
    recovery = module("finish_recovery")
    monkeypatch.setattr(recovery, "REPO", tmp_path)
    work = tmp_path / "scratch/wbgt_route_b"
    out = tmp_path / "docs/diagnostics/wbgt_outdoor_route_b_pilot"
    work.mkdir(parents=True)
    (out / "ACCESS-CM2").mkdir(parents=True)
    (out / "CMCC-CM2-SR5").mkdir()
    (work / "finish_verify_reference.log").write_text("Verified 10957 dates")
    (out / "solar_precision_cache_audit.json").write_text(json.dumps({
        "only_solar_function_changed": True, "cache_count": 1,
        "arrays_unchanged": [{}], "new_signature": "new"}))
    artifact = out / "ACCESS-CM2/output.csv"
    artifact.write_text("original")
    (out / "ACCESS-CM2/run_manifest.json").write_text(json.dumps({
        "status": "PILOT_COMPLETE_DIAGNOSTIC_ONLY", "signature_sha256": "new",
        "artifact_sha256": {"output.csv": hashlib.sha256(artifact.read_bytes()).hexdigest()}}))
    (out / "CMCC-CM2-SR5/probe.json").write_text(json.dumps({
        "total_cell_series": 1, "total_unit_series": 1,
        "timings": [{"kind": "cell", "seconds": 1}, {"kind": "unit", "seconds": 1}]}))
    calls = []
    monkeypatch.setattr(recovery.subprocess, "run", lambda args, **kwargs: calls.append(args))
    recovery.finish()
    assert len(calls) == 3
    assert json.loads((out / "execution_status.json").read_text())["status"] == "PILOT_COMPLETE_REVIEW_REQUIRED"
    artifact.write_text("changed")
    calls.clear()
    with pytest.raises(ValueError, match="Changed ACCESS output"):
        recovery.finish()
    assert calls == []
    assert json.loads((out / "execution_status.json").read_text())["status"] == "FAILED"
