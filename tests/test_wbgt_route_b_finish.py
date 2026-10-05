"""The completion chain must stop on failed science checks and record why."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tools.diagnostics import wbgt_route_b_finish as coordinator


@pytest.mark.parametrize("fail_smoke", [False, True])
def test_completion_dependency_order_and_failure_status(tmp_path, monkeypatch, fail_smoke):
    # POSIX locking is exercised by the real Linux coordinator; allow this
    # subprocess/state contract test to run in IRT's Windows test environment.
    monkeypatch.setitem(sys.modules, "fcntl", SimpleNamespace(LOCK_EX=1, LOCK_NB=2, flock=lambda *args: None))
    monkeypatch.setattr(coordinator, "expected_inputs", lambda *args: ([], []))
    calls = []

    def execute(command, **kwargs):
        module = command[command.index("-m") + 1]
        calls.append((module, list(command)))
        if "--smoke" in command and fail_smoke:
            raise subprocess.CalledProcessError(1, command)
        if "--probe" in command:
            out = Path(command[command.index("--out-dir") + 1])
            model = command[command.index("--model") + 1]
            (out / model).mkdir(parents=True)
            probe = {"total_cell_series": 3, "total_unit_series": 3,
                     "timings": [{"kind": "cell", "seconds": 1}, {"kind": "unit", "seconds": 2}]}
            (out / model / "probe.json").write_text(json.dumps(probe))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(coordinator.subprocess, "run", execute)
    args = argparse.Namespace(work_dir=tmp_path / "work", out_dir=tmp_path / "out",
                              reference_dir=tmp_path / "reference", upstream=tmp_path / "upstream",
                              acquisition_log=tmp_path / "acquisition.log", wait_hours=1., max_qdm_hours_per_model=1.)
    if fail_smoke:
        with pytest.raises(subprocess.CalledProcessError):
            coordinator.finish(args)
    else:
        coordinator.finish(args)
    status = json.loads((args.out_dir / "execution_status.json").read_text())
    assert calls[0][0] == "tools.data_acquisition.uhe_daily"
    assert "--verify" in calls[0][1]
    assert calls[1][0] == "tools.diagnostics.wbgt_route_b_prepare"
    assert "--smoke" in calls[2][1]
    if fail_smoke:
        assert status["status"] == "FAILED" and len(calls) == 3
        assert status["error_type"] == "CalledProcessError"
    else:
        assert status["status"] == "PILOT_COMPLETE_REVIEW_REQUIRED"
        assert len(calls) == 8 and calls[-1][0] == "tools.diagnostics.wbgt_route_b_report"
        assert status["publication"] is False
