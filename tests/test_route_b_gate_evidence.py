"""Guards on the Route B release test gate (CHG-0681, CHG-0682).

The gate reads pytest's own output to decide whether the suite regressed against
a recorded baseline of known-failing tests. The bug these tests exist to prevent
was silent and pointed the wrong way: the verdict was derived from the parsed
``FAILED`` lines alone, and *every* way of failing to run the suite produces no
``FAILED`` lines. A mistyped flag, an interrupt, a collection error or an empty
selection therefore yielded zero failures, zero regressions and a ``PASS`` --
a green test gate for a suite that never executed.

This is not hypothetical: a run in this campaign exited 4 with
``unrecognized arguments: --timeout=900`` and ran nothing at all.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GATE = REPO / "scratch" / "wbgt_route_b_national" / "gate_evidence.py"
CANDIDATE = REPO / "scratch" / "wbgt_route_b_national" / "release_candidate.py"

pytestmark = pytest.mark.skipif(
    not GATE.is_file(), reason="Route B release scripts are not present in this tree"
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _load(GATE, "_rb_gate_evidence")


@pytest.fixture(scope="module")
def candidate_mod():
    return _load(CANDIDATE, "_rb_release_candidate")


def _log(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pytest_output.log"
    path.write_text(text, encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# A completed run is admissible, and its numbers must reconcile.
# --------------------------------------------------------------------------

def test_completed_run_with_reconciling_failures_is_admissible(gate, tmp_path):
    log = _log(tmp_path, "FAILED tests/test_a.py::test_one\n"
                         "15 failed, 2194 passed, 5 skipped, 1591 warnings in 8099.97s (2:14:59)\n")
    outcome = gate._pytest_outcome(1, log, ["tests/test_a.py::test_one"] * 1 + [])
    # One parsed id against a summary claiming 15 must not reconcile.
    assert outcome["admissible"] is False

    failures = [f"tests/test_a.py::test_{i}" for i in range(15)]
    outcome = gate._pytest_outcome(1, log, failures)
    assert outcome["admissible"] is True
    assert outcome["results_reconciled"] is True
    assert outcome["tests_executed"] == 15 + 2194 + 5
    assert outcome["summary_seconds"] == pytest.approx(8099.97)


def test_all_green_run_is_admissible(gate, tmp_path):
    log = _log(tmp_path, "2214 passed, 5 skipped in 8099.97s\n")
    outcome = gate._pytest_outcome(0, log, [])
    assert outcome["admissible"] is True
    assert outcome["tests_executed"] == 2219


# --------------------------------------------------------------------------
# The rejection cases. Each of these produces zero FAILED lines, which is
# exactly why the old logic called them PASS.
# --------------------------------------------------------------------------

def test_usage_error_is_rejected(gate, tmp_path):
    """The real regression: exit 4 from an unrecognized flag, nothing executed."""
    log = _log(tmp_path, "ERROR: usage: __main__.py [options] [file_or_dir]\n"
                         "__main__.py: error: unrecognized arguments: --timeout=900\n")
    outcome = gate._pytest_outcome(4, log, [])
    assert outcome["admissible"] is False
    assert any("exit 4" in r and "usage error" in r for r in outcome["rejections"])


def test_interruption_is_rejected(gate, tmp_path):
    log = _log(tmp_path, "!!!! Interrupted: 1 error during collection !!!!\n")
    outcome = gate._pytest_outcome(2, log, [])
    assert outcome["admissible"] is False
    assert any("interrupted" in r.lower() for r in outcome["rejections"])


def test_internal_error_is_rejected(gate, tmp_path):
    log = _log(tmp_path, "INTERNALERROR> Traceback (most recent call last):\n")
    outcome = gate._pytest_outcome(3, log, [])
    assert outcome["admissible"] is False
    assert any("internal error" in r.lower() for r in outcome["rejections"])


def test_no_tests_collected_is_rejected(gate, tmp_path):
    log = _log(tmp_path, "no tests ran in 0.01s\n")
    outcome = gate._pytest_outcome(5, log, [])
    assert outcome["admissible"] is False
    assert any("no test" in r.lower() for r in outcome["rejections"])


def test_truncated_log_without_a_summary_is_rejected(gate, tmp_path):
    """A killed run can exit 1 and still leave no summary line."""
    log = _log(tmp_path, "FAILED tests/test_a.py::test_one\n")
    outcome = gate._pytest_outcome(1, log, ["tests/test_a.py::test_one"])
    assert outcome["admissible"] is False
    assert any("summary" in r for r in outcome["rejections"])


def test_summary_accounting_for_no_executed_tests_is_rejected(gate, tmp_path):
    log = _log(tmp_path, "0 passed, 12 deselected in 1.20s\n")
    outcome = gate._pytest_outcome(0, log, [])
    assert outcome["admissible"] is False
    assert any("no executed tests" in r for r in outcome["rejections"])


def test_unreconciled_results_are_rejected(gate, tmp_path):
    """Summary and parsed node ids disagreeing means the log cannot be trusted."""
    log = _log(tmp_path, "FAILED tests/test_a.py::test_one\n"
                         "3 failed, 10 passed in 2.00s\n")
    outcome = gate._pytest_outcome(1, log, ["tests/test_a.py::test_one"])
    assert outcome["admissible"] is False
    assert outcome["results_reconciled"] is False
    assert any("do not reconcile" in r for r in outcome["rejections"])


def test_errors_count_toward_reconciliation(gate, tmp_path):
    log = _log(tmp_path, "2 errors, 10 passed in 2.00s\n")
    outcome = gate._pytest_outcome(1, log, ["tests/test_a.py::test_one",
                                            "tests/test_b.py::test_two"])
    assert outcome["admissible"] is True
    assert outcome["reported_failing"] == 2


def test_missing_log_is_rejected(gate, tmp_path):
    outcome = gate._pytest_outcome(0, tmp_path / "absent.log", [])
    assert outcome["admissible"] is False


def test_unknown_exit_code_is_rejected(gate, tmp_path):
    log = _log(tmp_path, "15 failed, 2194 passed in 10.0s\n")
    outcome = gate._pytest_outcome(99, log, ["x"] * 15)
    assert outcome["admissible"] is False
    assert any("unknown exit code" in r for r in outcome["rejections"])


# --------------------------------------------------------------------------
# A baseline must never forgive a run that did not happen.
# --------------------------------------------------------------------------

def test_baseline_cannot_forgive_an_inadmissible_run(gate, tmp_path):
    """Zero regressions is meaningless if no test executed.

    The baseline exists so known-failing tests do not block the gate. Combined
    with a run that produced no failures *because it never ran*, that mechanism
    would turn every catastrophic invocation into a PASS.
    """
    baseline = tmp_path / "tests_baseline_failures.json"
    baseline.write_text(json.dumps({"failures": ["tests/test_a.py::test_one"]}),
                        encoding="utf-8")
    known, provenance = gate._baseline(baseline)
    assert known == {"tests/test_a.py::test_one"}
    assert provenance["present"] is True

    log = _log(tmp_path, "__main__.py: error: unrecognized arguments: --nope\n")
    failures = gate._parse_failures(log)
    assert sorted(set(failures) - known) == []          # no regressions, and yet:
    assert gate._pytest_outcome(4, log, failures)["admissible"] is False


def test_absent_baseline_tolerates_nothing(gate, tmp_path):
    known, provenance = gate._baseline(tmp_path / "absent.json")
    assert known == set()
    assert provenance["present"] is False


# --------------------------------------------------------------------------
# CHG-0682: the gate's own instruments are part of the candidate.
# --------------------------------------------------------------------------

def test_verification_component_covers_the_gate_scripts(candidate_mod):
    component = candidate_mod.verification_component(REPO)
    for expected in ("scratch/wbgt_route_b_national/gate_evidence.py",
                     "scratch/wbgt_route_b_national/release_candidate.py",
                     "scratch/wbgt_route_b_national/tests_baseline_failures.json"):
        assert component[expected] not in ("", None)
        assert component[expected] != "ABSENT", f"{expected} should have been hashed"


def test_absent_test_config_is_recorded_rather_than_omitted(candidate_mod):
    """An added pytest.ini must move the digest, so absence is a recorded value."""
    component = candidate_mod.verification_component(REPO)
    assert "pytest.ini" in component
    if not (REPO / "pytest.ini").exists():
        assert component["pytest.ini"] == "ABSENT"


def test_changed_verification_digest_invalidates_evidence(candidate_mod):
    recorded = {"candidate_id": "x",
                "digests": {"code": "a", "tests": "b", "verification": "c",
                            "roster": "d", "campaign": "e", "bundle": "f"},
                "components": {"bundle": {}}}
    current = dict(recorded)
    current["digests"] = dict(recorded["digests"], verification="CHANGED")
    problems = candidate_mod.compare(recorded, current)
    assert len(problems) == 1
    assert problems[0].startswith("verification changed")


def test_evidence_without_any_fingerprint_is_rejected(candidate_mod):
    assert candidate_mod.compare(None, {"digests": {}}) == [
        "carries no release-candidate fingerprint at all"]


# --------------------------------------------------------------------------
# Per-gate binding: a gate is bound to what decides *its* verdict.
# --------------------------------------------------------------------------

def test_each_gate_binds_the_gate_logic_itself(candidate_mod):
    """No gate may be judged without the code that decides its verdict."""
    for gate in ("tests", "audit", "dashboard"):
        names = candidate_mod.VERIFICATION_SETS[gate]
        assert "scratch/wbgt_route_b_national/gate_evidence.py" in names
        assert "scratch/wbgt_route_b_national/release_candidate.py" in names


def test_test_gate_binds_the_baseline_and_config_but_not_the_dashboard_scripts(candidate_mod):
    names = candidate_mod.VERIFICATION_SETS["tests"]
    assert "scratch/wbgt_route_b_national/tests_baseline_failures.json" in names
    assert "pytest.ini" in names
    # verify_streamlit.py cannot change a single pytest outcome, so a change to
    # it must not void a completed full-suite run.
    assert "scratch/wbgt_route_b_national/verify_streamlit.py" not in names


def test_dashboard_gate_binds_both_checkers(candidate_mod):
    names = candidate_mod.VERIFICATION_SETS["dashboard"]
    assert "scratch/wbgt_route_b_national/verify_release.py" in names
    assert "scratch/wbgt_route_b_national/verify_streamlit.py" in names


def test_scope_is_recorded_and_differs_between_gates(candidate_mod):
    tests_scope = candidate_mod.verification_component(REPO, gate="tests")
    dash_scope = candidate_mod.verification_component(REPO, gate="dashboard")
    assert set(tests_scope) != set(dash_scope)


def test_unknown_gate_is_refused(candidate_mod):
    with pytest.raises(ValueError, match="unknown gate"):
        candidate_mod.verification_component(REPO, gate="nope")


def test_evidence_from_a_different_scope_is_not_judged_on_digests(candidate_mod):
    """Comparing digests over different file sets would misreport the reason."""
    recorded = {"candidate_id": "x", "verification_scope": "dashboard",
                "digests": {"verification": "a"}, "components": {"bundle": None}}
    current = {"candidate_id": "y", "verification_scope": "tests",
               "digests": {"verification": "b"}, "components": {"bundle": None}}
    problems = candidate_mod.compare(recorded, current)
    assert len(problems) == 1
    assert "verification scope differs" in problems[0]


def test_publisher_expects_one_scope_per_evidence_file():
    import importlib.util
    path = REPO / "scratch" / "wbgt_route_b_national" / "publish_production.py"
    spec = importlib.util.spec_from_file_location("_rb_publish", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.EVIDENCE_SCOPES["tests"] == "tests"
    assert module.EVIDENCE_SCOPES["staged strict audit"] == "audit"
    assert module.EVIDENCE_SCOPES["staged dashboard verification"] == "dashboard"


# --------------------------------------------------------------------------
# A gate must not fail open in its exit status.
# --------------------------------------------------------------------------

def test_exit_code_follows_the_recorded_verdict(gate):
    assert gate._exit_code({"verdict": "PASS"}) == 0
    assert gate._exit_code({"verdict": "FAIL"}) == 1


def test_drift_fail_produces_a_nonzero_exit_code(gate):
    """The regression: evidence said FAIL, the process exited 0, the chain ran on.

    A gate can pass on its own terms (no regressions) and still be downgraded to
    FAIL by the drift check. The exit code has to follow the recorded verdict, or
    run_staged_chain.bat's errorlevel test cannot stop the chain.
    """
    payload = {"verdict": "FAIL", "drift": ["tests changed since ..."],
               "stable_during_run": False}
    assert gate._exit_code(payload) == 1


def test_missing_verdict_is_not_treated_as_success(gate):
    assert gate._exit_code({}) == 1
