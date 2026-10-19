"""Contract checks for the reproduction's scoring gates and sun adjustment.

Self-contained and run manually in the pinned Python 3.10 environment. These are
deliberately NOT under tests/, and the file is deliberately NOT named test_*.py:
reproduce.py imports xclim 0.44.0 and thermofeel 1.3.0, which are pinned older
than IRT's conda environment and must not be installed into it. The repo has no
pytest configuration, so `python -m pytest -q` from the root collects the whole
tree; a test_*.py name here would abort that run with a collection error on
`import xclim`. The sibling multicity diagnostic can use test_contracts.py
because it needs only modules the conda environment already has.

    /tmp/irt-carbonplan-py310/bin/python docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/contract_checks.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reproduce import (DECLARED_PARITY_C, INPUT_VERSION_TOLERANCE_C, PRIMARY,  # noqa: E402
                       metrics, sun_delta, verdicts)

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    """Record one named contract outcome."""
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name} {detail}")
        FAILURES.append(name)


def test_metrics_gates() -> None:
    """Identical series reach parity; a 1e-5 offset matches counts but is not parity."""
    print("metrics gates")
    a = np.array([27.0, 29.0, 31.0, 33.0])
    same = metrics(a, a)
    check("identical series meet the declared parity gate", same["parity_declared_1e6"])
    check("identical series match counts", same["counts_match"])
    check("identical series report zero max_abs", same["max_abs_c"] == 0.0)

    near = metrics(a + 1e-5, a)
    check("1e-5 offset matches counts", near["counts_match"])
    check("1e-5 offset fails the declared 1e-6 gate", not near["parity_declared_1e6"])
    check("1e-5 offset sits inside the 1e-3 band", near["within_input_version_tolerance_1e3"])

    far = metrics(a + 1e-2, a)
    check("1e-2 offset fails the 1e-3 band", not far["within_input_version_tolerance_1e3"])
    check("1e-2 offset can still match counts here", far["counts_match"])
    check("bands are ordered", DECLARED_PARITY_C < INPUT_VERSION_TOLERANCE_C)


def test_thresholds_are_inclusive() -> None:
    """A value exactly on a threshold counts, matching the published comparison."""
    print("inclusive thresholds")
    exact = metrics(np.array([28.0, 30.0, 32.0]), np.array([28.0, 30.0, 32.0]))
    check("28.0 counts at >=28", exact["days_ge_28"] == 3)
    check("30.0 counts at >=30", exact["days_ge_30"] == 2)
    check("32.0 counts at >=32", exact["days_ge_32"] == 1)
    below = metrics(np.array([27.999999]), np.array([27.999999]))
    check("just below 28 does not count", below["days_ge_28"] == 0)


def test_metrics_refuses_bad_input() -> None:
    """Incomplete or mismatched series fail loudly rather than being silently dropped."""
    print("metrics input refusal")
    for name, args in [
        ("mismatched lengths", (np.array([1.0, 2.0]), np.array([1.0]))),
        ("NaN in candidate", (np.array([1.0, np.nan]), np.array([1.0, 2.0]))),
        ("NaN in reference", (np.array([1.0, 2.0]), np.array([1.0, np.nan]))),
        ("empty series", (np.array([]), np.array([]))),
        ("two-dimensional input", (np.ones((2, 2)), np.ones((2, 2)))),
    ]:
        try:
            metrics(*args)
            check(f"{name} is refused", False, "no exception raised")
        except ValueError:
            check(f"{name} is refused", True)


def test_sun_delta() -> None:
    """The sun adjustment is positive, rises with radiation and clips as notebook 08 does."""
    print("sun adjustment")
    wind = np.array([0.5, 0.5, 0.5, 0.5])
    peak = np.array([400.0, 800.0, 1200.0, 1600.0]) / 0.75
    d = sun_delta(peak, wind)
    check("adjustment is positive (sun exceeds shade)", bool((d > 0).all()), str(d))
    check("adjustment increases with radiation", d[1] > d[0])
    check("radiation clips at 900 W/m2", np.isclose(d[2], d[3]))

    calm, breezy = sun_delta(np.array([800.0]), np.array([0.5])), sun_delta(np.array([800.0]), np.array([3.0]))
    check("wind reduces the adjustment", breezy[0] < calm[0])
    low, lower = sun_delta(np.array([800.0]), np.array([0.5])), sun_delta(np.array([800.0]), np.array([0.1]))
    check("wind clips at 0.5 m/s", np.isclose(low[0], lower[0]))
    high, higher = sun_delta(np.array([800.0]), np.array([3.0])), sun_delta(np.array([800.0]), np.array([9.0]))
    check("wind clips at 3 m/s", np.isclose(high[0], higher[0]))


def _frame(max_abs: float, counts_match: bool, interpretation: str) -> pd.DataFrame:
    """Build a minimal summary frame shaped like reproduce.py's output."""
    rows = []
    for interp, ma, cm in [(interpretation, max_abs, counts_match), ("literal", 2.8, False)]:
        for year in (2005, 2007):
            rows.append({"city": "X", "year": year, "stage": "full_sun_independent",
                         "interpretation": interp, "max_abs_c": ma, "mae_c": ma,
                         "counts_match": cm,
                         "parity_declared_1e6": ma <= DECLARED_PARITY_C and cm,
                         "within_input_version_tolerance_1e3": ma <= INPUT_VERSION_TOLERANCE_C and cm})
    rows.append({"city": "X", "year": "1985-2014", "stage": "shade_independent",
                 "interpretation": "n/a", "max_abs_c": max_abs, "mae_c": max_abs,
                 "counts_match": counts_match, "parity_declared_1e6": False,
                 "within_input_version_tolerance_1e3": False})
    return pd.DataFrame(rows)


def test_verdicts() -> None:
    """The compound verdict distinguishes the four outcomes and never keys to the literal source."""
    print("verdicts")
    cases = [
        (1e-9, True, "REPRODUCED"),
        (1e-5, True, "COUNTS_EXACT_WITHIN_INPUT_VERSION_TOLERANCE_DECLARED_PARITY_NOT_MET"),
        (1e-2, True, "COUNTS_EXACT_CONTINUOUS_PARITY_NOT_MET"),
        (1e-2, False, "NOT_REPRODUCED"),
    ]
    for max_abs, counts_match, expected in cases:
        got = verdicts(_frame(max_abs, counts_match, PRIMARY))
        check(f"max_abs={max_abs:g} counts={counts_match} -> {expected}",
              got["verdict"] == expected, f"got {got['verdict']}")
    got = verdicts(_frame(1e-9, True, PRIMARY))
    check("literal source reported separately as failing",
          not got["literal_source_interpretation"]["counts_match_all"])
    check("verdict basis names the primary interpretation", PRIMARY in got["verdict_basis"])
    check("the 1e-3 band is labelled an assumption",
          got["input_version_tolerance_is_an_assumption_not_a_met_criterion"] is True)


def test_counts_alone_are_not_parity() -> None:
    """Matching every count while differing continuously must not read as parity."""
    print("counts are not parity")
    a = np.array([29.0, 31.0, 33.0])
    out = metrics(a + 0.4, a)
    check("counts match", out["counts_match"])
    check("declared parity still fails", not out["parity_declared_1e6"])
    check("1e-3 band still fails", not out["within_input_version_tolerance_1e3"])


def main() -> int:
    """Run every contract check and report a single exit status."""
    for test in (test_metrics_gates, test_thresholds_are_inclusive, test_metrics_refuses_bad_input,
                 test_sun_delta, test_verdicts, test_counts_alone_are_not_parity):
        test()
    print()
    if FAILURES:
        print(f"{len(FAILURES)} contract failure(s): {', '.join(FAILURES)}")
        return 1
    print("all contract checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
