import pandas as pd
import pytest

from tools.optimized import build_processed_optimised as B


def _master(districts):
    return pd.DataFrame(
        {
            "state": ["Telangana"] * len(districts),
            "district": districts,
            "district_key": [f"{B.alias('Telangana')}|{B.alias(d)}" for d in districts],
            "txge35_extreme_heat_days__historical__1995_2014__mean": [1.0] * len(districts),
        }
    )


@pytest.fixture
def stub_roster(monkeypatch):
    canonical = frozenset({f"{B.alias('Telangana')}|{B.alias('Jagitial')}"})
    calls = {"n": 0}

    def _fake(level, source):
        calls["n"] += 1
        return canonical

    monkeypatch.setattr(B, "_canonical_admin_keys", _fake)
    return calls


def test_strict_skips_bad_master_and_reports(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "strict")
    frame, offenders = B._check_canonical_roster(_master(["Jagitial", "JAGTIAL"]), slug="x", level="district")
    assert frame is None  # strict -> caller skips the write; clean bundles still publish
    assert offenders and any("JAGTIAL" in o for o in offenders)


def test_warn_drops_offenders_and_warns(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "warn")
    with pytest.warns(UserWarning):
        frame, offenders = B._check_canonical_roster(_master(["Jagitial", "JAGTIAL"]), slug="x", level="district")
    assert list(frame["district"]) == ["Jagitial"]
    assert offenders


def test_clean_master_passes(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "strict")
    frame, offenders = B._check_canonical_roster(_master(["Jagitial"]), slug="x", level="district")
    assert list(frame["district"]) == ["Jagitial"]
    assert offenders == []


def test_off_short_circuits_before_roster(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "off")
    frame, offenders = B._check_canonical_roster(_master(["JAGTIAL"]), slug="x", level="district")
    assert len(frame) == 1 and offenders == []
    assert stub_roster["n"] == 0  # roster never consulted


def test_hydro_level_is_noop(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "strict")
    df = pd.DataFrame({"basin_id": ["B1"], "basin_name": ["X"]})
    frame, offenders = B._check_canonical_roster(df, slug="x", level="basin")
    assert frame is df and offenders == []
    assert stub_roster["n"] == 0


def test_unknown_mode_falls_back_to_strict(monkeypatch, stub_roster):
    monkeypatch.setenv("IRT_ROSTER_GATE", "bogus")
    with pytest.warns(UserWarning):
        frame, offenders = B._check_canonical_roster(_master(["JAGTIAL"]), slug="x", level="district")
    assert frame is None and offenders  # behaves as strict


def test_violations_report_lists_slugs_and_units():
    msg = B._roster_violations_report({"spi3_drought_index": ["Telangana | JAGTIAL"]})
    assert "spi3_drought_index" in msg and "JAGTIAL" in msg
