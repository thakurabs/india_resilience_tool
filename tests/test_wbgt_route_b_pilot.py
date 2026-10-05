"""Protect grid-first selection, signed provenance and missing-data semantics."""
import csv
import hashlib
import json

import pytest

from tools.diagnostics import wbgt_route_b_pilot as pilot


def fixture(tmp_path, *, eligible=True):
    source = tmp_path / "source"
    source.mkdir()
    with (source / "unit_support.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["unit_index", "state_name", "level", "district_name", "block_name", "lgd_code", "retained_fraction", "eligible"])
        writer.writerow([0, "Kerala", "district", "Example", "", "1", 1 if eligible else .5, eligible])
    with (source / "yearly_metrics.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["unit_index", "scenario", "year", "method", *pilot.METRICS])
        for scenario, (first, last) in pilot.YEARS.items():
            for year in range(first, last + 1):
                writer.writerow([0, scenario, year, pilot.METHOD, *([25., 35., 12.25, 0., 0.] if eligible else [""] * 5)])
                writer.writerow([0, scenario, year, "region_first_series", 99, 99, 99, 99, 99])
    sign(source)
    return source


def sign(source):
    (source / "run_manifest.json").write_text(json.dumps({
        "model": "ACCESS-CM2", "status": "PILOT_COMPLETE_DIAGNOSTIC_ONLY", "signature_sha256": "test",
        "artifact_sha256": {name: hashlib.sha256((source / name).read_bytes()).hexdigest()
                            for name in ("yearly_metrics.csv", "unit_support.csv")}}))


def test_only_gridfirst_values_and_real_zeros_are_exported(tmp_path):
    payload, provenance = pilot.load_pilot(fixture(tmp_path))
    assert payload["units"][0]["series"]["historical"][0] == [25., 35., 12.25, 0., 0.]
    assert len(payload["units"][0]["series"]["ssp585"]) == 61
    assert provenance["eligible_units"] == 1 and provenance["production"] is False


def test_excluded_unit_stays_missing(tmp_path):
    payload, provenance = pilot.load_pilot(fixture(tmp_path, eligible=False))
    assert payload["units"][0]["series"]["historical"][0] == [None] * 5
    assert provenance["eligible_units"] == 0
    assert "NaN" not in json.dumps(payload, allow_nan=False)


def test_changed_evidence_refused(tmp_path):
    source = fixture(tmp_path)
    with (source / "yearly_metrics.csv").open("a") as stream:
        stream.write("\n")
    with pytest.raises(ValueError, match="Changed diagnostic evidence"):
        pilot.load_pilot(source)


@pytest.mark.parametrize("mutation,match", [("missing", "Incomplete annual"), ("duplicate", "Duplicate grid-first"),
                                            ("extreme", "Annual count outside")])
def test_bad_annual_records_refused(tmp_path, mutation, match):
    source = fixture(tmp_path)
    p = source / "yearly_metrics.csv"
    lines = p.read_text().splitlines()
    if mutation == "missing":
        del lines[1]
    elif mutation == "duplicate":
        lines.append(lines[1])
    else:
        lines[1] = lines[1].replace("12.25", "366")
    p.write_text("\n".join(lines) + "\n")
    sign(source)
    with pytest.raises(ValueError, match=match):
        pilot.load_pilot(source)


def test_plan_does_not_create_output(tmp_path):
    out = tmp_path / "absent"
    assert pilot.main(["--source", str(tmp_path / "missing"), "--out-dir", str(out)]) == 0
    assert not out.exists()


def test_empty_roster_is_not_a_working_pilot(tmp_path):
    source = fixture(tmp_path)
    path = source / "unit_support.csv"
    path.write_text(path.read_text().splitlines()[0] + "\n")
    sign(source)
    with pytest.raises(ValueError, match="Empty pilot roster"):
        pilot.load_pilot(source)
