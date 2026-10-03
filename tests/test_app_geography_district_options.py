"""
Tests for the district-option helpers behind the state="All" selector (CHG-0279).

Data-contract guards: composite "<district>||<state>" values are produced only
for duplicated district names, "All" and unique names stay bare, and the
seed/reset logic keeps the widget option in sync with the canonical
`selected_district`.
"""

from __future__ import annotations

from india_resilience_tool.app.geography import (
    DISTRICT_OPTION_DELIMITER,
    build_district_options,
    district_option_label,
    resolve_district_option,
    resolve_effective_state,
    split_district_option,
)

PAIRS = [
    ("Adilabad", "Telangana"),
    ("Hamirpur", "Uttar Pradesh"),
    ("Hamirpur", "Himachal Pradesh"),
    ("Nirmal", "Telangana"),
]


def test_build_district_options_duplicates_become_composites() -> None:
    options = build_district_options(PAIRS)
    assert "Adilabad" in options
    assert "Nirmal" in options
    assert "Hamirpur||Himachal Pradesh" in options
    assert "Hamirpur||Uttar Pradesh" in options
    assert "Hamirpur" not in options


def test_build_district_options_never_composes_all_or_unique_names() -> None:
    options = build_district_options(PAIRS + [("All", "Telangana")])
    assert "All" not in options
    assert not any(opt.startswith("All" + DISTRICT_OPTION_DELIMITER) for opt in options)
    assert "Adilabad" in options  # unique names stay bare


def test_build_district_options_repeated_same_state_pair_stays_bare() -> None:
    # The same (district, state) pair appearing twice is not a duplicate name.
    options = build_district_options([("Adilabad", "Telangana"), ("Adilabad", "Telangana")])
    assert options == ["Adilabad"]


def test_split_district_option_round_trip() -> None:
    for option in build_district_options(PAIRS):
        district, state = split_district_option(option)
        assert DISTRICT_OPTION_DELIMITER not in district
        if state is None:
            assert option == district
        else:
            assert option == f"{district}{DISTRICT_OPTION_DELIMITER}{state}"


def test_split_district_option_all_and_bare_names() -> None:
    assert split_district_option("All") == ("All", None)
    assert split_district_option("Adilabad") == ("Adilabad", None)
    assert split_district_option("Hamirpur||Uttar Pradesh") == ("Hamirpur", "Uttar Pradesh")


def test_district_option_label_formats() -> None:
    assert district_option_label("All") == "All"
    assert district_option_label("Adilabad") == "Adilabad"
    assert district_option_label("Hamirpur||Uttar Pradesh") == "Hamirpur — Uttar Pradesh"


def test_resolve_effective_state() -> None:
    district_state_map = {
        "Adilabad": ["Telangana"],
        "Hamirpur": ["Uttar Pradesh", "Himachal Pradesh"],
    }
    assert resolve_effective_state("Adilabad", district_state_map) == "Telangana"
    assert resolve_effective_state("Hamirpur", district_state_map) is None
    assert resolve_effective_state("Unknown", district_state_map) is None


OPTIONS = ["All"] + build_district_options(PAIRS)


def test_resolve_district_option_keeps_valid_stored_option() -> None:
    ss = {
        "selected_district": "Hamirpur",
        "selected_district_option": "Hamirpur||Uttar Pradesh",
    }
    assert resolve_district_option(ss, OPTIONS) == "Hamirpur||Uttar Pradesh"


def test_resolve_district_option_derives_unique_bare_name() -> None:
    ss = {"selected_district": "Adilabad"}
    assert resolve_district_option(ss, OPTIONS) == "Adilabad"


def test_resolve_district_option_uses_stored_effective_state_for_duplicates() -> None:
    ss = {
        "selected_district": "Hamirpur",
        "_district_effective_state": "Himachal Pradesh",
    }
    assert resolve_district_option(ss, OPTIONS) == "Hamirpur||Himachal Pradesh"


def test_resolve_district_option_duplicate_without_state_picks_first_match() -> None:
    ss = {"selected_district": "Hamirpur"}
    resolved = resolve_district_option(ss, OPTIONS)
    assert split_district_option(resolved)[0] == "Hamirpur"


def test_resolve_district_option_falls_back_to_all() -> None:
    assert resolve_district_option({}, OPTIONS) == "All"
    assert resolve_district_option({"selected_district": "All"}, OPTIONS) == "All"
    assert resolve_district_option({"selected_district": "Nowhere"}, OPTIONS) == "All"


def test_resolve_district_option_fresh_widget_selection_is_never_clobbered() -> None:
    # At rerun start the widget key holds the user's new pick while the
    # canonical key still holds the old value; the stored option must win.
    ss = {
        "selected_district": "All",
        "selected_district_option": "Hamirpur||Uttar Pradesh",
    }
    assert resolve_district_option(ss, OPTIONS) == "Hamirpur||Uttar Pradesh"


def test_resolve_district_option_rederives_after_reset_pops_stored_option() -> None:
    # External canonical changes (map click / reset) pop the stored option via
    # reset_district_option_state; only then is it re-derived from canonical.
    ss = {"selected_district": "Nirmal"}
    assert resolve_district_option(ss, OPTIONS) == "Nirmal"


def test_resolve_district_option_invalid_stored_option_rederived() -> None:
    # A stored option that fell out of the option list self-heals.
    ss = {
        "selected_district": "Adilabad",
        "selected_district_option": "Gone||Nowhere",
    }
    assert resolve_district_option(ss, OPTIONS) == "Adilabad"


def test_delimiter_produced_only_by_build_district_options() -> None:
    # Grep-style containment audit: no other app module may emit the "||"
    # composite delimiter into district option values.
    from pathlib import Path

    app_dir = Path(__file__).resolve().parents[1] / "india_resilience_tool" / "app"
    offenders = []
    for path in app_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if path.name == "geography.py" and "DISTRICT_OPTION_DELIMITER = " in line:
                continue
            # Pre-existing internal DataFrame join key in the ranking merge;
            # never surfaced as a district option value.
            if path.name == "map_pipeline.py" and "str.cat" in line:
                continue
            # The standalone quoted delimiter or an f-string composing around it.
            if '"||"' in line or "'||'" in line or "||{" in line or "}||" in line:
                offenders.append(f"{path.name}:{line_no}: {line.strip()}")
    assert offenders == [], f"'||' delimiter produced outside geography.py: {offenders}"
