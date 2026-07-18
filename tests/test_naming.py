"""
Unit tests for utils.naming.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.utils.naming import (
    alias,
    hydro_fs_token,
    normalize_compact,
    normalize_name,
    safe_fs_component,
)


def test_normalize_name_basic() -> None:
    assert normalize_name(" Jayashankar-Bhupalpally ") == "jayashankar bhupalpally"
    assert normalize_name("Sanga_Reddy") == "sanga reddy"


def test_alias_applies_default_aliases() -> None:
    # from default mapping in naming.py
    assert alias("Hanamkonda") == "hanumakonda"


def test_normalize_compact_removes_spaces() -> None:
    assert normalize_compact("Sanga Reddy") == "sangareddy"


def test_maharashtra_block_and_district_spellings_reconcile() -> None:
    # CHG-0066: block-boundary vs district-boundary spellings for 6 Maharashtra
    # districts must compact to the same key so block<->district joins succeed.
    pairs = [
        ("Ahamadnagar", "AHMEDNAGAR"),
        ("Amaravati", "AMRAVATI"),
        ("Bid", "BEED"),
        ("Mumbai City", "MUMBAI"),
        ("Sub Urban Mumbai", "MUMBAI SUBURBAN"),
        ("Raygad", "RAIGARH"),
    ]
    for block_name, district_name in pairs:
        assert normalize_compact(block_name) == normalize_compact(district_name), (
            f"{block_name!r} did not reconcile with {district_name!r}"
        )

    # Distinct Mumbai districts must NOT collapse into each other.
    assert normalize_compact("Mumbai City") != normalize_compact("Sub Urban Mumbai")


def test_boundary_vs_master_state_spellings_reconcile() -> None:
    # CHG-0272: districts_4326 STATE_UT spellings vs flood-depth master state
    # spellings must alias to the same key so state-prefixed joins succeed.
    # normalize_name strips '&' but keeps 'and', so these diverge without aliases.
    pairs = [
        ("ANDAMAN AND NICOBAR ISLANDS", "Andaman & Nicobar Islands"),
        ("CHHATISGARH", "Chhattisgarh"),
        ("JAMMU AND KASHMIR", "Jammu & Kashmir"),
        ("Lakshadweep-UT", "Lakshadweep"),
    ]
    for boundary_name, master_name in pairs:
        assert alias(boundary_name) == alias(master_name), (
            f"{boundary_name!r} did not reconcile with {master_name!r}"
        )

    # Neighbouring UT/state must stay distinct from Jammu & Kashmir.
    assert alias("LADAKH") != alias("Jammu & Kashmir")


def test_safe_fs_component_strips_trailing_dot_the_bug() -> None:
    # CHG-0115: Maharashtra block "Parali V ." produced folder token "Parali_V_."
    # ending in a dot, which Win32 scandir cannot traverse (WinError 3). The fixed
    # token must not end in a dot or space.
    result = safe_fs_component("Parali V .")
    assert result == "Parali_V_"
    assert not result.endswith((".", " "))


def test_safe_fs_component_trailing_dot_variants() -> None:
    # Pure trailing dots (no interior space).
    assert safe_fs_component("Foo..") == "Foo"
    # Interior space becomes "_" BEFORE rstrip; only the trailing dot is stripped.
    # "Foo . " -> strip() "Foo ." -> replace " " "Foo_." -> rstrip " ." "Foo_".
    assert safe_fs_component("Foo . ") == "Foo_"


def test_safe_fs_component_noop_for_ordinary_names() -> None:
    # No-orphaning guard: ordinary tokens are byte-identical after the fix.
    assert safe_fs_component("Hyderabad") == "Hyderabad"
    assert safe_fs_component("Sangareddy") == "Sangareddy"
    assert safe_fs_component("Mahbubnagar") == "Mahbubnagar"


def test_safe_fs_component_preserves_interior_dot() -> None:
    assert safe_fs_component("St. Thomas") == "St._Thomas"


def test_safe_fs_component_must_not_strip_trailing_underscore() -> None:
    # A trailing space legitimately becomes "_"; that underscore must survive.
    assert safe_fs_component("Foo Bar ") == "Foo_Bar"
    # A token already ending in "_" is returned unchanged.
    assert safe_fs_component("Already_") == "Already_"


def test_safe_fs_component_is_idempotent() -> None:
    for value in ("Parali V .", "Foo..", "St. Thomas", "Foo Bar ", "Hyderabad"):
        once = safe_fs_component(value)
        assert safe_fs_component(once) == once


def test_safe_fs_component_degenerate_input_safe_fallback() -> None:
    # All-dot/space input must not raise and must not yield a dot/space-terminated
    # (or empty) token — it falls back to the Win32-safe sentinel "_".
    for value in (".", "  ", " . "):
        result = safe_fs_component(value)
        assert result == "_"
        assert not result.endswith((".", " "))


def test_hydro_fs_token_short_path_inherits_fix() -> None:
    # The short-name branch returns safe_fs_component verbatim, so it inherits the
    # trailing-dot strip.
    result = hydro_fs_token("Parali V .")
    assert not result.endswith((".", " "))
    assert result == "Parali_V_"
