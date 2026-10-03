from __future__ import annotations

"""Regression guards for ``--state`` resolution in master_builder batch mode.

Guards CHG-0297. ``--state`` is documented as comma-separated, but the canonical
name of one admin state — "Dadra, Nagar Haveli, Daman & Diu" — itself contains
commas. Splitting it produced fragments matching no scope, so batch runs targeting
that state silently built nothing and exited 0. That is how the state's masters
stayed missing from 54 metrics while its per-district compute sat complete on disk.
"""

import pytest

from india_resilience_tool.compute import master_builder as bmm


DNHDD = "Dadra, Nagar Haveli, Daman & Diu"


class TestParseStateFilter:
    def test_comma_named_state_resolves_to_single_scope(self) -> None:
        """The whole string wins when it names a real scope."""
        available = {DNHDD, "Goa", "Telangana"}
        assert bmm._parse_state_filter(DNHDD, available) == [DNHDD]

    def test_comma_named_state_without_availability_falls_back_to_split(self) -> None:
        """Absent scope knowledge, the documented split behaviour is preserved."""
        assert bmm._parse_state_filter(DNHDD, None) == [
            "Dadra",
            "Nagar Haveli",
            "Daman & Diu",
        ]

    def test_multi_state_value_still_splits(self) -> None:
        """A genuine comma-separated list is unaffected by the exact-match branch."""
        available = {DNHDD, "Goa", "Kerala"}
        assert bmm._parse_state_filter("Goa,Kerala", available) == ["Goa", "Kerala"]

    def test_single_state_unaffected(self) -> None:
        assert bmm._parse_state_filter("Telangana", {"Telangana"}) == ["Telangana"]

    def test_whitespace_is_stripped(self) -> None:
        assert bmm._parse_state_filter("  Goa , Kerala ", {"Goa"}) == ["Goa", "Kerala"]

    @pytest.mark.parametrize("raw", ["", "   ", ",", " , "])
    def test_empty_values_yield_none(self, raw: str) -> None:
        assert bmm._parse_state_filter(raw, {"Goa"}) is None


def _make_district_scope(processed_root, metric: str, state: str, district: str) -> None:
    """Create the minimal district tree that ``_looks_like_state_dir`` accepts.

    A state directory only counts when it holds real leaf data at
    ``districts/<district>/<model>/<scenario>/<district>_periods.csv`` — empty
    directories are correctly ignored.
    """
    leaf = (
        processed_root / metric / state / "districts" / district / "CanESM5" / "historical"
    )
    leaf.mkdir(parents=True)
    (leaf / f"{district}_periods.csv").write_text("period,value\n2020-2040,1.0\n")


class TestScopeNameExists:
    def test_finds_name_under_any_metric(self, tmp_path) -> None:
        _make_district_scope(tmp_path, "tas_annual_mean", "Goa", "North_Goa")
        _make_district_scope(tmp_path, "hwa_heatwave_amplitude", DNHDD, "Daman")

        assert bmm._scope_name_exists(tmp_path, DNHDD) is True
        assert bmm._scope_name_exists(tmp_path, "Goa") is True

    def test_absent_name_returns_false(self, tmp_path) -> None:
        _make_district_scope(tmp_path, "tas_annual_mean", "Goa", "North_Goa")
        assert bmm._scope_name_exists(tmp_path, DNHDD) is False

    def test_is_name_only_and_does_not_validate_contents(self, tmp_path) -> None:
        """Name-only is intentional: validating contents costs minutes per metric.

        A bare directory still counts. That is safe because the result only decides
        whether a comma-containing --state names a directory at all; real scope
        validation still happens per metric inside build_all_master_metrics.
        """
        (tmp_path / "tas_annual_mean" / DNHDD).mkdir(parents=True)
        assert bmm._scope_name_exists(tmp_path, DNHDD) is True

    def test_empty_name_returns_false(self, tmp_path) -> None:
        assert bmm._scope_name_exists(tmp_path, "") is False

    def test_empty_root_returns_false(self, tmp_path) -> None:
        assert bmm._scope_name_exists(tmp_path, DNHDD) is False


class TestEndToEndResolution:
    """The actual CHG-0297 regression: discovery + parsing composed together.

    Testing the two helpers in isolation is not enough — the bug only bites when
    the scope set feeding ``_parse_state_filter`` comes from a real tree.
    """

    def test_comma_named_state_resolves_against_real_tree(self, tmp_path) -> None:
        _make_district_scope(tmp_path, "tas_annual_mean", DNHDD, "Daman")
        _make_district_scope(tmp_path, "tas_annual_mean", "Goa", "North_Goa")

        available = {DNHDD} if bmm._scope_name_exists(tmp_path, DNHDD) else set()
        assert bmm._parse_state_filter(DNHDD, available) == [DNHDD]

    def test_pre_fix_behaviour_would_have_matched_nothing(self, tmp_path) -> None:
        """Pin the old failure mode so a regression is unambiguous."""
        _make_district_scope(tmp_path, "tas_annual_mean", DNHDD, "Daman")

        available = {DNHDD} if bmm._scope_name_exists(tmp_path, DNHDD) else set()
        naive = [s.strip() for s in DNHDD.split(",") if s.strip()]
        assert not (set(naive) & available), "naive split must not match any scope"
        assert set(bmm._parse_state_filter(DNHDD, available)) & available


class TestBuildAllReturnsMatchCount:
    def test_no_eligible_metrics_returns_zero(self, tmp_path) -> None:
        """A zero return is what lets the CLI fail loudly instead of exiting 0."""
        assert (
            bmm.build_all_master_metrics(tmp_path, level="district", verbose=False) == 0
        )
