"""Filesystem-backed geography discovery helpers for dashboard selectors."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from india_resilience_tool.data.optimized_bundle import list_optimized_states_for_metric_root

_RESERVED_PROCESSED_DIR_NAMES = {"hydro"}
_INVALID_STATE_DIR_NAMES = {"", "nan", "none", "null", "nat"}


def _has_master_csv(root: Path) -> bool:
    """Return True when ``root`` contains a canonical admin master CSV."""
    if not root.is_dir():
        return False
    try:
        return any(
            child.is_file()
            and child.name in {"master_metrics_by_district.csv", "master_metrics_by_block.csv"}
            for child in root.iterdir()
        )
    except Exception:
        return False


def _is_valid_state_dir_name(name: str) -> bool:
    """Return True when a processed-root child name is safe to treat as an admin state."""
    norm = str(name or "").strip()
    if not norm:
        return False
    lowered = norm.lower()
    if lowered in _RESERVED_PROCESSED_DIR_NAMES:
        return False
    if lowered in _INVALID_STATE_DIR_NAMES:
        return False
    return True


def _has_nested_directory(root: Path, *, min_levels: int = 1, max_depth: int = 3) -> bool:
    """Return True when ``root`` contains a descendant directory at least ``min_levels`` deep."""
    if not root.is_dir():
        return False

    queue: deque[tuple[Path, int]] = deque([(root, 0)])
    while queue:
        current, depth = queue.popleft()
        if depth >= max_depth:
            continue
        try:
            children = list(current.iterdir())
        except Exception:
            continue

        for child in children:
            if not child.is_dir() or child.name.startswith("."):
                continue
            level_from_root = depth + 1
            if level_from_root >= min_levels:
                return True
            queue.append((child, depth + 1))
    return False


def _state_has_available_data(state_dir: Path) -> bool:
    """Check whether a state directory has any supported processed-data structure."""
    if not state_dir.is_dir():
        return False

    if _has_master_csv(state_dir):
        return True
    if _has_nested_directory(state_dir / "districts", min_levels=2, max_depth=4):
        return True
    if _has_nested_directory(state_dir / "blocks", min_levels=3, max_depth=5):
        return True

    for child in state_dir.iterdir():
        if (
            not child.is_dir()
            or child.name.startswith(".")
            or child.name in {"districts", "blocks"}
        ):
            continue
        if _has_nested_directory(child, min_levels=1, max_depth=2):
            return True
    return False


def list_available_states_from_processed_root(processed_root_str: str) -> list[str]:
    """List state folders under processed root that contain usable data structures."""
    processed_root = Path(processed_root_str)
    if not processed_root.exists() or not processed_root.is_dir():
        return []

    masters_admin = processed_root / "masters" / "admin"
    if masters_admin.exists():
        for level in ("district", "block"):
            states = list_optimized_states_for_metric_root(processed_root, level=level)
            if states:
                return states
        return []

    states: list[str] = []
    for entry in processed_root.iterdir():
        if not entry.is_dir() or entry.name.startswith(".") or not _is_valid_state_dir_name(entry.name):
            continue
        if _state_has_available_data(entry):
            states.append(entry.name)

    return sorted(states)


# -----------------------------------------------------------------------------
# District-option helpers for the state="All" district selector (CHG-0279)
# -----------------------------------------------------------------------------

# Composite option values are ONLY produced by build_district_options; no other
# code may emit this delimiter (guarded by tests + grep audit).
DISTRICT_OPTION_DELIMITER = "||"


def build_district_options(district_state_pairs: "Iterable[tuple[str, str]]") -> list[str]:
    """Build district option values for the all-India district selector.

    Unique district names stay bare (contract-preserving); names appearing in
    more than one state become ``"<district>||<state>"`` composites so the
    selection is unambiguous. Options are sorted by district name, then state.

    Args:
        district_state_pairs: Iterable of (district_name, state_name) pairs.

    Returns:
        Sorted list of option values (bare names and composites).
    """
    states_by_district: dict[str, set[str]] = {}
    for district, state in district_state_pairs:
        district_norm = str(district or "").strip()
        state_norm = str(state or "").strip()
        if not district_norm or district_norm == "All":
            continue
        states_by_district.setdefault(district_norm, set()).add(state_norm)

    options: list[tuple[str, str]] = []
    for district, states in states_by_district.items():
        if len(states) > 1:
            for state in states:
                options.append((district, state))
        else:
            options.append((district, ""))

    return [
        district if not state else f"{district}{DISTRICT_OPTION_DELIMITER}{state}"
        for district, state in sorted(options)
    ]


def split_district_option(value: str) -> "tuple[str, Optional[str]]":
    """Parse an option value back to ``(district_name, state_or_None)``.

    ``"All"`` is handled explicitly as ``("All", None)``; bare names return
    ``(name, None)``.
    """
    text = str(value or "").strip()
    if text == "All":
        return "All", None
    if DISTRICT_OPTION_DELIMITER in text:
        district, _, state = text.partition(DISTRICT_OPTION_DELIMITER)
        return district.strip(), (state.strip() or None)
    return text, None


def district_option_label(value: str) -> str:
    """Display label for a district option: bare name, or ``"District — State"``."""
    district, state = split_district_option(value)
    if state:
        return f"{district} — {state}"
    return district


def resolve_effective_state(
    district_name: str,
    district_state_map: "Mapping[str, Sequence[str]]",
) -> "Optional[str]":
    """Resolve a bare district name to its state when the mapping is unambiguous.

    Args:
        district_name: Bare district name (no composite delimiter).
        district_state_map: ``{district_name: [state, ...]}`` built from the
            same frame that produced the district options.

    Returns:
        The state name when the district maps to exactly one state, else None.
    """
    states = district_state_map.get(str(district_name or "").strip())
    if not states:
        return None
    unique = {str(s).strip() for s in states if str(s).strip()}
    if len(unique) == 1:
        return next(iter(unique))
    return None


def resolve_district_option(
    session_state: "Mapping[str, Any]",
    options: "Sequence[str]",
) -> str:
    """Return the option value to seed into ``selected_district_option``.

    A still-valid stored option is always kept — at rerun start a fresh widget
    selection lands in ``selected_district_option`` before the canonical
    ``selected_district`` is updated, so disagreement with the canonical value
    must never trigger re-derivation. External canonical changes (map clicks,
    resets, mode coercions) pop the stored option via
    ``reset_district_option_state``; only then is the option re-derived from
    ``selected_district`` (bare match first, then the composite matching a
    previously stored effective state), falling back to ``"All"``.
    """
    canonical = str(session_state.get("selected_district") or "All").strip() or "All"
    stored = str(session_state.get("selected_district_option") or "").strip()

    if stored and stored in options:
        return stored
    if canonical == "All":
        return "All"
    if canonical in options:
        return canonical
    effective = str(session_state.get("_district_effective_state") or "").strip()
    if effective and effective != "All":
        composite = f"{canonical}{DISTRICT_OPTION_DELIMITER}{effective}"
        if composite in options:
            return composite
    for option in options:
        if split_district_option(option)[0] == canonical:
            return option
    return "All"
