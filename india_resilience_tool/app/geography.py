"""Filesystem-backed geography discovery helpers for dashboard selectors."""

from __future__ import annotations

from collections import deque
from pathlib import Path



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

    states: list[str] = []
    for entry in processed_root.iterdir():
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        if _state_has_available_data(entry):
            states.append(entry.name)

    return sorted(states)
