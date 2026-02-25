"""Guardrail test to ensure emoji-like symbols are not used in UI/docs/scripts."""

from __future__ import annotations

from pathlib import Path
import re

EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE0F]")


TEXT_SUFFIXES = {".py", ".md", ".txt"}
ROOT_SCRIPTS = {
    "spi_diagnostic.py",
    "debug_build_master.py",
    "convert_blocks_shp_to_geojsion.py",
}
SCAN_DIRS = ["india_resilience_tool", "tests", "docs"]
SKIP_PARTS = {".git", "__pycache__", ".pytest_cache", "artifacts", "data", ".venv", "venv"}


def _iter_scan_paths(repo_root: Path) -> list[Path]:
    paths: list[Path] = []

    for rel_dir in SCAN_DIRS:
        base = repo_root / rel_dir
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            paths.append(path)

    for rel_file in ROOT_SCRIPTS:
        candidate = repo_root / rel_file
        if candidate.exists() and candidate.is_file():
            paths.append(candidate)

    return sorted(set(paths))


def test_no_emoji_characters_in_repo_text() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []

    for path in _iter_scan_paths(repo_root):
        content = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(content.splitlines(), start=1):
            match = EMOJI_RE.search(line)
            if match:
                rel = path.relative_to(repo_root)
                offenders.append(
                    f"{rel}:{line_no}: U+{ord(match.group(0)):04X} in {line.strip()}"
                )

    assert not offenders, "Emoji-like characters found:\n" + "\n".join(offenders)
