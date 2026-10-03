r"""
Migrate processed directories/files whose name component ends in a Windows-illegal
trailing dot or space.

Why this exists
---------------
``india_resilience_tool.utils.naming.safe_fs_component`` historically produced folder
tokens by only replacing spaces/slashes with underscores. A block such as
Maharashtra's **"Parali V ."** therefore became the on-disk folder ``Parali_V_.`` --
a directory name ending in a literal ``.``. Win32 path resolution silently strips
trailing dots/spaces, so the directory is *un-addressable* on native Windows: a
recursive glob (e.g. ``tools/optimized/build_processed_optimised``) raises
``[WinError 3] The system cannot find the path specified`` and aborts the whole run,
while single-level globs (ensembling, master collection) silently skip the unit.

``safe_fs_component`` has since been hardened (it strips trailing dots/spaces), so all
*future* writes are safe. This helper migrates the *existing* on-disk tree that was
written before the fix: it renames each offending directory to the sanitized token and
renames the descendant files that carry the old token as their stem prefix
(``Parali_V_._periods.csv`` -> ``Parali_V__periods.csv``), so ``master_builder`` -- which
reads ``{token}_periods.csv`` / ``{token}_yearly.csv`` -- still finds them.

Design contract
---------------
- **Dry-run by default.** Nothing is renamed unless ``--apply`` is passed.
- **Idempotent.** Re-running after a successful migration is a no-op (no offending
  components remain).
- **Collision-guarded.** If the sanitized target directory already exists, the helper
  REFUSES to migrate that subtree (it never merges/overwrites) and exits non-zero --
  a dotted and an un-dotted dir for the same unit must never coexist (both normalize to
  the same admin key -> "Duplicate admin_key" downstream).
- **Win32 trailing-dot trap.** On native Windows the source ``...\Parali_V_.`` is itself
  un-addressable by Win32 path normalization; the helper prefixes paths with the
  extended-length ``\\?\`` form to bypass it. Running from the WSL/ext4 view
  (``/mnt/d/...``), where ``Parali_V_.`` is a real addressable filename, needs no prefix
  and is the simplest path.

Usage
-----
    # Preview (read-only) over the resolved processed root
    python -m tools.diagnostics.migrate_trailing_dot_dirs

    # Preview a single state
    python -m tools.diagnostics.migrate_trailing_dot_dirs --state Maharashtra

    # Apply (renames on disk)
    python -m tools.diagnostics.migrate_trailing_dot_dirs --state Maharashtra --apply

    # Explicit root override (otherwise resolved from IRT_DATA_DIR / paths config)
    python -m tools.diagnostics.migrate_trailing_dot_dirs --root /mnt/d/projects/irt_data/processed --apply

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from india_resilience_tool.utils.naming import safe_fs_component

# Components are "offending" when, after stripping, they differ from the original --
# i.e. the original ends in a Windows-illegal trailing dot or space.
_TRAILING_ILLEGAL = (".", " ")


@dataclass
class DirMigration:
    """A single directory rename plus the descendant files it carries as a prefix."""

    src_dir: Path
    dst_dir: Path
    file_renames: list[tuple[Path, Path]] = field(default_factory=list)
    collision: bool = False


def _is_offending(name: str) -> bool:
    """True when the component name ends in a Windows-illegal trailing dot/space."""
    return name != name.rstrip("".join(_TRAILING_ILLEGAL)) and bool(name.strip(" ."))


def _long_path(p: Path) -> str:
    r"""Return an os.rename-safe path string.

    On native Windows, prefix with the extended-length ``\\?\`` form so a source
    component ending in a trailing dot/space (which Win32 would otherwise normalize
    away to a non-existent path) stays addressable. On POSIX/WSL the literal path is
    already addressable, so it is returned unchanged.
    """
    if sys.platform.startswith("win"):
        abs_str = os.path.abspath(str(p))
        if abs_str.startswith("\\\\?\\"):
            return abs_str
        if abs_str.startswith("\\\\"):  # UNC path -> \\?\UNC\server\share\...
            return "\\\\?\\UNC\\" + abs_str.lstrip("\\")
        return "\\\\?\\" + abs_str
    return str(p)


def _walk_roots(root: Path, state_token: Optional[str]) -> list[Path]:
    """Return the subtree roots to scan.

    The processed layout is ``<root>/<metric_slug>/<State>/...``. With no state filter
    we scan the whole root. With a state filter we descend into each slug's ``<State>``
    subdir only, so we never walk other states' (large) trees -- the scan stays O(state)
    instead of O(processed-root).
    """
    if state_token is None:
        return [root]
    roots: list[Path] = []
    try:
        for slug_dir in sorted(root.iterdir()):
            if not slug_dir.is_dir():
                continue
            state_dir = slug_dir / state_token
            if state_dir.is_dir():
                roots.append(state_dir)
    except OSError:
        return [root]
    return roots


# Offending *admin-unit* directories live near the top of each state tree
# (``blocks/<district>/<block>`` is the deepest at relative depth 3; ``districts/<district>``
# at depth 2; ``ensembles/<district>/<block>`` at depth 3). Below that is the
# model/scenario/file explosion, which carries no offending component but dominates the
# walk cost over a slow mount. We therefore prune the *detection* walk at this depth; the
# per-offender file sweep that follows is unbounded (it must reach the leaf CSVs) but runs
# only inside the small offending subtree.
_DEFAULT_MAX_DEPTH = 4


def find_offending_dir_migrations(
    root: Path,
    state_token: Optional[str] = None,
    max_depth: int = _DEFAULT_MAX_DEPTH,
) -> list[DirMigration]:
    """Enumerate every directory under ``root`` whose name ends in a trailing dot/space.

    For each, computes the sanitized target name, flags a collision if that target
    already exists, and collects the descendant *files* whose name begins with the old
    directory token (so the stem prefix can be rewritten to the sanitized token).

    When ``state_token`` is given the scan is narrowed to ``<root>/<slug>/<state_token>``
    subtrees only. The detection walk is pruned at ``max_depth`` levels below each scan
    root (``max_depth <= 0`` disables pruning). Returns migrations ordered deepest-first
    so child renames never invalidate a not-yet-processed parent path.
    """
    migrations: list[DirMigration] = []
    for scan_root in _walk_roots(root, state_token):
        migrations.extend(_scan_one(scan_root, max_depth))
    # Deepest-first so a nested offending dir is renamed before its ancestor.
    migrations.sort(key=lambda m: len(m.src_dir.parts), reverse=True)
    return migrations


def _scan_one(scan_root: Path, max_depth: int) -> list[DirMigration]:
    migrations: list[DirMigration] = []
    base_depth = str(scan_root).rstrip(os.sep).count(os.sep)
    # topdown=True so we can prune dirnames in place below max_depth.
    for dirpath, dirnames, _filenames in os.walk(scan_root, topdown=True):
        for dname in dirnames:
            if not _is_offending(dname):
                continue
            src_dir = Path(dirpath) / dname
            new_name = safe_fs_component(dname)
            if new_name == dname:
                # Sanitizer is a no-op for this name (should not happen given the
                # _is_offending gate) -- nothing to migrate.
                continue
            dst_dir = Path(dirpath) / new_name
            mig = DirMigration(src_dir=src_dir, dst_dir=dst_dir)
            if dst_dir.exists():
                mig.collision = True
                migrations.append(mig)
                continue
            # Rewrite descendant files that carry the OLD directory token as a prefix
            # (e.g. "Parali_V_._periods.csv" under ".../Parali_V_."). Unbounded walk, but
            # scoped to this single offending subtree.
            for sub_dirpath, _sub_dirnames, sub_filenames in os.walk(src_dir):
                for fname in sub_filenames:
                    if fname.startswith(dname):
                        new_fname = new_name + fname[len(dname):]
                        if new_fname != fname:
                            src_file = Path(sub_dirpath) / fname
                            dst_file = Path(sub_dirpath) / new_fname
                            mig.file_renames.append((src_file, dst_file))
            migrations.append(mig)

        # Prune the detection walk below max_depth (skip the deep model/scenario/file
        # layers that hold no offending admin-unit component). Pruning is by dirnames[:]
        # mutation, which only works with topdown=True.
        if max_depth > 0:
            depth = str(dirpath).rstrip(os.sep).count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []
    return migrations


def _resolve_root(explicit: Optional[str]) -> Path:
    """Resolve the processed root from --root, else the project paths config."""
    if explicit:
        return Path(explicit)
    try:
        from india_resilience_tool.config.paths import get_paths_config

        return Path(get_paths_config().base_output_root)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            "Could not resolve the processed root from the paths config; "
            f"pass --root explicitly. ({exc})"
        )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Rename processed directories/files whose name ends in a Windows-illegal "
            "trailing dot/space (dry-run by default)."
        )
    )
    parser.add_argument(
        "--root",
        default=None,
        help=(
            "Processed root to scan. Defaults to the resolved base_output_root "
            "(honours IRT_DATA_DIR / IRT_PROCESSED_ROOT via the paths config)."
        ),
    )
    parser.add_argument(
        "--state",
        default=None,
        help="Optional state filter; only migrate trees under this state token.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the renames. Without this flag the helper only previews (dry-run).",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=_DEFAULT_MAX_DEPTH,
        help=(
            "Prune the detection walk this many levels below each scan root "
            f"(default {_DEFAULT_MAX_DEPTH}; covers blocks/<district>/<block> and "
            "districts/<district>). Use 0 to disable pruning and walk the full tree "
            "(much slower over a network/9p mount)."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="List every file rename, not just a per-directory summary.",
    )
    args = parser.parse_args(argv)

    root = _resolve_root(args.root)
    if not root.exists():
        print(f"ERROR: processed root does not exist: {root}", file=sys.stderr)
        return 2

    state_token = safe_fs_component(args.state) if args.state else None

    print(f"Scanning processed root: {root}")
    if state_token:
        print(f"State filter: {args.state!r} -> token {state_token!r}")
    print(f"Mode: {'APPLY (renaming on disk)' if args.apply else 'DRY-RUN (no writes)'}")
    print("", flush=True)

    migrations = find_offending_dir_migrations(root, state_token, max_depth=args.max_depth)

    if not migrations:
        print("No offending directories found. Nothing to migrate.")
        return 0

    collisions = [m for m in migrations if m.collision]
    clean = [m for m in migrations if not m.collision]

    total_files = sum(len(m.file_renames) for m in clean)
    print(
        f"Found {len(migrations)} offending director(ies): "
        f"{len(clean)} migratable, {len(collisions)} blocked by collision; "
        f"{total_files} descendant file(s) to rename."
    )
    print("")

    for m in clean:
        print(f"DIR  {m.src_dir}")
        print(f"  -> {m.dst_dir}  ({len(m.file_renames)} file(s))")
        if args.verbose:
            for src_file, dst_file in m.file_renames:
                print(f"     FILE {src_file.name} -> {dst_file.name}")

    if collisions:
        print("")
        print("COLLISIONS (NOT migrated -- sanitized target already exists):")
        for m in collisions:
            print(f"  {m.src_dir}")
            print(f"    target already exists: {m.dst_dir}")
        print("")
        print(
            "Refusing to merge/overwrite. Resolve the duplicate (quarantine or remove "
            "one side) before re-running. A dotted and an un-dotted dir for the same "
            "unit must never coexist."
        )

    if not args.apply:
        print("")
        print("Dry-run only. Re-run with --apply to perform the renames above.")
        # Non-zero when collisions exist so a caller/agent can detect the blocked state.
        return 1 if collisions else 0

    # APPLY: rename descendant files first (parent dir still has its old name and is
    # addressable), then the directory itself. Deepest-first ordering from os.walk means
    # nested offending dirs are already handled before their parents.
    renamed_files = 0
    renamed_dirs = 0
    for m in clean:
        for src_file, dst_file in m.file_renames:
            if dst_file.exists():
                print(
                    f"ERROR: file target already exists, skipping: {dst_file}",
                    file=sys.stderr,
                )
                continue
            os.rename(_long_path(src_file), _long_path(dst_file))
            renamed_files += 1
        if m.dst_dir.exists():
            print(
                f"ERROR: dir target appeared during apply, skipping: {m.dst_dir}",
                file=sys.stderr,
            )
            continue
        os.rename(_long_path(m.src_dir), _long_path(m.dst_dir))
        renamed_dirs += 1

    print("")
    print(f"APPLIED: renamed {renamed_dirs} director(ies) and {renamed_files} file(s).")
    if collisions:
        print(
            f"{len(collisions)} director(ies) were left unmigrated due to collisions "
            "(see above)."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
