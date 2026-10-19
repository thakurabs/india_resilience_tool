"""Journaled production cutover and rollback for the CarbonPlan shade release (CHG-0702).

Replaces the four optimised ``metrics/wbgt_shade_stull_*`` directories with the
verified staged ones, moves the four legacy ``processed/wbgt_shade_stull_*``
trees aside (or, with ``--processed-mode replace``, swaps in pre-copied staged
processed trees), and regenerates ``bundle_manifest.json`` -- all as same-volume
renames recorded in a journal, so the dashboard is down only for the swap.

Phases, each read-only unless ``--run`` is given:

``prepare``
    Verify every staged parquet is CarbonPlan-signed and hash the staged
    inventory; verify every live legacy file still matches the identity sidecar;
    copy the staged directories onto the production volume and re-hash the
    copies; time the manifest rescan; estimate downtime and refuse above
    ``--max-downtime-minutes`` (default 30). No live path is touched.
``swap``
    Requires a PASS ``prepare`` record and a stopped dashboard. Re-checks
    sentinels, then renames live -> backup and incoming -> live for each
    directory, journaling every rename before and after it happens, backs up
    and regenerates the manifest, and verifies directory identity, inventory and
    every promoted file hash. Records the publication time that opens the
    seven-day rollback window.
``rollback``
    Reverses the journal (both completed and half-completed swaps), restores the
    shared files from backup, and verifies the legacy files against the sidecar.
``status`` / ``close``
    Report the journal and rollback window; ``close`` records the user's
    sign-off after seven days. Retiring the sidecar, backups and compatibility
    readers is a separate, explicit change.

Run with the Windows ``irt`` interpreter (the app's), against ``D:`` paths.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

from india_resilience_tool.data.wbgt_contract import (
    CARBONPLAN_SHADE_SIGNATURE,
    LEGACY_SHADE_IDENTITY,
    SHADE_IDENTITY_SIDECAR,
    SHADE_SLUGS,
    shade_artifact_identity,
)

SLUGS = tuple(sorted(SHADE_SLUGS))
SHARED_FILES = ("bundle_manifest.json", "parity_report.json", SHADE_IDENTITY_SIDECAR)
ROLLBACK_DAYS = 7
TIERS = ("masters", "yearly_models", "yearly_ensemble", "state_values")


def sha256(path: Path) -> str:
    """Hash one file in bounded memory."""
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def inventory(root: Path) -> dict[str, str]:
    """Hash every regular file under ``root`` by POSIX relative path."""
    files = sorted(path for path in root.rglob("*") if path.is_file())
    return {path.relative_to(root).as_posix(): sha256(path) for path in files}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically so a crash never leaves a half record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, path)


class Release:
    """Paths for one release id under ``<data_dir>/_shade_cutover/<id>``."""

    def __init__(self, data_dir: Path, release_id: str):
        self.data_dir = data_dir
        self.bundle = data_dir / "processed_optimised"
        self.processed = data_dir / "processed"
        self.root = data_dir / "_shade_cutover" / release_id
        self.incoming = self.root / "incoming"
        self.backup = self.root / "backup"
        self.prepare_record = self.root / "prepare.json"
        self.journal = self.root / "journal.json"

    def live_optimised(self, slug: str) -> Path:
        return self.bundle / "metrics" / slug

    def live_processed(self, slug: str) -> Path:
        return self.processed / slug


def verify_staged_bundle(staged_bundle: Path) -> dict[str, dict[str, str]]:
    """Every staged shade tier exists and every parquet is CarbonPlan-signed."""
    result = {}
    for slug in SLUGS:
        metric = staged_bundle / "metrics" / slug
        missing = [tier for tier in TIERS if not (metric / tier).is_dir()]
        if missing:
            raise ValueError(f"{metric}: missing tiers {missing}")
        for path in sorted(metric.rglob("*.parquet")):
            if shade_artifact_identity(path) != CARBONPLAN_SHADE_SIGNATURE:
                raise ValueError(f"{path}: not CarbonPlan-signed")
        result[slug] = inventory(metric)
    return result


def verify_live_legacy(release: Release) -> dict:
    """Every live optimised shade file must still be the sidecar-recorded legacy file."""
    sidecar = release.bundle / SHADE_IDENTITY_SIDECAR
    if not sidecar.is_file():
        raise ValueError(f"No legacy identity sidecar at {sidecar}; install it before cutover")
    record = json.loads(sidecar.read_text(encoding="utf-8"))
    if record.get("method") != LEGACY_SHADE_IDENTITY:
        raise ValueError("Identity sidecar does not describe the legacy method")
    live = {}
    for slug in SLUGS:
        for relative, digest in inventory(release.live_optimised(slug)).items():
            live[f"metrics/{slug}/{relative}"] = digest
    if live != record["optimized_files"]:
        changed = sorted(set(live.items()) ^ set(record["optimized_files"].items()))[:5]
        raise ValueError(f"Live shade trees differ from the identity sidecar: {changed}")
    return {"sidecar_sha256": sha256(sidecar), "files_verified": len(live)}


def time_manifest_rescan() -> float:
    """Time the manifest payload computation against production without writing."""
    from tools.optimized.build_processed_optimised import _manifest_payload
    from india_resilience_tool.config.paths import get_paths_config

    started = time.monotonic()
    _manifest_payload(get_paths_config().data_dir)
    return time.monotonic() - started


def prepare(args: argparse.Namespace, release: Release) -> dict:
    """Stage on the production volume and measure; never touch a live path."""
    if Path(release.bundle.anchor) != Path(release.root.anchor):
        raise ValueError("Cutover area must be on the production volume for atomic renames")
    staged = verify_staged_bundle(args.staged_bundle)
    legacy = verify_live_legacy(release)
    if args.processed_mode == "replace":
        if args.staged_processed is None:
            raise ValueError("--processed-mode replace needs --staged-processed")
        for slug in SLUGS:
            if not (args.staged_processed / slug).is_dir():
                raise ValueError(f"Missing staged processed tree for {slug}")
    if not args.run:
        return {"phase": "prepare", "writes": False, "staged_files": {s: len(v) for s, v in staged.items()},
                "legacy": legacy}
    if release.incoming.exists():
        raise ValueError(f"{release.incoming} exists; use a new --release-id")
    copies = {}
    started = time.monotonic()
    for slug in SLUGS:
        target = release.incoming / "optimised" / slug
        shutil.copytree(args.staged_bundle / "metrics" / slug, target)
        copied = inventory(target)
        if copied != staged[slug]:
            raise ValueError(f"{slug}: copy differs from the staged inventory")
        copies[slug] = {"files": len(copied), "inode": os.stat(target).st_ino, "inventory": copied}
    copy_seconds = time.monotonic() - started
    processed_copies = {}
    if args.processed_mode == "replace":
        for slug in SLUGS:
            target = release.incoming / "processed" / slug
            shutil.copytree(args.staged_processed / slug, target)
            processed_copies[slug] = {"inode": os.stat(target).st_ino}
    manifest_seconds = time_manifest_rescan()
    hash_seconds_started = time.monotonic()
    for slug in SLUGS:
        inventory(release.incoming / "optimised" / slug)
    rehash_seconds = time.monotonic() - hash_seconds_started
    # Renames are O(1); downtime is the manifest rescan plus one full re-hash of
    # the promoted optimised files and a margin for the scoped audit.
    estimate_minutes = (manifest_seconds + rehash_seconds) / 60 + args.audit_minutes
    record = {"phase": "prepare", "status": "PASS", "at": now(),
              "staged_bundle": str(args.staged_bundle), "processed_mode": args.processed_mode,
              "legacy": legacy, "optimised": copies, "processed": processed_copies,
              "timings_seconds": {"copy": round(copy_seconds, 1),
                                  "manifest_rescan": round(manifest_seconds, 1),
                                  "rehash_promoted": round(rehash_seconds, 1)},
              "audit_minutes_allowance": args.audit_minutes,
              "estimated_downtime_minutes": round(estimate_minutes, 1),
              "max_downtime_minutes": args.max_downtime_minutes}
    if estimate_minutes > args.max_downtime_minutes:
        record["status"] = "ABORT_DOWNTIME"
    write_json(release.prepare_record, record)
    return record


def _rename(journal: dict, path: Path, source: Path, target: Path) -> None:
    """Journal intent, rename, journal completion."""
    entry = {"source": str(source), "target": str(target), "state": "intent"}
    journal["renames"].append(entry)
    write_json(path, journal)
    os.rename(source, target)
    entry["state"] = "done"
    write_json(path, journal)


def swap(args: argparse.Namespace, release: Release) -> dict:
    """Swap staged trees in, journaled; the dashboard must be stopped."""
    record = json.loads(release.prepare_record.read_text(encoding="utf-8"))
    if record.get("status") != "PASS":
        raise ValueError(f"prepare status is {record.get('status')}; replan before swapping")
    if release.journal.exists():
        raise ValueError("A journal already exists for this release; use status or rollback")
    verify_live_legacy(release)
    for slug in SLUGS:
        incoming = release.incoming / "optimised" / slug
        if os.stat(incoming).st_ino != record["optimised"][slug]["inode"]:
            raise ValueError(f"{incoming}: directory identity changed since prepare")
    if not args.run:
        return {"phase": "swap", "writes": False, "ready": True}
    journal = {"phase": "swap", "started_at": now(), "renames": [], "shared": {},
               "processed_mode": record["processed_mode"], "status": "IN_PROGRESS"}
    write_json(release.journal, journal)
    for name in SHARED_FILES:
        source = release.bundle / name
        if source.is_file():
            target = release.backup / "shared" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            journal["shared"][name] = {"sha256": sha256(target)}
    write_json(release.journal, journal)
    for slug in SLUGS:
        (release.backup / "optimised").mkdir(parents=True, exist_ok=True)
        _rename(journal, release.journal, release.live_optimised(slug),
                release.backup / "optimised" / slug)
        _rename(journal, release.journal, release.incoming / "optimised" / slug,
                release.live_optimised(slug))
        if release.live_processed(slug).exists():
            (release.backup / "processed").mkdir(parents=True, exist_ok=True)
            _rename(journal, release.journal, release.live_processed(slug),
                    release.backup / "processed" / slug)
        if record["processed_mode"] == "replace":
            _rename(journal, release.journal, release.incoming / "processed" / slug,
                    release.live_processed(slug))
    from tools.optimized.build_processed_optimised import write_manifest_only

    journal["manifest"] = write_manifest_only(release.data_dir)
    write_json(release.journal, journal)
    verification = {}
    for slug in SLUGS:
        live = release.live_optimised(slug)
        if os.stat(live).st_ino != record["optimised"][slug]["inode"]:
            raise ValueError(f"{live}: promoted directory identity differs; run rollback")
        if inventory(live) != record["optimised"][slug]["inventory"]:
            raise ValueError(f"{live}: promoted inventory differs; run rollback")
        verification[slug] = "inode+inventory"
    manifest = json.loads((release.bundle / "bundle_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("shade_method_signature") != CARBONPLAN_SHADE_SIGNATURE:
        raise ValueError("Regenerated manifest does not report the CarbonPlan identity; run rollback")
    journal.update({"status": "PUBLISHED", "published_at": now(), "verification": verification,
                    "rollback_window_days": ROLLBACK_DAYS, "rollback_window_closed": False})
    write_json(release.journal, journal)
    return journal


def rollback(args: argparse.Namespace, release: Release) -> dict:
    """Undo every completed or intended rename in reverse, then restore shared files."""
    journal = json.loads(release.journal.read_text(encoding="utf-8"))
    if journal.get("rollback_window_closed"):
        raise ValueError("The rollback window was closed with sign-off; rollback is retired")
    pending = [entry for entry in reversed(journal["renames"]) if entry["state"] in {"done", "intent"}]
    if not args.run:
        return {"phase": "rollback", "writes": False, "renames_to_reverse": len(pending)}
    for entry in pending:
        source, target = Path(entry["source"]), Path(entry["target"])
        if entry["state"] == "intent" and source.exists() and not target.exists():
            entry["state"] = "reverted"  # the rename never happened
            continue
        os.rename(target, source)
        entry["state"] = "reverted"
        write_json(release.journal, journal)
    for name, meta in journal.get("shared", {}).items():
        backup = release.backup / "shared" / name
        if sha256(backup) != meta["sha256"]:
            raise ValueError(f"Backup of {name} changed; restore it manually")
        temporary = release.bundle / f"{name}.part"
        shutil.copy2(backup, temporary)
        os.replace(temporary, release.bundle / name)
    legacy = verify_live_legacy(release)
    journal.update({"status": "ROLLED_BACK", "rolled_back_at": now(), "legacy_verified": legacy})
    write_json(release.journal, journal)
    return journal


def status(release: Release) -> dict:
    """Report the journal and whether the rollback window may close."""
    if not release.journal.exists():
        return {"release": str(release.root), "status": "NOT_SWAPPED",
                "prepare": json.loads(release.prepare_record.read_text()) if release.prepare_record.exists() else None}
    journal = json.loads(release.journal.read_text(encoding="utf-8"))
    out = {key: journal.get(key) for key in ("status", "started_at", "published_at",
                                              "rollback_window_closed", "signed_off_by")}
    if journal.get("published_at"):
        opens = datetime.fromisoformat(journal["published_at"])
        out["earliest_close"] = (opens + timedelta(days=ROLLBACK_DAYS)).isoformat(timespec="seconds")
    return out


def close(args: argparse.Namespace, release: Release) -> dict:
    """Record sign-off once seven consecutive days have passed since publication."""
    journal = json.loads(release.journal.read_text(encoding="utf-8"))
    if journal.get("status") != "PUBLISHED":
        raise ValueError(f"Release status is {journal.get('status')}, not PUBLISHED")
    published = datetime.fromisoformat(journal["published_at"])
    if datetime.now(timezone.utc) < published + timedelta(days=ROLLBACK_DAYS):
        raise ValueError(f"The rollback window cannot close before {published + timedelta(days=ROLLBACK_DAYS)}")
    if not args.signed_off_by:
        raise ValueError("--signed-off-by is required: the window closes only with explicit sign-off")
    if not args.run:
        return {"phase": "close", "writes": False, "eligible": True}
    journal.update({"rollback_window_closed": True, "closed_at": now(),
                    "signed_off_by": args.signed_off_by})
    write_json(release.journal, journal)
    return journal


def main(argv: list[str] | None = None) -> int:
    """Run one cutover phase; every phase plans only unless ``--run`` is passed."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phase", choices=("prepare", "swap", "rollback", "status", "close"))
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--staged-bundle", type=Path)
    parser.add_argument("--staged-processed", type=Path)
    parser.add_argument("--processed-mode", choices=("aside", "replace"), default="aside")
    parser.add_argument("--max-downtime-minutes", type=float, default=30.0)
    parser.add_argument("--audit-minutes", type=float, default=5.0,
                        help="Allowance for the scoped cutover audit, measured during staging")
    parser.add_argument("--signed-off-by")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args(argv)
    if args.data_dir is None:
        from india_resilience_tool.config.paths import get_paths_config
        args.data_dir = get_paths_config().data_dir
    release = Release(args.data_dir.resolve(), args.release_id)
    if args.phase == "prepare":
        if args.staged_bundle is None:
            parser.error("prepare needs --staged-bundle")
        result = prepare(args, release)
    elif args.phase == "swap":
        result = swap(args, release)
    elif args.phase == "rollback":
        result = rollback(args, release)
    elif args.phase == "close":
        result = close(args, release)
    else:
        result = status(release)
    printable = {key: value for key, value in result.items() if key != "optimised"}
    print(json.dumps(printable, indent=2, default=str))
    return 0 if result.get("status") not in {"ABORT_DOWNTIME"} else 3


if __name__ == "__main__":
    raise SystemExit(main())
