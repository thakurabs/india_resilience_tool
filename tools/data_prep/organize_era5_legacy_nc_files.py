#!/usr/bin/env python3
"""
organize_era5_legacy_nc_files.py

Find legacy ERA5 daily-statistics NetCDF files in a flat nc directory:

  era5_daily_2m_temperature_daily_mean_YYYYMM_tel.nc
  era5_daily_total_precipitation_daily_sum_YYYYMM_tel.nc

Rename them to CMIP-style aliases:

  era5_daily_tas_daily_mean_YYYYMM_tel.nc
  era5_daily_pr_daily_sum_YYYYMM_tel.nc

…then move them into per-variable subfolders:

  <NC_DIR>\\tas\\
  <NC_DIR>\\pr\\

This script ONLY handles the two legacy patterns above (as requested).

Windows note:
- If a file is open in another process, Windows will lock it and moves/renames will fail
  with WinError 32. This script will retry a few times and then skip locked files
  instead of crashing.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class RenameRule:
    pattern: re.Pattern[str]
    target_subdir: str
    target_prefix: str


LEGACY_RULES: List[RenameRule] = [
    RenameRule(
        pattern=re.compile(r"^era5_daily_2m_temperature_daily_mean_(\d{6})_tel\.nc$"),
        target_subdir="tas",
        target_prefix="era5_daily_tas_daily_mean_",
    ),
    RenameRule(
        pattern=re.compile(r"^era5_daily_total_precipitation_daily_sum_(\d{6})_tel\.nc$"),
        target_subdir="pr",
        target_prefix="era5_daily_pr_daily_sum_",
    ),
]


def _match_rule(filename: str) -> Optional[Tuple[RenameRule, str]]:
    """Return (rule, yyyymm) if filename matches a legacy rule; otherwise None."""
    for rule in LEGACY_RULES:
        m = rule.pattern.match(filename)
        if m:
            return rule, m.group(1)
    return None


def _safe_move_with_retries(
    src: Path,
    dst: Path,
    retries: int = 8,
    initial_sleep_s: float = 0.25,
) -> Tuple[bool, str]:
    """
    Try to move src -> dst with retries. Returns (success, message).

    On Windows, PermissionError/WinError 32 typically indicates the file is open elsewhere.
    """
    sleep_s = initial_sleep_s
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            shutil.move(str(src), str(dst))
            return True, f"moved on attempt {attempt}"
        except PermissionError as e:
            last_err = e
            # Backoff and retry
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.5, 3.0)
        except OSError as e:
            last_err = e
            # Some OS-level issues are transient; retry similarly
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.5, 3.0)

    return False, f"failed after {retries} attempts: {last_err}"


def organize_flat_nc_dir(nc_dir: Path, dry_run: bool = False) -> None:
    """
    Rename and move legacy ERA5 NetCDFs found directly under nc_dir into subfolders.

    Behavior:
    - Only files directly under nc_dir are considered (not recursive).
    - If the target file already exists and is non-empty, the source is left untouched.
    - If the target file exists but is empty (0 bytes), it is replaced.
    - If a file is locked (WinError 32), the script retries and then skips it.

    Args:
        nc_dir: Path to the flat NetCDF directory (e.g., D:\\projects\\irt_data\\era5\\nc)
        dry_run: If True, prints intended actions without modifying files.
    """
    if not nc_dir.exists():
        raise FileNotFoundError(f"nc_dir not found: {nc_dir}")

    moved = 0
    skipped = 0
    unmatched = 0
    locked = 0
    errors = 0

    for p in sorted(nc_dir.iterdir()):
        if not p.is_file():
            continue
        if p.suffix.lower() != ".nc":
            continue

        match = _match_rule(p.name)
        if not match:
            unmatched += 1
            continue

        rule, yyyymm = match
        target_dir = nc_dir / rule.target_subdir
        target_dir.mkdir(parents=True, exist_ok=True)

        target_name = f"{rule.target_prefix}{yyyymm}_tel.nc"
        target_path = target_dir / target_name

        # Skip if target exists and looks valid
        if target_path.exists() and target_path.stat().st_size > 0:
            print(f"[SKIP] Target exists: {target_path} (leaving source: {p.name})")
            skipped += 1
            continue

        if dry_run:
            print(f"[DRY] MOVE {p} -> {target_path}")
            moved += 1
            continue

        # If target exists but empty, remove it so move succeeds
        if target_path.exists():
            try:
                target_path.unlink()
            except Exception as e:
                print(f"[ERR ] Could not remove existing empty target {target_path}: {e}")
                errors += 1
                continue

        ok, msg = _safe_move_with_retries(p, target_path, retries=10, initial_sleep_s=0.25)
        if ok:
            print(f"[MOVE] {p.name} -> {target_path.relative_to(nc_dir)} ({msg})")
            moved += 1
        else:
            # Likely locked; keep going
            print(f"[LOCK] Could not move {p.name} -> {target_path.name} ({msg})")
            locked += 1

    print("\n=== Summary ===")
    print(f"Moved        : {moved}")
    print(f"Skipped      : {skipped}")
    print(f"Locked (skip): {locked}")
    print(f"Errors       : {errors}")
    print(f"Unmatched nc files (ignored): {unmatched}")

    if locked > 0:
        print(
            "\nTip: Some files are locked by another process. Common causes are a Python/xarray session, "
            "Streamlit, VS Code preview, or Jupyter. Close those and re-run; only the locked ones will remain."
        )


def main() -> None:
    # Adjust if needed
    nc_dir = Path(r"D:\projects\irt_data\era5\nc")

    # Set True to preview actions
    dry_run = False

    organize_flat_nc_dir(nc_dir=nc_dir, dry_run=dry_run)


if __name__ == "__main__":
    main()
