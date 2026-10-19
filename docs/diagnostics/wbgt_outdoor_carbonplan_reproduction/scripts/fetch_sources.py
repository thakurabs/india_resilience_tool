"""Fetch only checksum-pinned, inspected upstream source into the private scratch area."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import urllib.request


def main() -> None:
    """Download pinned sources without installing or importing them."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/carbonplan_reproduction"))
    parser.add_argument("--verify-remote", action="store_true", help="Re-fetch even when a valid local copy exists")
    args = parser.parse_args()
    work = args.work_dir.resolve()
    if "scratch" not in work.parts:
        raise ValueError("Source downloads must remain under scratch")
    target = work / "upstream"
    target.mkdir(parents=True, exist_ok=True)
    lock = json.loads((Path(__file__).resolve().parents[1] / "source_lock.json").read_text())
    for name, record in lock["files"].items():
        path = target / name
        data = path.read_bytes() if path.exists() and not args.verify_remote else None
        if data is None:
            with urllib.request.urlopen(record["url"], timeout=90) as response:
                data = response.read(1_000_001)
            if len(data) > 1_000_000:
                raise ValueError("Unexpectedly large source file")
        if hashlib.sha256(data).hexdigest() != record["sha256"]:
            raise ValueError(f"Source checksum mismatch: {name}")
        if not path.exists():
            path.write_bytes(data)
        print("Verified", name, flush=True)


if __name__ == "__main__":
    main()
