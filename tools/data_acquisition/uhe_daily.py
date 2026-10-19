"""Acquire bounded UHE-Daily India windows for the diagnostic Route B pilot.

Only the measured classic, uncompressed, row-stripped float32 GeoTIFF layout is
supported. This deliberately narrow reader fails closed on layout changes. It
does not regrid, bias-correct, interpolate missing days, or publish IRT outputs.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import struct
import time
from typing import Callable

import numpy as np

BASE_URL = "https://data.chc.ucsb.edu/people/cascade/UHE-daily/wbgtmax"
SCHEMA = "irt-uhe-window-v1"
HEADER_BYTES = 65536
WIDTH, HEIGHT = 7200, 2600
BOUNDS = (68.0, 6.0, 98.0, 38.0)  # west, south, east, north; includes islands
SAMPLE_DATES = tuple(date(y, m, 15) for y in (1985, 2000, 2014) for m in (1, 4, 7, 10))


def source_url(day: date) -> str:
    """Return the fixed public source object for one Gregorian day."""
    return f"{BASE_URL}/{day.year}/wbgtmax.{day:%Y.%m.%d}.tif"


def window_indices(bounds: tuple[float, ...]) -> tuple[int, int, int, int]:
    """Require source-cell edges and return row start/stop, column start/stop."""
    if len(bounds) != 4 or not all(math.isfinite(x) for x in bounds):
        raise ValueError("Bounds must be four finite west/south/east/north values")
    west, south, east, north = bounds
    if not (-180 <= west < east <= 180 and -60 <= south < north <= 70):
        raise ValueError("Empty or out-of-source bounds")
    indices = ((70 - north) * 20, (70 - south) * 20, (west + 180) * 20, (east + 180) * 20)
    if any(abs(x - round(x)) > 1e-7 for x in indices):
        raise ValueError("Bounds must align to 0.05 degree source-cell edges")
    return tuple(int(round(x)) for x in indices)


def parse_header(header: bytes, total_size: int) -> dict:
    """Validate the known TIFF layout, georeferencing, and contiguous strips."""
    if len(header) < 8 or header[:2] not in (b"II", b"MM"):
        raise ValueError("Not a classic TIFF header")
    endian = "<" if header[:2] == b"II" else ">"
    magic, offset = struct.unpack_from(endian + "HI", header, 2)
    if magic != 42 or offset + 2 > len(header):
        raise ValueError("Unsupported TIFF/IFD layout")
    count = struct.unpack_from(endian + "H", header, offset)[0]
    end = offset + 2 + 12 * count
    if end + 4 > len(header) or struct.unpack_from(endian + "I", header, end)[0] != 0:
        raise ValueError("IFD exceeds bounded header or multiple images present")
    tags = {}
    formats = {2: ("s", 1), 3: ("H", 2), 4: ("I", 4), 12: ("d", 8)}
    needed = {256, 257, 258, 259, 262, 273, 274, 277, 278, 279, 284, 339,
              33550, 33922, 34264, 34735, 42113}
    for i in range(count):
        pos = offset + 2 + i * 12
        tag, kind, number, pointer = struct.unpack_from(endian + "HHII", header, pos)
        if tag not in needed:
            continue
        if tag in tags or kind not in formats:
            raise ValueError(f"Duplicate/unsupported TIFF tag {tag}")
        fmt, size = formats[kind]
        nbytes = number * size
        start = pos + 8 if nbytes <= 4 else pointer
        if number == 0 or start + nbytes > len(header):
            raise ValueError(f"Tag {tag} exceeds bounded header")
        raw = header[start:start + nbytes]
        tags[tag] = raw.rstrip(b"\0").decode("ascii") if kind == 2 else struct.unpack(endian + str(number) + fmt, raw)
    expected = {256: WIDTH, 257: HEIGHT, 258: 32, 259: 1, 262: 1,
                277: 1, 278: 1, 284: 1, 339: 3}
    for tag, value in expected.items():
        if tags.get(tag) != (value,):
            raise ValueError(f"Unexpected TIFF tag {tag}: {tags.get(tag)}; expected {value}")
    if tags.get(274, (1,)) != (1,) or 34264 in tags:
        raise ValueError("Unsupported orientation or transformation matrix")
    scale = tags.get(33550, ())
    if (len(scale) != 3 or not np.allclose(scale, (0.05, 0.05, 0.), rtol=0, atol=1e-9)
            or scale[0] != scale[1] or tags.get(33922) != (0., 0., 0., -180., 70., 0.)):
        raise ValueError("Unexpected pixel scale/tiepoint")
    keys = tags.get(34735, ())
    if len(keys) < 4 or keys[:3] != (1, 1, 0) or len(keys) != 4 + 4 * keys[3]:
        raise ValueError("Invalid GeoKey directory")
    geo = {keys[i]: keys[i + 1:i + 4] for i in range(4, len(keys), 4)}
    if geo.get(1024) != (0, 1, 2) or geo.get(1025) != (0, 1, 1) or geo.get(2048) != (0, 1, 4326):
        raise ValueError("Expected EPSG:4326 geographic PixelIsArea raster")
    offsets, sizes = tags.get(273, ()), tags.get(279, ())
    if len(offsets) != HEIGHT or len(sizes) != HEIGHT or any(n != WIDTH * 4 for n in sizes):
        raise ValueError("Unexpected strip count/size")
    if any(b != a + WIDTH * 4 for a, b in zip(offsets, offsets[1:])):
        raise ValueError("Source rows are not contiguous")
    if offsets[0] < end + 4 or offsets[-1] + sizes[-1] > total_size:
        raise ValueError("Strip offsets outside source object")
    nodata = float(tags[42113]) if 42113 in tags else None
    return {"endian": endian, "offsets": offsets, "nodata": nodata, "pixel_scale": scale[0]}


class RangeClient:
    """Single-stream HTTP reader with per-request pacing and a hard attempt budget."""

    def __init__(self, max_bytes: int, interval: float = 2.0, retries: int = 2,
                 session=None, sleep: Callable[[float], None] = time.sleep):
        import requests

        self.session = session or requests.Session()
        self.max_bytes, self.interval, self.retries = max_bytes, interval, retries
        self.reserved_bytes = self.received_bytes = 0
        self.sleep = sleep
        self.last_request: float | None = None
        self.identities: dict[str, tuple] = {}

    def read(self, url: str, start: int, length: int) -> bytes:
        """Require an exact identity-encoded 206 response; never accept a full file."""
        import requests
        from urllib3.exceptions import HTTPError as StreamError

        if start < 0 or length <= 0:
            raise ValueError("Invalid byte range")
        for attempt in range(self.retries + 1):
            if self.reserved_bytes + length > self.max_bytes:
                raise ValueError("Transfer-attempt budget exhausted; resume with an explicit budget")
            self.reserved_bytes += length  # count retry reservations, including failed attempts
            if self.last_request is not None:
                self.sleep(max(0.0, self.interval - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            headers = {"Range": f"bytes={start}-{start + length - 1}",
                       "Accept-Encoding": "identity", "User-Agent": "IRT-UHE-pilot/1.0"}
            identity = self.identities.get(url)
            if identity:
                headers["If-Range"] = identity[1]
            try:
                with self.session.get(url, headers=headers, stream=True, timeout=(20, 90)) as response:
                    if response.status_code == 429:
                        raise ValueError("Host rate limit (429); stop and resume later")
                    if response.status_code >= 500:
                        response.raise_for_status()
                    if response.status_code != 206:
                        raise ValueError(f"Expected HTTP 206, got {response.status_code}; full downloads forbidden")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
                    if not match or tuple(map(int, match.groups()[:2])) != (start, start + length - 1):
                        raise ValueError("Incorrect Content-Range")
                    total = int(match[3])
                    if total < start + length or response.headers.get("Content-Encoding", "identity") != "identity":
                        raise ValueError("Invalid object length or encoded response")
                    if "Content-Length" in response.headers and int(response.headers["Content-Length"]) != length:
                        raise ValueError("Incorrect Content-Length")
                    etag = response.headers.get("ETag", "")
                    validator = etag if etag and not etag.startswith("W/") else response.headers.get("Last-Modified")
                    if not validator:
                        raise ValueError("Missing ETag/Last-Modified; cannot bind header and rows")
                    current = (total, validator)
                    if identity and current != identity:
                        raise ValueError("Remote source changed between range requests")
                    self.identities[url] = current
                    blocks, remaining = [], length
                    while remaining:
                        block = response.raw.read(min(1024 * 1024, remaining))
                        if not block:
                            raise ValueError("Truncated range response")
                        blocks.append(block)
                        self.received_bytes += len(block)
                        remaining -= len(block)
                    return b"".join(blocks)
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError, StreamError):
                if attempt == self.retries:
                    raise
                self.sleep(min(30.0, 5.0 * 2 ** attempt))
        raise RuntimeError("Unreachable retry state")


def quality(values: np.ndarray) -> dict:
    """Summarize valid support; reject empty/all-missing or implausible finite data.

    The -100..100 degC screen is only a corruption/sentinel screen, not a
    scientific acceptance band. Missing values remain NaN, never zero.
    """
    valid = values[np.isfinite(values)]
    if not valid.size:
        raise ValueError("Empty/all-missing reference window")
    if valid.min() < -100 or valid.max() > 100:
        raise ValueError("Unrecognized finite sentinel or implausible WBGT outside -100..100 degC")
    return {"valid_cells": int(valid.size), "missing_cells": int(values.size - valid.size),
            "valid_fraction": float(valid.size / values.size), "min_c": float(valid.min()),
            "max_c": float(valid.max()), "mean_c": float(valid.mean(dtype=np.float64))}


def acquire_day(client: RangeClient, day: date, bounds: tuple[float, ...]) -> tuple[np.ndarray, dict]:
    """Fetch one header and contiguous full-width rows, then crop longitude locally."""
    r0, r1, c0, c1 = window_indices(bounds)
    url = source_url(day)
    before = client.received_bytes
    header = client.read(url, 0, HEADER_BYTES)
    total, validator = client.identities[url]
    layout = parse_header(header, total)
    start, length = layout["offsets"][r0], (r1 - r0) * WIDTH * 4
    payload = client.read(url, start, length)
    rows = np.frombuffer(payload, dtype=layout["endian"] + "f4").reshape(r1 - r0, WIDTH)
    values = rows[:, c0:c1].astype("float32", copy=True)
    if layout["nodata"] is not None:
        values[values == layout["nodata"]] = np.nan
    values[~np.isfinite(values)] = np.nan
    metadata = {"schema": SCHEMA, "date": day.isoformat(), "source_url": url,
                "source_bytes": total, "source_validator": validator,
                "acquired_utc": datetime.now(timezone.utc).isoformat(),
                "header_sha256": hashlib.sha256(header).hexdigest(),
                "row_payload_sha256": hashlib.sha256(payload).hexdigest(),
                "array_sha256": hashlib.sha256(values.astype("<f4").tobytes()).hexdigest(),
                "ranges": [[0, HEADER_BYTES], [start, length]],
                "received_bytes": client.received_bytes - before,
                "bounds": list(bounds), "crs": "EPSG:4326", "units": "degC",
                "resolution_degrees": layout["pixel_scale"], "source_nodata": str(layout["nodata"]),
                "qc": quality(values), "status": "ACQUIRED_NOT_SCIENTIFICALLY_VALIDATED"}
    return values, metadata


def coordinates(bounds: tuple[float, ...], pixel_scale: float = 0.05) -> tuple[np.ndarray, np.ndarray]:
    """Return native pixel-centre latitude (north to south) and longitude."""
    r0, r1, c0, c1 = window_indices(bounds)
    if not math.isfinite(pixel_scale) or abs(pixel_scale - 0.05) > 1e-9:
        raise ValueError("Unexpected native pixel scale")
    return 70 - (np.arange(r0, r1) + 0.5) * pixel_scale, -180 + (np.arange(c0, c1) + 0.5) * pixel_scale


def save_day(path: Path, values: np.ndarray, metadata: dict) -> None:
    """Atomically store values, coordinates, and provenance as one compressed NPZ."""
    if path.exists():
        raise FileExistsError(f"Refusing overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".npz.part")
    lat, lon = coordinates(tuple(metadata["bounds"]), metadata["resolution_degrees"])
    try:
        with temporary.open("wb") as stream:
            np.savez_compressed(stream, wbgtmax_c=values, latitude=lat, longitude=lon,
                                metadata=np.array(json.dumps(metadata, sort_keys=True, allow_nan=False)))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def verify_day(path: Path, day: date, bounds: tuple[float, ...]) -> dict:
    """Verify cached date, spatial contract, array digest, coordinates, and QC."""
    with np.load(path, allow_pickle=False) as data:
        metadata = json.loads(str(data["metadata"]))
        values = data["wbgtmax_c"]
        lat, lon = coordinates(bounds, metadata["resolution_degrees"])
        if (metadata.get("schema") != SCHEMA or metadata.get("date") != day.isoformat()
                or metadata.get("source_url") != source_url(day) or metadata.get("bounds") != list(bounds)
                or metadata.get("crs") != "EPSG:4326" or metadata.get("units") != "degC"
                or values.shape != (len(lat), len(lon)) or values.dtype != np.dtype("float32")
                or not np.array_equal(data["latitude"], lat) or not np.array_equal(data["longitude"], lon)):
            raise ValueError(f"Cached contract mismatch: {path}")
        if hashlib.sha256(values.astype("<f4").tobytes()).hexdigest() != metadata.get("array_sha256"):
            raise ValueError(f"Cached array checksum mismatch: {path}")
        if quality(values) != metadata.get("qc"):
            raise ValueError(f"Cached QC mismatch: {path}")
        return metadata


def selected_dates(args: argparse.Namespace) -> list[date]:
    """Resolve a bounded QC sample or an explicitly selected inclusive date range."""
    if args.sample:
        if args.start or args.end:
            raise ValueError("--sample cannot be combined with --start/--end")
        return list(SAMPLE_DATES)
    start, end = args.start or date(1985, 1, 1), args.end or date(2014, 12, 31)
    if not date(1983, 1, 1) <= start <= end <= date(2016, 12, 31):
        raise ValueError("Require 1983-01-01 <= start <= end <= 2016-12-31")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def main(argv: list[str] | None = None) -> int:
    """Print a plan by default; acquire or verify only with explicit mode flags."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print plan; no network or writes (default)")
    mode.add_argument("--download", action="store_true", help="Download selected dates to diagnostic NPZ files")
    mode.add_argument("--verify", action="store_true", help="Verify all selected local dates; no network or writes")
    parser.add_argument("--sample", action="store_true", help="12 seasonal dates in 1985, 2000, 2014")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--bounds", type=float, nargs=4, default=BOUNDS, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    parser.add_argument("--output-dir", type=Path, default=Path("scratch/uhe_daily"))
    parser.add_argument("--max-transfer-gb", type=float, default=0.25,
                        help="Decimal GB cap on requested response bodies, including retry attempts")
    parser.add_argument("--request-interval", type=float, default=2.0, help="Minimum seconds between request starts (>=2)")
    parser.add_argument("--retries", type=int, default=2, choices=range(3))
    args = parser.parse_args(argv)
    try:
        bounds = tuple(args.bounds)
        r0, r1, c0, c1 = window_indices(bounds)
        days = selected_dates(args)
        if not math.isfinite(args.max_transfer_gb) or args.max_transfer_gb <= 0:
            raise ValueError("Transfer budget must be finite and positive")
        if not math.isfinite(args.request_interval) or args.request_interval < 2:
            raise ValueError("Request interval must be finite and >=2 seconds")
        daily_bytes = HEADER_BYTES + (r1 - r0) * WIDTH * 4
        plan = {"schema": SCHEMA, "mode": "download" if args.download else "verify" if args.verify else "dry-run",
                "dates": len(days), "start": days[0].isoformat(), "end": days[-1].isoformat(),
                "sample_dates": [d.isoformat() for d in days] if args.sample else None,
                "bounds": bounds, "shape": [r1 - r0, c1 - c0], "output_dir": str(args.output_dir),
                "estimated_transfer_gb_no_retries": daily_bytes * len(days) / 1e9,
                "uncompressed_arrays_gb": (r1 - r0) * (c1 - c0) * 4 * len(days) / 1e9,
                "max_transfer_gb": args.max_transfer_gb, "request_interval_seconds": args.request_interval,
                "note": "Estimates assume verified row-strip layout. NPZ storage and elapsed time must be measured."}
        print(json.dumps(plan, indent=2))
        if not (args.download or args.verify):
            return 0
        pending = []
        for day in days:
            path = args.output_dir / str(day.year) / f"wbgtmax.{day.isoformat()}.npz"
            if path.exists():
                verify_day(path, day, bounds)
            elif args.verify:
                raise FileNotFoundError(f"Missing expected date: {path}")
            else:
                pending.append((day, path))
        if args.verify:
            print(f"Verified {len(days)} dates; acquisition integrity only, not scientific acceptance.")
            return 0
        budget = int(args.max_transfer_gb * 1e9)
        if daily_bytes * len(pending) > budget:
            raise ValueError(f"{len(pending)} missing days exceed budget before retries; select a smaller date range or raise the explicit cap")
        client = RangeClient(budget, args.request_interval, args.retries)
        started = time.monotonic()
        for day, path in pending:
            values, metadata = acquire_day(client, day, bounds)
            save_day(path, values, metadata)
            print(json.dumps({"date": str(day), "path": str(path), "stored_bytes": path.stat().st_size,
                              "received_bytes_total": client.received_bytes, "reserved_bytes_total": client.reserved_bytes,
                              "elapsed_seconds": round(time.monotonic() - started, 2), "qc": metadata["qc"]}), flush=True)
        print(f"Complete: {len(pending)} acquired, {len(days) - len(pending)} verified cached dates.")
        return 0
    except (ValueError, OSError) as exc:
        parser.exit(1, f"UHE acquisition stopped: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
