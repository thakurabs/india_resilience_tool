#!/usr/bin/env python3
"""Parallel pan-India NEX-GDDP-CMIP6 downloader (v2).

See plan: build a manifest of `(model, experiment, member, variable, year)` tasks
by listing each scope from S3 exactly once, then download in parallel with a
thread pool, atomic writes, and classified retries. Output layout is distinct
from `_v1.py` to avoid disturbing the compute pipeline:

    ${out_dir}/${member_dir}/${experiment}/${variable}/${model}/${year}.nc

`_v1.py` and `download_pan_india_raw.sh` are intentionally left unchanged.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import re
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable

import boto3
import botocore.exceptions
import xarray as xr
from botocore import UNSIGNED
from botocore.config import Config

S3_BUCKET = "nex-gddp-cmip6"
S3_PREFIX = "NEX-GDDP-CMIP6"

DEFAULT_MEMBER = "r1i1p1f1"
DEFAULT_MEMBER_DIRNAME = "r1i1p1f1_panIndia"
DEFAULT_VARIABLES = ("pr", "tas", "tasmax", "tasmin", "hurs")
DEFAULT_EXPERIMENTS = ("historical", "ssp245", "ssp585")
DEFAULT_WORKERS = 8
DEFAULT_OPEN_MODE = "download-first"
INDIA_BBOX = (6.0, 38.0, 68.0, 98.0)  # south, north, west, east
PROGRESS_EVERY = 25

_YEAR_RE = re.compile(r"_(\d{4})\.nc$")
_RETRY_BACKOFF_SECONDS = (0, 1, 4, 16)

logger = logging.getLogger("nex_v2")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DownloadTask:
    s3_key: str
    out_path: Path
    variable: str
    model: str
    experiment: str
    member: str
    year: int


class LocalStatus(str, Enum):
    SKIP_GOOD = "skip_good"
    ENQUEUE_FRESH = "enqueue_fresh"
    ENQUEUE_FORCE = "enqueue_force"
    ENQUEUE_AFTER_QUARANTINE = "enqueue_after_quarantine"


@dataclass
class LocalDecision:
    out_path: Path
    variable: str
    model: str
    experiment: str
    year: int
    local_status: LocalStatus
    was_quarantined: bool
    s3_key: str | None


class TaskStatus(str, Enum):
    DOWNLOADED = "downloaded"
    FAILED = "failed"


@dataclass
class TaskResult:
    task: DownloadTask
    status: TaskStatus
    error_type: str | None = None
    error_message: str | None = None


@dataclass
class RunSummary:
    candidate_files: int = 0
    queued: int = 0
    skipped_existing: int = 0
    missing_on_s3: int = 0
    downloaded: int = 0
    failed: int = 0
    quarantined: int = 0
    quarantined_unreplaceable: int = 0
    scope_fatal_duplicates: int = 0


# ---------------------------------------------------------------------------
# Thread-local boto3 client
# ---------------------------------------------------------------------------


_thread_local = threading.local()

# Global HDF5 serialization lock. The HDF5 C library is not fully thread-safe;
# concurrent open_dataset/to_netcdf across workers can crash with
# "NetCDF: String match to name in use". Wrap all HDF5-touching code under this
# lock. S3 downloads (the network-bound part) stay outside so workers still run
# in parallel for I/O.
_HDF5_LOCK = threading.Lock()


def get_s3_client():
    cli = getattr(_thread_local, "s3", None)
    if cli is None:
        cli = boto3.client(
            "s3",
            region_name="us-west-2",
            config=Config(
                signature_version=UNSIGNED,
                retries={"max_attempts": 10, "mode": "adaptive"},
                max_pool_connections=32,
            ),
        )
        _thread_local.s3 = cli
    return cli


# ---------------------------------------------------------------------------
# Retry classification
# ---------------------------------------------------------------------------


TRANSIENT_CLIENT_CODES = {
    "SlowDown",
    "RequestTimeout",
    "InternalError",
    "ServiceUnavailable",
    "503",
    "500",
}
TRANSIENT_EXC: tuple[type[BaseException], ...] = (
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectionClosedError,
    botocore.exceptions.IncompleteReadError,
    OSError,
)
FATAL_EXC: tuple[type[BaseException], ...] = (KeyError, ValueError, RuntimeError)

# Substrings that mark a RuntimeError as an HDF5 thread-race symptom rather
# than a genuine logic failure. The lock (see _HDF5_LOCK) should prevent these,
# but treating them as transient is cheap insurance for any path that slips by.
_HDF5_RACE_SIGNATURES: tuple[str, ...] = ("name in use",)


def is_transient(exc: BaseException) -> bool:
    # HDF5 thread-race RuntimeError: retry. Checked before the FATAL_EXC
    # short-circuit because RuntimeError is otherwise fatal.
    if isinstance(exc, RuntimeError) and any(
        sig in str(exc) for sig in _HDF5_RACE_SIGNATURES
    ):
        return True
    if isinstance(exc, FATAL_EXC):
        return False
    if isinstance(exc, botocore.exceptions.ClientError):
        return (
            exc.response.get("Error", {}).get("Code") in TRANSIENT_CLIENT_CODES
        )
    if isinstance(exc, PermissionError):
        return True
    return isinstance(exc, TRANSIENT_EXC)


# ---------------------------------------------------------------------------
# Year policy
# ---------------------------------------------------------------------------


def _policy_years(exp: str) -> list[int]:
    if exp == "historical":
        return list(range(1951, 2015))
    if exp in ("ssp245", "ssp585"):
        return list(range(2015, 2101))
    return []


def resolve_years_for_experiment(
    exp: str, user_years: set[int] | None
) -> list[int]:
    policy = set(_policy_years(exp))
    if user_years is None:
        return sorted(policy)
    return sorted(policy & user_years)


# ---------------------------------------------------------------------------
# Coordinate helpers (ported / extended from _v1.detect_lat_lon_vars)
# ---------------------------------------------------------------------------


def detect_lat_lon_vars(ds: xr.Dataset) -> tuple[str | None, str | None]:
    lat_var = None
    lon_var = None
    for c in ds.coords:
        lc = str(c).lower()
        if lc in ("lat", "latitude") and lat_var is None:
            lat_var = c
        if lc in ("lon", "longitude") and lon_var is None:
            lon_var = c
    if lat_var is None:
        for c in list(ds.coords) + list(ds.data_vars):
            if "lat" in str(c).lower():
                lat_var = c
                break
    if lon_var is None:
        for c in list(ds.coords) + list(ds.data_vars):
            if "lon" in str(c).lower():
                lon_var = c
                break
    return lat_var, lon_var


def _ordered_slice(coord_vals, lo: float, hi: float) -> slice:
    if float(coord_vals[0]) <= float(coord_vals[-1]):
        return slice(lo, hi)
    return slice(hi, lo)


def normalize_bbox_lon_for_coord(
    lon_values, lon_min: float, lon_max: float
) -> tuple[float, float]:
    src_min = float(min(lon_values))
    src_max = float(max(lon_values))
    if src_max > 180 and lon_min < 0:
        return lon_min + 360, lon_max + 360
    if src_min < 0 and lon_min > 180:
        return lon_min - 360, lon_max - 360
    return lon_min, lon_max


def validate_subset_nonempty(
    ds_subset: xr.Dataset, lat_var: str, lon_var: str, key: str
) -> None:
    if ds_subset[lat_var].size == 0 or ds_subset[lon_var].size == 0:
        raise RuntimeError(
            f"bbox selection produced empty subset for {key} "
            f"(lat n={ds_subset[lat_var].size}, lon n={ds_subset[lon_var].size})"
        )


# ---------------------------------------------------------------------------
# S3 discovery
# ---------------------------------------------------------------------------


def list_models_from_s3(s3) -> list[str]:
    models: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/", Delimiter="/"
    ):
        for pref in page.get("CommonPrefixes", []) or []:
            models.add(pref["Prefix"].split("/", 1)[1].strip("/"))
    return sorted(models)


def list_year_keys_for_scope(
    s3, model: str, exp: str, member: str, variable: str
) -> tuple[dict[int, str], list[tuple[int, list[str]]]]:
    """Single paginated listing per scope.

    Returns:
        year_to_key: {year: s3_key} for years with exactly one matching key.
        duplicate_years: [(year, [keys, ...])] for years with multiple distinct keys.
    """
    folder = f"{S3_PREFIX}/{model}/{exp}/{member}/{variable}/"
    paginator = s3.get_paginator("list_objects_v2")
    year_to_keys: dict[int, list[str]] = {}
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=folder):
        for obj in page.get("Contents", []) or []:
            k = obj["Key"]
            tail = k.rsplit("/", 1)[-1]
            m = _YEAR_RE.search(tail)
            if not m:
                continue
            year = int(m.group(1))
            year_to_keys.setdefault(year, []).append(k)

    year_to_key: dict[int, str] = {}
    duplicate_years: list[tuple[int, list[str]]] = []
    for year, keys in year_to_keys.items():
        unique = sorted(set(keys))
        if len(unique) == 1:
            year_to_key[year] = unique[0]
        else:
            duplicate_years.append((year, unique))
    return year_to_key, duplicate_years


# ---------------------------------------------------------------------------
# Local output policy
# ---------------------------------------------------------------------------


def _try_open(path: Path) -> bool:
    try:
        with xr.open_dataset(path, engine="h5netcdf"):
            return True
    except Exception:
        return False


def handle_existing(
    out_path: Path,
    *,
    skip_existing: bool,
    verify: bool,
    delete_bad: bool,
) -> tuple[LocalStatus, bool]:
    """Decide what to do with whatever is currently at `out_path`.

    Returns (status, was_quarantined).
    """
    exists = out_path.exists() and out_path.stat().st_size > 0
    if not exists:
        return LocalStatus.ENQUEUE_FRESH, False

    if not verify:
        if skip_existing:
            return LocalStatus.SKIP_GOOD, False
        return LocalStatus.ENQUEUE_FORCE, False

    # verify == True
    if _try_open(out_path):
        if skip_existing:
            return LocalStatus.SKIP_GOOD, False
        return LocalStatus.ENQUEUE_FORCE, False

    # corrupt — quarantine or delete
    if delete_bad:
        try:
            out_path.unlink(missing_ok=True)
        except OSError:
            pass
    else:
        bad = out_path.with_suffix(out_path.suffix + ".bad")
        try:
            os.replace(out_path, bad)
        except OSError:
            # best effort; if move fails fall back to delete
            try:
                out_path.unlink(missing_ok=True)
            except OSError:
                pass
    return LocalStatus.ENQUEUE_AFTER_QUARANTINE, True


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def _out_path_for(
    out_dir: Path,
    member_dir: str,
    experiment: str,
    variable: str,
    model: str,
    year: int,
) -> Path:
    return out_dir / member_dir / experiment / variable / model / f"{year}.nc"


def build_manifest(
    *,
    s3,
    out_dir: Path,
    member: str,
    member_dir: str,
    variables: Iterable[str],
    experiments: Iterable[str],
    models: Iterable[str],
    user_years: set[int] | None,
    skip_existing: bool,
    verify: bool,
    delete_bad: bool,
    scope_lister=list_year_keys_for_scope,
) -> tuple[list[DownloadTask], list[LocalDecision], RunSummary, list[tuple[str, int, list[str]]]]:
    """Build the full manifest.

    Returns:
        tasks: enqueued DownloadTask records.
        decisions: every LocalDecision considered (for telemetry & exit code).
        summary: counts.
        scope_failures: [(scope_label, year, keys)] of duplicate-year fatal scopes.
    """
    tasks: list[DownloadTask] = []
    decisions: list[LocalDecision] = []
    summary = RunSummary()
    scope_failures: list[tuple[str, int, list[str]]] = []

    for variable in variables:
        for experiment in experiments:
            years = resolve_years_for_experiment(experiment, user_years)
            if not years:
                continue
            year_set = set(years)
            for model in models:
                scope_label = f"{model}/{experiment}/{member}/{variable}"
                try:
                    year_to_key, duplicate_years = scope_lister(
                        s3, model, experiment, member, variable
                    )
                except Exception as exc:
                    logger.error("[SCOPE-ERR] %s: %s", scope_label, exc)
                    summary.scope_fatal_duplicates += 1
                    summary.failed += 1
                    continue

                if duplicate_years:
                    for year, keys in duplicate_years:
                        logger.error(
                            "[DUP] %s year=%d has %d distinct keys: %s",
                            scope_label,
                            year,
                            len(keys),
                            keys,
                        )
                        scope_failures.append((scope_label, year, keys))
                    summary.scope_fatal_duplicates += 1
                    summary.failed += 1
                    # Skip entire scope to avoid mixing variants.
                    continue

                summary.candidate_files += sum(
                    1 for y in year_to_key if y in year_set
                )

                for year in years:
                    out_path = _out_path_for(
                        out_dir, member_dir, experiment, variable, model, year
                    )
                    status, quarantined = handle_existing(
                        out_path,
                        skip_existing=skip_existing,
                        verify=verify,
                        delete_bad=delete_bad,
                    )
                    s3_key = year_to_key.get(year)

                    decision = LocalDecision(
                        out_path=out_path,
                        variable=variable,
                        model=model,
                        experiment=experiment,
                        year=year,
                        local_status=status,
                        was_quarantined=quarantined,
                        s3_key=s3_key,
                    )
                    decisions.append(decision)

                    if quarantined:
                        summary.quarantined += 1
                        if s3_key is None:
                            summary.quarantined_unreplaceable += 1

                    if status is LocalStatus.SKIP_GOOD:
                        summary.skipped_existing += 1
                        continue

                    if s3_key is None:
                        summary.missing_on_s3 += 1
                        continue

                    tasks.append(
                        DownloadTask(
                            s3_key=s3_key,
                            out_path=out_path,
                            variable=variable,
                            model=model,
                            experiment=experiment,
                            member=member,
                            year=year,
                        )
                    )

    summary.queued = len(tasks)
    return tasks, decisions, summary, scope_failures


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_one(task: DownloadTask, bbox: tuple[float, float, float, float], open_mode: str) -> TaskResult:
    south, north, west, east = bbox
    out_path = task.out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out_path.with_name(
        f".{out_path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    )

    def _attempt() -> None:
        src_tmp: str | None = None
        try:
            # S3 download: outside the HDF5 lock so multiple workers fetch in
            # parallel. Direct mode pulls bytes via h5netcdf during open(), so
            # its network I/O is necessarily serialized — that's an opt-in cost.
            if open_mode != "direct":
                s3 = get_s3_client()
                fd, src_tmp = tempfile.mkstemp(suffix=".nc")
                os.close(fd)
                with open(src_tmp, "wb") as fh:
                    s3.download_fileobj(S3_BUCKET, task.s3_key, fh)

            with _HDF5_LOCK:
                if open_mode == "direct":
                    ctx = xr.open_dataset(
                        f"s3://{S3_BUCKET}/{task.s3_key}",
                        engine="h5netcdf",
                        storage_options={"anon": True},
                    )
                else:
                    ctx = xr.open_dataset(src_tmp, engine="h5netcdf")

                with ctx as ds:
                    lat_var, lon_var = detect_lat_lon_vars(ds)
                    if lat_var is None or lon_var is None:
                        raise RuntimeError(
                            f"lat/lon not detected; coords={list(ds.coords)}"
                        )
                    w, e = normalize_bbox_lon_for_coord(
                        ds[lon_var].values, west, east
                    )
                    subset = ds.sel(
                        {
                            lat_var: _ordered_slice(
                                ds[lat_var].values, south, north
                            ),
                            lon_var: _ordered_slice(ds[lon_var].values, w, e),
                        }
                    )
                    validate_subset_nonempty(
                        subset, lat_var, lon_var, task.s3_key
                    )
                    subset = subset.load()
                subset.to_netcdf(tmp_out)
                try:
                    subset.close()
                except Exception:
                    pass

            os.replace(tmp_out, out_path)
        finally:
            if src_tmp is not None:
                try:
                    os.remove(src_tmp)
                except OSError:
                    pass

    last_exc: BaseException | None = None
    for delay in _RETRY_BACKOFF_SECONDS:
        if delay:
            time.sleep(delay)
        try:
            _attempt()
            return TaskResult(task, TaskStatus.DOWNLOADED)
        except Exception as exc:
            try:
                tmp_out.unlink(missing_ok=True)
            except OSError:
                pass
            last_exc = exc
            if not is_transient(exc):
                break

    assert last_exc is not None
    hint = (
        " (destination may be open in another process)"
        if isinstance(last_exc, PermissionError)
        else ""
    )
    return TaskResult(
        task,
        TaskStatus.FAILED,
        error_type=type(last_exc).__name__,
        error_message=f"{last_exc}{hint}",
    )


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _log_progress(completed: int, queued: int, summary: RunSummary) -> None:
    logger.info(
        "[%d/%d done] downloaded=%d failed=%d skipped=%d missing=%d quarantined=%d queued=%d",
        completed,
        queued,
        summary.downloaded,
        summary.failed,
        summary.skipped_existing,
        summary.missing_on_s3,
        summary.quarantined,
        summary.queued,
    )


def run_pool(
    tasks: list[DownloadTask],
    workers: int,
    bbox: tuple[float, float, float, float],
    open_mode: str,
    summary: RunSummary,
) -> RunSummary:
    if not tasks:
        return summary

    in_flight: set = set()
    it = iter(tasks)
    cap = max(4 * workers, 16)
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in range(min(cap, len(tasks))):
            try:
                in_flight.add(ex.submit(download_one, next(it), bbox, open_mode))
            except StopIteration:
                break

        try:
            while in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    res = fut.result()
                    completed += 1
                    if res.status is TaskStatus.DOWNLOADED:
                        summary.downloaded += 1
                        logger.info(
                            "[OK ] %s",
                            res.task.out_path,
                        )
                    else:
                        summary.failed += 1
                        logger.error(
                            "[FAIL] %s :: %s: %s",
                            res.task.out_path,
                            res.error_type,
                            res.error_message,
                        )
                    if completed % PROGRESS_EVERY == 0:
                        _log_progress(completed, len(tasks), summary)
                    try:
                        in_flight.add(
                            ex.submit(download_one, next(it), bbox, open_mode)
                        )
                    except StopIteration:
                        pass
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt — cancelling pending futures")
            for fut in in_flight:
                fut.cancel()
            _log_progress(completed, len(tasks), summary)
            raise

    _log_progress(completed, len(tasks), summary)
    return summary


# ---------------------------------------------------------------------------
# Exit code
# ---------------------------------------------------------------------------


def compute_exit_code(summary: RunSummary) -> int:
    if summary.failed > 0:
        return 1
    if summary.quarantined_unreplaceable > 0:
        return 2
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_csv(s: str | None) -> list[str] | None:
    if s is None:
        return None
    items = [tok.strip() for tok in s.split(",") if tok.strip()]
    return items or None


def _parse_year_spec(s: str | None) -> set[int] | None:
    if s is None:
        return None
    years: set[int] = set()
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            a, b = tok.split("-", 1)
            years.update(range(int(a), int(b) + 1))
        else:
            years.add(int(tok))
    return years or None


def _parse_bbox(s: str | None) -> tuple[float, float, float, float]:
    if s is None:
        return INDIA_BBOX
    parts = [float(t) for t in s.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "bbox must be 'south,north,west,east'"
        )
    return tuple(parts)  # type: ignore[return-value]


def _check_dependencies(open_mode: str) -> None:
    required = ["boto3", "botocore", "xarray", "h5netcdf"]
    if open_mode == "direct":
        required.extend(["s3fs", "fsspec"])
    missing = []
    for mod in required:
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        sys.stderr.write(
            f"[ERROR] missing required modules for open-mode={open_mode}: "
            f"{', '.join(missing)}\n"
        )
        sys.stderr.write(
            "  Install with: pip install " + " ".join(missing) + "\n"
        )
        sys.exit(3)


EPILOG = """\
Exit codes:
  0  no failures.
  1  at least one task failed (download error or duplicate-year scope).
  2  no failures, but a corrupt local file was quarantined and no S3 key
     was available to replace it.

Open modes:
  download-first  (default, safer) — download to a local temp, open with
                  h5netcdf, subset, write atomically. Requires boto3 + xarray
                  + h5netcdf.
  direct          — open the NetCDF in place from s3://. Additionally requires
                  s3fs + fsspec. Can issue many range reads; slower under high
                  worker counts.

Windows tip: if HDF5 writes get flaky under parallelism, fall back to
--workers 2.
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="nex_india_subset_download_s3_v2",
        description=(
            "Parallel pan-India NEX-GDDP-CMIP6 downloader. Lands files under "
            "${out_dir}/${member_dir}/${experiment}/${variable}/${model}/${year}.nc"
        ),
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--variables",
        type=_parse_csv,
        default=None,
        help=f"Comma-separated. Default: {','.join(DEFAULT_VARIABLES)}",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output base directory. Falls back to env IRT_DATA_DIR.",
    )
    ap.add_argument("--member", default=DEFAULT_MEMBER, help="S3 ensemble member.")
    ap.add_argument(
        "--member-dir",
        default=DEFAULT_MEMBER_DIRNAME,
        help="On-disk folder name under --out-dir.",
    )
    ap.add_argument(
        "--bbox",
        type=_parse_bbox,
        default=INDIA_BBOX,
        help="south,north,west,east  (default: India).",
    )
    ap.add_argument(
        "--experiments",
        type=_parse_csv,
        default=None,
        help=f"Comma-separated. Default: {','.join(DEFAULT_EXPERIMENTS)}",
    )
    ap.add_argument(
        "--models",
        type=_parse_csv,
        default=None,
        help="Comma-separated. Default: all models discovered from S3.",
    )
    ap.add_argument(
        "--years",
        type=_parse_year_spec,
        default=None,
        help="e.g. 1990-2010,2050. Intersected with each experiment's policy.",
    )
    ap.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS, help="Thread workers."
    )
    ap.add_argument(
        "--open-mode",
        choices=("direct", "download-first"),
        default=DEFAULT_OPEN_MODE,
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Build manifest (still lists S3) but do not download.",
    )
    ap.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip non-empty existing files (default). Use --no-skip-existing to force.",
    )
    ap.add_argument(
        "--verify",
        action="store_true",
        help="Open existing files to validate; corrupt files are quarantined.",
    )
    ap.add_argument(
        "--delete-bad-existing",
        action="store_true",
        help="With --verify, delete corrupt files instead of moving to *.bad.",
    )
    return ap.parse_args(argv)


def _resolve_out_dir(args: argparse.Namespace) -> Path:
    if args.out_dir is not None:
        return args.out_dir
    env = os.environ.get("IRT_DATA_DIR")
    if env:
        return Path(env)
    sys.stderr.write(
        "[ERROR] --out-dir not provided and IRT_DATA_DIR is not set.\n"
    )
    sys.exit(3)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    args = parse_args(argv)
    _check_dependencies(args.open_mode)

    out_dir = _resolve_out_dir(args)
    variables = args.variables or list(DEFAULT_VARIABLES)
    experiments = args.experiments or list(DEFAULT_EXPERIMENTS)
    user_years = args.years

    s3 = get_s3_client()
    if args.models:
        models = list(args.models)
    else:
        logger.info("Discovering all models from S3...")
        models = list_models_from_s3(s3)
        if not models:
            logger.error("No models discovered from S3.")
            return 3
        logger.info("Discovered %d models.", len(models))

    logger.info(
        "Building manifest: out=%s member=%s member_dir=%s bbox=%s "
        "variables=%s experiments=%s models=%d open-mode=%s workers=%d",
        out_dir,
        args.member,
        args.member_dir,
        args.bbox,
        variables,
        experiments,
        len(models),
        args.open_mode,
        args.workers,
    )

    tasks, decisions, summary, scope_failures = build_manifest(
        s3=s3,
        out_dir=out_dir,
        member=args.member,
        member_dir=args.member_dir,
        variables=variables,
        experiments=experiments,
        models=models,
        user_years=user_years,
        skip_existing=args.skip_existing,
        verify=args.verify,
        delete_bad=args.delete_bad_existing,
    )

    logger.info(
        "Manifest: candidate_files=%d queued=%d skipped=%d missing=%d "
        "quarantined=%d quarantined_unreplaceable=%d scope_failures=%d",
        summary.candidate_files,
        summary.queued,
        summary.skipped_existing,
        summary.missing_on_s3,
        summary.quarantined,
        summary.quarantined_unreplaceable,
        summary.scope_fatal_duplicates,
    )

    if args.dry_run:
        logger.info("[DRY-RUN] not downloading; exiting.")
        return compute_exit_code(summary)

    if tasks:
        run_pool(tasks, args.workers, args.bbox, args.open_mode, summary)

    logger.info(
        "Final: downloaded=%d failed=%d skipped=%d missing=%d "
        "quarantined=%d quarantined_unreplaceable=%d scope_failures=%d",
        summary.downloaded,
        summary.failed,
        summary.skipped_existing,
        summary.missing_on_s3,
        summary.quarantined,
        summary.quarantined_unreplaceable,
        summary.scope_fatal_duplicates,
    )
    return compute_exit_code(summary)


if __name__ == "__main__":
    sys.exit(main())
