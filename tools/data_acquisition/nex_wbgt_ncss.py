#!/usr/bin/env python3
"""Acquire NASA NEX-GDDP-CMIP6 **v2.0** `rsds` / `sfcWind` India subsets via NCSS.

This module exists to supply the two daily weather variables the outdoor WBGT
metric still lacks. It is deliberately separate from
`nex_india_subset_download_s3_v2.py`: that tool lists S3 (whose keys carry no
`_v2.0` suffix), assumes a single supplied member, and can quarantine local
files while building its manifest. None of that is wanted here.

Three subcommands, in order:

    plan      read local identity/grid, discover remote v2.0 datasets, write a
              durable plan + manifest. Never downloads a climate payload.
    download  consume the saved plan, fetch, validate, publish atomically.
    verify    re-check every expected output against the plan. Never fetches.

Output layout (outside the repository, under ``--out-root``)::

    <out_root>/acquisition/{plan.json,manifest.csv,events.jsonl,...}
    <out_root>/<member>/<experiment>/<variable>/<model>/<year>.nc

Raw daily variables are preserved as published: no unit conversion, no peak-
radiation reconstruction, no clipping, no interpolation, no WBGT computation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import random
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import requests
import xarray as xr

# ---------------------------------------------------------------------------
# Acquisition contract
# ---------------------------------------------------------------------------

TOOL_NAME = "nex_wbgt_ncss"
SCHEMA_VERSION = 1

CATALOG_BASE = "https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6"
NCSS_BASE = "https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6"

SOURCE_VERSION = "2.0"
DEFAULT_MEMBER = "r1i1p1f1"
VARIABLES: tuple[str, ...] = ("rsds", "sfcWind")

#: Experiment -> inclusive (first_year, last_year). Matches the production
#: epoch windows in ``tools/pipeline/compute_indices_multiprocess.py``.
EXPERIMENT_YEARS: dict[str, tuple[int, int]] = {
    "historical": (1990, 2010),
    "ssp245": (2020, 2080),
    "ssp585": (2020, 2080),
}

#: The 21 models held by *all* of `tas`, `tasmax` and `hurs` locally. IITM-ESM
#: is absent from `tasmax` and so cannot feed an outdoor WBGT; BCC-CSM2-MR and
#: NESM3 are absent from `hurs`. Verified against the archive by `plan`.
CANDIDATE_MODELS: tuple[str, ...] = (
    "ACCESS-CM2",
    "ACCESS-ESM1-5",
    "CMCC-CM2-SR5",
    "CMCC-ESM2",
    "CanESM5",
    "EC-Earth3",
    "EC-Earth3-Veg-LR",
    "GFDL-CM4",
    "GFDL-ESM4",
    "INM-CM4-8",
    "INM-CM5-0",
    "IPSL-CM6A-LR",
    "KACE-1-0-G",
    "KIOST-ESM",
    "MIROC6",
    "MPI-ESM1-2-HR",
    "MPI-ESM1-2-LR",
    "MRI-ESM2-0",
    "NorESM2-LM",
    "NorESM2-MM",
    "TaiESM1",
)

#: Requested NCSS bounding box (south, north, west, east). NCSS returns one
#: extra boundary row/column for this request; it is trimmed, never regridded.
REQUEST_SOUTH, REQUEST_NORTH = 6.0, 38.0
REQUEST_WEST, REQUEST_EAST = 68.0, 98.0

#: The existing IRT India grid, to which every response is trimmed.
TARGET_LAT = np.round(6.125 + 0.25 * np.arange(128), 6)
TARGET_LON = np.round(68.125 + 0.25 * np.arange(120), 6)
GRID_TOL = 1e-4

#: Recognised published units, by variable. Mismatches are flagged, not fixed.
EXPECTED_UNITS: dict[str, tuple[str, ...]] = {
    "rsds": ("W m-2", "W/m2", "W m**-2"),
    "sfcWind": ("m s-1", "m/s", "m s**-1"),
}

#: Local variable used as the expected-timestamp reference for a model-year.
TIME_REFERENCE_VAR = "tasmax"

OUTPUT_COMPLEVEL = 5
OUTPUT_CHUNKSIZES = (1, 128, 120)
FALLBACK_FILL_VALUE = np.float32(1.0e20)

DEFAULT_BULK_WORKERS = 16
#: Hard ceiling on concurrent transfers. Never raised automatically; the
#: operator must pass --allow-workers-above-ceiling to exceed it, and the
#: absolute limit below still applies.
MAX_WORKERS_CEILING = 16
MAX_WORKERS_HARD_LIMIT = 32
DEFAULT_CONNECT_TIMEOUT = 30.0
DEFAULT_READ_TIMEOUT = 900.0
DEFAULT_OVERALL_DEADLINE = 3600.0
DEFAULT_MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 5.0
BACKOFF_CAP_SECONDS = 300.0
MANIFEST_FLUSH_EVERY = 10

#: Adaptive-concurrency governor defaults. The governor may only ever lower
#: in-flight concurrency below what the operator requested.
GOVERNOR_FLOOR = 2
GOVERNOR_WINDOW = 20
GOVERNOR_DISTRESS_RATIO = 0.25
GOVERNOR_RECOVER_AFTER = 40

_SOURCE_FILENAME_RE = re.compile(
    r"^(?P<variable>[A-Za-z][A-Za-z0-9]*)"
    r"_(?P<frequency>day)"
    r"_(?P<model>[A-Za-z0-9][A-Za-z0-9.\-]*)"
    r"_(?P<experiment>[A-Za-z0-9\-]+)"
    r"_(?P<member>r\d+i\d+p\d+f\d+)"
    r"_(?P<grid_label>g[a-z0-9]*)"
    r"_(?P<year>\d{4})"
    r"(?:_v(?P<version>\d+(?:\.\d+)*))?"
    r"\.nc$"
)

_THREDDS_NS = {"t": "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"}

#: The HDF5 C library is not fully thread-safe. Every xarray/HDF5 touch is
#: serialised here; HTTP transfers deliberately run outside the lock.
_HDF5_LOCK = threading.Lock()

logger = logging.getLogger(TOOL_NAME)


class TaskStatus(str, Enum):
    PLANNED = "planned"
    MISSING_REMOTE = "missing_remote"
    AMBIGUOUS_REMOTE = "ambiguous_remote"
    DOWNLOADING = "downloading"
    VERIFIED = "verified"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"
    INVALID_EXISTING = "invalid_existing"


#: Statuses that mean "nothing more to try automatically".
TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.VERIFIED.value,
        TaskStatus.MISSING_REMOTE.value,
        TaskStatus.AMBIGUOUS_REMOTE.value,
        TaskStatus.PERMANENT_FAILURE.value,
    }
)

MANIFEST_FIELDS: tuple[str, ...] = (
    "task_id",
    "model",
    "experiment",
    "member",
    "variable",
    "year",
    "grid_label",
    "source_version",
    "source_dataset_path",
    "catalog_url",
    "ncss_url",
    "request_parameters",
    "source_calendar",
    "expected_time_count",
    "expected_time_start",
    "expected_time_end",
    "expected_time_sha256",
    "expected_time_reference",
    "output_path",
    "status",
    "attempt_count",
    "last_error_type",
    "last_error_message",
    "downloaded_bytes",
    "elapsed_seconds",
    "output_bytes",
    "output_sha256",
    "verified_at",
    "notes",
)


class PermanentTaskError(RuntimeError):
    """Raised for a failure that retrying cannot fix."""


class RetryableTaskError(RuntimeError):
    """Raised for a transient transport or server failure."""


class ScopeError(RuntimeError):
    """Raised when the requested scope cannot be reconciled with a saved plan."""


# ---------------------------------------------------------------------------
# Filename / catalog parsing
# ---------------------------------------------------------------------------


def parse_source_filename(name: str) -> dict[str, Any] | None:
    """Parse a NEX-GDDP-CMIP6 daily filename.

    Handles both the bare ``..._gn_1990.nc`` form and the THREDDS
    ``..._gn_1990_v2.0.nc`` form, and any of the `gn` / `gr` / `gr1` grid
    labels. Returns ``None`` when the name is not a recognised daily file.
    """
    match = _SOURCE_FILENAME_RE.match(str(name).strip())
    if match is None:
        return None
    parsed = match.groupdict()
    parsed["year"] = int(parsed["year"])
    parsed["version"] = parsed["version"] or None
    return parsed


def parse_catalog_datasets(xml_text: str) -> list[dict[str, str]]:
    """Return ``[{name, url_path}]`` for every dataset in a THREDDS catalog."""
    root = ET.fromstring(xml_text)
    out: list[dict[str, str]] = []
    for node in root.iter():
        tag = node.tag.rsplit("}", 1)[-1]
        if tag != "dataset":
            continue
        url_path = node.get("urlPath")
        name = node.get("name")
        if not url_path or not name:
            continue
        out.append({"name": name, "url_path": url_path})
    return out


def catalog_url(model: str, experiment: str, member: str, variable: str) -> str:
    return f"{CATALOG_BASE}/{model}/{experiment}/{member}/{variable}/catalog.xml"


def build_request_parameters(variable: str, time_selector: str,
                             time_start: str | None, time_end: str | None,
                             accept: str) -> dict[str, str]:
    """Return the NCSS query parameters for one whole-year request."""
    params: dict[str, str] = {
        "var": variable,
        "south": f"{REQUEST_SOUTH:g}",
        "north": f"{REQUEST_NORTH:g}",
        "west": f"{REQUEST_WEST:g}",
        "east": f"{REQUEST_EAST:g}",
        "horizStride": "1",
        "timeStride": "1",
        "accept": accept,
    }
    if time_selector == "all":
        params["time"] = "all"
    else:
        if not time_start or not time_end:
            raise PermanentTaskError(
                "explicit time selector needs a source-derived start and end"
            )
        params["time_start"] = f"{time_start}T00:00:00Z"
        params["time_end"] = f"{time_end}T23:59:59Z"
    return params


def ncss_url(source_dataset_path: str) -> str:
    """Return the NCSS endpoint for a catalog ``urlPath``."""
    path = str(source_dataset_path).strip().lstrip("/")
    prefix = "AMES/NEX/GDDP-CMIP6/"
    if path.startswith(prefix):
        path = path[len(prefix):]
    return f"{NCSS_BASE}/{path}"


# ---------------------------------------------------------------------------
# Local identity and expected-timestamp contract
# ---------------------------------------------------------------------------


def _open_local(path: Path) -> xr.Dataset:
    last_error: Exception | None = None
    for engine in ("h5netcdf", "netcdf4", "scipy"):
        try:
            return xr.open_dataset(path, engine=engine)
        except Exception as exc:  # noqa: PERF203 - engine probing
            last_error = exc
    raise PermanentTaskError(f"cannot open local reference {path}: {last_error}")


def local_source_dir(data_root: Path, member: str, experiment: str,
                     variable: str, model: str) -> Path:
    return Path(data_root) / member / experiment / variable / model


def read_local_identity(data_root: Path, member: str, experiment: str,
                        variable: str, model: str) -> dict[str, Any]:
    """Read member/version identity from the first and last local year file.

    The ``r1i1p1f1`` directory name is not evidence; this reads the file
    metadata. Conflicting or absent identity is reported, not assumed away.
    """
    directory = local_source_dir(data_root, member, experiment, variable, model)
    files = sorted(p for p in directory.glob("*.nc") if p.stem.isdigit())
    if not files:
        return {
            "model": model,
            "experiment": experiment,
            "variable": variable,
            "variant_label": None,
            "version": None,
            "issue": "no_local_files",
        }
    labels: set[str] = set()
    versions: set[str] = set()
    missing_label = False
    for path in (files[0], files[-1]):
        with _open_local(path) as ds:
            label = ds.attrs.get("variant_label")
            if label is None:
                missing_label = True
            else:
                labels.add(str(label).strip())
            version = ds.attrs.get("version")
            if version is not None:
                versions.add(str(version).strip())
    issue = None
    if len(labels) > 1:
        issue = "conflicting_variant_label"
    elif missing_label and not labels:
        issue = "absent_variant_label"
    elif missing_label:
        issue = "partially_absent_variant_label"
    return {
        "model": model,
        "experiment": experiment,
        "variable": variable,
        "variant_label": sorted(labels)[0] if labels else None,
        "all_variant_labels": sorted(labels),
        "version": sorted(versions)[0] if len(versions) == 1 else None,
        "all_versions": sorted(versions),
        "issue": issue,
    }


def _calendar_of(ds: xr.Dataset) -> str | None:
    time = ds.get("time")
    if time is None:
        return None
    calendar = time.encoding.get("calendar") or time.attrs.get("calendar")
    if calendar:
        return str(calendar)
    values = np.asarray(time.values)
    if values.size and hasattr(values.flat[0], "calendar"):
        return str(values.flat[0].calendar)
    return "proleptic_gregorian"


def date_strings(ds: xr.Dataset) -> list[str]:
    """Return ``YYYY-MM-DD`` for each timestamp, calendar-agnostic.

    Daily NEX timestamps sit at 12:00Z; comparing dates rather than instants
    keeps a 360-day source comparable with a Gregorian one without inventing
    any date the source does not contain.
    """
    out: list[str] = []
    for value in np.asarray(ds["time"].values).ravel():
        if hasattr(value, "year") and hasattr(value, "month"):
            out.append(f"{int(value.year):04d}-{int(value.month):02d}-{int(value.day):02d}")
        else:
            stamp = np.datetime_as_string(np.datetime64(value), unit="D")
            out.append(str(stamp))
    return out


def time_contract_sha256(dates: Sequence[str]) -> str:
    payload = "\n".join(dates).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def read_time_contract(path: Path) -> dict[str, Any]:
    """Derive the expected daily timestamps for one model-year from local data.

    The acquired radiation/wind must line up with the temperature it will be
    combined with, so the local `tasmax` year file is the contract. A remote
    response that disagrees is a flagged source/data exception, never
    something to interpolate over.
    """
    with _open_local(path) as ds:
        dates = date_strings(ds)
        calendar = _calendar_of(ds)
    return {
        "calendar": calendar,
        "count": len(dates),
        "start": dates[0] if dates else None,
        "end": dates[-1] if dates else None,
        "sha256": time_contract_sha256(dates),
        "reference": str(path),
        "dates": dates,
    }


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


@dataclass
class PlanTask:
    task_id: str
    model: str
    experiment: str
    member: str
    variable: str
    year: int
    grid_label: str | None = None
    source_version: str | None = None
    source_dataset_path: str | None = None
    catalog_url: str = ""
    ncss_url: str | None = None
    request_parameters: str = ""
    source_calendar: str | None = None
    expected_time_count: int | None = None
    expected_time_start: str | None = None
    expected_time_end: str | None = None
    expected_time_sha256: str | None = None
    expected_time_reference: str | None = None
    output_path: str = ""
    status: str = TaskStatus.PLANNED.value
    attempt_count: int = 0
    last_error_type: str | None = None
    last_error_message: str | None = None
    downloaded_bytes: int | None = None
    elapsed_seconds: float | None = None
    output_bytes: int | None = None
    output_sha256: str | None = None
    verified_at: str | None = None
    notes: str = ""

    def manifest_row(self) -> dict[str, Any]:
        row = asdict(self)
        return {key: ("" if row.get(key) is None else row[key]) for key in MANIFEST_FIELDS}


def make_task_id(model: str, experiment: str, member: str, variable: str, year: int) -> str:
    return f"{variable}.{model}.{experiment}.{member}.{year}"


def acquisition_dir(out_root: Path) -> Path:
    return Path(out_root) / "acquisition"


def plan_path(out_root: Path) -> Path:
    return acquisition_dir(out_root) / "plan.json"


def manifest_path(out_root: Path) -> Path:
    return acquisition_dir(out_root) / "manifest.csv"


def events_path(out_root: Path) -> Path:
    return acquisition_dir(out_root) / "events.jsonl"


def verification_path(out_root: Path) -> Path:
    return acquisition_dir(out_root) / "verification.csv"


def scoped_verification_path(out_root: Path, task_ids: Sequence[str]) -> Path:
    """Where a partial verification pass writes, so it cannot pose as the whole.

    `verification.csv` is the archive-wide record. A one-model spot check that
    overwrote it would leave the archive looking verified on the strength of a
    handful of files - which is how the record came to hold a bare header.
    """
    blob = "\n".join(sorted(task_ids)).encode("ascii")
    digest = hashlib.sha256(blob).hexdigest()[:12]
    return acquisition_dir(out_root) / f"verification.scoped-{digest}.csv"


def summary_path(out_root: Path) -> Path:
    return acquisition_dir(out_root) / "summary.json"


def output_path_for(out_root: Path, member: str, experiment: str,
                    variable: str, model: str, year: int) -> Path:
    return Path(out_root) / member / experiment / variable / model / f"{year}.nc"


def scope_signature(models: Sequence[str], experiments: Sequence[str],
                    variables: Sequence[str], member: str,
                    experiment_years: Mapping[str, tuple[int, int]]) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_version": SOURCE_VERSION,
        "member": member,
        "models": sorted(models),
        "experiments": sorted(experiments),
        "variables": sorted(variables),
        "experiment_years": {k: list(experiment_years[k]) for k in sorted(experiments)},
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(blob).hexdigest()


def years_for(experiment: str, experiment_years: Mapping[str, tuple[int, int]]) -> list[int]:
    first, last = experiment_years[experiment]
    return list(range(int(first), int(last) + 1))


class HttpClient:
    """Thread-safe `requests` wrapper with classified retries."""

    def __init__(self, *, connect_timeout: float, read_timeout: float,
                 max_attempts: int, user_agent: str | None = None) -> None:
        self.connect_timeout = float(connect_timeout)
        self.read_timeout = float(read_timeout)
        self.max_attempts = int(max_attempts)
        self.user_agent = user_agent or f"IRT-{TOOL_NAME}/{SCHEMA_VERSION}"
        self._local = threading.local()

    @property
    def session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": self.user_agent,
                # Keep Content-Length directly comparable to the bytes received:
                # that comparison is the primary truncation check (truncation
                # arrives as HTTP 200), and NetCDF4 payloads are already
                # internally compressed, so transport gzip buys nothing.
                "Accept-Encoding": "identity",
            })
            self._local.session = session
        return session

    def get(self, url: str, *, params: Mapping[str, str] | None = None,
            stream: bool = False) -> requests.Response:
        return self.session.get(
            url,
            params=dict(params or {}),
            stream=stream,
            timeout=(self.connect_timeout, self.read_timeout),
        )

    def get_text(self, url: str) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.get(url)
                if response.status_code == 404:
                    raise PermanentTaskError(f"catalog not found: {url}")
                if response.status_code in _RETRYABLE_STATUS or response.status_code >= 500:
                    raise RetryableTaskError(f"HTTP {response.status_code} for {url}")
                response.raise_for_status()
                return response.text
            except PermanentTaskError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_attempts:
                    break
                time.sleep(backoff_seconds(attempt))
        raise RetryableTaskError(f"catalog fetch failed for {url}: {last_error}")


_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509})


def backoff_seconds(attempt: int, *, retry_after: float | None = None) -> float:
    """Exponential backoff with jitter, honouring ``Retry-After`` when given."""
    if retry_after is not None and retry_after >= 0:
        return float(min(retry_after, BACKOFF_CAP_SECONDS))
    span = min(BACKOFF_BASE_SECONDS * (2 ** (max(int(attempt), 1) - 1)), BACKOFF_CAP_SECONDS)
    return float(span * (0.5 + random.random() * 0.5))


def select_remote_candidate(candidates: Sequence[Mapping[str, Any]],
                            *, member: str, version: str = SOURCE_VERSION,
                            ) -> tuple[Mapping[str, Any] | None, str]:
    """Pick the single v2.0 dataset for one model-year, or report why not.

    Returns ``(candidate, reason)``. ``candidate`` is ``None`` when the source
    is absent or ambiguous; no other version or member is ever substituted to
    make the manifest look complete, and a lexicographic tie-break is refused.
    """
    versioned = [c for c in candidates if str(c.get("version") or "") == version]
    if not versioned:
        if candidates:
            seen = sorted({str(c.get("version") or "unversioned") for c in candidates})
            return None, f"no v{version} dataset (found: {', '.join(seen)})"
        return None, "no remote dataset"
    matched = [c for c in versioned if str(c.get("member")) == member]
    if not matched:
        seen = sorted({str(c.get("member")) for c in versioned})
        return None, f"member mismatch (wanted {member}, found: {', '.join(seen)})"
    distinct = sorted({str(c["url_path"]) for c in matched})
    if len(distinct) > 1:
        grids = sorted({str(c.get("grid_label")) for c in matched})
        return None, (
            f"{len(distinct)} compatible v{version} candidates "
            f"(grid labels: {', '.join(grids)}) need a documented resolution"
        )
    return matched[0], "ok"


def discover_scope(client: HttpClient, *, model: str, experiment: str,
                   member: str, variable: str,
                   cache: dict[str, list[dict[str, str]]],
                   ) -> dict[int, list[dict[str, Any]]]:
    """Return ``{year: [candidate, ...]}`` for one catalog directory."""
    url = catalog_url(model, experiment, member, variable)
    datasets = cache.get(url)
    if datasets is None:
        datasets = parse_catalog_datasets(client.get_text(url))
        cache[url] = datasets
    by_year: dict[int, list[dict[str, Any]]] = {}
    for entry in datasets:
        parsed = parse_source_filename(entry["name"])
        if parsed is None:
            continue
        if parsed["variable"] != variable or parsed["model"] != model:
            continue
        if parsed["experiment"] != experiment:
            continue
        candidate = dict(parsed)
        candidate["url_path"] = entry["url_path"]
        candidate["name"] = entry["name"]
        candidate["catalog_url"] = url
        by_year.setdefault(int(parsed["year"]), []).append(candidate)
    return by_year


def build_plan(*, client: HttpClient, data_root: Path, out_root: Path,
               models: Sequence[str], experiments: Sequence[str],
               variables: Sequence[str], member: str,
               experiment_years: Mapping[str, tuple[int, int]],
               time_selector: str, accept: str,
               ) -> dict[str, Any]:
    """Build the complete expected task inventory, gaps included."""
    catalog_cache: dict[str, list[dict[str, str]]] = {}
    # rsds and sfcWind share one model-year time contract; read it once.
    contract_cache: dict[tuple[str, str, int], dict[str, Any] | str] = {}
    tasks: list[PlanTask] = []
    identity: list[dict[str, Any]] = []
    roster_notes: list[str] = []

    local_models = {
        var: {
            exp: sorted(
                p.name
                for p in (Path(data_root) / member / exp / var).glob("*")
                if p.is_dir()
            )
            for exp in experiments
        }
        for var in ("tas", TIME_REFERENCE_VAR, "hurs")
    }
    for exp in experiments:
        available = set(local_models["tas"][exp]) & set(
            local_models[TIME_REFERENCE_VAR][exp]
        ) & set(local_models["hurs"][exp])
        extra = sorted(set(models) - available)
        if extra:
            roster_notes.append(
                f"{exp}: requested models absent from tas/{TIME_REFERENCE_VAR}/hurs "
                f"intersection: {', '.join(extra)}"
            )
        unrequested = sorted(available - set(models))
        if unrequested:
            roster_notes.append(
                f"{exp}: locally available but not requested: {', '.join(unrequested)}"
            )

    for model in models:
        for experiment in experiments:
            for var in (TIME_REFERENCE_VAR, "hurs"):
                identity.append(
                    read_local_identity(data_root, member, experiment, var, model)
                )

    identity_by_key = {
        (rec["model"], rec["experiment"], rec["variable"]): rec for rec in identity
    }

    for variable in variables:
        for model in models:
            for experiment in experiments:
                try:
                    by_year = discover_scope(
                        client,
                        model=model,
                        experiment=experiment,
                        member=member,
                        variable=variable,
                        cache=catalog_cache,
                    )
                    scope_error = None
                except PermanentTaskError as exc:
                    by_year, scope_error = {}, str(exc)
                except RetryableTaskError as exc:
                    by_year, scope_error = {}, str(exc)

                local_member = (
                    identity_by_key.get((model, experiment, TIME_REFERENCE_VAR), {})
                    .get("variant_label")
                )
                identity_issue = (
                    identity_by_key.get((model, experiment, TIME_REFERENCE_VAR), {})
                    .get("issue")
                )

                for year in years_for(experiment, experiment_years):
                    task = PlanTask(
                        task_id=make_task_id(model, experiment, member, variable, year),
                        model=model,
                        experiment=experiment,
                        member=member,
                        variable=variable,
                        year=year,
                        catalog_url=catalog_url(model, experiment, member, variable),
                        output_path=str(
                            output_path_for(out_root, member, experiment, variable, model, year)
                        ),
                    )
                    notes: list[str] = []
                    if identity_issue:
                        notes.append(f"local_identity:{identity_issue}")
                    if local_member and local_member != member:
                        notes.append(
                            f"local_member_mismatch:{local_member}!={member}"
                        )

                    cache_key = (experiment, model, year)
                    if cache_key not in contract_cache:
                        reference = local_source_dir(
                            data_root, member, experiment, TIME_REFERENCE_VAR, model
                        ) / f"{year}.nc"
                        if not reference.exists():
                            contract_cache[cache_key] = "time_contract_missing_local_reference"
                        else:
                            try:
                                contract_cache[cache_key] = read_time_contract(reference)
                            except Exception as exc:
                                contract_cache[cache_key] = (
                                    f"time_contract_unreadable:{type(exc).__name__}"
                                )
                    contract = contract_cache[cache_key]
                    if isinstance(contract, str):
                        notes.append(contract)
                    else:
                        task.source_calendar = contract["calendar"]
                        task.expected_time_count = contract["count"]
                        task.expected_time_start = contract["start"]
                        task.expected_time_end = contract["end"]
                        task.expected_time_sha256 = contract["sha256"]
                        task.expected_time_reference = contract["reference"]

                    if scope_error is not None:
                        task.status = TaskStatus.MISSING_REMOTE.value
                        task.last_error_type = "CatalogUnavailable"
                        task.last_error_message = scope_error
                    else:
                        candidate, reason = select_remote_candidate(
                            by_year.get(year, []), member=member
                        )
                        if candidate is None:
                            task.status = (
                                TaskStatus.AMBIGUOUS_REMOTE.value
                                if "candidates" in reason
                                else TaskStatus.MISSING_REMOTE.value
                            )
                            task.last_error_type = "RemoteSelection"
                            task.last_error_message = reason
                        else:
                            task.grid_label = candidate["grid_label"]
                            task.source_version = candidate["version"]
                            task.source_dataset_path = candidate["url_path"]
                            task.ncss_url = ncss_url(candidate["url_path"])
                            params = build_request_parameters(
                                variable,
                                time_selector,
                                task.expected_time_start,
                                task.expected_time_end,
                                accept,
                            )
                            task.request_parameters = json.dumps(params, sort_keys=True)
                    task.notes = ";".join(notes)
                    tasks.append(task)

    return {
        "schema_version": SCHEMA_VERSION,
        "tool": TOOL_NAME,
        "created_at": _now_iso(),
        "source_version": SOURCE_VERSION,
        "member": member,
        "models": list(models),
        "experiments": list(experiments),
        "variables": list(variables),
        "experiment_years": {k: list(experiment_years[k]) for k in experiments},
        "time_selector": time_selector,
        "accept": accept,
        "data_root": str(data_root),
        "out_root": str(out_root),
        "scope_signature": scope_signature(
            models, experiments, variables, member, experiment_years
        ),
        "roster_notes": roster_notes,
        "local_identity": identity,
        "tasks": [asdict(task) for task in tasks],
    }


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# Durable state
# ---------------------------------------------------------------------------


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    os.replace(tmp, path)


def write_manifest(path: Path, tasks: Iterable[PlanTask]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MANIFEST_FIELDS))
        writer.writeheader()
        for task in tasks:
            writer.writerow(task.manifest_row())
    os.replace(tmp, path)


def load_plan(out_root: Path) -> dict[str, Any]:
    path = plan_path(out_root)
    if not path.exists():
        raise ScopeError(
            f"no saved plan at {path}; run the `plan` subcommand first"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if int(payload.get("schema_version", -1)) != SCHEMA_VERSION:
        raise ScopeError(
            f"plan schema {payload.get('schema_version')} != {SCHEMA_VERSION}; "
            "create a new plan explicitly"
        )
    return payload


def plan_tasks(payload: Mapping[str, Any]) -> list[PlanTask]:
    return [PlanTask(**row) for row in payload.get("tasks", [])]


def merge_manifest_state(tasks: Sequence[PlanTask], path: Path) -> None:
    """Fold a previously written manifest's progress back onto plan tasks."""
    if not Path(path).exists():
        return
    by_id = {task.task_id: task for task in tasks}
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            task = by_id.get(str(row.get("task_id", "")).strip())
            if task is None:
                continue
            status = str(row.get("status", "")).strip()
            if status:
                task.status = status
            task.attempt_count = int(row.get("attempt_count") or 0)
            task.last_error_type = row.get("last_error_type") or None
            task.last_error_message = row.get("last_error_message") or None
            task.output_sha256 = row.get("output_sha256") or None
            task.output_bytes = int(row["output_bytes"]) if row.get("output_bytes") else None
            task.verified_at = row.get("verified_at") or None
            task.downloaded_bytes = (
                int(row["downloaded_bytes"]) if row.get("downloaded_bytes") else None
            )
            task.elapsed_seconds = (
                float(row["elapsed_seconds"]) if row.get("elapsed_seconds") else None
            )


class StateWriter:
    """Single coordinated manifest writer plus a durable per-task event log."""

    def __init__(self, out_root: Path, tasks: Sequence[PlanTask], *, enabled: bool) -> None:
        self.out_root = Path(out_root)
        self.tasks = list(tasks)
        self.enabled = bool(enabled)
        self._lock = threading.Lock()
        self._since_flush = 0

    def record(self, task: PlanTask, event: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        with self._lock:
            path = events_path(self.out_root)
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"at": _now_iso(), "task_id": task.task_id, **dict(event)}
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True) + "\n")
                handle.flush()
            self._since_flush += 1
            if self._since_flush >= MANIFEST_FLUSH_EVERY:
                write_manifest(manifest_path(self.out_root), self.tasks)
                self._since_flush = 0

    def flush(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            write_manifest(manifest_path(self.out_root), self.tasks)
            self._since_flush = 0


# ---------------------------------------------------------------------------
# Response validation, trimming and publication
# ---------------------------------------------------------------------------


def sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def sniff_netcdf_magic(path: Path) -> str:
    with Path(path).open("rb") as handle:
        head = handle.read(8)
    if head[:3] == b"CDF":
        return "netcdf3"
    if head[:8] == b"\x89HDF\r\n\x1a\n":
        return "netcdf4"
    raise PermanentTaskError(
        f"response is not NetCDF (first bytes {head[:8]!r}); "
        "an HTML/XML error body can arrive with HTTP 200"
    )


def open_response(path: Path) -> xr.Dataset:
    kind = sniff_netcdf_magic(path)
    engines = ("h5netcdf", "netcdf4") if kind == "netcdf4" else ("scipy", "netcdf4")
    last_error: Exception | None = None
    for engine in engines:
        try:
            return xr.open_dataset(path, engine=engine, mask_and_scale=True)
        except Exception as exc:  # noqa: PERF203 - engine probing
            last_error = exc
    raise PermanentTaskError(f"cannot open {kind} response: {last_error}")


def trim_to_target_grid(ds: xr.Dataset) -> xr.Dataset:
    """Trim the extra NCSS boundary row/column down to the existing IRT grid.

    Selection is by coordinate matching, never by regridding or interpolation.
    """
    for name, target in (("lat", TARGET_LAT), ("lon", TARGET_LON)):
        if name not in ds.coords:
            raise PermanentTaskError(f"response has no `{name}` coordinate")
        values = np.asarray(ds[name].values, dtype="float64")
        index = []
        for want in target:
            hits = np.nonzero(np.isclose(values, want, atol=GRID_TOL, rtol=0.0))[0]
            if hits.size != 1:
                raise PermanentTaskError(
                    f"{name}={want:g} matched {hits.size} response cells "
                    f"(response {name} spans {values.min():g}..{values.max():g}, "
                    f"n={values.size})"
                )
            index.append(int(hits[0]))
        ds = ds.isel({name: index})
        got = np.asarray(ds[name].values, dtype="float64")
        if not np.allclose(got, target, atol=GRID_TOL, rtol=0.0):
            raise PermanentTaskError(f"trimmed `{name}` does not equal the IRT grid")
    return ds


def validate_response(ds: xr.Dataset, task: PlanTask, *,
                      expected_dates: Sequence[str]) -> dict[str, Any]:
    """Check identity, time coverage, grid, units and payload; never repair."""
    variable = task.variable
    if variable not in ds.variables:
        raise PermanentTaskError(
            f"requested variable `{variable}` absent (got {sorted(ds.data_vars)})"
        )

    version = ds.attrs.get("version")
    if version is not None and str(version).strip() != str(task.source_version):
        raise PermanentTaskError(
            f"source version {version!r} != requested {task.source_version!r}"
        )
    member = ds.attrs.get("variant_label")
    if member is not None and str(member).strip() != task.member:
        raise PermanentTaskError(
            f"ensemble member {member!r} != requested {task.member!r}"
        )

    dates = date_strings(ds)
    if len(set(dates)) != len(dates):
        duplicates = sorted({d for d in dates if dates.count(d) > 1})[:5]
        raise PermanentTaskError(f"duplicate timestamps, e.g. {duplicates}")
    if dates != sorted(dates):
        raise PermanentTaskError("timestamps are not in ascending order")
    if expected_dates and dates != list(expected_dates):
        missing = sorted(set(expected_dates) - set(dates))[:5]
        unexpected = sorted(set(dates) - set(expected_dates))[:5]
        raise PermanentTaskError(
            f"time coverage differs from the local {TIME_REFERENCE_VAR} contract: "
            f"got {len(dates)} of {len(expected_dates)} days; "
            f"missing e.g. {missing}; unexpected e.g. {unexpected}"
        )

    trimmed = trim_to_target_grid(ds)
    array = trimmed[variable]
    missing_dims = [d for d in ("time", "lat", "lon") if d not in array.dims]
    if missing_dims:
        raise PermanentTaskError(f"`{variable}` is missing dimensions {missing_dims}")
    array = array.transpose("time", "lat", "lon")
    if array.shape != (len(dates), TARGET_LAT.size, TARGET_LON.size):
        raise PermanentTaskError(
            f"`{variable}` shape {array.shape} != "
            f"{(len(dates), TARGET_LAT.size, TARGET_LON.size)}"
        )

    units = str(array.attrs.get("units", "")).strip()
    recognised = EXPECTED_UNITS.get(variable, ())
    if recognised and units not in recognised:
        raise PermanentTaskError(
            f"unrecognised units {units!r} for `{variable}` (expected one of {recognised})"
        )

    values = np.asarray(array.values)
    finite = np.isfinite(values)
    valid_count = int(finite.sum())
    if np.isinf(values).any():
        raise PermanentTaskError("response contains infinities")
    if valid_count == 0:
        raise PermanentTaskError("response payload is entirely missing")

    return {
        "dataset": trimmed,
        "array": array,
        "dates": dates,
        "units": units,
        "valid_count": valid_count,
        "missing_count": int(values.size - valid_count),
        "minimum": float(np.nanmin(values)) if valid_count else None,
        "maximum": float(np.nanmax(values)) if valid_count else None,
    }


def acquisition_provenance(task: PlanTask, *, params: Mapping[str, str],
                           response_bytes: int) -> dict[str, str]:
    return {
        "irt_acquisition_tool": f"{TOOL_NAME}/{SCHEMA_VERSION}",
        "irt_acquisition_source_url": f"{task.ncss_url}",
        "irt_acquisition_source_dataset_path": f"{task.source_dataset_path}",
        "irt_acquisition_source_version": f"{task.source_version}",
        "irt_acquisition_member": f"{task.member}",
        "irt_acquisition_grid_label": f"{task.grid_label}",
        "irt_acquisition_request_parameters": json.dumps(dict(params), sort_keys=True),
        "irt_acquisition_response_bytes": str(int(response_bytes)),
        "irt_acquisition_retrieved_at": _now_iso(),
        "irt_acquisition_time_reference": f"{task.expected_time_reference}",
    }


def source_value_encoding(array: xr.DataArray) -> dict[str, Any]:
    """Capture the published missing-value and dtype semantics of a variable."""
    fill = array.encoding.get("_FillValue", array.attrs.get("_FillValue"))
    missing = array.encoding.get("missing_value", array.attrs.get("missing_value"))
    if fill is None:
        fill = missing if missing is not None else FALLBACK_FILL_VALUE
    dtype = np.dtype(array.encoding.get("dtype", array.dtype))
    if dtype.kind != "f":
        dtype = np.dtype("float32")
    return {"_FillValue": fill, "missing_value": missing, "dtype": dtype}


def encode_output(ds: xr.Dataset, variable: str, destination: Path,
                  value_encoding: Mapping[str, Any]) -> None:
    """Write the trimmed subset as compressed NetCDF4, losslessly."""
    dtype = np.dtype(value_encoding["dtype"])
    chunks = (
        min(OUTPUT_CHUNKSIZES[0], int(ds.sizes["time"])),
        min(OUTPUT_CHUNKSIZES[1], int(ds.sizes["lat"])),
        min(OUTPUT_CHUNKSIZES[2], int(ds.sizes["lon"])),
    )
    encoding: dict[str, Any] = {
        variable: {
            "dtype": str(dtype),
            "_FillValue": dtype.type(value_encoding["_FillValue"]),
            "zlib": True,
            "complevel": OUTPUT_COMPLEVEL,
            "shuffle": True,
            "chunksizes": chunks,
        }
    }
    if value_encoding.get("missing_value") is not None:
        encoding[variable]["missing_value"] = dtype.type(value_encoding["missing_value"])
    for dropped in ("_FillValue", "missing_value"):
        ds[variable].attrs.pop(dropped, None)
    ds.to_netcdf(destination, engine="h5netcdf", format="NETCDF4", encoding=encoding)


def compare_published(destination: Path, variable: str,
                      reference: np.ndarray, dates: Sequence[str]) -> dict[str, Any]:
    """Reopen the published file and require decoded value/mask equality."""
    with xr.open_dataset(destination, engine="h5netcdf", mask_and_scale=True) as ds:
        if variable not in ds.variables:
            raise PermanentTaskError("published file lost the requested variable")
        array = ds[variable].transpose("time", "lat", "lon")
        published = np.asarray(array.values)
        if published.shape != reference.shape:
            raise PermanentTaskError(
                f"published shape {published.shape} != response {reference.shape}"
            )
        if date_strings(ds) != list(dates):
            raise PermanentTaskError("published timestamps differ from the response")
        mask_published = np.isnan(published)
        mask_reference = np.isnan(reference)
        if not np.array_equal(mask_published, mask_reference):
            raise PermanentTaskError("published missing-mask differs from the response")
        if not np.array_equal(published[~mask_published], reference[~mask_reference]):
            raise PermanentTaskError("published values differ from the response")
        return {
            "valid_count": int((~mask_published).sum()),
            "missing_count": int(mask_published.sum()),
        }


def _reconstruct_expected_dates(task: PlanTask) -> list[str]:
    """Rebuild the expected timestamps from the recorded local reference.

    An unreadable reference is a hard failure, never an empty list. Returning
    ``[]`` silently disabled every downstream time comparison: a verification
    pass run where the local archive is not reachable - from WSL against a
    plan that records Windows paths, say - would then report a fully verified
    archive without having compared a single date.
    """
    if not task.expected_time_reference:
        raise PermanentTaskError(
            f"{task.task_id} records no local time reference; re-plan rather "
            "than accepting an unchecked time axis"
        )
    reference = Path(task.expected_time_reference)
    if not reference.exists():
        raise PermanentTaskError(
            f"local time reference is unreachable ({reference}); run where the "
            "local archive is mounted - an unreadable reference is not a pass"
        )
    contract = read_time_contract(reference)
    if task.expected_time_sha256 and contract["sha256"] != task.expected_time_sha256:
        raise PermanentTaskError(
            "local time reference changed since planning "
            f"({reference}); reconcile the plan rather than downloading"
        )
    return list(contract["dates"])


def download_task(task: PlanTask, client: HttpClient, *,
                  overall_deadline: float) -> dict[str, Any]:
    """Fetch, validate, publish one task. Raises on failure; never partial."""
    if not task.ncss_url:
        raise PermanentTaskError("task has no resolved NCSS url")
    params = json.loads(task.request_parameters or "{}")
    destination = Path(task.output_path)
    tmp_dir = destination.parent / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    raw_path = tmp_dir / f"{destination.stem}.{token}.download"
    final_tmp = tmp_dir / f"{destination.stem}.{token}.publish"

    started = time.monotonic()
    try:
        # Establish the expected timestamps before spending a transfer, so a
        # plan that no longer matches the local reference fails fast.
        expected_dates = _reconstruct_expected_dates(task)
        response = client.get(task.ncss_url, params=params, stream=True)
        status = response.status_code
        retry_after = response.headers.get("Retry-After")
        if status == 404:
            raise PermanentTaskError("HTTP 404 — source path is not present")
        if status in _RETRYABLE_STATUS or status >= 500:
            raise RetryableTaskError(
                f"HTTP {status}"
                + (f" (Retry-After {retry_after})" if retry_after else "")
            )
        if status != 200:
            raise PermanentTaskError(f"HTTP {status}")

        content_type = str(response.headers.get("Content-Type", "")).lower()
        if any(bad in content_type for bad in ("html", "text/plain", "application/xml")):
            preview = response.text[:200].replace("\n", " ")
            raise PermanentTaskError(
                f"error document returned with HTTP 200 ({content_type}): {preview}"
            )
        advertised = response.headers.get("Content-Length")
        encoded = bool(str(response.headers.get("Content-Encoding", "")).strip())

        received = 0
        with raw_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                handle.write(chunk)
                received += len(chunk)
                if time.monotonic() - started > overall_deadline:
                    # Enforced mid-stream: a slow drip that keeps delivering a
                    # chunk inside every read-timeout window would otherwise
                    # hold a worker indefinitely.
                    raise RetryableTaskError(
                        f"attempt exceeded the {overall_deadline:g}s overall "
                        f"deadline after {received} bytes"
                    )
            handle.flush()
            os.fsync(handle.fileno())
        response.close()

        if received == 0:
            raise RetryableTaskError("empty response body")
        if advertised and not encoded:
            try:
                expected_bytes = int(advertised)
            except ValueError:
                expected_bytes = -1
            if expected_bytes >= 0 and received != expected_bytes:
                raise RetryableTaskError(
                    f"truncated body: {received} of {expected_bytes} bytes "
                    "(HTTP 200 does not prove completeness)"
                )
        if time.monotonic() - started > overall_deadline:
            # Reachable only for a zero-chunk body; the in-loop check above is
            # what actually bounds a slow transfer.
            raise RetryableTaskError("attempt exceeded the overall deadline")

        with _HDF5_LOCK:
            with open_response(raw_path) as response_ds:
                checked = validate_response(
                    response_ds, task, expected_dates=expected_dates
                )
                trimmed = checked["dataset"]
                reference_values = np.asarray(
                    checked["array"].values, dtype=checked["array"].dtype
                )
                value_encoding = source_value_encoding(checked["array"])
                payload = trimmed[[task.variable]].transpose("time", "lat", "lon")
                for key, value in acquisition_provenance(
                    task, params=params, response_bytes=received
                ).items():
                    payload.attrs.setdefault(key, value)
                encode_output(payload.load(), task.variable, final_tmp, value_encoding)
            published = compare_published(
                final_tmp, task.variable, reference_values, checked["dates"]
            )

        digest = sha256_file(final_tmp)
        output_bytes = final_tmp.stat().st_size
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(final_tmp, destination)
        return {
            "downloaded_bytes": received,
            "output_bytes": int(output_bytes),
            "output_sha256": digest,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "units": checked["units"],
            "valid_count": published["valid_count"],
            "missing_count": published["missing_count"],
            "minimum": checked["minimum"],
            "maximum": checked["maximum"],
            "time_count": len(checked["dates"]),
        }
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError) as exc:
        raise RetryableTaskError(f"{type(exc).__name__}: {exc}") from exc
    except OSError as exc:
        if getattr(exc, "errno", None) == 28:  # ENOSPC — local, never a server fault
            raise PermanentTaskError(f"local disk full: {exc}") from exc
        raise
    finally:
        for leftover in (raw_path, final_tmp):
            try:
                if leftover.exists():
                    leftover.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Resume decisions
# ---------------------------------------------------------------------------


def existing_output_state(task: PlanTask) -> tuple[bool, str]:
    """Decide whether a previously published output can be skipped.

    A size match is not enough: the recorded checksum is re-computed, so an
    altered file of the same length is re-acquired rather than trusted.
    """
    path = Path(task.output_path)
    if not path.exists():
        return False, "absent"
    if task.status != TaskStatus.VERIFIED.value:
        return False, f"status={task.status}"
    if not task.output_sha256:
        return False, "no recorded checksum"
    if task.output_bytes is not None and path.stat().st_size != int(task.output_bytes):
        return False, "size differs from manifest"
    if sha256_file(path) != task.output_sha256:
        return False, "checksum differs from manifest"
    return True, "verified"


# ---------------------------------------------------------------------------
# Subcommand: plan
# ---------------------------------------------------------------------------


def command_plan(args: argparse.Namespace) -> int:
    out_root = Path(args.out_root)
    data_root = Path(args.data_root)
    models = list(args.models or CANDIDATE_MODELS)
    experiments = list(args.experiments or EXPERIMENT_YEARS.keys())
    variables = list(args.variables or VARIABLES)
    client = HttpClient(
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        max_attempts=args.max_attempts,
    )
    signature = scope_signature(
        models, experiments, variables, args.member, EXPERIMENT_YEARS
    )
    existing = plan_path(out_root)
    if existing.exists() and not args.reconcile:
        previous = json.loads(existing.read_text(encoding="utf-8"))
        if str(previous.get("scope_signature")) != signature:
            raise ScopeError(
                f"a plan with a different scope already exists at {existing}; "
                "pass --reconcile to replace it deliberately"
            )

    logger.info(
        "planning %d models x %d experiments x %d variables (member %s, v%s)",
        len(models), len(experiments), len(variables), args.member, SOURCE_VERSION,
    )
    payload = build_plan(
        client=client,
        data_root=data_root,
        out_root=out_root,
        models=models,
        experiments=experiments,
        variables=variables,
        member=args.member,
        experiment_years=EXPERIMENT_YEARS,
        time_selector=args.time_selector,
        accept=args.accept,
    )
    tasks = plan_tasks(payload)
    counts = _status_counts(tasks)
    for note in payload["roster_notes"]:
        logger.warning("roster: %s", note)
    for record in payload["local_identity"]:
        if record.get("issue"):
            logger.warning(
                "identity: %s/%s/%s %s",
                record["model"], record["experiment"], record["variable"], record["issue"],
            )
    logger.info("planned %d tasks: %s", len(tasks), counts)
    for task in tasks:
        if task.status == TaskStatus.AMBIGUOUS_REMOTE.value:
            logger.warning("ambiguous: %s — %s", task.task_id, task.last_error_message)
    unresolved = [t for t in tasks if t.status == TaskStatus.MISSING_REMOTE.value]
    for task in unresolved[:40]:
        logger.warning("missing remote: %s — %s", task.task_id, task.last_error_message)
    if len(unresolved) > 40:
        logger.warning("... and %d further missing-remote tasks", len(unresolved) - 40)

    if args.dry_run:
        logger.info("[dry-run] would write %s and %s", plan_path(out_root), manifest_path(out_root))
        return 0

    acquisition_dir(out_root).mkdir(parents=True, exist_ok=True)
    (acquisition_dir(out_root) / "logs").mkdir(parents=True, exist_ok=True)
    write_json_atomic(plan_path(out_root), payload)
    write_manifest(manifest_path(out_root), tasks)
    logger.info("wrote %s", plan_path(out_root))
    logger.info("wrote %s", manifest_path(out_root))
    return 0


def _status_counts(tasks: Iterable[PlanTask]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        counts[task.status] = counts.get(task.status, 0) + 1
    return dict(sorted(counts.items()))


# ---------------------------------------------------------------------------
# Subcommand: download
# ---------------------------------------------------------------------------


def select_tasks(tasks: Sequence[PlanTask], args: argparse.Namespace) -> list[PlanTask]:
    wanted_models = set(args.models or [])
    wanted_experiments = set(args.experiments or [])
    wanted_variables = set(args.variables or [])
    wanted_years = set(args.years or [])
    wanted_ids = set(getattr(args, "task_ids", None) or [])
    out: list[PlanTask] = []
    for task in tasks:
        if wanted_ids and task.task_id not in wanted_ids:
            continue
        if wanted_models and task.model not in wanted_models:
            continue
        if wanted_experiments and task.experiment not in wanted_experiments:
            continue
        if wanted_variables and task.variable not in wanted_variables:
            continue
        if wanted_years and int(task.year) not in wanted_years:
            continue
        out.append(task)
    return out


_STOP = threading.Event()


def _install_interrupt_handler() -> None:
    def _handler(signum, frame):  # noqa: ANN001, ARG001
        if not _STOP.is_set():
            logger.warning("interrupt received — finishing in-flight tasks, no new submissions")
            _STOP.set()
        else:
            logger.warning("second interrupt — exiting")
            raise KeyboardInterrupt

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", signal.SIGINT)):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass


class ConcurrencyGovernor:
    """In-flight limiter that may only ever *lower* the operator's request.

    The public NASA NCSS service degrades under sustained subsetting load:
    latency climbs and connections are dropped mid-transfer. This tracks a
    sliding window of task outcomes, halves the in-flight target toward
    ``floor`` when that window shows distress, and steps back up after a clean
    streak. It never exceeds ``requested``, so an operator's ceiling holds.
    """

    def __init__(self, requested: int, *, floor: int = GOVERNOR_FLOOR,
                 window: int = GOVERNOR_WINDOW,
                 distress_ratio: float = GOVERNOR_DISTRESS_RATIO,
                 recover_after: int = GOVERNOR_RECOVER_AFTER) -> None:
        self.requested = max(1, int(requested))
        self.floor = max(1, min(int(floor), self.requested))
        self.window = max(1, int(window))
        self.distress_ratio = float(distress_ratio)
        self.recover_after = max(1, int(recover_after))
        self._target = self.requested
        self._recent: deque[bool] = deque(maxlen=self.window)
        self._clean = 0
        self._reductions = 0
        self._lock = threading.Lock()

    @property
    def target(self) -> int:
        with self._lock:
            return self._target

    @property
    def reductions(self) -> int:
        with self._lock:
            return self._reductions

    def record(self, *, failed: bool) -> tuple[int, str | None]:
        """Register one task outcome; return (target, message-if-changed)."""
        with self._lock:
            self._recent.append(bool(failed))
            self._clean = 0 if failed else self._clean + 1
            distress = sum(self._recent)
            if (len(self._recent) == self.window
                    and distress >= self.distress_ratio * self.window
                    and self._target > self.floor):
                previous = self._target
                self._target = max(self.floor, self._target // 2)
                self._reductions += 1
                self._recent.clear()
                self._clean = 0
                return self._target, (
                    f"reduced in-flight {previous} -> {self._target} after "
                    f"{distress}/{self.window} recent failures"
                )
            if self._clean >= self.recover_after and self._target < self.requested:
                previous = self._target
                self._target = min(self.requested, self._target + 1)
                self._clean = 0
                return self._target, f"restored in-flight {previous} -> {self._target}"
            return self._target, None


def command_download(args: argparse.Namespace) -> int:
    out_root = Path(args.out_root)
    payload = load_plan(out_root)
    tasks = plan_tasks(payload)
    merge_manifest_state(tasks, manifest_path(out_root))
    selected = select_tasks(tasks, args)
    if args.limit:
        selected = selected[: int(args.limit)]
    if not selected:
        raise ScopeError(
            f"the requested scope selected 0 of {len(tasks)} planned tasks; "
            "check the --models/--experiments/--variables/--years spellings"
        )

    requested_workers = int(args.workers)
    if requested_workers < 1:
        raise ScopeError(f"--workers must be at least 1 (got {requested_workers})")
    ceiling = MAX_WORKERS_CEILING
    if getattr(args, "allow_workers_above_ceiling", False):
        ceiling = MAX_WORKERS_HARD_LIMIT
        if requested_workers > MAX_WORKERS_CEILING:
            logger.warning(
                "--allow-workers-above-ceiling: running %d workers against a "
                "public NASA service, above the %d default ceiling",
                min(requested_workers, ceiling), MAX_WORKERS_CEILING,
            )
    workers = min(requested_workers, ceiling)
    if requested_workers > ceiling:
        logger.warning(
            "requested %d workers; capped at %d (public NASA service)",
            requested_workers, ceiling,
        )

    runnable: list[PlanTask] = []
    skipped = 0
    for task in selected:
        if task.status in (TaskStatus.MISSING_REMOTE.value, TaskStatus.AMBIGUOUS_REMOTE.value):
            continue
        ok, reason = (False, "dry-run") if args.dry_run and not Path(task.output_path).exists() \
            else existing_output_state(task)
        if ok:
            skipped += 1
            continue
        if reason not in ("absent", "dry-run") and Path(task.output_path).exists():
            task.status = TaskStatus.INVALID_EXISTING.value
            task.notes = ";".join(filter(None, [task.notes, f"existing_rejected:{reason}"]))
            logger.warning("existing output rejected (%s): %s", reason, task.output_path)
        runnable.append(task)

    logger.info(
        "selected=%d runnable=%d skipped_verified=%d unresolved=%d workers=%d",
        len(selected), len(runnable), skipped,
        len(selected) - len(runnable) - skipped, workers,
    )
    if args.dry_run:
        for task in runnable[:10]:
            logger.info("[dry-run] would GET %s?%s", task.ncss_url, task.request_parameters)
            logger.info("[dry-run] would publish %s", task.output_path)
        if len(runnable) > 10:
            logger.info("[dry-run] ... and %d more tasks", len(runnable) - 10)
        logger.info("[dry-run] no files written, nothing quarantined")
        return 0
    if not runnable:
        logger.info("nothing to download")
        _write_summary(out_root, payload, tasks, mode="download")
        return compute_exit_code(tasks, selected)

    _install_interrupt_handler()
    client = HttpClient(
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        max_attempts=args.max_attempts,
    )
    state = StateWriter(out_root, tasks, enabled=True)
    governor = ConcurrencyGovernor(workers)
    completed = 0
    total_bytes = 0
    started = time.monotonic()

    def _run(task: PlanTask) -> PlanTask:
        for attempt in range(1, int(args.max_attempts) + 1):
            if _STOP.is_set():
                break
            task.attempt_count += 1
            task.status = TaskStatus.DOWNLOADING.value
            try:
                result = download_task(task, client, overall_deadline=args.overall_deadline)
            except PermanentTaskError as exc:
                task.status = TaskStatus.PERMANENT_FAILURE.value
                task.last_error_type = type(exc).__name__
                task.last_error_message = str(exc)[:500]
                state.record(task, {"event": "permanent_failure", "error": str(exc)[:500]})
                return task
            except RetryableTaskError as exc:
                task.status = TaskStatus.RETRYABLE_FAILURE.value
                task.last_error_type = type(exc).__name__
                task.last_error_message = str(exc)[:500]
                state.record(
                    task,
                    {"event": "retryable_failure", "attempt": attempt, "error": str(exc)[:500]},
                )
                if attempt >= int(args.max_attempts):
                    return task
                time.sleep(backoff_seconds(attempt))
                continue
            except Exception as exc:  # unexpected: treat as permanent, keep the detail
                task.status = TaskStatus.PERMANENT_FAILURE.value
                task.last_error_type = type(exc).__name__
                task.last_error_message = str(exc)[:500]
                state.record(task, {"event": "unexpected_failure", "error": str(exc)[:500]})
                return task
            task.status = TaskStatus.VERIFIED.value
            task.last_error_type = None
            task.last_error_message = None
            task.downloaded_bytes = result["downloaded_bytes"]
            task.output_bytes = result["output_bytes"]
            task.output_sha256 = result["output_sha256"]
            task.elapsed_seconds = result["elapsed_seconds"]
            task.verified_at = _now_iso()
            state.record(task, {"event": "verified", **{
                k: result[k] for k in ("downloaded_bytes", "output_bytes", "elapsed_seconds",
                                       "units", "valid_count", "missing_count",
                                       "minimum", "maximum", "time_count")
            }})
            return task
        return task

    interrupted = False
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending: set[Any] = set()
            queue: Iterator[PlanTask] = iter(runnable)
            while True:
                while len(pending) < governor.target and not _STOP.is_set():
                    try:
                        pending.add(pool.submit(_run, next(queue)))
                    except StopIteration:
                        break
                if not pending:
                    break
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    finished = future.result()
                    completed += 1
                    total_bytes += int(finished.downloaded_bytes or 0)
                    _, change = governor.record(
                        failed=finished.status != TaskStatus.VERIFIED.value
                    )
                    if change:
                        logger.warning("concurrency governor: %s", change)
                    if completed % 10 == 0 or completed == len(runnable):
                        rate = total_bytes / max(time.monotonic() - started, 1e-6) / 1e6
                        logger.info(
                            "[%d/%d] %s=%s bytes=%.2f GB rate=%.2f MB/s",
                            completed, len(runnable), finished.task_id, finished.status,
                            total_bytes / 1e9, rate,
                        )
    except KeyboardInterrupt:
        interrupted = True
        _STOP.set()
    finally:
        state.flush()

    elapsed = time.monotonic() - started
    logger.info(
        "download finished: completed=%d elapsed=%.1f s transferred=%.2f GB",
        completed, elapsed, total_bytes / 1e9,
    )
    _write_summary(out_root, payload, tasks, mode="download",
                   extra={"interrupted": interrupted,
                          "transferred_bytes": total_bytes,
                          "elapsed_seconds": round(elapsed, 1),
                          "requested_workers": workers,
                          "final_in_flight_target": governor.target,
                          "concurrency_reductions": governor.reductions})
    if interrupted or _STOP.is_set():
        logger.warning("run was interrupted; state flushed, resume with the same command")
        return 4
    return compute_exit_code(tasks, selected)


# ---------------------------------------------------------------------------
# Subcommand: verify
# ---------------------------------------------------------------------------


def verify_output(task: PlanTask) -> dict[str, Any]:
    """Re-check one published output. Never fetches and never repairs."""
    row: dict[str, Any] = {
        "task_id": task.task_id,
        "model": task.model,
        "experiment": task.experiment,
        "variable": task.variable,
        "year": task.year,
        "output_path": task.output_path,
        "status": task.status,
        "verified": False,
        "reason": "",
        "output_bytes": "",
        "output_sha256": "",
        "time_count": "",
        "calendar": "",
        "time_sha256": "",
        "valid_count": "",
        "missing_count": "",
        "minimum": "",
        "maximum": "",
        "units": "",
    }
    if task.status in (TaskStatus.MISSING_REMOTE.value, TaskStatus.AMBIGUOUS_REMOTE.value):
        row["reason"] = f"unresolved source: {task.last_error_message or task.status}"
        return row
    path = Path(task.output_path)
    if not path.exists():
        row["reason"] = "output absent"
        return row
    row["output_bytes"] = path.stat().st_size
    try:
        digest = sha256_file(path)
        row["output_sha256"] = digest
        if task.output_sha256 and digest != task.output_sha256:
            row["reason"] = "checksum differs from manifest"
            return row
        expected_dates = _reconstruct_expected_dates(task)
        with _HDF5_LOCK:
            with xr.open_dataset(path, engine="h5netcdf", mask_and_scale=True) as ds:
                if task.variable not in ds.variables:
                    row["reason"] = "variable absent"
                    return row
                # Mirror the download-time identity checks: without them an
                # output carrying no manifest checksum could pass verification
                # while holding the wrong dataset version or ensemble member.
                # NASA's own attributes are the authority here. This used to
                # prefer `irt_acquisition_source_version`, which this tool
                # writes from the *requested* version - so the check compared
                # the request against its own echo and could never disagree.
                source_version = ds.attrs.get("version")
                if source_version is None:
                    row["reason"] = "no source 'version' attribute to verify against"
                    return row
                if (task.source_version
                        and str(source_version).strip() != str(task.source_version)):
                    row["reason"] = (
                        f"source version {source_version!r} != "
                        f"requested {task.source_version!r}"
                    )
                    return row
                recorded_version = ds.attrs.get("irt_acquisition_source_version")
                if (recorded_version is not None
                        and str(recorded_version).strip() != str(source_version).strip()):
                    row["reason"] = (
                        f"acquisition provenance version {recorded_version!r} "
                        f"disagrees with source version {source_version!r}"
                    )
                    return row
                member = ds.attrs.get("variant_label")
                if member is None:
                    row["reason"] = "no 'variant_label' attribute to verify against"
                    return row
                if str(member).strip() != task.member:
                    row["reason"] = (
                        f"ensemble member {member!r} != requested {task.member!r}"
                    )
                    return row
                recorded_member = ds.attrs.get("irt_acquisition_member")
                if (recorded_member is not None
                        and str(recorded_member).strip() != str(member).strip()):
                    row["reason"] = (
                        f"acquisition provenance member {recorded_member!r} "
                        f"disagrees with source member {member!r}"
                    )
                    return row
                calendar = _calendar_of(ds)
                row["calendar"] = "" if calendar is None else str(calendar)
                if task.source_calendar and row["calendar"] != str(task.source_calendar):
                    row["reason"] = (
                        f"calendar {row['calendar']!r} != planned "
                        f"{task.source_calendar!r}"
                    )
                    return row
                array = ds[task.variable].transpose("time", "lat", "lon")
                values = np.asarray(array.values)
                dates = date_strings(ds)
                row["time_sha256"] = time_contract_sha256(dates)
                if (task.expected_time_count
                        and len(dates) != int(task.expected_time_count)):
                    # A short but complete annual response stays a flagged
                    # source/data issue; it is never coverage to interpolate.
                    row["reason"] = (
                        f"time count {len(dates)} != planned "
                        f"{int(task.expected_time_count)}"
                    )
                    return row
                if dates != list(expected_dates):
                    first = next((i for i, (a, b) in enumerate(zip(dates, expected_dates))
                                  if a != b), None)
                    row["reason"] = (
                        "timestamps differ from the local reference"
                        + (f" (first at index {first}: {dates[first]!r} != "
                           f"{expected_dates[first]!r})" if first is not None else "")
                    )
                    return row
                if (task.expected_time_sha256
                        and row["time_sha256"] != task.expected_time_sha256):
                    row["reason"] = (
                        f"date hash {row['time_sha256'][:12]} != planned "
                        f"{task.expected_time_sha256[:12]}"
                    )
                    return row
                if values.shape != (len(dates), TARGET_LAT.size, TARGET_LON.size):
                    row["reason"] = f"shape {values.shape} unexpected"
                    return row
                if not np.allclose(np.asarray(ds["lat"].values, dtype="float64"),
                                   TARGET_LAT, atol=GRID_TOL, rtol=0.0):
                    row["reason"] = "lat grid differs from the IRT grid"
                    return row
                if not np.allclose(np.asarray(ds["lon"].values, dtype="float64"),
                                   TARGET_LON, atol=GRID_TOL, rtol=0.0):
                    row["reason"] = "lon grid differs from the IRT grid"
                    return row
                finite = np.isfinite(values)
                row["time_count"] = len(dates)
                row["valid_count"] = int(finite.sum())
                row["missing_count"] = int(values.size - finite.sum())
                row["units"] = str(array.attrs.get("units", ""))
                recognised = EXPECTED_UNITS.get(task.variable, ())
                if recognised and row["units"] not in recognised:
                    row["reason"] = (
                        f"units {row['units']!r} not in {recognised} "
                        "(flagged, never converted)"
                    )
                    return row
                if np.isinf(values).any():
                    row["reason"] = "contains infinities"
                    return row
                if not finite.any():
                    row["reason"] = "payload entirely missing"
                    return row
                row["minimum"] = float(np.nanmin(values))
                row["maximum"] = float(np.nanmax(values))
    except Exception as exc:
        row["reason"] = f"{type(exc).__name__}: {exc}"[:300]
        return row
    row["verified"] = True
    row["reason"] = "ok"
    return row


def command_verify(args: argparse.Namespace) -> int:
    out_root = Path(args.out_root)
    payload = load_plan(out_root)
    tasks = plan_tasks(payload)
    merge_manifest_state(tasks, manifest_path(out_root))
    selected = select_tasks(tasks, args)
    if not selected:
        # Raised before any write: an empty scope used to report success *and*
        # overwrite verification.csv with a fallback one-line header.
        raise ScopeError(
            f"the requested scope selected 0 of {len(tasks)} planned tasks; "
            "check the --models/--experiments/--variables/--years spellings"
        )
    full_scope = len(selected) == len(tasks)
    report = (verification_path(out_root) if full_scope
              else scoped_verification_path(out_root, [t.task_id for t in selected]))
    if not full_scope:
        logger.warning(
            "scoped pass: %d of %d planned tasks — writing %s, leaving %s alone",
            len(selected), len(tasks), report.name, verification_path(out_root).name,
        )
    rows = [verify_output(task) for task in selected]
    verified = sum(1 for row in rows if row["verified"])
    logger.info("verified %d of %d expected outputs", verified, len(rows))
    for row in rows:
        if not row["verified"]:
            logger.warning("not verified: %s — %s", row["task_id"], row["reason"])
    if args.dry_run:
        logger.info("[dry-run] would write %s and %s", report, summary_path(out_root))
        return 0 if verified == len(rows) else 3
    acquisition_dir(out_root).mkdir(parents=True, exist_ok=True)
    tmp = report.with_suffix(f".csv.tmp.{os.getpid()}")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, report)
    record = {
        "scope": "full" if full_scope else "scoped",
        "report": report.name,
        "checked": len(rows),
        "verified": verified,
        "not_verified": len(rows) - verified,
        "written_at": _now_iso(),
    }
    _write_summary(out_root, payload, tasks, mode="verify",
                   extra={"verified_now": verified, "checked": len(rows)},
                   verification=record)
    logger.info("wrote %s", report)
    return 0 if verified == len(rows) else 3


# ---------------------------------------------------------------------------
# Summary and exit codes
# ---------------------------------------------------------------------------


def _read_summary(out_root: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(summary_path(out_root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _write_summary(out_root: Path, payload: Mapping[str, Any],
                   tasks: Sequence[PlanTask], *, mode: str,
                   extra: Mapping[str, Any] | None = None,
                   verification: Mapping[str, Any] | None = None) -> None:
    by_variable: dict[str, dict[str, int]] = {}
    by_experiment: dict[str, dict[str, int]] = {}
    by_model: dict[str, dict[str, int]] = {}
    for task in tasks:
        for bucket, key in ((by_variable, task.variable),
                            (by_experiment, task.experiment),
                            (by_model, task.model)):
            slot = bucket.setdefault(key, {})
            slot[task.status] = slot.get(task.status, 0) + 1
    retained = sum(int(t.output_bytes or 0) for t in tasks
                   if t.status == TaskStatus.VERIFIED.value)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "tool": TOOL_NAME,
        "mode": mode,
        "written_at": _now_iso(),
        "scope_signature": payload.get("scope_signature"),
        "source_version": payload.get("source_version"),
        "member": payload.get("member"),
        "expected_tasks": len(tasks),
        # Acquisition state as the manifest records it. This is what the
        # downloader concluded at publication time; it is NOT the outcome of an
        # independent re-read, which lives under "verification" alone.
        "status_counts": _status_counts(tasks),
        "status_counts_source": "acquisition manifest (not independent verification)",
        "by_variable": by_variable,
        "by_experiment": by_experiment,
        "by_model": by_model,
        "retained_bytes": retained,
        "roster_notes": payload.get("roster_notes", []),
        "known_downstream_issues": KNOWN_DOWNSTREAM_ISSUES,
        **dict(extra or {}),
    }
    # A scoped pass never displaces the archive-wide verification record, and a
    # download never erases one: whatever the last full pass concluded survives
    # until another full pass replaces it.
    previous = _read_summary(out_root)
    carried = previous.get("verification")
    if verification is None:
        if isinstance(carried, dict):
            summary["verification"] = carried
    elif verification.get("scope") == "full":
        summary["verification"] = dict(verification)
    else:
        if isinstance(carried, dict):
            summary["verification"] = carried
        summary["verification_scoped"] = dict(verification)
    write_json_atomic(summary_path(out_root), summary)


#: Recorded separately from acquisition failures: these are not this tool's
#: to fix, but they do block a WBGT computation over the affected model-years.
KNOWN_DOWNSTREAM_ISSUES: tuple[str, ...] = (
    "existing tas/tasmax/hurs are NASA dataset version 1.0; rsds/sfcWind acquired here are v2.0",
    "local r1i1p1f1/ssp245/hurs/KIOST-ESM/2058.nc is absent",
    "IITM-ESM is excluded: the local archive has no tasmax for it",
)


def compute_exit_code(tasks: Sequence[PlanTask], selected: Sequence[PlanTask]) -> int:
    """0 complete; 3 incomplete. A partial download is never reported as done."""
    incomplete = [
        task for task in selected if task.status != TaskStatus.VERIFIED.value
    ]
    if incomplete:
        logger.warning("%d of %d selected tasks are not verified", len(incomplete), len(selected))
        return 3
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _csv_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _year_list(value: str | None) -> list[int] | None:
    if not value:
        return None
    years: set[int] = set()
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            first, last = part.split("-", 1)
            years.update(range(int(first), int(last) + 1))
        else:
            years.add(int(part))
    return sorted(years)


def _default_data_root() -> str:
    return os.environ.get("IRT_DATA_DIR", "D:/projects/irt_data")


def _add_scope_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--models", type=_csv_list, default=None,
                        help="Comma-separated model subset (default: the planned roster).")
    parser.add_argument("--experiments", type=_csv_list, default=None,
                        help="Comma-separated experiment subset.")
    parser.add_argument("--variables", type=_csv_list, default=None,
                        help="Comma-separated variable subset (rsds, sfcWind).")
    parser.add_argument("--years", type=_year_list, default=None,
                        help="Year subset, e.g. '1990,2020-2022'.")


def _add_http_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT,
                        help="Connection timeout in seconds.")
    parser.add_argument("--read-timeout", type=float, default=DEFAULT_READ_TIMEOUT,
                        help="Read-inactivity timeout in seconds (NCSS latency is bimodal).")
    parser.add_argument("--overall-deadline", type=float, default=DEFAULT_OVERALL_DEADLINE,
                        help="Overall per-attempt deadline in seconds.")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS,
                        help="Maximum attempts per task.")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=f"python -m tools.data_acquisition.{TOOL_NAME}",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--out-root", required=True,
                        help="Acquisition root, outside the repository "
                             "(e.g. D:/projects/irt_data/nex_gddp_cmip6_v2_wbgt).")
    parser.add_argument("--member", default=DEFAULT_MEMBER,
                        help="Ensemble member to acquire and require in metadata.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Perform no file writes. Remote metadata reads still happen "
                             "during `plan`.")
    parser.add_argument("--quiet", action="store_true", help="Warnings and errors only.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_plan = sub.add_parser("plan", help="Discover remote v2.0 files and write the plan.")
    p_plan.add_argument("--data-root", default=_default_data_root(),
                        help="Existing archive root holding <member>/<exp>/<var>/<model>.")
    p_plan.add_argument("--time-selector", choices=("all", "explicit"), default="all",
                        help="'all' asks NCSS for every timestep (calendar-agnostic); "
                             "'explicit' uses the source-derived first/last dates.")
    p_plan.add_argument("--accept", default="netcdf4",
                        help="NCSS response format (netcdf4 or netcdf).")
    p_plan.add_argument("--reconcile", action="store_true",
                        help="Replace an existing plan whose scope differs.")
    _add_scope_arguments(p_plan)
    _add_http_arguments(p_plan)
    p_plan.set_defaults(func=command_plan)

    p_dl = sub.add_parser("download", help="Download planned tasks.")
    p_dl.add_argument("--workers", type=int, default=DEFAULT_BULK_WORKERS,
                      help=f"Concurrent transfers (capped at {MAX_WORKERS_CEILING}).")
    p_dl.add_argument("--allow-workers-above-ceiling", action="store_true",
                      help=(f"Permit --workers above the {MAX_WORKERS_CEILING} default "
                            f"ceiling, up to {MAX_WORKERS_HARD_LIMIT}. Intended for "
                            "deliberate concurrency measurement against a healthy "
                            "service, not for routine bulk acquisition."))
    p_dl.add_argument("--limit", type=int, default=None,
                      help="Stop after this many runnable tasks (pilot use).")
    p_dl.add_argument("--task-ids", type=_csv_list, default=None,
                      help="Explicit task_id subset.")
    _add_scope_arguments(p_dl)
    _add_http_arguments(p_dl)
    p_dl.set_defaults(func=command_download)

    p_vf = sub.add_parser("verify", help="Re-check published outputs against the plan.")
    _add_scope_arguments(p_vf)
    p_vf.set_defaults(func=command_verify)

    args = parser.parse_args(list(argv) if argv is not None else None)
    if getattr(args, "workers", None) is None:
        args.workers = DEFAULT_BULK_WORKERS
    for name, default in (("connect_timeout", DEFAULT_CONNECT_TIMEOUT),
                          ("read_timeout", DEFAULT_READ_TIMEOUT),
                          ("overall_deadline", DEFAULT_OVERALL_DEADLINE),
                          ("max_attempts", DEFAULT_MAX_ATTEMPTS)):
        if getattr(args, name, None) is None:
            setattr(args, name, default)
    return args


def _setup_logging(quiet: bool) -> None:
    logging.basicConfig(
        level=logging.WARNING if quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    _setup_logging(bool(args.quiet))
    try:
        return int(args.func(args))
    except ScopeError as exc:
        logger.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        logger.warning("interrupted")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
