"""Persistent raw NetCDF inventory helpers for climate compute.

This module caches yearly source-file validation per `(scenario, var, model)`
shard so task planning and metric compute can reuse the same discovery and
validation results without reopening unchanged NetCDFs repeatedly.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import xarray as xr


INVENTORY_SCHEMA_VERSION = 1
DEFAULT_ENGINES = ("netcdf4", "h5netcdf", "scipy")


@dataclass(frozen=True)
class InventoryYearRecord:
    """Cached validation result for one yearly source file."""

    year: int
    path: Path
    size: int
    mtime_ns: int
    engine: str | None
    open_status: bool
    validation_reason: str
    var_present: bool

    @property
    def valid(self) -> bool:
        return self.open_status and self.var_present and self.validation_reason == "ok"


@dataclass(frozen=True)
class SourceInventoryShard:
    """One persistent inventory shard for `(scenario, var, model)`."""

    schema_version: int
    scenario: str
    varname: str
    model: str
    records: tuple[InventoryYearRecord, ...]
    source_signature: str

    def valid_year_files(self) -> dict[int, Path]:
        return {record.year: record.path for record in self.records if record.valid}

    def valid_year_records(self) -> dict[int, InventoryYearRecord]:
        """Return valid cached records keyed by year."""
        return {record.year: record for record in self.records if record.valid}

    def invalid_year_details(self) -> dict[int, dict[str, Any]]:
        return {
            record.year: {
                "path": record.path,
                "reason": record.validation_reason,
                "engine": record.engine,
            }
            for record in self.records
            if not record.valid
        }


def shard_cache_path(
    cache_root: Path,
    *,
    scenario: str,
    varname: str,
    model: str,
) -> Path:
    """Return the on-disk JSON cache path for one shard."""
    return (
        Path(cache_root)
        / str(scenario).strip()
        / str(varname).strip()
        / f"{str(model).strip()}.json"
    )


def yearly_files_for_dir(dirpath: Path) -> dict[int, Path]:
    """Return yearly `.nc` files keyed by integer year."""
    out: dict[int, Path] = {}
    for path in sorted(Path(dirpath).glob("*.nc")):
        stem = path.stem
        if stem.isdigit():
            out[int(stem)] = path.resolve()
    return out


def _record_signature_payload(record: InventoryYearRecord) -> tuple[object, ...]:
    return (
        int(record.year),
        str(record.path),
        int(record.size),
        int(record.mtime_ns),
        str(record.validation_reason),
        bool(record.var_present),
    )


def _source_signature(records: Iterable[InventoryYearRecord]) -> str:
    payload = [
        _record_signature_payload(record)
        for record in sorted(records, key=lambda item: item.year)
    ]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _record_from_payload(payload: Mapping[str, Any]) -> InventoryYearRecord:
    return InventoryYearRecord(
        year=int(payload["year"]),
        path=Path(str(payload["path"])).resolve(),
        size=int(payload["size"]),
        mtime_ns=int(payload["mtime_ns"]),
        engine=str(payload["engine"]).strip() if payload.get("engine") else None,
        open_status=bool(payload["open_status"]),
        validation_reason=str(payload["validation_reason"]),
        var_present=bool(payload["var_present"]),
    )


def _shard_from_payload(payload: Mapping[str, Any]) -> SourceInventoryShard:
    records = tuple(
        _record_from_payload(record_payload)
        for record_payload in payload.get("records", [])
    )
    return SourceInventoryShard(
        schema_version=int(payload.get("schema_version", -1)),
        scenario=str(payload.get("scenario", "")).strip(),
        varname=str(payload.get("varname", "")).strip(),
        model=str(payload.get("model", "")).strip(),
        records=records,
        source_signature=str(payload.get("source_signature", "")).strip(),
    )


def _payload_from_shard(shard: SourceInventoryShard) -> dict[str, Any]:
    return {
        "schema_version": int(shard.schema_version),
        "scenario": shard.scenario,
        "varname": shard.varname,
        "model": shard.model,
        "source_signature": shard.source_signature,
        "records": [
            {
                "year": int(record.year),
                "path": str(record.path),
                "size": int(record.size),
                "mtime_ns": int(record.mtime_ns),
                "engine": record.engine,
                "open_status": bool(record.open_status),
                "validation_reason": record.validation_reason,
                "var_present": bool(record.var_present),
            }
            for record in shard.records
        ],
    }


def _read_shard(path: Path) -> Optional[SourceInventoryShard]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    shard = _shard_from_payload(payload)
    if shard.schema_version != INVENTORY_SCHEMA_VERSION:
        return None
    return shard


def _write_shard_atomic(path: Path, shard: SourceInventoryShard) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp_path.write_text(
        json.dumps(_payload_from_shard(shard), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp_path, path)


def _try_validate_year_file(
    path: Path,
    *,
    varname: str,
    engines: tuple[str, ...],
) -> InventoryYearRecord:
    try:
        stat_result = path.stat()
    except Exception as exc:
        return InventoryYearRecord(
            year=int(path.stem),
            path=path.resolve(),
            size=0,
            mtime_ns=0,
            engine=None,
            open_status=False,
            validation_reason=f"stat_failed:{exc}",
            var_present=False,
        )

    if stat_result.st_size == 0:
        return InventoryYearRecord(
            year=int(path.stem),
            path=path.resolve(),
            size=int(stat_result.st_size),
            mtime_ns=int(stat_result.st_mtime_ns),
            engine=None,
            open_status=False,
            validation_reason="zero_size",
            var_present=False,
        )

    last_error = "open_failed"
    for engine in engines:
        try:
            ds = xr.open_dataset(path, engine=engine)
            try:
                if varname in ds and getattr(ds[varname], "size", 0) > 0:
                    return InventoryYearRecord(
                        year=int(path.stem),
                        path=path.resolve(),
                        size=int(stat_result.st_size),
                        mtime_ns=int(stat_result.st_mtime_ns),
                        engine=engine,
                        open_status=True,
                        validation_reason="ok",
                        var_present=True,
                    )
                return InventoryYearRecord(
                    year=int(path.stem),
                    path=path.resolve(),
                    size=int(stat_result.st_size),
                    mtime_ns=int(stat_result.st_mtime_ns),
                    engine=engine,
                    open_status=True,
                    validation_reason=f"missing_variable:{varname}",
                    var_present=False,
                )
            finally:
                ds.close()
        except Exception as exc:
            last_error = f"open_failed:{type(exc).__name__}"

    return InventoryYearRecord(
        year=int(path.stem),
        path=path.resolve(),
        size=int(stat_result.st_size),
        mtime_ns=int(stat_result.st_mtime_ns),
        engine=None,
        open_status=False,
        validation_reason=last_error,
        var_present=False,
    )


def refresh_inventory_shard(
    cache_root: Path,
    *,
    data_dir: Path,
    scenario: str,
    varname: str,
    model: str,
    engines: tuple[str, ...] = DEFAULT_ENGINES,
) -> SourceInventoryShard:
    """Rebuild and persist one inventory shard from current source files."""
    records = tuple(
        _try_validate_year_file(path, varname=varname, engines=engines)
        for _year, path in sorted(yearly_files_for_dir(data_dir).items())
    )
    shard = SourceInventoryShard(
        schema_version=INVENTORY_SCHEMA_VERSION,
        scenario=str(scenario).strip(),
        varname=str(varname).strip(),
        model=str(model).strip(),
        records=records,
        source_signature=_source_signature(records),
    )
    _write_shard_atomic(
        shard_cache_path(
            cache_root,
            scenario=scenario,
            varname=varname,
            model=model,
        ),
        shard,
    )
    return shard


def _shard_matches_files(shard: SourceInventoryShard, current_year_files: Mapping[int, Path]) -> bool:
    current_years = sorted(int(year) for year in current_year_files)
    cached_years = sorted(record.year for record in shard.records)
    if current_years != cached_years:
        return False
    for record in shard.records:
        path = current_year_files.get(record.year)
        if path is None:
            return False
        try:
            stat_result = path.stat()
        except Exception:
            return False
        if (
            Path(path).resolve() != record.path
            or int(stat_result.st_size) != record.size
            or int(stat_result.st_mtime_ns) != record.mtime_ns
        ):
            return False
    return True


def load_or_refresh_inventory_shard(
    cache_root: Path,
    *,
    data_dir: Path,
    scenario: str,
    varname: str,
    model: str,
    allow_write: bool,
    engines: tuple[str, ...] = DEFAULT_ENGINES,
) -> SourceInventoryShard:
    """Return a fresh shard, optionally rebuilding stale or missing cache entries."""
    cache_path = shard_cache_path(
        cache_root,
        scenario=scenario,
        varname=varname,
        model=model,
    )
    shard = _read_shard(cache_path)
    current_year_files = yearly_files_for_dir(data_dir)
    if shard is not None and _shard_matches_files(shard, current_year_files):
        return shard
    if not allow_write:
        raise FileNotFoundError(
            f"Inventory shard missing or stale for scenario={scenario}, var={varname}, model={model}: {cache_path}"
        )
    return refresh_inventory_shard(
        cache_root,
        data_dir=data_dir,
        scenario=scenario,
        varname=varname,
        model=model,
        engines=engines,
    )


def combine_shard_signatures(shards: Mapping[str, SourceInventoryShard]) -> str:
    """Return a deterministic combined signature for one logical source role."""
    payload = [
        (
            role,
            shard.scenario,
            shard.varname,
            shard.model,
            shard.source_signature,
        )
        for role, shard in sorted(shards.items())
    ]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
