"""Lightweight shade artifact provenance shared by compute, publishing and freshness."""

from __future__ import annotations
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

SHADE_SLUGS = frozenset(
    {
        "wbgt_shade_stull_annual_mean",
        *(f"wbgt_shade_stull_days_ge_{t}" for t in (28, 30, 32)),
    }
)
SHADE_METHOD_SIGNATURE = "shade-peak-v1:tas,tasmax,hurs:magnus-17.62-243.12:rh-percent-no-floor:complete-365:noleap-or-gregorian-drop-feb29:cell-first-area-weighted:structural-idw"
CARBONPLAN_SHADE_SIGNATURE = "carbonplan-shade-gridfirst-qdm-v1"
LEGACY_SHADE_IDENTITY = "legacy-original-stull-gridfirst-v2"
SHADE_SIGNED_IDENTITIES = frozenset({SHADE_METHOD_SIGNATURE, CARBONPLAN_SHADE_SIGNATURE})


def require_shade_signature(frame: pd.DataFrame, *, context: str) -> None:
    """Reject missing, mixed or unknown signed shade provenance before writes."""
    if "shade_method_signature" not in frame:
        raise ValueError(
            f"{context}: missing or stale shade signature; rebuild all four shade metrics in staging"
        )
    if frame.empty:
        # Emptiness is a coverage failure; callers report it with their own context.
        return
    identities = set(frame["shade_method_signature"].dropna().astype(str))
    if len(identities) != 1 or not identities <= SHADE_SIGNED_IDENTITIES or (
        frame["shade_method_signature"].isna().any()
    ):
        raise ValueError(
            f"{context}: mixed, unknown or stale shade signature; "
            "rebuild all four shade metrics in staging"
        )


#: Bundle-root file that identifies the unsigned, original-formula production shade.
SHADE_IDENTITY_SIDECAR = "shade_method_identity.json"

#: User-facing method text per artifact identity. The registry describes the release
#: target; while legacy or peak artifacts are still deployed, the dashboard shows the
#: text for the identity it actually verified instead (CHG-0701).
SHADE_IDENTITY_DESCRIPTIONS = {
    LEGACY_SHADE_IDENTITY: (
        "Deployed legacy method: 0.7 × Stull wet-bulb(tas, hurs) + 0.3 × tas from daily-mean "
        "inputs (tas, hurs), without bias correction, thresholded per grid cell and then "
        "area-weighted. This is the previous shade release; the CarbonPlan-aligned "
        "replacement is being staged."
    ),
    SHADE_METHOD_SIGNATURE: (
        "shade-peak-v1 method: 0.7 × Stull(tasmax, RH reconstructed at tasmax) + 0.3 × tasmax "
        "from tas, tasmax and hurs, without bias correction."
    ),
}


def _sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


@lru_cache(maxsize=4096)
def _cached_sha256(path_text: str, size: int, mtime_ns: int) -> str:
    """Hash once per file version; size and mtime are part of the key."""
    return _sha256(Path(path_text))


@lru_cache(maxsize=8)
def _cached_sidecar(path_text: str, size: int, mtime_ns: int) -> dict:
    return json.loads(Path(path_text).read_text(encoding="utf-8"))


def _file_sha256(path: Path) -> str:
    stat = path.stat()
    return _cached_sha256(str(path), stat.st_size, stat.st_mtime_ns)


def _legacy_identity_from_sidecar(path: Path) -> str | None:
    """Accept an unsigned file only when its exact hash is in the sidecar.

    Files are matched by their path relative to the optimised bundle root (the
    parent of ``metrics/``) or the processed root, never by absolute path, so the
    record is valid from Windows and WSL interpreters alike. The per-file SHA-256
    is the deciding check; a file absent from the record is unknown.
    """
    bundle = processed = None
    for parent in path.parents:
        if parent.name == "metrics" and parent.parent != parent:
            bundle = parent.parent
            break
        if parent.name == "processed":
            bundle, processed = parent.parent / "processed_optimised", parent
            break
    if bundle is None:
        return None
    sidecar = bundle / SHADE_IDENTITY_SIDECAR
    try:
        stat = sidecar.stat()
        record = _cached_sidecar(str(sidecar), stat.st_size, stat.st_mtime_ns)
        if record.get("schema") != 1 or record.get("method") != LEGACY_SHADE_IDENTITY:
            return None
        if processed is None:
            expected = record["optimized_files"].get(path.relative_to(bundle).as_posix())
        else:
            expected = record["processed_master_files"].get(path.relative_to(processed).as_posix())
        return LEGACY_SHADE_IDENTITY if expected and _file_sha256(path) == expected else None
    except (OSError, ValueError, KeyError, TypeError, AttributeError, json.JSONDecodeError):
        return None


def shade_artifact_identity(path: str | Path) -> str | None:
    """Identify signed shade or hash-matched unsigned legacy data, failing closed."""
    import pandas as pd

    path = Path(path).resolve()
    if not path.is_file():
        return None
    try:
        if path.suffix == ".parquet":
            frame = pd.read_parquet(path, columns=["shade_method_signature"])
        else:
            frame = pd.read_csv(path, usecols=["shade_method_signature"])
    except Exception:
        # The legacy path still requires an exact recorded file hash. Parquet
        # backends use different exception classes for a missing column.
        return _legacy_identity_from_sidecar(path)
    if frame.empty:
        return None
    try:
        require_shade_signature(frame, context=str(path))
    except ValueError:
        return None
    return str(frame["shade_method_signature"].iloc[0])


def shade_artifact_current(path: str | Path) -> bool:
    """Return whether a shade master has a recognized, verified identity."""
    return shade_artifact_identity(path) is not None


def shade_sources_identity(paths) -> str | None:
    """Return the one identity shared by every source, or ``None`` if any differs.

    A mixed selection (for example one state still legacy and another CarbonPlan)
    is rejected rather than shown under one label.
    """
    identities = {shade_artifact_identity(path) for path in paths}
    if len(identities) != 1:
        return None
    return identities.pop()


def shade_identity_description(identity: str | None, registry_description: str) -> str:
    """Pick the method text for the verified identity; the registry text is the target."""
    if identity == CARBONPLAN_SHADE_SIGNATURE:
        return registry_description
    return SHADE_IDENTITY_DESCRIPTIONS.get(identity or "", registry_description)
