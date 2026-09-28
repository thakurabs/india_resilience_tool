"""Lightweight shade artifact provenance shared by compute, publishing and freshness."""

from __future__ import annotations
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


def require_shade_signature(frame: pd.DataFrame, *, context: str) -> None:
    """Reject missing, mixed or stale shade provenance before release writes."""
    if (
        "shade_method_signature" not in frame
        or not frame["shade_method_signature"].eq(SHADE_METHOD_SIGNATURE).all()
    ):
        raise ValueError(
            f"{context}: missing or stale shade signature; rebuild all four shade metrics in staging"
        )


def shade_artifact_current(path: str | Path) -> bool:
    """Read just provenance to determine whether a CSV/parquet shade master is current."""
    import pandas as pd

    path = Path(path)
    try:
        if path.suffix == ".parquet":
            frame = pd.read_parquet(path, columns=["shade_method_signature"])
        else:
            frame = pd.read_csv(path, usecols=["shade_method_signature"])
        require_shade_signature(frame, context=str(path))
        return not frame.empty
    except (ValueError, OSError, KeyError):
        return False
