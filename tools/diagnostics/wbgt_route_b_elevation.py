"""Build the national Route B cell-elevation table from CarbonPlan's published elevation.

CHG-0657. ``wbgt_route_b_prepare --stage support`` refuses any climate cell without an
elevation, deliberately, because a sea-level fallback would be a silent edit. The pilot's
table covers only the three pilot states, so a national roster needs a national table
before support can be built.

The source is CarbonPlan's ``inputs/elevation.zarr``, which is what the pilot used and what
the pinned chain requires: Route B's contract is to reproduce CarbonPlan's shade -> QDM ->
sun chain, and ``raw_shade`` turns elevation into station pressure, so substituting another
elevation product would change the physics away from the method that was validated.
IRT's own GMTED2010 field is deliberately *not* used here; it is a continuous
cos-latitude-weighted cell mean, whereas CarbonPlan's values are quantised to half-metres,
and the two disagree by about 43 m on average over the pilot cells.

Reads are chunk-wise and cached by URL through the reproduction script's ``Archive``, so
re-running costs nothing and the recorded provenance describes the bytes actually used.
Default is a read-only plan; ``--run`` fetches any absent chunk and writes the table.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

from tools.data_acquisition.uhe_daily import BOUNDS

REPO = Path(__file__).resolve().parents[2]

#: The pinned reproduction script owns the bounded, cached Zarr reader and the published
#: base URL. It is loaded by path because it lives under docs/, not in an import package.
PREPARE_INPUTS = REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/prepare_inputs.py"

#: CarbonPlan's elevation store, on the same 0.25-degree grid as the climate cells.
ELEVATION_STORE = "inputs/elevation.zarr"

#: Any NEX grid file serves as the coordinate authority; support uses this same one, so the
#: emitted lat/lon reproduce its float values exactly and join without rounding.
GRID_RELATIVE = Path("r1i1p1f1/historical/tas/ACCESS-CM2/2005.nc")


def load_archive(work_dir: Path):
    """Load the pinned reproduction module and return an Archive over its shared cache."""
    if not PREPARE_INPUTS.is_file():
        raise FileNotFoundError(f"Pinned reproduction script missing: {PREPARE_INPUTS}")
    spec = importlib.util.spec_from_file_location("route_b_carbonplan_inputs", PREPARE_INPUTS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, module.Archive(work_dir)


def nex_grid(data_dir: Path, bounds: tuple[float, float, float, float]
             ) -> tuple[np.ndarray, np.ndarray]:
    """Return the NEX latitudes and longitudes whose cell centres fall inside bounds."""
    import xarray as xr

    west, south, east, north = bounds
    path = data_dir / GRID_RELATIVE
    if not path.is_file():
        raise FileNotFoundError(f"NEX grid authority missing: {path}")
    with xr.open_dataset(path) as ds:
        lat = np.asarray(ds.lat.values, dtype=float)
        lon = np.asarray(ds.lon.values, dtype=float)
    lat = np.sort(lat[(lat >= south) & (lat <= north)])
    lon = np.sort(lon[(lon >= west) & (lon <= east)])
    if not lat.size or not lon.size:
        raise ValueError(f"No NEX cell centres inside {bounds}")
    return lat, lon


def match_axis(published: np.ndarray, wanted: np.ndarray, name: str) -> np.ndarray:
    """Resolve each wanted coordinate to exactly one published index, by value.

    Matching by value rather than position is what the pilot does, and it is what makes a
    silent half-cell offset impossible: a coordinate that does not land on exactly one
    published centre aborts instead of snapping to a neighbour.
    """
    indices = []
    for value in wanted:
        found = np.flatnonzero(np.isclose(published, value))
        if len(found) != 1:
            raise ValueError(f"{name} {value} matched {len(found)} published centres, expected 1")
        indices.append(int(found[0]))
    return np.array(indices, dtype=int)


def build(args: argparse.Namespace) -> dict[str, object]:
    """Fetch the national elevation block and write the cell table with its provenance."""
    started = time.monotonic()
    lat, lon = nex_grid(args.data_dir, tuple(args.bounds))
    module, archive = load_archive(args.work_dir)
    meta = archive.metadata(ELEVATION_STORE)
    iy = match_axis(archive.coord(ELEVATION_STORE, meta, "lat"), lat, "lat")
    ix = match_axis(archive.coord(ELEVATION_STORE, meta, "lon"), lon, "lon")
    block = np.asarray(archive.take(ELEVATION_STORE, meta, "elevation", [iy, ix]), dtype=float)
    if block.shape != (lat.size, lon.size):
        raise ValueError(f"Elevation block shape {block.shape} != {(lat.size, lon.size)}")
    grid_lat, grid_lon = np.meshgrid(lat, lon, indexing="ij")
    frame = pd.DataFrame({"lat": grid_lat.ravel(), "lon": grid_lon.ravel(),
                          "elevation_m": block.ravel()})
    # The store's unwritten-chunk fill value is finite and enormous, so magnitude is the
    # only reliable gate; a cell failing it is dropped, never filled, so build_support
    # fails loudly and names it rather than running that cell at a fabricated elevation.
    plausible = np.isfinite(frame.elevation_m) & frame.elevation_m.between(-500, 9000)
    missing = frame[~plausible]
    frame = frame[plausible].reset_index(drop=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temp = args.out.with_suffix(args.out.suffix + ".part")
    frame.to_csv(temp, index=False)
    temp.replace(args.out)
    provenance = {
        "change": "CHG-0657",
        "source": module.BASE + ELEVATION_STORE,
        "objects": {url: record for url, record in sorted(archive.records.items())},
        "bounds": list(args.bounds),
        "grid_authority": str(GRID_RELATIVE),
        "lat_cells": int(lat.size), "lon_cells": int(lon.size),
        "cells_written": int(len(frame)),
        "cells_rejected": int(len(missing)),
        "elevation_min_m": float(frame.elevation_m.min()),
        "elevation_median_m": float(frame.elevation_m.median()),
        "elevation_max_m": float(frame.elevation_m.max()),
        "half_metre_quantised_fraction": float(
            np.isclose(frame.elevation_m * 2, np.round(frame.elevation_m * 2)).mean()),
        "elapsed_seconds": time.monotonic() - started,
    }
    if len(missing):
        rejected = args.out.with_name(args.out.stem + "_rejected.csv")
        missing.to_csv(rejected, index=False)
        provenance["cells_rejected_path"] = str(rejected)
    args.out.with_name(args.out.stem + "_provenance.json").write_text(
        json.dumps(provenance, indent=2, default=str))
    print(json.dumps(provenance, indent=2, default=str), flush=True)
    return provenance


def main(argv: list[str] | None = None) -> int:
    """Default to a read-only plan; --run fetches chunks and writes the table."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--bounds", type=float, nargs=4, default=list(BOUNDS),
                        metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    parser.add_argument("--data-dir", type=Path, default=Path("D:/projects/irt_data"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/carbonplan_reproduction"),
                        help="Shared CarbonPlan download cache root")
    parser.add_argument("--out", type=Path,
                        default=Path("scratch/wbgt_route_b_national/elevation_cells.csv"))
    args = parser.parse_args(argv)
    west, south, east, north = args.bounds
    if not (west < east and south < north):
        parser.error("Bounds must be west < east and south < north")
    if not args.run:
        lat, lon = nex_grid(args.data_dir, tuple(args.bounds))
        print(json.dumps({"action": "plan_only", "bounds": list(args.bounds),
                          "source": ELEVATION_STORE, "cells": int(lat.size * lon.size),
                          "lat_cells": int(lat.size), "lon_cells": int(lon.size),
                          "cache": str(args.work_dir / "cache"), "out": str(args.out)}, indent=2))
        return 0
    build(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
