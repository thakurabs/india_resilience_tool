"""Fetch one validation day of hourly ERA5 single-level fields for WBGT comparison.

Downloads the seven variables needed to evaluate all four WBGT methods on the
same hourly driver:

    IRT shade (Stull)   Ta, RH
    IRT sWBGT           Ta, RH
    Bernard  (indoor)   Ta, Td
    Liljegren (outdoor) Ta, RH, surface pressure, wind, ssrd, fdir

ERA5 has no 2 m relative humidity, so RH is derived from Ta and Td downstream.

Two UTC days are requested, not one. IST is UTC+05:30, so a single UTC day
covers 05:30 IST to 05:30 IST the next morning and clips the afternoon peak of
one local day or the other. Two consecutive UTC days let the comparison cut a
clean local day and still have the full diurnal cycle inside it.

Unit traps handled downstream, noted here so the raw file is not misread:
  * surface_pressure                         Pa      -> / 100 for hPa
  * surface_solar_radiation_downwards        J m-2   accumulated over the
  * total_sky_direct_solar_radiation_at_surface      preceding hour -> / 3600
                                                     for W m-2
  * thermofeel's Liljegren wants fdir as a 0-1 FRACTION of ssrd, not W m-2.

Needs ``cdsapi`` and a ``~/.cdsapirc`` holding a Copernicus CDS key. Reads
nothing under ``IRT_DATA_DIR`` and touches no config, master or bundle.

    python -m tools.diagnostics.wbgt_era5_grid_download --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

DATASET = "reanalysis-era5-single-levels"

#: Every field needed by at least one of the four WBGT methods.
VARIABLES = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "surface_pressure",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_solar_radiation_downwards",
    "total_sky_direct_solar_radiation_at_surface",
]

ALL_HOURS = [f"{h:02d}:00" for h in range(24)]

#: [North, West, South, East] -- the Telangana box. Contains Hyderabad
#: (17.39 N, 78.49 E), which is already in the six-site Open-Meteo run, so the
#: gridded result has a driver-independent consistency check.
DEFAULT_AREA = [20, 77, 16, 81]

DEFAULT_DATE = "2024-05-20"
DEFAULT_OUT = Path("scratch/wbgt_era5_grid")


def build_request(day: dt.date, days: int, area: list[float]) -> dict:
    """Build a CDS request covering ``days`` consecutive UTC days from ``day``."""
    dates = [day + dt.timedelta(days=i) for i in range(days)]
    years = sorted({f"{d.year:04d}" for d in dates})
    months = sorted({f"{d.month:02d}" for d in dates})
    day_nums = sorted({f"{d.day:02d}" for d in dates})
    if len(years) > 1 or len(months) > 1:
        raise ValueError(
            "CDS expands year x month x day as a cross product, so a span "
            "crossing a month boundary would over-request. Pick a start date "
            f"clear of the boundary; got {dates[0]} .. {dates[-1]}."
        )
    return {
        "product_type": ["reanalysis"],
        "variable": VARIABLES,
        "year": years,
        "month": months,
        "day": day_nums,
        "time": ALL_HOURS,
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": area,
    }


def extract_if_zip(target: Path) -> list[Path]:
    """Unpack the archive CDS returns for a mixed step-type request.

    ``download_format: unarchived`` is honoured only when every requested
    variable shares a step type. This request mixes instantaneous fields
    (t2m, d2m, sp, u10, v10) with accumulated ones (ssrd, fdir), so CDS splits
    the result server-side into two NetCDF files and returns a zip -- still
    named ``.nc``. Left unextracted it fails downstream as an unreadable
    NetCDF, so it is unpacked here rather than diagnosed later.
    """
    import zipfile

    if not zipfile.is_zipfile(target):
        return []
    with zipfile.ZipFile(target) as archive:
        names = archive.namelist()
        archive.extractall(target.parent)
    return [target.parent / name for name in names]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=DEFAULT_DATE, help="start date, YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=2, help="consecutive UTC days")
    parser.add_argument(
        "--area",
        default=",".join(str(v) for v in DEFAULT_AREA),
        help="N,W,S,E in degrees",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--dry-run", action="store_true", help="print the request, fetch nothing")
    args = parser.parse_args()

    day = dt.date.fromisoformat(args.date)
    area = [float(v) for v in args.area.split(",")]
    if len(area) != 4:
        parser.error("--area needs exactly four values: N,W,S,E")

    request = build_request(day, args.days, area)

    n_lat = int(round((area[0] - area[2]) / 0.25)) + 1
    n_lon = int(round((area[3] - area[1]) / 0.25)) + 1
    n_steps = 24 * args.days
    approx_mb = n_lat * n_lon * n_steps * len(VARIABLES) * 4 / 1e6

    print(f"dataset   : {DATASET}")
    print(f"variables : {len(VARIABLES)}")
    for name in VARIABLES:
        print(f"            {name}")
    print(f"dates     : {day} .. {day + dt.timedelta(days=args.days - 1)} ({n_steps} hourly steps)")
    print(f"area      : N={area[0]} W={area[1]} S={area[2]} E={area[3]}  ({n_lat} x {n_lon} = {n_lat * n_lon} cells)")
    print(f"est. size : ~{approx_mb:.1f} MB uncompressed")

    if args.dry_run:
        print("\n--dry-run: nothing submitted.")
        return 0

    import cdsapi

    args.out_dir.mkdir(parents=True, exist_ok=True)
    target = args.out_dir / f"era5_wbgt_{day:%Y%m%d}_{args.days}d.nc"

    client = cdsapi.Client()
    client.retrieve(DATASET, request).download(str(target))
    print(f"\nwrote {target}  ({target.stat().st_size / 1e6:.1f} MB)")

    for member in extract_if_zip(target):
        print(f"extracted {member}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
