"""Build the data-centre case-study site CSVs.

Emits two families of output:

* ``upload/`` — the exact four-column contract the dashboard's coordinate upload
  expects (``id, custom_name, lat, long``), per ``sample_coordinates.xlsx``.
  Operational sites, announced sites, and one file per state.
* ``reference/`` — the full site table with operator, district and source, kept
  because the upload format has nowhere to carry provenance.

Coordinates are OpenStreetMap (Nominatim) geocodes of localities that were each
verified against a public source (operator location page, DataCenterDynamics,
or a data-centre aggregator listing). They are locality-precise, not
campus-precise: adequate for resolving a site to its district, not for
plotting a building footprint. See README.md for provenance and caveats.

Run:
    python case_study/data_centres/build_site_csvs.py
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent

# The dashboard's coordinate upload contract, per sample_coordinates.xlsx.
UPLOAD_FIELDNAMES = ["id", "custom_name", "lat", "long"]

FIELDNAMES = [
    "site_id",
    "operator",
    "facility",
    "status",
    "cluster",
    "locality",
    "latitude",
    "longitude",
    "district",
    "state",
    "precision",
    "source",
]

# site_id, operator, facility, status, cluster, locality, lat, lon, district,
# state, precision, source
SITES: tuple[tuple[str, ...], ...] = (
    # --- Mumbai Metropolitan Region ---
    ("DC01", "Yotta", "NM1 (Yotta Datacenter Park)", "operational", "Mumbai MMR",
     "Hiranandani Fortune City, Panvel", "18.91347", "73.19478", "Raigad",
     "Maharashtra", "locality", "colocation.yotta.com; datacenterhawk.com"),
    ("DC02", "NTT (Netmagic)", "GDC Mumbai 6", "operational", "Mumbai MMR",
     "Chandivali, Andheri East", "19.10915", "72.89458", "Mumbai Suburban",
     "Maharashtra", "locality", "services.global.ntt; datacentermap.com"),
    ("DC03", "NTT (Netmagic)", "GDC Mumbai 2", "operational", "Mumbai MMR",
     "Vikhroli West", "19.11454", "72.92572", "Mumbai Suburban",
     "Maharashtra", "locality", "datacentercatalog.com; datacentermap.com"),
    ("DC04", "NTT (Netmagic)", "Mumbai (Mahape)", "operational", "Mumbai MMR",
     "Mahape MIDC, Navi Mumbai", "19.10930", "73.02357", "Thane",
     "Maharashtra", "locality", "services.global.ntt"),
    ("DC05", "Equinix", "MB2 Mumbai IBX", "operational", "Mumbai MMR",
     "Chandivali Farm Road, Andheri East", "19.11588", "72.85420", "Mumbai Suburban",
     "Maharashtra", "locality", "equinix.com; datacenters.com"),
    ("DC06", "Sify", "Mumbai 02 Airoli", "operational", "Mumbai MMR",
     "Airoli MIDC, Navi Mumbai", "19.15851", "72.99940", "Thane",
     "Maharashtra", "locality", "sifytechnologies.com; datacenters.com"),
    ("DC07", "Sify", "Mumbai 03 Rabale", "operational", "Mumbai MMR",
     "Rabale MIDC, Navi Mumbai", "19.13664", "73.00278", "Thane",
     "Maharashtra", "locality", "sifytechnologies.com; datacenters.com"),
    # --- Pune / Nashik ---
    ("DC08", "STT GDC India", "STT Pune DC 1", "operational", "Pune",
     "Hinjawadi", "18.59275", "73.73822", "Pune",
     "Maharashtra", "locality", "sttelemediagdc.com/in-en/locations/pune"),
    ("DC09", "Nxtra by Airtel", "Nxtra Pune", "operational", "Pune",
     "Kharadi MIDC Knowledge Park", "18.55128", "73.94166", "Pune",
     "Maharashtra", "locality", "baxtel.com/data-center/nxtra-data-pune"),
    ("DC10", "Web Werks", "Web Werks Pune", "operational", "Pune",
     "Rajiv Gandhi Infotech Park, Hinjawadi", "18.58107", "73.74056", "Pune",
     "Maharashtra", "locality", "webwerks.in/data-center-in-pune"),
    ("DC11", "ESDS", "ESDS Nashik DC", "operational", "Nashik",
     "Satpur MIDC", "20.00448", "73.73754", "Nashik",
     "Maharashtra", "locality", "esds.co.in; datacentermap.com"),
    # --- Chennai region ---
    ("DC12", "STT GDC India", "STT Chennai DC 2/3", "operational", "Chennai",
     "Ambattur Industrial Estate", "13.09524", "80.16717", "Tiruvallur",
     "Tamil Nadu", "locality", "sttelemediagdc.com/in-en/locations/chennai"),
    ("DC13", "STT GDC India", "STT Chennai DC 7", "operational", "Chennai",
     "SIPCOT IT Park, Siruseri", "12.82358", "80.22215", "Chengalpattu",
     "Tamil Nadu", "locality", "sttelemediagdc.com; w.media"),
    ("DC14", "Sify", "Chennai 02 Siruseri", "operational", "Chennai",
     "SIPCOT IT Park, Siruseri", "12.82358", "80.22215", "Chengalpattu",
     "Tamil Nadu", "locality", "sifytechnologies.com"),
    ("DC15", "AdaniConneX", "Chennai 1", "operational", "Chennai",
     "SIPCOT IT Park, Siruseri", "12.82358", "80.22215", "Chengalpattu",
     "Tamil Nadu", "locality", "adaniconnex.com press release"),
    ("DC16", "CtrlS", "Chennai DC 2", "operational", "Chennai",
     "Ambattur Industrial Estate", "13.09524", "80.16717", "Tiruvallur",
     "Tamil Nadu", "locality", "ctrls.com; datacenters.com"),
    # --- Bengaluru ---
    ("DC17", "NTT (Netmagic)", "NTT Bengaluru 1", "operational", "Bengaluru",
     "ITPL, Whitefield", "12.98772", "77.73692", "Bengaluru Urban",
     "Karnataka", "locality", "datacentermap.com/india/bangalore/netmagic-bangalore"),
    ("DC18", "Sify", "Bangalore 01", "operational", "Bengaluru",
     "Electronic City Phase 1", "12.84968", "77.66497", "Bengaluru Urban",
     "Karnataka", "locality", "sifytechnologies.com; datacenters.com"),
    ("DC19", "CtrlS", "Bengaluru DC", "operational", "Bengaluru",
     "Electronic City Phase 1", "12.84968", "77.66497", "Bengaluru Urban",
     "Karnataka", "locality", "ctrls.com; datacenters.com"),
    # --- Hyderabad region ---
    ("DC20", "CtrlS", "Hyderabad DC 1", "operational", "Hyderabad",
     "Financial District, Nanakramguda", "17.41997", "78.32869", "Ranga Reddy",
     "Telangana", "locality", "ctrls.com/datacenter-hyderabad"),
    ("DC21", "AWS", "Chandanvelly campus (HYD)", "operational", "Hyderabad",
     "Chandanvelly", "17.23062", "78.17098", "Ranga Reddy",
     "Telangana", "locality", "datacenterdynamics.com; baxtel.com"),
    ("DC22", "Microsoft", "Hyderabad region (Kottur)", "announced", "Hyderabad",
     "Kottur", "17.15129", "78.28624", "Ranga Reddy",
     "Telangana", "locality", "local.microsoft.com India datacentres PDF"),
    ("DC23", "Sify", "Hyderabad 01", "operational", "Hyderabad",
     "Banjara Hills", "17.41775", "78.43990", "Hyderabad",
     "Telangana", "locality", "sifytechnologies.com; datacenters.com"),
    # --- Delhi NCR ---
    ("DC24", "Yotta", "D1 Greater Noida", "operational", "Delhi NCR",
     "Knowledge Park V, Greater Noida", "28.46707", "77.51376", "Gautam Buddha Nagar",
     "Uttar Pradesh", "locality", "colocation.yotta.com; peeringdb.com/fac/13846"),
    ("DC25", "Sify", "Noida 01", "operational", "Delhi NCR",
     "Sector 132, Noida", "28.51171", "77.37678", "Gautam Buddha Nagar",
     "Uttar Pradesh", "locality", "datacenters.com sify-noida-01-dc"),
    ("DC26", "CtrlS", "Noida DC", "operational", "Delhi NCR",
     "Sector 127, Noida", "28.47117", "77.53157", "Gautam Buddha Nagar",
     "Uttar Pradesh", "locality", "ctrls.com; datacenters.com"),
    ("DC27", "Nxtra by Airtel", "Nxtra Manesar", "operational", "Delhi NCR",
     "IMT Manesar", "28.36719", "76.92067", "Gurugram",
     "Haryana", "locality", "baxtel.com/data-center/nxtra-data-manesar"),
    # --- East / West / other ---
    ("DC28", "STT GDC India", "STT Kolkata DC 1", "operational", "Kolkata",
     "Ultadanga", "22.59613", "88.38528", "Kolkata",
     "West Bengal", "locality", "sttelemediagdc.com/in-en/locations/kolkata"),
    ("DC29", "Nxtra by Airtel", "Nxtra Bhubaneswar", "operational", "Bhubaneswar",
     "Infocity, Patia", "20.36045", "85.82477", "Khordha",
     "Odisha", "locality", "datacenters.com nxtra locations"),
    ("DC30", "STT GDC India", "STT Jaipur 1", "operational", "Jaipur",
     "RIICO Industrial Area, Tonk Road", "26.78275", "75.83795", "Jaipur",
     "Rajasthan", "locality", "sttelemediagdc.com/in-en/locations/jaipur"),
    ("DC31", "STT GDC India", "STT Gandhinagar (GIFT City)", "operational", "Gandhinagar",
     "GIFT City", "23.16291", "72.68870", "Gandhinagar",
     "Gujarat", "locality", "tiaonline.org TIA-942 listing"),
    ("DC32", "Reliance", "Jamnagar AI DC (GW-scale)", "announced", "Jamnagar",
     "Jamnagar", "22.47324", "70.05521", "Jamnagar",
     "Gujarat", "city", "datacenterdynamics.com"),
    ("DC33", "Google", "Visakhapatnam AI hub", "announced", "Visakhapatnam",
     "Madhurawada", "17.81025", "83.35402", "Visakhapatnam",
     "Andhra Pradesh", "locality", "techcrunch.com; datacenterdynamics.com"),
    ("DC34", "Sify", "Lucknow DC", "announced", "Lucknow",
     "Lucknow", "26.83810", "80.93460", "Lucknow",
     "Uttar Pradesh", "city", "datacenterdynamics.com"),
)


def as_dicts() -> list[dict[str, str]]:
    """Return the site table as a list of column-keyed dicts."""
    return [dict(zip(FIELDNAMES, row, strict=True)) for row in SITES]


def slugify(value: str) -> str:
    """Lowercase a state name into a filename-safe slug."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write the full reference table to ``path``, creating parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{path.relative_to(OUT_DIR.parent.parent)}  ({len(rows)} rows)")


def upload_name(row: dict[str, str]) -> str:
    """Return an ASCII-only map label for a site.

    The upload path's value handling is not well characterised, so labels stay
    ASCII and never begin with a character a spreadsheet would read as a formula.
    """
    name = f"{row['operator']} - {row['facility']}"
    name = name.encode("ascii", "replace").decode("ascii")
    return name.lstrip("=+-@ ")


def merged_name(group: list[dict[str, str]]) -> str:
    """Return the upload label for one coordinate point.

    Co-located campuses share a single point, so their operators are merged into
    one label rather than dropped — the dashboard rejects duplicate coordinates,
    but losing an operator from the map would misrepresent the cluster.
    """
    if len(group) == 1:
        return upload_name(group[0])

    operators: list[str] = []
    for row in group:
        if row["operator"] not in operators:
            operators.append(row["operator"])
    label = f"{group[0]['locality']} - {', '.join(operators)}"
    return label.encode("ascii", "replace").decode("ascii").lstrip("=+-@ ")


def dedupe_points(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    """Group rows by coordinate, preserving first-appearance order."""
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        groups.setdefault(f"{row['latitude']},{row['longitude']}", []).append(row)
    return list(groups.values())


def write_upload_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write ``rows`` in the dashboard's four-column upload format.

    Coordinates are deduplicated: the dashboard flags repeated lat/long pairs.
    ``id`` restarts at 1 in every file, since each file is uploaded on its own.
    """
    groups = dedupe_points(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=UPLOAD_FIELDNAMES)
        writer.writeheader()
        for index, group in enumerate(groups, start=1):
            writer.writerow({
                "id": index,
                "custom_name": merged_name(group),
                "lat": group[0]["latitude"],
                "long": group[0]["longitude"],
            })
    merged = sum(len(g) - 1 for g in groups)
    suffix = f", {merged} co-located merged" if merged else ""
    print(f"{path.relative_to(OUT_DIR.parent.parent)}  "
          f"({len(groups)} points from {len(rows)} sites{suffix})")


def main() -> None:
    rows = as_dicts()
    operational = [r for r in rows if r["status"] == "operational"]
    announced = [r for r in rows if r["status"] == "announced"]

    points: dict[str, list[str]] = {}
    for row in rows:
        points.setdefault(f"{row['latitude']},{row['longitude']}", []).append(row["site_id"])
    shared = {point: ids for point, ids in points.items() if len(ids) > 1}

    write_csv(OUT_DIR / "reference" / "dc_sites_all.csv", rows)

    write_upload_csv(OUT_DIR / "upload" / "dc_sites_operational.csv", operational)
    write_upload_csv(OUT_DIR / "upload" / "dc_sites_announced.csv", announced)

    # State files carry operational sites only: the upload format has no status
    # column, so mixing in announced sites would make them indistinguishable.
    by_state: dict[str, list[dict[str, str]]] = {}
    for row in operational:
        by_state.setdefault(row["state"], []).append(row)

    for state, state_rows in sorted(by_state.items()):
        write_upload_csv(OUT_DIR / "upload" / "by_state" / f"{slugify(state)}.csv", state_rows)

    print(
        f"\ntotal={len(rows)}  operational={len(operational)}  "
        f"announced={len(announced)}  states={len(by_state)}  "
        f"districts={len({r['district'] for r in rows})}"
    )
    for point, ids in sorted(shared.items()):
        print(f"co-located, merged to one upload point {point}: {', '.join(ids)}")


if __name__ == "__main__":
    main()
