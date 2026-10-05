"""Build an offline grid-first outdoor WBGT pilot from completed ACCESS outputs.

CHG-0654. Default is a plan; --build verifies signed diagnostic evidence and
writes an HTML explorer, annual CSV and provenance. No climate calculations,
network access, production writes or second-model dependency.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path

METHOD = "grid_first_cell_metrics"
METRICS = ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32")
YEARS = {"historical": (1990, 2010), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}
DEFAULT = Path("docs/diagnostics/wbgt_outdoor_route_b_pilot")


def load_pilot(source: Path) -> tuple[dict, dict]:
    """Validate signed outputs, full calendars, missing support and count semantics."""
    manifest_path = source / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("status") != "PILOT_COMPLETE_DIAGNOSTIC_ONLY" or manifest.get("model") != "ACCESS-CM2":
        raise ValueError("A completed ACCESS-CM2 diagnostic manifest is required")
    required = {"yearly_metrics.csv", "unit_support.csv"}
    if not required.issubset(manifest["artifact_sha256"]):
        raise ValueError("Manifest does not sign the pilot inputs")
    for name, expected in manifest["artifact_sha256"].items():
        path = source / name
        if path.parent.resolve() != source.resolve():
            raise ValueError("Artifact must be inside the source directory")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Changed diagnostic evidence: {name}")
    units = {}
    with (source / "unit_support.csv").open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            uid = int(row["unit_index"])
            if uid in units:
                raise ValueError("Duplicate roster unit")
            eligible = row["eligible"].lower() == "true"
            coverage = float(row["retained_fraction"])
            if eligible != (coverage >= .95):
                raise ValueError("Eligibility differs from the declared coverage screen")
            units[uid] = dict(id=uid, state=row["state_name"], level=row["level"],
                              district=row["district_name"], block=row["block_name"],
                              lgd=row["lgd_code"], coverage=coverage, eligible=eligible,
                              series={s: {} for s in YEARS})
    if not units:
        raise ValueError("Empty pilot roster")
    with (source / "yearly_metrics.csv").open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            if row["method"] != METHOD:
                continue
            uid, scenario, year = int(row["unit_index"]), row["scenario"], int(row["year"])
            if uid not in units or scenario not in YEARS or not YEARS[scenario][0] <= year <= YEARS[scenario][1]:
                raise ValueError("Unknown unit, scenario or year")
            series = units[uid]["series"][scenario]
            if year in series:
                raise ValueError("Duplicate grid-first annual record")
            values = [float(row[m]) if row[m] else float("nan") for m in METRICS]
            if any(math.isinf(v) for v in values):
                raise ValueError("Infinite annual value")
            if not units[uid]["eligible"] and any(math.isfinite(v) for v in values):
                raise ValueError("Excluded unit has headline values")
            if any(math.isfinite(v) and not -1e-8 <= v <= 365 + 1e-8 for v in values[2:]):
                raise ValueError("Annual count outside complete-365 bounds")
            series[year] = [v if math.isfinite(v) else None for v in values]
    for unit in units.values():
        for scenario, (first, last) in YEARS.items():
            series = unit["series"][scenario]
            if sorted(series) != list(range(first, last + 1)):
                raise ValueError("Incomplete annual roster")
            unit["series"][scenario] = [series[y] for y in range(first, last + 1)]
    payload = dict(model="ACCESS-CM2", method=METHOD, metrics=METRICS, years=YEARS,
                   units=list(units.values()))
    provenance = dict(change="CHG-0654", model="ACCESS-CM2", method=METHOD,
                      source_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                      source_signature=manifest["signature_sha256"], source_artifact_sha256=manifest["artifact_sha256"],
                      units=len(units), eligible_units=sum(u["eligible"] for u in units.values()),
                      calibration="1985-2014", reporting_baseline="1990-2010",
                      annual_calendar="365 days, excluding February 29", production=False,
                      second_model="Stopped by user; partial caches excluded")
    return payload, provenance


def build(source: Path, out: Path) -> dict:
    """Write a self-contained pilot and full-precision CSV from verified results."""
    if out.resolve() == source.resolve() or source.resolve() in out.resolve().parents:
        raise ValueError("Pilot output must be separate from source evidence")
    allowed = (Path("docs/diagnostics").resolve(), Path("scratch").resolve())
    if not any(out.resolve().is_relative_to(root) for root in allowed):
        raise ValueError("Pilot output must be under docs/diagnostics or scratch")
    payload, provenance = load_pilot(source)
    template = Path(__file__).with_name("templates") / "wbgt_route_b_pilot.html"
    encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    html = template.read_text(encoding="utf-8").replace("__PILOT_DATA__", encoded)
    out.mkdir(parents=True, exist_ok=True)
    temp = out / "index.html.part"
    temp.write_text(html, encoding="utf-8")
    temp.replace(out / "index.html")
    with (out / "pilot_yearly.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["model", "method", "unit_index", "state", "level", "district", "block", "lgd_code",
                         "retained_fraction", "eligible", "scenario", "year", *METRICS])
        for unit in payload["units"]:
            for scenario, values in unit["series"].items():
                for year, row in enumerate(values, YEARS[scenario][0]):
                    writer.writerow([payload["model"], METHOD, unit["id"], unit["state"], unit["level"],
                                     unit["district"], unit["block"], unit["lgd"], unit["coverage"], unit["eligible"],
                                     scenario, year, *row])
    provenance["output_sha256"] = {name: hashlib.sha256((out / name).read_bytes()).hexdigest()
                                  for name in ("index.html", "pilot_yearly.csv")}
    (out / "manifest.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    (out / "README.md").write_text(
        "# Grid-first outdoor WBGT working pilot — CHG-0654\n\n"
        "Open `index.html` directly in a browser. No server, network or Python environment is required.\n\n"
        "ACCESS-CM2 only; Kerala, Rajasthan and Himachal Pradesh; 75 districts and 581 blocks. "
        "Historical 1990–2010; SSP245/SSP585 2020–2080. Select a place, metric and period; "
        "inspect annual values and baseline differences, or download CSVs.\n\n"
        "Computation: daily outdoor WBGT per cell → annual metric per cell → fixed area-weighted "
        "admin mean. Counts are area-average cell exceedance days; annual maximum is an "
        "area-average of cell annual maxima, not a polygon-wide simultaneous extreme. "
        "Complete 365-day cell-years are required; missing support stays missing.\n\n"
        "630/656 units meet the 95% retained-area screen. The 26 exclusions are in Kerala. "
        "This is a single-model diagnostic pilot, not an ensemble or production release. "
        "UHE-Daily is a provisional shade calibration reference. Rajasthan ≥32 °C held-out "
        "errors worsen; future QDM application-window sensitivity remains material.\n\n"
        "`pilot_yearly.csv` retains full-precision annual values and roster metadata; "
        "`manifest.json` records source and output hashes. No further climate calculations "
        "are launched by this viewer or builder. Rebuild with "
        "`python -m tools.diagnostics.wbgt_route_b_pilot --build`.\n", encoding="utf-8")
    return provenance


def main(argv: list[str] | None = None) -> int:
    """Plan by default; --build creates the offline working pilot."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--source", type=Path, default=DEFAULT / "ACCESS-CM2")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT / "working_pilot")
    args = parser.parse_args(argv)
    result = build(args.source, args.out_dir) if args.build else dict(action="plan_only", source=str(args.source),
                                                                    output=str(args.out_dir), climate_compute=False)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
