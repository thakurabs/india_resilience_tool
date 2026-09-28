"""Read-only published WBGT audit and local shade model-year inventory."""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd


def audit_calendars(data_root: Path, out_dir: Path) -> None:
    """Audit every selected input header and reject unsupported model calendars."""
    import xarray as xr

    roster = json.loads((out_dir / "roster.json").read_text())
    rows = []
    rejected = set()
    supported = {"standard", "gregorian", "proleptic_gregorian", "noleap", "365_day"}
    for model in roster["models"]:
        for scenario, years in roster["required_years"].items():
            for var in ("tas", "tasmax", "hurs"):
                counts = {}
                for year in years:
                    path = (
                        data_root / "r1i1p1f1" / scenario / var / model / f"{year}.nc"
                    )
                    with xr.open_dataset(path, decode_times=False) as ds:
                        calendar = str(ds.time.attrs.get("calendar", "standard"))
                    counts[calendar] = counts.get(calendar, 0) + 1
                    if calendar not in supported:
                        rejected.add(model)
                rows.append(
                    dict(
                        model=model,
                        scenario=scenario,
                        var=var,
                        calendars=json.dumps(counts),
                    )
                )
        print(f"Calendar headers checked: {model}", flush=True)
    pd.DataFrame(rows).to_csv(out_dir / "source_calendars.csv", index=False)
    roster["input_intersection_models"] = roster["models"]
    roster["models"] = sorted(set(roster["models"]) - rejected)
    roster["calendar_exclusions"] = sorted(rejected)
    roster["model_years"] = len(roster["models"]) * sum(
        len(y) for y in roster["required_years"].values()
    )
    (out_dir / "release_roster.json").write_text(json.dumps(roster, indent=2))


def main(argv: list[str] | None = None) -> int:
    """Audit all eight WBGT metrics and select one roster across seven slices."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", type=Path, required=True)
    p.add_argument(
        "--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_shade_release")
    )
    p.add_argument("--calendars-only", action="store_true")
    args = p.parse_args(argv)
    if args.calendars_only:
        audit_calendars(args.data_root, args.out_dir)
        return 0
    args.out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for family in ("wbgt_shade_stull", "swbgt_empirical"):
        for suffix in ("annual_mean", "days_ge_28", "days_ge_30", "days_ge_32"):
            slug = f"{family}_{suffix}"
            for path in sorted(
                (args.data_root / "processed" / slug).glob("*/master_metrics_by_*.csv")
            ):
                df = pd.read_csv(path)
                for c in df.columns:
                    if not c.endswith("__mean"):
                        continue
                    _, scenario, period, _ = c.rsplit("__", 3)
                    prefix = c[:-4]
                    models = set()
                    mc = prefix + "values_per_model"
                    if mc in df:
                        for value in df[mc].dropna():
                            try:
                                models.update(json.loads(value))
                            except (ValueError, TypeError):
                                pass
                    rows.append(
                        dict(
                            slug=slug,
                            state=path.parent.name,
                            level=path.stem.removeprefix("master_metrics_by_"),
                            scenario=scenario,
                            period=period,
                            units=len(df),
                            zero_fraction=float(df[c].eq(0).mean()),
                            nan_fraction=float(df[c].isna().mean()),
                            models=json.dumps(sorted(models)),
                            coverage="No temporal completeness or valid-area data in legacy master",
                        )
                    )
    pd.DataFrame(rows).to_csv(args.out_dir / "published_baseline.csv", index=False)
    source = args.data_root / "r1i1p1f1"
    required = {
        "historical": set(range(1990, 2011)),
        "ssp245": set(range(2020, 2081)),
        "ssp585": set(range(2020, 2081)),
    }
    models = sorted(
        {
            p.name
            for scenario in required
            for v in ("tas", "tasmax", "hurs")
            for p in (source / scenario / v).glob("*")
            if p.is_dir()
        }
    )
    inventory = []
    roster = []
    for model in models:
        ok = True
        for scenario, years in required.items():
            for var in ("tas", "tasmax", "hurs"):
                files = {
                    int(f.stem): f
                    for f in (source / scenario / var / model).glob("*.nc")
                    if f.stem.isdigit()
                }
                missing = sorted(years - files.keys())
                ok &= not missing
                inventory.append(
                    dict(
                        model=model,
                        scenario=scenario,
                        var=var,
                        required=len(years),
                        missing_years=json.dumps(missing),
                        bytes=sum(
                            files[y].stat().st_size for y in years & files.keys()
                        ),
                    )
                )
        if ok:
            roster.append(model)
    pd.DataFrame(inventory).to_csv(args.out_dir / "source_inventory.csv", index=False)
    (args.out_dir / "roster.json").write_text(
        json.dumps(
            {
                "models": roster,
                "excluded": sorted(set(models) - set(roster)),
                "required_years": {s: sorted(y) for s, y in required.items()},
                "model_years": len(roster) * sum(map(len, required.values())),
                "rule": "Intersection of tas/tasmax/hurs across every publication year; no radiation/wind restriction",
            },
            indent=2,
        )
    )
    print(
        f"Audited {len(rows)} metric/state/level/slice summaries; stable roster {len(roster)} models: {roster}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
