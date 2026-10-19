"""Local NEX shade residuals: distributions and complete annual counts only."""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
from india_resilience_tool.compute.wbgt import shade_daily, shade_annual
from tools.diagnostics.wbgt_deployed_vs_reference import SITES
from tools.diagnostics.wbgt_shade_release import daily_frame


def summarize(out_dir: Path) -> None:
    """Report both methods on the same local model/year roster."""
    frame = pd.read_csv(out_dir / "nex_residuals.csv")
    lines = ["# Shade-specific NEX residual assessment", "",
             "1991–2010 complete historical cell-years compared as distributions and annual counts. "
             "No same-date weather comparison; no QDM. KACE-1-0-G is excluded for its unsupported "
             "360-day calendar. Values equally weight the other 19 models on a common roster.", "",
             "| Site | Shipped / peak mean bias (C) | Shipped / peak P95 bias (C) | >=28 shipped / peak / reference days | >=30 shipped / peak / reference days | >=32 shipped / peak / reference days |",
             "|---|---:|---:|---:|---:|---:|"]
    for site, group in frame.groupby("site", sort=False):
        m = group.mean(numeric_only=True)
        cells = [site, f"{m.shipped_mean_bias:.3f} / {m.mean_bias:.3f}",
                 f"{m['shipped_quantile_0.95_bias']:.3f} / {m['quantile_0.95_bias']:.3f}"]
        cells.extend(f"{m[f'shipped_days_ge_{t}']:.2f} / {m[f'model_days_ge_{t}']:.2f} / "
                     f"{m[f'reference_days_ge_{t}']:.2f}" for t in (28, 30, 32))
        lines.append("| " + " | ".join(cells) + " |")
    lines.extend(["", "Residual climate-model bias remains after the method correction. "
                  "These local reference comparisons do not establish national absolute accuracy. "
                  "Full model-level quantile and count results are in `nex_residuals.csv`.", ""])
    (out_dir / "NEX_RESIDUALS.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Compare local historical NEX distributions with cached ERA5 reference."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-root", type=Path, required=True)
    p.add_argument("--summarize-only", action="store_true")
    p.add_argument(
        "--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_shade_release")
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("scratch/wbgt_deployed_vs_reference_cache"),
    )
    args = p.parse_args(argv)
    if args.summarize_only:
        summarize(args.out_dir)
        return 0
    roster = json.loads((args.out_dir / "roster.json").read_text())["models"]
    references = {}
    for site in SITES:
        raw = pd.concat(
            [
                pd.read_parquet(args.cache_dir / f"{site.name.lower()}_{y}.parquet")
                for y in range(1990, 2011)
            ]
        )
        d = daily_frame(raw)
        d = d.loc[d.eligible & ~((d.index.month == 2) & (d.index.day == 29))]
        sizes = d.groupby(d.index.year).size()
        d = d.loc[d.index.year.isin(sizes.index[sizes == 365])]
        references[site.name] = d
    lat = xr.DataArray(
        [s.lat for s in SITES], dims="site", coords={"site": [s.name for s in SITES]}
    )
    lon = xr.DataArray(
        [s.lon for s in SITES], dims="site", coords={"site": [s.name for s in SITES]}
    )
    rows = []
    excluded = []
    for model in roster:
        samples = {s.name: [] for s in SITES}
        annual = {s.name: [] for s in SITES}
        old_samples = {s.name: [] for s in SITES}
        old_annual = {s.name: [] for s in SITES}
        try:
            for year in range(1991, 2011):
                arrays = []
                for var in ("tas", "tasmax", "hurs"):
                    with xr.open_dataset(
                        args.source_root / "historical" / var / model / f"{year}.nc"
                    ) as ds:
                        # All variables must select the identical native source cells.
                        a = ds[var].sel(lat=lat, lon=lon, method="nearest").load()
                        arrays.append(a)
                d = shade_daily(*arrays)
                a = shade_annual(d, year)
                old_daily = shade_daily(arrays[0], arrays[0], arrays[2])
                old_a = shade_annual(old_daily, year)
                for site in SITES:
                    if bool(a.incomplete_year.sel(site=site.name)) or bool(
                        old_a.incomplete_year.sel(site=site.name)
                    ):
                        continue
                    sample = d.peak.sel(site=site.name)
                    sample = sample.sel(
                        time=~((sample.time.dt.month == 2) & (sample.time.dt.day == 29))
                    )
                    samples[site.name].extend(sample.values.tolist())
                    old_sample = old_daily.peak.sel(site=site.name)
                    old_sample = old_sample.sel(
                        time=~(
                            (old_sample.time.dt.month == 2)
                            & (old_sample.time.dt.day == 29)
                        )
                    )
                    old_samples[site.name].extend(old_sample.values.tolist())
                    old_annual[site.name].append(
                        {
                            t: float(old_a[f"days_ge_{t}"].sel(site=site.name))
                            for t in (28, 30, 32)
                        }
                    )
                    annual[site.name].append(
                        {
                            t: float(a[f"days_ge_{t}"].sel(site=site.name))
                            for t in (28, 30, 32)
                        }
                    )
            for site in SITES:
                reference = references[site.name].bernard
                values = np.asarray(samples[site.name])
                counts = annual[site.name]
                if not values.size:
                    raise ValueError(f"No complete cell years: {site.name}")
                row = {
                    "model": model,
                    "site": site.name,
                    "complete_years": len(counts),
                    "mean_bias": float(values.mean() - reference.mean()),
                    "shipped_mean_bias": float(
                        np.mean(old_samples[site.name]) - reference.mean()
                    ),
                }
                for q in (0.05, 0.5, 0.95, 0.99):
                    row[f"shipped_quantile_{q}_bias"] = float(
                        np.quantile(old_samples[site.name], q) - reference.quantile(q)
                    )
                    row[f"quantile_{q}_bias"] = float(
                        np.quantile(values, q) - reference.quantile(q)
                    )
                for t in (28, 30, 32):
                    row[f"model_days_ge_{t}"] = float(np.mean([c[t] for c in counts]))
                    row[f"shipped_days_ge_{t}"] = float(
                        np.mean([c[t] for c in old_annual[site.name]])
                    )
                    row[f"reference_days_ge_{t}"] = float(
                        (reference >= t).groupby(reference.index.year).sum().mean()
                    )
                rows.append(row)
            print(f"{model}: distribution/count residuals complete", flush=True)
        except ValueError as exc:
            excluded.append({"model": model, "reason": str(exc)})
            print(f"{model}: excluded: {exc}", flush=True)
    pd.DataFrame(rows).to_csv(args.out_dir / "nex_residuals.csv", index=False)
    (args.out_dir / "nex_exclusions.json").write_text(json.dumps(excluded, indent=2))
    summarize(args.out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
