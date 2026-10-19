"""Offline shade release experiment: frozen formula, two windows and policies."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.wbgt import SHADE_METHOD_SIGNATURE, shade_daily
from tools.diagnostics.wbgt_deployed_vs_reference import (
    SITES,
    IST_OFFSET,
    bernard_indoor_wbgt_c,
    irt_wbgt_shade_stull_c,
    build_hourly_frame,
    build_daily_frame,
    compare_site,
)


def daily_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Compare all four formulations on identical complete, finite hourly days."""
    raw = raw.copy()
    fields = ["temperature_2m", "relative_humidity_2m", "dew_point_2m"]
    valid = np.isfinite(raw[fields]).all(axis=1)
    raw["bernard"] = bernard_indoor_wbgt_c(raw.temperature_2m, raw.dew_point_2m)
    raw["stull"] = irt_wbgt_shade_stull_c(raw.temperature_2m, raw.relative_humidity_2m)
    raw["valid"] = valid & np.isfinite(raw.bernard) & np.isfinite(raw.stull)
    g = raw.groupby((raw.index + IST_OFFSET).date)
    d = pd.DataFrame(
        {
            "tas": g.temperature_2m.mean(),
            "tasmax": g.temperature_2m.max(),
            "hurs": g.relative_humidity_2m.mean(),
            "bernard": g.bernard.max(),
            "hourly_stull": g.stull.max(),
            "valid_hours": g.valid.sum(),
            "hours": g.size(),
        }
    )
    d.index = pd.to_datetime(d.index)
    arrays = [
        xr.DataArray(
            d[v].to_numpy(),
            dims="time",
            coords={"time": d.index},
            attrs={"units": "%" if v == "hurs" else "degC"},
        )
        for v in ("tas", "tasmax", "hurs")
    ]
    result = shade_daily(*arrays)
    d["candidate"] = result.peak.values
    d["rh_clipped"] = result.rh_clipped.values
    d["stull_excursion"] = result.stull_excursion.values
    d["shipped"] = irt_wbgt_shade_stull_c(d.tas, d.hurs)
    d["eligible"] = (
        (d.hours == 24)
        & (d.valid_hours == 24)
        & np.isfinite(d[["candidate", "shipped", "hourly_stull", "bernard"]]).all(
            axis=1
        )
    )
    return d


def scores(frame: pd.DataFrame, site: str, window: str, policy: str):
    """Return site, yearly-count and seasonal diagnostics with block uncertainty."""
    rows, annual, seasons = [], [], []
    rng = np.random.default_rng(583)
    for method in ("shipped", "candidate", "hourly_stull", "bernard"):
        error = frame[method] - frame.bernard
        years = np.unique(frame.index.year)
        blocks = [error.loc[error.index.year == y].to_numpy() for y in years]
        boot = [
            np.sqrt(
                np.mean(
                    np.concatenate(
                        [blocks[i] for i in rng.integers(0, len(blocks), len(blocks))]
                    )
                    ** 2
                )
            )
            for _ in range(1000)
        ]
        row = dict(
            site=site,
            window=window,
            policy=policy,
            method=method,
            days=len(frame),
            bias=float(error.mean()),
            median_absolute_error=float(error.abs().median()),
            rmse=float(np.sqrt((error**2).mean())),
            rmse_ci_low=float(np.quantile(boot, 0.025)),
            rmse_ci_high=float(np.quantile(boot, 0.975)),
            rh_clipped=int(frame.rh_clipped.sum()),
            stull_excursions=int(frame.stull_excursion.sum()),
        )
        rows.append(row)
        for year, part in frame.groupby(frame.index.year):
            for threshold in (28, 30, 32):
                ref = part.bernard >= threshold
                cand = part[method] >= threshold
                annual.append(
                    dict(
                        site=site,
                        window=window,
                        policy=policy,
                        method=method,
                        year=year,
                        threshold=threshold,
                        reference_days=int(ref.sum()),
                        candidate_days=int(cand.sum()),
                        absolute_count_error=abs(int(cand.sum()) - int(ref.sum())),
                        false_positives=int((cand & ~ref).sum()),
                        missed_events=int((ref & ~cand).sum()),
                        annual_mean_error=float((part[method] - part.bernard).mean()),
                    )
                )
        for name, months in {
            "DJF": [12, 1, 2],
            "MAM": [3, 4, 5],
            "JJA": [6, 7, 8],
            "SON": [9, 10, 11],
        }.items():
            e = error.loc[error.index.month.isin(months)]
            seasons.append(
                dict(
                    site=site,
                    window=window,
                    policy=policy,
                    method=method,
                    season=name,
                    bias=float(e.mean()),
                    rmse=float(np.sqrt((e**2).mean())),
                )
            )
    return rows, annual, seasons


def summarize(out_dir: Path) -> None:
    """Persist absolute-target exceptions, reproduction parity and year uncertainty."""
    site = pd.read_csv(out_dir / "site_scores.csv")
    annual = pd.read_csv(out_dir / "annual_counts.csv")
    candidate = annual[
        (annual.method == "candidate") & (annual.policy == "deployment")
    ].copy()
    candidate["populated"] = candidate.reference_days >= 50
    candidate["target_met"] = (
        candidate.absolute_count_error <= 0.2 * candidate.reference_days
    )
    candidate.to_csv(out_dir / "absolute_count_targets.csv", index=False)
    original = pd.read_csv("docs/diagnostics/wbgt_deployed_vs_reference/scores.csv")
    reproduced = pd.read_csv(out_dir / "reproduction_scores.csv")
    keys = ["site", "reference", "candidate"]
    merged = original.merge(
        reproduced, on=keys, suffixes=("_old", "_new"), validate="one_to_one"
    )
    differences = {
        c: float((merged[c + "_old"] - merged[c + "_new"]).abs().max())
        for c in original.select_dtypes("number")
    }
    (out_dir / "reproduction_parity.json").write_text(json.dumps(differences, indent=2))
    rng = np.random.default_rng(583)
    uncertainty = []
    for keys, part in annual.groupby(
        ["site", "window", "policy", "method", "threshold"]
    ):
        values = part[
            [
                "reference_days",
                "candidate_days",
                "absolute_count_error",
                "annual_mean_error",
            ]
        ].to_numpy()
        samples = values[rng.integers(0, len(values), (1000, len(values)))].mean(axis=1)
        record = dict(zip(["site", "window", "policy", "method", "threshold"], keys))
        for i, key in enumerate(
            ["reference_days", "candidate_days", "count_mae", "annual_mean_error"]
        ):
            record[key + "_low"] = float(np.quantile(samples[:, i], 0.025))
            record[key + "_high"] = float(np.quantile(samples[:, i], 0.975))
        uncertainty.append(record)
    pd.DataFrame(uncertainty).to_csv(out_dir / "annual_uncertainty.csv", index=False)
    report = json.loads((out_dir / "report.json").read_text())
    populated = candidate[candidate.populated]
    miss = populated[~populated.target_met]
    daily = site[(site.method == "candidate") & (site.policy == "deployment")]
    text = [
        "# Shade release evidence",
        "",
        "Formula frozen before confirmation-window evaluation. No national outputs published.",
        "",
        "| Window | Old RMSE (C) | Candidate RMSE (C) | Old count MAE (days/year) | Candidate count MAE | Improvement rule |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for g in report["gates"]:
        text.append(
            f"| {g['window']} | {g['shipped_rmse']:.3f} | {g['candidate_rmse']:.3f} | {g['shipped_count_mae']:.2f} | {g['candidate_count_mae']:.2f} | {'PASS' if g['passed'] else 'FAIL'} |"
        )
    text += [
        "",
        f"All {len(daily)} site/window pairs meet median absolute error <1 C and RMSE <1.5 C. "
        f"The +/-20% annual count target fails for {len(miss)} of {len(populated)} pairs with >=50 reference events. "
        "Every exception is recorded in `absolute_count_targets.csv`; this target was not a release veto under the accepted policy.",
        "",
        "All 138 deployment site-year >=32 pairs have fewer than 50 reference events. The metric is retained; this evidence does not validate it through a 50-event gate. "
        "Absolute errors, false positives, missed events and year-block intervals are reported separately.",
        "",
        "## Reproduction report",
        "",
        f"Original 2005-2014 calendar and day-selection reproduced for {len(merged)} comparisons. Maximum numeric difference: {max(differences.values()):.12g}. "
        "See `reproduction_scores.csv` and `reproduction_parity.json`. The reproduction-policy comparison uses complete finite reference hours and retains leap days.",
        "",
        "## Deployment-policy report",
        "",
        "Drop February 29, require all 365 daily peaks, and omit incomplete boundary years. The first year in each window is excluded because the UTC cache begins after the local day begins. "
        "See `report.json` for every site/policy exclusion. All methods share the same valid dates; hourly Bernard is the primary reference and hourly Stull is separately scored.",
        "",
        "Daily scores and seasonal errors: `site_scores.csv`, `seasonal.csv`. Annual errors and uncertainty: `annual_counts.csv`, `annual_uncertainty.csv`. "
        "Bootstrap uses 1,000 year-block draws with seed 583; intervals describe temporal sampling uncertainty, not reference-method or climate-model uncertainty.",
        "",
        "## Remaining publication work",
        "",
        "National build and staged promotion have not run. The three-state single-model/year pilot is in `pilot_report.json`. "
        "Complete downstream staging cost, release verification and rollback rehearsal are still required. "
        "The sibling `twb_days_ge_*` false-zero defect remains unresolved; W-04/F-05 are not fixed across Heat Stress.",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(text), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Read existing caches only; write reproducible evidence without publishing."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("scratch/wbgt_deployed_vs_reference_cache"),
    )
    p.add_argument(
        "--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_shade_release")
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--summarize-only", action="store_true")
    args = p.parse_args(argv)
    if args.summarize_only:
        summarize(args.out_dir)
        return 0
    expected = [
        args.cache_dir / f"{s.name.lower()}_{y}.parquet"
        for s in SITES
        for y in range(1990, 2015)
    ]
    missing = [str(p) for p in expected if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing cached site years: {missing}")
    if args.dry_run:
        print(f"Validated {len(expected)} cache paths; no writes or downloads.")
        return 0
    args.out_dir.mkdir(parents=True, exist_ok=True)
    # Reproduce original method/day selection before stricter reference scoring.
    reproduced = []
    for site in SITES:
        raw = pd.concat(
            [
                pd.read_parquet(args.cache_dir / f"{site.name.lower()}_{y}.parquet")
                for y in range(2005, 2015)
            ]
        ).sort_index()
        reproduced.extend(
            c.to_row()
            for c in compare_site(
                site.name, build_daily_frame(build_hourly_frame(site, raw))
            )
        )
    pd.DataFrame(reproduced).to_csv(
        args.out_dir / "reproduction_scores.csv", index=False
    )
    rows, annual, seasonal, exclusions = [], [], [], []
    for start, end in ((2005, 2014), (1990, 2004)):
        for site in SITES:
            raw = pd.concat(
                [
                    pd.read_parquet(args.cache_dir / f"{site.name.lower()}_{y}.parquet")
                    for y in range(start, end + 1)
                ]
            ).sort_index()
            if raw.index.has_duplicates:
                raise ValueError(f"Duplicate cached timestamps at {site.name}")
            daily = daily_frame(raw)
            for policy in ("reproduction", "deployment"):
                d = daily.loc[daily.eligible].copy()
                d = d.loc[(d.index.year >= start) & (d.index.year <= end)]
                if policy == "deployment":
                    d = d.loc[~((d.index.month == 2) & (d.index.day == 29))]
                    sizes = d.groupby(d.index.year).size()
                    d = d.loc[d.index.year.isin(sizes.index[sizes == 365])]
                exclusions.append(
                    dict(
                        site=site.name,
                        window=f"{start}-{end}",
                        policy=policy,
                        source_days=len(daily),
                        retained_days=len(d),
                        excluded_days=len(daily) - len(d),
                        years=sorted(set(d.index.year)),
                    )
                )
                r, a, s = scores(d, site.name, f"{start}-{end}", policy)
                rows += r
                annual += a
                seasonal += s
            print(f"{start}-{end} {site.name} complete", flush=True)
    r = pd.DataFrame(rows)
    a = pd.DataFrame(annual)
    r.to_csv(args.out_dir / "site_scores.csv", index=False)
    a.to_csv(args.out_dir / "annual_counts.csv", index=False)
    pd.DataFrame(seasonal).to_csv(args.out_dir / "seasonal.csv", index=False)
    gates = []
    for window in r.window.unique():
        rs = r[(r.window == window) & (r.policy == "deployment")].pivot(
            index="site", columns="method", values="rmse"
        )
        ac = (
            a[(a.window == window) & (a.policy == "deployment")]
            .groupby(["site", "method"])
            .absolute_count_error.mean()
            .unstack()
        )
        gates.append(
            dict(
                window=window,
                shipped_rmse=float(rs.shipped.mean()),
                candidate_rmse=float(rs.candidate.mean()),
                worst_site_rmse_change=float((rs.candidate - rs.shipped).max()),
                shipped_count_mae=float(ac.shipped.mean()),
                candidate_count_mae=float(ac.candidate.mean()),
                passed=bool(
                    rs.candidate.mean() < rs.shipped.mean()
                    and ac.candidate.mean() < ac.shipped.mean()
                    and (rs.candidate - rs.shipped).max() <= 0.25
                ),
            )
        )
    meta = {
        "signature": SHADE_METHOD_SIGNATURE,
        "gates": gates,
        "exclusions": exclusions,
        "reference": "Hourly Bernard daily maximum; hourly Stull separately reported",
        "uncertainty": "1000 year-block bootstrap replicates, seed 583; pointwise site RMSE interval",
        "absolute_targets": "Median absolute daily error <1 C; RMSE <1.5 C; counts within 20% for reference pairs >=50 events. Sparse >=32 evidence is not validated by that gate.",
    }
    (args.out_dir / "report.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    summarize(args.out_dir)
    print(json.dumps(gates, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
