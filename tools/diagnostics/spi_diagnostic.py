#!/usr/bin/env python3
"""
SPI Diagnostic Analysis Script

Analyzes SPI output values to verify they make scientific sense.

Expected SPI characteristics:
- Mean ≈ 0 for the calibration period (historical)
- Std ≈ 1 for the calibration period
- Values typically range from -3 to +3
- Future scenarios may show shifts if climate is changing

Author: Abu Bakar Siddiqui Thakur
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

def _resolve_processed_root(arg: str | None) -> Path:
    """
    Resolve the processed root directory.

    Priority:
      1) --processed-root
      2) IRT_PROCESSED_ROOT
      3) paths.BASE_OUTPUT_ROOT (DATA_DIR/processed)
    """
    if arg:
        return Path(arg).expanduser().resolve()

    env = os.getenv("IRT_PROCESSED_ROOT")
    if env:
        return Path(env).expanduser().resolve()

    try:
        from paths import BASE_OUTPUT_ROOT  # repo-root canonical
        return Path(BASE_OUTPUT_ROOT).expanduser().resolve()
    except Exception:
        return Path(".").resolve()


def _resolve_metric_root(processed_root: Path, metric_slug: str) -> Path:
    """
    Allow either:
      - processed_root = <...>/processed
      - processed_root = <...>/processed/<metric_slug>
    """
    metric_slug = str(metric_slug).strip()
    if not metric_slug:
        return processed_root

    if processed_root.name == metric_slug:
        return processed_root

    candidate = processed_root / metric_slug
    return candidate if candidate.exists() else candidate


def load_district_yearly(base_path: Path, state: str, metric: str, district: str, model: str, scenario: str) -> pd.DataFrame:
    """Load yearly CSV for a district/model/scenario combination."""
    district_safe = district.replace(" ", "_").replace("/", "_")
    csv_path = base_path / metric / state / "districts" / district_safe / model / scenario / f"{district_safe}_yearly.csv"
    
    if not csv_path.exists():
        print(f"  File not found: {csv_path}")
        return pd.DataFrame()
    
    df = pd.read_csv(csv_path)
    df["scenario"] = scenario
    return df


def analyze_spi_values(df: pd.DataFrame, scenario: str, baseline_years: tuple[int, int] = (1981, 2010)) -> dict:
    """Compute diagnostic statistics for SPI values."""
    if df.empty or "value" not in df.columns:
        return {"error": "No data"}
    
    values = pd.to_numeric(df["value"], errors="coerce").dropna()
    years = pd.to_numeric(df["year"], errors="coerce")
    
    # Overall stats
    stats = {
        "scenario": scenario,
        "n_years": len(values),
        "year_range": f"{years.min()}-{years.max()}",
        "mean": values.mean(),
        "std": values.std(),
        "min": values.min(),
        "max": values.max(),
        "median": values.median(),
        "pct_below_minus1": (values < -1).mean() * 100,  # Moderate drought
        "pct_below_minus2": (values < -2).mean() * 100,  # Severe drought
        "pct_above_plus1": (values > 1).mean() * 100,    # Moderately wet
        "pct_above_plus2": (values > 2).mean() * 100,    # Severely wet
    }
    
    # For historical, compute stats specifically for baseline period
    if scenario == "historical":
        baseline_mask = (years >= baseline_years[0]) & (years <= baseline_years[1])
        baseline_values = values[baseline_mask]
        if len(baseline_values) > 0:
            stats["baseline_mean"] = baseline_values.mean()
            stats["baseline_std"] = baseline_values.std()
            stats["baseline_n_years"] = len(baseline_values)
    
    return stats


def print_diagnostic_report(district: str, stats_by_scenario: dict):
    """Print a formatted diagnostic report."""
    print("\n" + "=" * 70)
    print(f"SPI DIAGNOSTIC REPORT: {district}")
    print("=" * 70)
    
    for scenario, stats in stats_by_scenario.items():
        print(f"\n{'─' * 35}")
        print(f"Scenario: {scenario.upper()}")
        print(f"{'─' * 35}")
        
        if "error" in stats:
            print(f"  ERROR: {stats['error']}")
            continue
        
        print(f"  Years: {stats['year_range']} (n={stats['n_years']})")
        print(f"  Mean:   {stats['mean']:+.3f}  (expected ≈ 0 for baseline)")
        print(f"  Std:    {stats['std']:.3f}   (expected ≈ 1 for baseline)")
        print(f"  Min:    {stats['min']:+.3f}")
        print(f"  Max:    {stats['max']:+.3f}")
        print(f"  Median: {stats['median']:+.3f}")
        print()
        print(f"  Drought frequency:")
        print(f"    Moderate (SPI < -1): {stats['pct_below_minus1']:.1f}%  (expected ~16%)")
        print(f"    Severe   (SPI < -2): {stats['pct_below_minus2']:.1f}%  (expected ~2%)")
        print(f"  Wet frequency:")
        print(f"    Moderate (SPI > +1): {stats['pct_above_plus1']:.1f}%  (expected ~16%)")
        print(f"    Severe   (SPI > +2): {stats['pct_above_plus2']:.1f}%  (expected ~2%)")
        
        if "baseline_mean" in stats:
            print()
            print(f"  Baseline period ({stats.get('baseline_n_years', 'N/A')} years):")
            print(f"    Mean: {stats['baseline_mean']:+.3f}")
            print(f"    Std:  {stats['baseline_std']:.3f}")


def check_scientific_validity(stats_by_scenario: dict) -> list:
    """Check for potential issues in the SPI values."""
    issues = []
    
    hist_stats = stats_by_scenario.get("historical", {})
    
    # Check 1: Historical baseline mean should be close to 0
    if "baseline_mean" in hist_stats:
        if abs(hist_stats["baseline_mean"]) > 0.3:
            issues.append(f"WARN: Historical baseline mean ({hist_stats['baseline_mean']:.3f}) deviates significantly from 0")
    
    # Check 2: Historical baseline std should be close to 1
    if "baseline_std" in hist_stats:
        if abs(hist_stats["baseline_std"] - 1.0) > 0.3:
            issues.append(f"WARN: Historical baseline std ({hist_stats['baseline_std']:.3f}) deviates significantly from 1")
    
    # Check 3: Values should be in reasonable range
    for scenario, stats in stats_by_scenario.items():
        if "min" in stats and stats["min"] < -4:
            issues.append(f"WARN: {scenario}: Extremely low SPI values ({stats['min']:.2f}) - check data quality")
        if "max" in stats and stats["max"] > 4:
            issues.append(f"WARN: {scenario}: Extremely high SPI values ({stats['max']:.2f}) - check data quality")
    
    # Check 4: Compare historical vs SSP
    ssp_stats = stats_by_scenario.get("ssp245", {})
    if "mean" in hist_stats and "mean" in ssp_stats:
        mean_diff = ssp_stats["mean"] - hist_stats["mean"]
        if abs(mean_diff) > 0.5:
            direction = "drier" if mean_diff < 0 else "wetter"
            issues.append(f"INFO: SSP245 shows {direction} conditions (mean shift: {mean_diff:+.3f})")
    
    return issues


def plot_timeseries(
    dfs: dict,
    district: str,
    *,
    model: str,
    baseline_years: tuple[int, int],
    output_path: Path | None = None,
) -> None:
    """Plot SPI timeseries for visual inspection."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n(matplotlib not available - skipping plot)")
        return
    
    fig, ax = plt.subplots(figsize=(14, 5))
    
    colors = {"historical": "blue", "ssp245": "red", "ssp585": "darkred"}
    
    for scenario, df in dfs.items():
        if df.empty:
            continue
        color = colors.get(scenario, "gray")
        ax.plot(df["year"], df["value"], label=scenario, color=color, alpha=0.8, linewidth=1)
        ax.scatter(df["year"], df["value"], color=color, s=15, alpha=0.5)
    
    # Add reference lines
    ax.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    ax.axhline(y=-1, color="orange", linestyle="--", linewidth=0.5, label="Moderate drought (-1)")
    ax.axhline(y=-2, color="red", linestyle="--", linewidth=0.5, label="Severe drought (-2)")
    ax.axhline(y=1, color="lightblue", linestyle="--", linewidth=0.5)
    ax.axhline(y=2, color="blue", linestyle="--", linewidth=0.5)
    
    # Add baseline period shading
    ax.axvspan(baseline_years[0], baseline_years[1], alpha=0.1, color="green", label="Baseline period")
    
    ax.set_xlabel("Year")
    ax.set_ylabel("SPI-3")
    ax.set_title(f"SPI Time Series: {district} ({model})")
    ax.legend(loc="upper left", fontsize=8)
    ax.set_ylim(-4, 4)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"\nPlot saved to: {output_path}")
    else:
        plt.show()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SPI diagnostic checks for IRT processed outputs.")
    p.add_argument("--processed-root", default=None, help="Path to DATA_DIR/processed (or to processed/<metric_slug>).")
    p.add_argument("--state", default=os.getenv("IRT_PILOT_STATE", "Telangana"), help="State folder name.")
    p.add_argument("--metric-slug", default="spi6_drought_index", help="SPI metric slug under processed root.")
    p.add_argument("--model", default="CanESM5", help="Climate model folder name.")
    p.add_argument("--scenarios", nargs="+", default=["historical", "ssp245"], help="Scenarios to analyze.")
    p.add_argument("--district", default=None, help="District folder name to analyze (default: first available).")
    p.add_argument("--baseline-start", type=int, default=1981, help="Baseline start year (historical).")
    p.add_argument("--baseline-end", type=int, default=2010, help="Baseline end year (historical).")
    p.add_argument("--plot", action="store_true", help="Plot the SPI time series (requires matplotlib).")
    p.add_argument("--plot-out", default=None, help="If set, save plot to this path instead of showing it.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    processed_root = _resolve_processed_root(args.processed_root)
    metric_root = _resolve_metric_root(processed_root, args.metric_slug)
    baseline_years = (int(args.baseline_start), int(args.baseline_end))

    print("SPI Diagnostic Analysis")
    print(f"Metric: {args.metric_slug}")
    print(f"Model: {args.model}")
    print(f"State: {args.state}")
    print(f"Processed root: {processed_root}")
    print(f"Metric root: {metric_root}")
    
    # Find available districts
    districts_path = metric_root / str(args.state) / "districts"
    if not districts_path.exists():
        print(f"\nERROR: Districts path not found: {districts_path}")
        print("Pass --processed-root (or set IRT_PROCESSED_ROOT / IRT_DATA_DIR).")
        sys.exit(1)
    
    available_districts = [d.name for d in districts_path.iterdir() if d.is_dir()]
    print(f"\nFound {len(available_districts)} districts")
    
    if not available_districts:
        print("No districts found!")
        sys.exit(1)
    
    # Analyze selected district (or first available)
    if args.district:
        district = str(args.district)
    else:
        district = available_districts[0].replace("_", " ")
    print(f"\nAnalyzing district: {district}")
    
    # Load data for each scenario
    dfs = {}
    stats_by_scenario = {}
    
    for scenario in args.scenarios:
        df = load_district_yearly(metric_root.parent, str(args.state), str(args.metric_slug), district, str(args.model), str(scenario))
        dfs[scenario] = df
        stats_by_scenario[scenario] = analyze_spi_values(df, str(scenario), baseline_years=baseline_years)
    
    # Print diagnostic report
    print_diagnostic_report(district, stats_by_scenario)
    
    # Check for issues
    issues = check_scientific_validity(stats_by_scenario)
    if issues:
        print("\n" + "=" * 70)
        print("VALIDATION CHECKS")
        print("=" * 70)
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\nOK: All validation checks passed!")
    
    # Print sample data
    print("\n" + "=" * 70)
    print("SAMPLE DATA (first 10 years per scenario)")
    print("=" * 70)
    for scenario, df in dfs.items():
        if not df.empty:
            print(f"\n{scenario}:")
            print(df[["year", "value"]].head(10).to_string(index=False))
    
    # Try to plot
    if args.plot:
        outp = Path(args.plot_out).expanduser().resolve() if args.plot_out else None
        plot_timeseries(dfs, district, model=str(args.model), baseline_years=baseline_years, output_path=outp)


if __name__ == "__main__":
    main()
