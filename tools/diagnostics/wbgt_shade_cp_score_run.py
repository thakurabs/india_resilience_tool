"""Score staged CarbonPlan shade holdouts against deployed shade and UHE.

The default command reports inputs. ``--run`` writes an isolated evidence
directory and refuses a model whose staged holdout is not fingerprinted PASS.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

from india_resilience_tool.utils.naming import alias
from tools.diagnostics.wbgt_route_b import annual_metrics, fixed_aggregate
from tools.diagnostics.wbgt_route_b_prepare import ENSEMBLE_MODELS, verify_support
from tools.diagnostics.wbgt_shade_cp_release_score import METRICS, YEARS, score_model
from tools.pipeline.build_carbonplan_shade_release import METHOD, checked_support, digest

SLUGS = {
    "mean_c": "wbgt_shade_stull_annual_mean",
    "days_ge_28": "wbgt_shade_stull_days_ge_28",
    "days_ge_30": "wbgt_shade_stull_days_ge_30",
    "days_ge_32": "wbgt_shade_stull_days_ge_32",
}


def unit_keys(units: pd.DataFrame) -> pd.Series:
    """Build the optimized bundle's state-aware district or block keys."""
    parts = []
    for row in units.itertuples():
        key = f"{alias(row.state_name)}|{alias(row.district_name)}"
        if row.level == "block":
            key += f"|{alias(row.block_name)}"
        parts.append(key)
    return pd.Series(parts, index=units.index)


def annual_matrix(frame: pd.DataFrame, metric: str, units: pd.DataFrame,
                  *, value_column: str = "value") -> np.ndarray:
    """Return exact year × unit order, leaving absent observations as NaN."""
    if frame.duplicated(["unit_index", "year"]).any():
        raise ValueError(f"Duplicate {metric} unit-year")
    table = frame.pivot(index="year", columns="unit_index", values=value_column)
    return table.reindex(index=YEARS, columns=units.unit_index).to_numpy(dtype=float)


def deployed_matrices(bundle: Path, model: str, units: pd.DataFrame
                      ) -> tuple[dict[str, np.ndarray], dict[str, str]]:
    """Read published original-formula yearly model Parquets by exact admin key."""
    keys = unit_keys(units)
    if keys.duplicated().any():
        raise ValueError("Ambiguous normalized admin identity")
    keyed = units[["unit_index", "state_name", "level"]].copy()
    keyed["key"] = keys
    matrices: dict[str, np.ndarray] = {}
    hashes: dict[str, str] = {}
    for metric, slug in SLUGS.items():
        pieces = []
        for (state, level), subset in keyed.groupby(["state_name", "level"], sort=True):
            path = bundle / "metrics" / slug / "yearly_models" / "admin" / level / f"state={state}.parquet"
            if not path.is_file():
                continue
            hashes[str(path.relative_to(bundle))] = digest(path)
            key_column = f"{level}_key"
            source = pd.read_parquet(path, columns=["year", "value", "scenario", "model", key_column])
            source = source.loc[(source.model == model) & (source.scenario == "historical") &
                                source.year.isin(YEARS)].copy()
            if source.empty:
                continue
            source["key"] = source[key_column].astype(str)
            matched = source.merge(subset[["unit_index", "key"]], on="key", how="inner",
                                   validate="many_to_one")
            pieces.append(matched[["unit_index", "year", "value"]])
        if not pieces:
            raise ValueError(f"No deployed model-years for {model}/{slug}")
        matrices[metric] = annual_matrix(pd.concat(pieces, ignore_index=True), metric, units)
    return matrices, hashes


def reference_matrices(route_b: Path, units: pd.DataFrame, cells_ok: np.ndarray,
                       units_ok: np.ndarray, weights: np.ndarray,
                       signature: str) -> dict[str, np.ndarray]:
    """Threshold UHE daily shade per eligible cell, then area-weight cell-years."""
    arrays = {metric: np.full((len(YEARS), len(units)), np.nan) for metric in METRICS}
    for row, year in enumerate(YEARS):
        path = route_b / "reference" / f"{year}.npz"
        with np.load(path, allow_pickle=False) as source:
            if str(source["signature"]) != signature:
                raise ValueError(f"UHE support signature mismatch: {path}")
            dates = pd.DatetimeIndex(source["dates"].tolist())
            daily = np.asarray(source["values"][:, cells_ok], dtype=float)
        annual = annual_metrics(dates, daily)
        if not np.array_equal(annual["years"], [year]):
            raise ValueError(f"Incomplete UHE year: {year}")
        for metric in METRICS:
            arrays[metric][row, units_ok] = fixed_aggregate(annual[metric], weights)[0]
    return arrays


def score_one(route_b: Path, stage: Path, bundle: Path, model: str,
              units: pd.DataFrame, areas: sparse.spmatrix, signature: str
              ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Validate stage and score one common model with full input provenance."""
    staged = stage / model
    status = json.loads((staged / "status.json").read_text())
    if status.get("status") != "PASS":
        raise ValueError(f"{model}: no complete staged shade result")
    if (status.get("identity", {}).get("method") != METHOD or
        status.get("holdout_training_years") != [1985, 1999] or
        status.get("holdout_application_years") != [2000, 2014]):
        raise ValueError(f"{model}: staged holdout method or split differs")
    name = "holdout_yearly_metrics.csv.gz"
    if digest(staged / name) != status["outputs"][name]:
        raise ValueError(f"{model}: staged holdout hash changed")
    route_manifest = json.loads((route_b / "assembly" / model / "run_manifest.json").read_text())
    if status["identity"]["route_b_manifest_sha256"] != digest(route_b / "assembly" / model / "run_manifest.json"):
        raise ValueError(f"{model}: Route B provenance changed")
    cells_ok, units_ok, weights = checked_support(route_b, model, units, areas, route_manifest)
    held = pd.read_csv(staged / name)
    if set(held.scenario) != {"historical"} or set(held.year) != set(range(2000, 2015)):
        raise ValueError(f"{model}: holdout is not the 2000–2014 application")
    holdout = {metric: annual_matrix(held.loc[held.year.isin(YEARS)], metric,
                                    units, value_column=metric) for metric in METRICS}
    incumbent, hashes = deployed_matrices(bundle, model, units)
    reference = reference_matrices(route_b, units, cells_ok, units_ok, weights, signature)
    scored = score_model(model, units, holdout, incumbent, reference)
    evidence = {"model": model, "stage_status_sha256": digest(staged / "status.json"),
                "route_b_manifest_sha256": digest(route_b / "assembly" / model / "run_manifest.json"),
                "deployed_yearly_sha256": hashes,
                "cells_eligible": int(cells_ok.sum()), "units_eligible": int(units_ok.sum()),
                "units_common_by_metric": {metric: int((scored.units.metric.eq(metric) &
                    scored.units.classification.ne("missing")).sum()) for metric in METRICS}}
    return scored.units, scored.cuts, evidence


def main(argv: list[str] | None = None) -> int:
    """Plan or run the frozen out-of-sample annual holdout comparison."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route-b-root", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--stage", type=Path, default=Path("scratch/wbgt_shade_carbonplan_release"))
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--model", action="append", choices=ENSEMBLE_MODELS)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args(argv)
    models = tuple(dict.fromkeys(args.model or ENSEMBLE_MODELS))
    print(json.dumps({"models": models, "stage": str(args.stage.resolve()),
                      "bundle": str(args.bundle.resolve()), "evidence": str(args.evidence.resolve()),
                      "writes": args.run}, indent=2), flush=True)
    if not args.run:
        return 0
    if args.evidence.resolve().is_relative_to(args.bundle.resolve()):
        parser.error("Evidence must be outside the deployed bundle")
    support = verify_support(args.route_b_root)
    units = pd.read_csv(args.route_b_root / "units.csv").fillna({"block_name": ""})
    areas = sparse.load_npz(args.route_b_root / "admin_areas.npz").tocsr()
    out = args.evidence.resolve()
    out.mkdir(parents=True, exist_ok=True)
    summaries = []
    for model in models:
        unit_scores, cuts, provenance = score_one(args.route_b_root, args.stage,
            args.bundle, model, units, areas, support["signature_sha256"])
        model_dir = out / model
        model_dir.mkdir(exist_ok=True)
        unit_scores.to_parquet(model_dir / "unit_scores.parquet", index=False)
        cuts.to_csv(model_dir / "cuts.csv", index=False)
        (model_dir / "provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
        summaries.append(cuts)
        print(f"{model}: {int(cuts.verdict.eq('worsened').sum())} worsened cuts", flush=True)
    all_cuts = pd.concat(summaries, ignore_index=True)
    all_cuts.to_csv(out / "all_cuts.csv", index=False)
    verdict = "BLOCK_USER_RULING" if all_cuts.verdict.eq("worsened").any() else "PASS"
    (out / "gate.json").write_text(json.dumps({"verdict": verdict,
        "models": models, "worsened_cuts": int(all_cuts.verdict.eq("worsened").sum()),
        "years": [min(YEARS), max(YEARS)], "method": "cache_holdout_vs_deployed_original",
        "reference": "UHE_grid_first_cell_counts"}, indent=2), encoding="utf-8")
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
