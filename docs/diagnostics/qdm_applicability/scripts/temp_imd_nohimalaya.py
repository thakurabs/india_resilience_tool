"""Verdicts from qdm_temp_imd.py unit rows with Himalayan boxes (lat >= 30) excluded, plus the
IMD train-vs-test shift that the 1990-2002 -> 2003-2010 holdout cannot carry (annual P, days >= 35 C)."""
import sys
import numpy as np, pandas as pd
sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts"); sys.path.insert(0, ".")
import qdm_temp_imd as T  # noqa: E402
from qdm_pr_imd import classify  # noqa: E402

u = pd.read_csv(T.OUT / "temperature_imd_unit_rows.csv")
s = u[u.lat < 30].groupby("metric").apply(classify)
s.round(3).to_csv(T.OUT / "summary_temp_imd_no_himalaya.csv")
print(s[["ref", "err_raw", "err_qdm", "shift_2050", "n_units", "verdict"]].round(2).sort_values("verdict").to_string())

boxes = [tuple(x) for x in pd.read_csv(T.SER / "boxes.csv").itertuples(index=False)]
refs = T.imd_refs(boxes)
r = pd.concat([v.assign(unit=f"{k[0]:.1f}_{k[1]:.1f}") for k, v in refs.items()])
r = r[r.unit.isin(u.unit.unique())]
ann = r.groupby(["unit", "year"]).agg(P=("pr", "sum"), d35=("tasmax", lambda s: (s >= 35).sum()))
ann = ann.reset_index(); ann["period"] = np.where(ann.year <= 2002, "train 1990-2002", "test 2003-2010")
print("\nIMD on the scored boxes:\n", ann.groupby("period")[["P", "d35"]].mean().round(1))
p = pd.read_csv(T.OUT / "aridity_parts.csv")
print("\nAnnual P, holdout years: IMD %.0f  raw %.0f  QDM %.0f mm" % (p.imd_P.mean(), p.raw_P.mean(), p.qdm_P.mean()))
