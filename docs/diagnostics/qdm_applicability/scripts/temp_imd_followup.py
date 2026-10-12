"""Follow-ups to qdm_temp_imd.py: (1) aridity split into annual P and annual PET, holdout 2003-2010
and ssp245 change; (2) how much of each metric's error reduction comes from Himalayan boxes (lat >= 30)."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts"); sys.path.insert(0, ".")
import qdm_applicability as Q  # noqa: E402
import qdm_temp_imd as T  # noqa: E402

OUT = T.OUT


def parts(e, lat):
    ra = T.extraterrestrial_radiation_mm(np.full(len(e), lat), e.doy.values)
    pet = T.hargreaves_pet_mm_per_day(e.tasmax.values, e.tasmin.values, ra)
    d = pd.DataFrame({"year": e.year.values, "pr": e.pr.values, "pet": pet}).groupby("year").sum()
    return d.pr.mean(), d.pet.mean()


def main():
    boxes = [tuple(x) for x in pd.read_csv(T.SER / "boxes.csv").itertuples(index=False)]
    refs = T.imd_refs(boxes)
    rows = []
    for model in T.MODELS:
        H = {v: pd.read_parquet(T.SER / f"{model}_historical_{v}.parquet") for v in ("tasmax", "tasmin", "pr")}
        F = {v: pd.read_parquet(T.SER / f"{model}_ssp245_{v}.parquet") for v in ("tasmax", "tasmin", "pr")}
        for la, lo in boxes:
            key = f"{la:.1f}_{lo:.1f}"; ref = refs[(la, lo)]
            if ref[["tasmax", "tasmin", "pr"]].isna().mean().max() > 0.01:
                continue
            fr = lambda D: Q._finish(pd.DataFrame({"year": D["tasmax"].year, "month": D["tasmax"].month, "day": D["tasmax"].day,
                                                   **{v: D[v][key].values for v in ("tasmax", "tasmin", "pr")}}))
            hist, fut = fr(H), fr(F)
            if hist[["tasmax", "tasmin"]].isna().any().any():
                continue
            tr_r, tr_h = ref[ref.year.isin(Q.TRAIN)], hist[hist.year.isin(Q.TRAIN)]
            hold = T.corrector(tr_r, tr_h, hist); qh = T.corrector(ref, hist, hist); qf = T.corrector(ref, hist, fut)
            te = lambda x: x[x.year.isin(Q.TEST)]
            (rp, re_), (hp, he), (qp, qe) = parts(te(ref), la), parts(te(hist), la), parts(te(hold), la)
            (h0p, h0e), (f0p, f0e), (q0p, q0e), (q1p, q1e) = parts(hist, la), parts(fut, la), parts(qh, la), parts(qf, la)
            rows.append(dict(unit=key, model=model, imd_P=rp, imd_PET=re_, raw_P=hp, raw_PET=he, qdm_P=qp, qdm_PET=qe,
                             dP_raw=100 * (f0p / h0p - 1), dPET_raw=100 * (f0e / h0e - 1),
                             dP_qdm=100 * (q1p / q0p - 1), dPET_qdm=100 * (q1e / q0e - 1)))
        print("done", model, flush=True)
    d = pd.DataFrame(rows); d.to_csv(OUT / "aridity_parts.csv", index=False)
    print(d.drop(columns=["unit", "model"]).mean().round(1).to_string())

    u = pd.read_csv(OUT / "temperature_imd_unit_rows.csv")
    u["gain"] = (u.raw_test - u.ref_test).abs() - (u.qdm_test - u.ref_test).abs()
    u["himalaya"] = u.lat >= 30
    s = u.groupby(["metric", "himalaya"]).gain.sum().unstack()
    share = (s[True] / s.sum(axis=1)).rename("himalaya_share_of_gain")
    nb = u.groupby("himalaya").unit.nunique()
    print(f"\nboxes: {nb.get(True, 0)} Himalayan (lat>=30) of {nb.sum()}")
    print(share.round(2).sort_values(ascending=False).to_string())
    share.round(3).to_csv(OUT / "himalaya_share_of_gain.csv")


if __name__ == "__main__":
    main()
