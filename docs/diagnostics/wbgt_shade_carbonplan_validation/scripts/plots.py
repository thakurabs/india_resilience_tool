"""Compact static plots of the held-out validation (CHG-0646). Run in the ``irt`` env after score.py.

Three figures, each with one panel per state x model (population weights):
  seasonal_bias.png   median monthly-climatology bias, raw vs held-out QDM (p10-p90 band)
  tail_errors.png     per-unit p90/p95/p99 bias, raw vs QDM
  count_errors.png    per-unit days>=28/30/32 error (d/yr), raw vs QDM; zero/zero units omitted
Raw = no WBGT-level QDM (NEX inputs are already BCSD-adjusted). Reference = UHE-Daily.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

RESULTS = Path(__file__).resolve().parents[1] / "results"
COLORS = {"raw": "#2a78d6", "qdm": "#eb6834"}
LABELS = {"raw": "raw (no WBGT QDM)", "qdm": "held-out QDM"}
INK, MUTED = "#222222", "#8a8a8a"


def runs() -> list[str]:
    return sorted(p.stem.split("scores__")[1] for p in (RESULTS / "units").glob("scores__*__population.csv")
                  if "H0" not in p.stem)


def grid(n: int, title: str):
    cols = min(n, 3)
    rows = -(-n // cols)
    fig, axes = plt.subplots(rows, cols, figsize=(4.2 * cols, 3.1 * rows), squeeze=False, sharey=True)
    fig.suptitle(title, color=INK, fontsize=11)
    for ax in axes.flat[n:]:
        ax.set_visible(False)
    return fig, list(axes.flat)


def style(ax, name: str) -> None:
    ax.set_title(name.replace("__population", "").replace("__", " · ").replace("_", " "), fontsize=9, color=INK)
    ax.axhline(0, color=MUTED, lw=1)
    ax.grid(axis="y", color="#e6e6e6", lw=.6)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(colors=INK, labelsize=8)


def seasonal(names: list[str]) -> None:
    fig, axes = grid(len(names), "Monthly bias vs UHE-Daily, held out 2000-2014\n(°C; median over units, p10-p90 band)")
    for ax, name in zip(axes, names):
        m = pd.read_csv(RESULTS / "units" / f"monthly_bias__{name}.csv")
        for kind in ("raw", "qdm"):
            k = m[m.series == kind]
            ax.fill_between(k.month, k.p10, k.p90, color=COLORS[kind], alpha=.15, lw=0)
            ax.plot(k.month, k.median_bias, color=COLORS[kind], lw=2, label=LABELS[kind])
        ax.set_xticks(range(1, 13), list("JFMAMJJASOND"))
        style(ax, name)
    axes[0].legend(fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(RESULTS / "seasonal_bias.png", dpi=130)


def boxes(names: list[str], metrics: list[str], fname: str, title: str, drop_zero: bool) -> None:
    fig, axes = grid(len(names), title)
    for ax, name in zip(axes, names):
        s = pd.read_csv(RESULTS / "units" / f"scores__{name}.csv", index_col=0)
        data, pos, cols = [], [], []
        for i, metric in enumerate(metrics):
            keep = s[metric + "__change"] != "zero/zero" if drop_zero else slice(None)
            for j, kind in enumerate(("raw", "qdm")):
                data.append(s.loc[keep, f"{metric}__{kind}"].dropna())
                pos.append(i * 3 + j)
                cols.append(COLORS[kind])
        bp = ax.boxplot(data, positions=pos, widths=.8, patch_artist=True, showfliers=True,
                        flierprops={"markersize": 2, "markeredgecolor": MUTED},
                        medianprops={"color": INK, "lw": 1.2})
        for patch, c in zip(bp["boxes"], cols):
            patch.set(facecolor=c, edgecolor=c, alpha=.7)
        ax.set_xticks([i * 3 + .5 for i in range(len(metrics))],
                      [m.replace("_bias", "").replace("days_ge_", "≥").replace("_error", "") for m in metrics])
        style(ax, name)
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[k], alpha=.7) for k in ("raw", "qdm")]
    axes[0].legend(handles, [LABELS[k] for k in ("raw", "qdm")], fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(RESULTS / fname, dpi=130)


def main() -> None:
    names = runs()
    seasonal(names)
    boxes(names, ["p90_bias", "p95_bias", "p99_bias"], "tail_errors.png",
          "Tail-quantile bias vs UHE-Daily, held out 2000-2014\n(°C per unit)", False)
    boxes(names, ["days_ge_28_error", "days_ge_30_error", "days_ge_32_error"], "count_errors.png",
          "Exceedance-day error vs UHE-Daily, held out 2000-2014\n(d/yr per unit; zero/zero omitted)", True)
    print("plots written to", RESULTS)


if __name__ == "__main__":
    main()
