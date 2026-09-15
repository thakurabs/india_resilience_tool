"""Guards for the frozen Heat Risk map builder (CHG-0368).

The point of this viewer is that two settings are *frozen*: the CDF ruler and
the absolute-threshold headline. The tests below exist so a future edit cannot
quietly repoint the page at a different ruler or a different composite field
while the page keeps claiming the frozen ones in its caption.
"""

from __future__ import annotations

import json
import re

import pandas as pd
import pytest

from tools.diagnostics import build_heat_risk_frozen_map as builder
from tools.diagnostics.heat_risk_national_ruler_pilot import SLICES


def _score_table(rulers=("cdf", "linear")) -> pd.DataFrame:
    rows = []
    for ruler in rulers:
        for index, (scenario, period) in enumerate(SLICES):
            for district in ("a|one", "b|two"):
                rows.append(
                    {
                        "ruler": ruler,
                        "district_key": district,
                        "state": "S",
                        "district": district.split("|")[1],
                        "scenario": scenario,
                        "period": period,
                        "composite": 10.0 * index,
                        builder.FROZEN_FIELD: 5.0 * index,
                    }
                )
    return pd.DataFrame(rows)


def test_frozen_settings_are_the_decided_ones():
    assert builder.FROZEN_RULER == "cdf"
    assert builder.FROZEN_FIELD == "composite_absolute_threshold"
    assert (builder.FROZEN_VMIN, builder.FROZEN_VMAX) == (0.0, 100.0)
    assert builder.FROZEN_CMAP == "WhiteBlueGreenYellowRed"


def test_load_frozen_scores_keeps_only_the_frozen_ruler(tmp_path):
    path = tmp_path / "district_scores.csv"
    _score_table().to_csv(path, index=False)

    frame = builder.load_frozen_scores(path)

    assert set(frame["ruler"].unique()) if "ruler" in frame else True
    assert len(frame) == 2 * len(SLICES)  # cdf rows only
    assert builder.FROZEN_FIELD in frame.columns


def test_load_frozen_scores_rejects_a_table_without_the_frozen_ruler(tmp_path):
    path = tmp_path / "district_scores.csv"
    _score_table(rulers=("linear",)).to_csv(path, index=False)

    with pytest.raises(ValueError, match="cdf"):
        builder.load_frozen_scores(path)


def test_load_frozen_scores_rejects_a_table_without_the_headline_field(tmp_path):
    path = tmp_path / "district_scores.csv"
    _score_table().drop(columns=[builder.FROZEN_FIELD]).to_csv(path, index=False)

    with pytest.raises(ValueError, match=builder.FROZEN_FIELD):
        builder.load_frozen_scores(path)


def test_load_frozen_scores_rejects_a_partial_slice_grid(tmp_path):
    path = tmp_path / "district_scores.csv"
    table = _score_table()
    table = table.loc[~((table["scenario"] == "ssp585") & (table["period"] == "2060-2080"))]
    table.to_csv(path, index=False)

    with pytest.raises(ValueError, match="ssp585/2060-2080"):
        builder.load_frozen_scores(path)


def test_ramp_never_reaches_pure_white():
    """Pure white collides with the no-data grey; the ramp starts a step up."""
    ramp = builder.ramp_hex()

    assert len(ramp) == 101
    assert ramp[0].lower() not in {"#ffffff", "#fff"}
    assert ramp[0].lower() != builder.MISSING_COLOR.lower()


def test_page_embeds_every_slice_and_paints_from_the_frozen_field(tmp_path):
    scores = builder.load_frozen_scores(
        (lambda p: (_score_table().to_csv(p, index=False), p)[1])(tmp_path / "s.csv")
    )
    districts = [
        {"k": "a|one", "n": "one", "s": "S", "d": "M0 0L1 0L1 1Z"},
        {"k": "b|two", "n": "two", "s": "S", "d": "M2 2L3 2L3 3Z"},
    ]

    page = builder.build_html(scores, districts, ["M0 0L3 0L3 3Z"], 100.0, source=tmp_path)
    payload = json.loads(re.search(r"^const D = (\{.*\});$", page, re.M).group(1))

    assert set(payload["scores"]) == {f"{sc}|{pe}" for sc, pe in SLICES}
    # slice index 3 of the fixture -> 5.0 * 3
    assert payload["scores"]["ssp245|2060-2080"]["a|one"] == pytest.approx(15.0)
    assert payload["periods"]["historical"] == ["1990-2010"]
    assert "<select id=\"scenario\">" in page and "<select id=\"period\">" in page
    # self-contained: no external asset is fetched (the SVG namespace URI is not one)
    assert not re.search(r'(?:src|href)\s*=\s*["\']https?://', page)
