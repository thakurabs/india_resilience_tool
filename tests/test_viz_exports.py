"""
Unit tests for viz.exports (fast, no big data).

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import pandas as pd

from india_resilience_tool.viz.exports import (
    make_case_study_zip,
    make_district_case_study_pdf,
    make_district_yearly_pdf,
)

import zipfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import pandas as pd

from india_resilience_tool.viz.exports import (
    make_case_study_zip,
    make_district_case_study_pdf,
    make_district_yearly_pdf,
)


def test_make_district_case_study_pdf_bytes_nonempty() -> None:
    summary = pd.DataFrame(
        [
            {
                "index_slug": "m",
                "index_label": "Metric",
                "group": "Temperature",
                "current": 10.0,
                "baseline": 9.0,
                "delta_abs": 1.0,
                "delta_pct": 11.11,
                "rank_in_state": 1,
                "percentile_in_state": 90.0,
                "risk_class": "Very High",
            }
        ]
    )

    ts_dict = {
        "m": {
            "historical": pd.DataFrame({"year": [1990, 1991], "mean": [1.0, 1.2], "p05": [0.8, 1.0], "p95": [1.2, 1.4]}),
            "scenario": pd.DataFrame({"year": [2020, 2021], "mean": [2.0, 2.2], "p05": [1.8, 2.0], "p95": [2.2, 2.4]}),
        }
    }

    panel = pd.DataFrame(
        {"scenario": ["historical", "ssp245"], "period": ["1990-2010", "2020-2040"], "value": [10.0, 12.0]}
    )
    pdf_bytes = make_district_case_study_pdf(
        state_name="Telangana",
        district_name="Alpha",
        summary_df=summary,
        ts_dict=ts_dict,
        panel_dict={"m": panel},
        sel_scenario="ssp585",
        sel_period="2020-2040",
        sel_stat="mean",
    )
    assert isinstance(pdf_bytes, (bytes, bytearray))
    assert len(pdf_bytes) > 1000


def test_make_case_study_zip_contains_files() -> None:
    summary = pd.DataFrame([{"index_slug": "m", "index_label": "Metric"}])
    ts_dict = {"m": {"historical": pd.DataFrame({"year": [1990], "mean": [1.0]})}}
    panel_dict = {"m": pd.DataFrame({"scenario": ["historical"], "period": ["1990-2010"], "value": [10.0]})}
    pdf_bytes = b"%PDF-1.4 dummy"

    zbytes = make_case_study_zip(
        state_name="Telangana",
        district_name="Alpha",
        summary_df=summary,
        ts_dict=ts_dict,
        panel_dict=panel_dict,
        pdf_bytes=pdf_bytes,
        index_label_lookup={"m": "Metric"},
    )
    assert len(zbytes) > 50

    with zipfile.ZipFile(io.BytesIO(zbytes), "r") as zf:  # type: ignore[name-defined]
        names = set(zf.namelist())
        assert "summary.csv" in names
        assert any(n.startswith("timeseries_") and n.endswith(".csv") for n in names)
        assert any(n.startswith("scenario_mean_") and n.endswith(".csv") for n in names)
        assert any(n.startswith("climate_profile_") and n.endswith(".pdf") for n in names)


def test_make_district_yearly_pdf_writes_file(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "state": ["Telangana", "Telangana"],
            "district": ["Alpha", "Alpha"],
            "scenario": ["ssp585", "ssp585"],
            "year": [2020, 2021],
            "mean": [2.0, 2.2],
            "p05": [1.8, 2.0],
            "p95": [2.2, 2.4],
        }
    )

    out = make_district_yearly_pdf(
        df_yearly=df,
        state_name="Telangana",
        district_name="Alpha",
        scenario_name="ssp585",
        metric_label="Metric",
        out_dir=tmp_path,
    )
    assert out is not None
    assert out.exists()
    assert out.suffix.lower() == ".pdf"
