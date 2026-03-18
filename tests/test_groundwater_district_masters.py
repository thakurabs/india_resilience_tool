from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile
from xml.sax.saxutils import escape

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from tools.geodata.build_groundwater_district_masters import (
    GROUNDWATER_EXTRACTABLE_RESOURCE_COL,
    GROUNDWATER_FUTURE_AVAILABILITY_COL,
    GROUNDWATER_STAGE_COL,
    GROUNDWATER_TOTAL_EXTRACTION_COL,
    build_groundwater_alias_template,
    build_groundwater_district_outputs,
    parse_groundwater_workbook,
)


def _sheet_row_xml(row_num: int, cells: dict[str, object], shared_lookup: dict[str, int]) -> str:
    parts = [f'<row r="{row_num}">']
    for col, raw in cells.items():
        ref = f"{col}{row_num}"
        if isinstance(raw, str):
            idx = shared_lookup.setdefault(raw, len(shared_lookup))
            parts.append(f'<c r="{ref}" t="s"><v>{idx}</v></c>')
        else:
            parts.append(f'<c r="{ref}"><v>{raw}</v></c>')
    parts.append("</row>")
    return "".join(parts)


def _shared_strings_xml(shared_strings: list[str]) -> str:
    items = "".join(f"<si><t>{escape(text)}</t></si>" for text in shared_strings)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        f'count="{len(shared_strings)}" uniqueCount="{len(shared_strings)}">{items}</sst>'
    )


def _write_groundwater_workbook(path: Path, rows: list[dict[str, object]]) -> Path:
    shared_lookup: dict[str, int] = {}
    xml_rows = [
        _sheet_row_xml(
            8,
            {
                "B": "STATE",
                "C": "DISTRICT",
                "D": "ASSESSMENT UNIT",
                "CP": "Annual Extractable Ground water Resource (ham)",
                "DF": "Ground Water Extraction for all uses (ha.m)",
                "DJ": "Stage of Ground Water Extraction (%)",
                "DR": "Net Annual Ground Water Availability for Future Use (ham)",
            },
            shared_lookup,
        ),
        _sheet_row_xml(
            9,
            {
                "B": "STATE",
                "C": "DISTRICT",
                "D": "ASSESSMENT UNIT",
                "CP": "Annual Extractable Ground water Resource (ham)",
                "DF": "Ground Water Extraction for all uses (ha.m)",
                "DJ": "Stage of Ground Water Extraction (%)",
                "DR": "Net Annual Ground Water Availability for Future Use (ham)",
            },
            shared_lookup,
        ),
        _sheet_row_xml(
            10,
            {
                "B": "STATE",
                "C": "DISTRICT",
                "D": "ASSESSMENT UNIT",
                "CP": "Total",
                "DF": "Total",
                "DJ": "Total",
                "DR": "Total",
            },
            shared_lookup,
        ),
    ]
    for idx, record in enumerate(rows, start=12):
        xml_rows.append(_sheet_row_xml(idx, record, shared_lookup))

    shared_strings = [None] * len(shared_lookup)
    for text, idx in shared_lookup.items():
        shared_strings[idx] = text

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:DR{11 + len(rows)}"/>'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        "</worksheet>"
    )
    with ZipFile(path, "w") as zf:
        zf.writestr("xl/sharedStrings.xml", _shared_strings_xml(shared_strings))
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return path


def _write_district_geojson(path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "state_name": ["Andhra Pradesh", "Bihar"],
            "district_name": ["Dr.B.R.Ambedkar Konaseema", "Purnia"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_district_geojson_with_lakshadweep(path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "state_name": ["Lakshadweep-UT"],
            "district_name": ["Lakshadweep"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_duplicate_resolution_geojson(path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "state_name": [
                "GUJARAT",
                "JAMMU AND KASHMIR",
                "JAMMU AND KASHMIR",
                "JAMMU AND KASHMIR",
                "WEST BENGAL",
                "WEST BENGAL",
                "Karnataka",
            ],
            "district_name": [
                "KACHCHH",
                "BARAMULA",
                "KUPWARA",
                "SRINAGAR",
                "NORTH TWENTY-FOUR PARGANAS",
                "SOUTH 24PARGANAS",
                "Bengaluru (Urban)",
            ],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
            Polygon([(3, 0), (4, 0), (4, 1), (3, 1)]),
            Polygon([(4, 0), (5, 0), (5, 1), (4, 1)]),
            Polygon([(5, 0), (6, 0), (6, 1), (5, 1)]),
            Polygon([(6, 0), (7, 0), (7, 1), (6, 1)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(path, driver="GeoJSON")
    return path


def test_parse_groundwater_workbook_extracts_selected_metrics(tmp_path: Path) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {
                "B": "Andhra Pradesh",
                "C": "Konaseema",
                "D": "",
                "CP": 69252.24,
                "DF": 21723.97,
                "DJ": 31.37,
                "DR": 45060.58,
            }
        ],
    )

    workbook = parse_groundwater_workbook(workbook_path)

    assert "Stage of Ground Water Extraction (%)" in workbook.header_map["DJ"]
    assert workbook.records_df["source_state"].tolist() == ["Andhra Pradesh"]
    assert workbook.records_df["source_district"].tolist() == ["Konaseema"]
    row = workbook.records_df.iloc[0]
    assert float(row[GROUNDWATER_STAGE_COL]) == pytest.approx(31.37)
    assert float(row[GROUNDWATER_FUTURE_AVAILABILITY_COL]) == pytest.approx(45060.58)
    assert float(row[GROUNDWATER_EXTRACTABLE_RESOURCE_COL]) == pytest.approx(69252.24)
    assert float(row[GROUNDWATER_TOTAL_EXTRACTION_COL]) == pytest.approx(21723.97)


def test_build_groundwater_district_outputs_writes_state_slice_with_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {
                "B": "Andhra Pradesh",
                "C": "Konaseema",
                "D": "",
                "CP": 69252.24,
                "DF": 21723.97,
                "DJ": 31.37,
                "DR": 45060.58,
            }
        ],
    )
    districts_path = _write_district_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {
                "source_state": "Andhra Pradesh",
                "source_district": "Konaseema",
                "canonical_state": "Andhra Pradesh",
                "canonical_district": "Dr.B.R.Ambedkar Konaseema",
            }
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    outputs = build_groundwater_district_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        alias_csv_path=alias_csv,
        overwrite=True,
        dry_run=False,
    )

    master_row = outputs["master_df"].iloc[0]
    assert master_row["district"] == "Dr.B.R.Ambedkar Konaseema"
    assert float(master_row[GROUNDWATER_STAGE_COL]) == pytest.approx(31.37)
    assert (qa_dir / "groundwater_district_crosswalk.csv").exists()
    assert (
        tmp_path
        / "processed"
        / "gw_stage_extraction_pct"
        / "Andhra Pradesh"
        / "master_metrics_by_district.csv"
    ).exists()


def test_build_groundwater_district_outputs_writes_alias_template_for_unmatched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {
                "B": "Bihar",
                "C": "Purnea",
                "D": "",
                "CP": 10.0,
                "DF": 5.0,
                "DJ": 25.0,
                "DR": 3.0,
            }
        ],
    )
    districts_path = _write_district_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="unmatched source districts"):
        build_groundwater_district_outputs(
            workbook_path=workbook_path,
            districts_path=districts_path,
            qa_dir=qa_dir,
            alias_csv_path=tmp_path / "missing_aliases.csv",
            overwrite=True,
            dry_run=False,
        )

    template_df = pd.read_csv(qa_dir / "groundwater_district_alias_template.csv")
    assert template_df["source_district"].tolist() == ["Purnea"]
    assert "Purnia" in template_df["suggested_canonical_district_1"].fillna("").tolist()
    assert template_df["canonical_district"].tolist() == ["Purnia"]
    assert template_df["prefill_status"].tolist() == ["auto_filled"]
    assert template_df["prefill_method"].tolist() == ["fuzzy_single_candidate"]


def test_build_groundwater_alias_template_leaves_ambiguous_matches_blank() -> None:
    unmatched_df = pd.DataFrame(
        [
            {
                "source_state": "ASSAM",
                "source_district": "KAMRUP",
                "source_state_norm": "assam",
                "source_district_norm": "kamrup",
            }
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_state": "ASSAM",
                "canonical_district": "KAMRUP RURAL",
                "district_key": "ASSAM::KAMRUP RURAL",
                "canonical_state_norm": "assam",
                "canonical_district_norm": "kamruprural",
            },
            {
                "canonical_state": "ASSAM",
                "canonical_district": "KAMRUP METRO",
                "district_key": "ASSAM::KAMRUP METRO",
                "canonical_state_norm": "assam",
                "canonical_district_norm": "kamrupmetro",
            },
        ]
    )

    out = build_groundwater_alias_template(unmatched_df, canonical_df)

    row = out.iloc[0]
    assert row["canonical_state"] == ""
    assert row["canonical_district"] == ""
    assert row["prefill_status"] == "needs_review"
    assert row["prefill_method"] == ""


def test_build_groundwater_alias_template_prefills_unique_token_subset_match() -> None:
    unmatched_df = pd.DataFrame(
        [
            {
                "source_state": "ANDHRA PRADESH",
                "source_district": "Konaseema",
                "source_state_norm": "andhrapradesh",
                "source_district_norm": "konaseema",
            }
        ]
    )
    canonical_df = pd.DataFrame(
        [
            {
                "canonical_state": "ANDHRA PRADESH",
                "canonical_district": "DR.B.R.AMBEDKAR KONASEEMA",
                "district_key": "ANDHRA PRADESH::DR.B.R.AMBEDKAR KONASEEMA",
                "canonical_state_norm": "andhrapradesh",
                "canonical_district_norm": "drbrambedkarkonaseema",
            }
        ]
    )

    out = build_groundwater_alias_template(unmatched_df, canonical_df)

    row = out.iloc[0]
    assert row["canonical_state"] == "ANDHRA PRADESH"
    assert row["canonical_district"] == "DR.B.R.AMBEDKAR KONASEEMA"
    assert row["prefill_status"] == "auto_filled"
    assert row["prefill_method"] == "token_subset_unique"


def test_build_groundwater_district_outputs_ignores_invalid_needs_review_alias_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {
                "B": "ANDAMAN AND NICOBAR ISLANDS",
                "C": "N & M ANDAMAN",
                "D": "",
                "CP": 10.0,
                "DF": 5.0,
                "DJ": 50.0,
                "DR": 2.0,
            }
        ],
    )
    districts_path = _write_district_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {
                "source_state": "ANDAMAN AND NICOBAR ISLANDS",
                "source_district": "N & M ANDAMAN",
                "canonical_state": "NORTH AND MIDDLE ANDAMAN",
                "canonical_district": "NORTH AND MIDDLE ANDAMAN",
                "prefill_status": "needs_review",
            }
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="unmatched source districts"):
        build_groundwater_district_outputs(
            workbook_path=workbook_path,
            districts_path=districts_path,
            qa_dir=qa_dir,
            alias_csv_path=alias_csv,
            overwrite=True,
            dry_run=False,
        )


def test_build_groundwater_district_outputs_aggregates_lakshadweep_islands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {"B": "LAKSHDWEEP", "C": "AMINI", "D": "", "CP": 127.50, "DF": 65.47, "DJ": 51.35, "DR": 52.42},
            {"B": "LAKSHDWEEP", "C": "ANDROTH", "D": "", "CP": 161.91, "DF": 86.68, "DJ": 53.54, "DR": 62.51},
            {"B": "LAKSHDWEEP", "C": "KAVARATTI", "D": "", "CP": 136.17, "DF": 101.66, "DJ": 74.66, "DR": 19.57},
            {"B": "LAKSHDWEEP", "C": "KILTAN", "D": "", "CP": 58.22, "DF": 25.51, "DJ": 43.82, "DR": 28.97},
            {"B": "LAKSHDWEEP", "C": "MINICOY", "D": "", "CP": 99.09, "DF": 57.53, "DJ": 58.06, "DR": 33.11},
        ],
    )
    districts_path = _write_district_geojson_with_lakshadweep(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    outputs = build_groundwater_district_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        alias_csv_path=tmp_path / "missing_aliases.csv",
        overwrite=True,
        dry_run=False,
    )

    assert outputs["raw_source_df"].shape[0] == 5
    assert outputs["source_df"].shape[0] == 1
    assert outputs["crosswalk_df"]["source_district"].tolist() == ["Lakshadweep"]
    master_row = outputs["master_df"].iloc[0]
    assert master_row["state"] == "Lakshadweep-UT"
    assert master_row["district"] == "Lakshadweep"
    assert float(master_row[GROUNDWATER_EXTRACTABLE_RESOURCE_COL]) == pytest.approx(582.89)
    assert float(master_row[GROUNDWATER_TOTAL_EXTRACTION_COL]) == pytest.approx(336.85)
    assert float(master_row[GROUNDWATER_FUTURE_AVAILABILITY_COL]) == pytest.approx(196.58)
    assert float(master_row[GROUNDWATER_STAGE_COL]) == pytest.approx((336.85 / 582.89) * 100.0)

    aggregation_df = pd.read_csv(qa_dir / "groundwater_source_aggregations.csv")
    assert aggregation_df["aggregation_rule"].tolist() == ["lakshadweep_island_to_district"]
    assert aggregation_df["aggregated_source_district"].tolist() == ["Lakshadweep"]


def test_build_groundwater_district_outputs_drops_zero_placeholder_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {"B": "GUJARAT", "C": "KACHCHH", "D": "", "CP": 192955.99, "DF": 118478.3, "DJ": 51.79, "DR": 43127.67},
            {"B": "GUJARAT", "C": "RANA AND KUTCH", "D": "", "CP": 0.0, "DF": "", "DJ": "", "DR": 0.0},
            {"B": "JAMMU AND KASHMIR", "C": "Baramulla", "D": "", "CP": 9834.88, "DF": 2215.84, "DJ": 22.53, "DR": 7619.04},
            {"B": "JAMMU AND KASHMIR", "C": "Barmulla", "D": "", "CP": 0.0, "DF": "", "DJ": "", "DR": 0.0},
            {"B": "JAMMU AND KASHMIR", "C": "Kupwara", "D": "", "CP": 0.0, "DF": "", "DJ": "", "DR": 0.0},
            {"B": "JAMMU AND KASHMIR", "C": "Kupwarar", "D": "", "CP": 5586.49, "DF": 1199.43, "DJ": 21.47, "DR": 4387.06},
            {"B": "JAMMU AND KASHMIR", "C": "Srinagar", "D": "", "CP": 12288.16, "DF": 6947.93, "DJ": 56.54, "DR": 5340.23},
            {"B": "JAMMU AND KASHMIR", "C": "Srinagar Hilly Area", "D": "", "CP": 0.0, "DF": 0.0, "DJ": "", "DR": 0.0},
        ],
    )
    districts_path = _write_duplicate_resolution_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {"source_state": "GUJARAT", "source_district": "RANA AND KUTCH", "canonical_state": "GUJARAT", "canonical_district": "KACHCHH"},
            {"source_state": "JAMMU AND KASHMIR", "source_district": "Baramulla", "canonical_state": "JAMMU AND KASHMIR", "canonical_district": "BARAMULA"},
            {"source_state": "JAMMU AND KASHMIR", "source_district": "Barmulla", "canonical_state": "JAMMU AND KASHMIR", "canonical_district": "BARAMULA"},
            {"source_state": "JAMMU AND KASHMIR", "source_district": "Kupwarar", "canonical_state": "JAMMU AND KASHMIR", "canonical_district": "KUPWARA"},
            {"source_state": "JAMMU AND KASHMIR", "source_district": "Srinagar Hilly Area", "canonical_state": "JAMMU AND KASHMIR", "canonical_district": "SRINAGAR"},
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    outputs = build_groundwater_district_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        alias_csv_path=alias_csv,
        overwrite=True,
        dry_run=False,
    )

    kept = outputs["crosswalk_df"][["source_state", "source_district"]].to_dict(orient="records")
    assert {"source_state": "GUJARAT", "source_district": "KACHCHH"} in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Baramulla"} in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Kupwarar"} in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Srinagar"} in kept
    assert {"source_state": "GUJARAT", "source_district": "RANA AND KUTCH"} not in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Barmulla"} not in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Kupwara"} not in kept
    assert {"source_state": "JAMMU AND KASHMIR", "source_district": "Srinagar Hilly Area"} not in kept

    resolution_df = pd.read_csv(qa_dir / "groundwater_duplicate_resolution.csv")
    assert set(resolution_df["dropped_source_district"].tolist()) == {
        "RANA AND KUTCH",
        "Barmulla",
        "Kupwara",
        "Srinagar Hilly Area",
    }


def test_build_groundwater_district_outputs_keeps_bad_alias_duplicate_fatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {"B": "WEST BENGAL", "C": "NORTH 24 PARGANAS", "D": "", "CP": 138984.52, "DF": 97709.84, "DJ": 70.30, "DR": 39552.91},
            {"B": "WEST BENGAL", "C": "SOUTH 24 PARGANAS", "D": "", "CP": 0.0, "DF": 0.0, "DJ": 0.0, "DR": 0.0},
        ],
    )
    districts_path = _write_duplicate_resolution_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {"source_state": "WEST BENGAL", "source_district": "NORTH 24 PARGANAS", "canonical_state": "WEST BENGAL", "canonical_district": "SOUTH 24PARGANAS"},
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="multiple source rows onto the same canonical district"):
        build_groundwater_district_outputs(
            workbook_path=workbook_path,
            districts_path=districts_path,
            qa_dir=qa_dir,
            alias_csv_path=alias_csv,
            overwrite=True,
            dry_run=False,
        )


def test_build_groundwater_district_outputs_aggregates_bengaluru_urban_and_south(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {"B": "KARNATAKA", "C": "Bengaluru (Urban)", "D": "", "CP": 30838.84, "DF": 54669.77, "DJ": 177.27, "DR": 0.0},
            {"B": "KARNATAKA", "C": "Bengaluru South", "D": "", "CP": 49111.96, "DF": 44780.88, "DJ": 91.18, "DR": 7030.41},
        ],
    )
    districts_path = _write_duplicate_resolution_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {"source_state": "KARNATAKA", "source_district": "Bengaluru South", "canonical_state": "Karnataka", "canonical_district": "Bengaluru (Urban)"},
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    outputs = build_groundwater_district_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        alias_csv_path=alias_csv,
        overwrite=True,
        dry_run=False,
    )

    crosswalk_df = outputs["crosswalk_df"]
    assert crosswalk_df["source_district"].tolist() == ["Bengaluru (Urban)"]
    master_row = outputs["master_df"].iloc[0]
    assert master_row["state"] == "Karnataka"
    assert master_row["district"] == "Bengaluru (Urban)"
    assert float(master_row[GROUNDWATER_EXTRACTABLE_RESOURCE_COL]) == pytest.approx(79950.80)
    assert float(master_row[GROUNDWATER_TOTAL_EXTRACTION_COL]) == pytest.approx(99450.65)
    assert float(master_row[GROUNDWATER_FUTURE_AVAILABILITY_COL]) == pytest.approx(7030.41)
    assert float(master_row[GROUNDWATER_STAGE_COL]) == pytest.approx((99450.65 / 79950.80) * 100.0)

    aggregation_df = pd.read_csv(qa_dir / "groundwater_source_aggregations.csv")
    assert aggregation_df["aggregation_rule"].tolist() == ["bengaluru_urban_plus_south_to_urban"]
    assert aggregation_df["aggregated_source_district"].tolist() == ["Bengaluru (Urban)"]


def test_build_groundwater_district_outputs_still_fails_on_non_bengaluru_conflicting_real_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workbook_path = _write_groundwater_workbook(
        tmp_path / "groundwater.xlsx",
        [
            {"B": "Bihar", "C": "Purnia", "D": "", "CP": 30838.84, "DF": 54669.77, "DJ": 177.27, "DR": 0.0},
            {"B": "Bihar", "C": "Purnia South", "D": "", "CP": 49111.96, "DF": 44780.88, "DJ": 91.18, "DR": 7030.41},
        ],
    )
    districts_path = _write_district_geojson(tmp_path / "districts.geojson")
    qa_dir = tmp_path / "groundwater"
    alias_csv = tmp_path / "groundwater_aliases.csv"
    pd.DataFrame(
        [
            {"source_state": "Bihar", "source_district": "Purnia South", "canonical_state": "Bihar", "canonical_district": "Purnia"},
        ]
    ).to_csv(alias_csv, index=False)
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="multiple source rows onto the same canonical district"):
        build_groundwater_district_outputs(
            workbook_path=workbook_path,
            districts_path=districts_path,
            qa_dir=qa_dir,
            alias_csv_path=alias_csv,
            overwrite=True,
            dry_run=False,
        )
