#!/usr/bin/env python3
"""
Build canonical district groundwater masters from the 2024-2025 GEC workbook.

This tool parses the IIT-H / GEC workbook without relying on Excel-only Python
dependencies, resolves source districts onto the canonical IRT district layer,
and writes one state-sliced district master CSV per onboarded groundwater metric.
"""

from __future__ import annotations

import argparse
import difflib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import pandas as pd

from india_resilience_tool.utils.naming import normalize_name
from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import load_district_boundaries


WORKBOOK_SHEET_PATH = "xl/worksheets/sheet1.xml"
SHARED_STRINGS_PATH = "xl/sharedStrings.xml"
XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
DEFAULT_WORKBOOK_NAME = "CentralReport1773820094787.xlsx"
GROUNDWATER_PERIOD = "2024-2025"

GROUNDWATER_STAGE_COL = "gw_stage_extraction_pct__snapshot__2024-2025__mean"
GROUNDWATER_FUTURE_AVAILABILITY_COL = "gw_future_availability_ham__snapshot__2024-2025__mean"
GROUNDWATER_EXTRACTABLE_RESOURCE_COL = "gw_extractable_resource_ham__snapshot__2024-2025__mean"
GROUNDWATER_TOTAL_EXTRACTION_COL = "gw_total_extraction_ham__snapshot__2024-2025__mean"
LAKSHADWEEP_CANONICAL_DISTRICT = "Lakshadweep"

GROUNDWATER_METRICS: dict[str, dict[str, str]] = {
    "gw_stage_extraction_pct": {
        "source_col": "DJ",
        "master_col": GROUNDWATER_STAGE_COL,
    },
    "gw_future_availability_ham": {
        "source_col": "DR",
        "master_col": GROUNDWATER_FUTURE_AVAILABILITY_COL,
    },
    "gw_extractable_resource_ham": {
        "source_col": "CP",
        "master_col": GROUNDWATER_EXTRACTABLE_RESOURCE_COL,
    },
    "gw_total_extraction_ham": {
        "source_col": "DF",
        "master_col": GROUNDWATER_TOTAL_EXTRACTION_COL,
    },
}

EXPECTED_HEADER_FRAGMENTS: dict[str, tuple[str, ...]] = {
    "B": ("STATE",),
    "C": ("DISTRICT",),
    "D": ("ASSESSMENT UNIT",),
    "DJ": ("Stage of Ground Water Extraction (%)", "Total"),
    "DR": ("Net Annual Ground Water Availability for Future Use (ham)", "Total"),
    "CP": ("Annual Extractable Ground water Resource (ham)", "Total"),
    "DF": ("Ground Water Extraction for all uses (ha.m)", "Total"),
}

STATE_ALIASES: dict[str, str] = {
    "chhattisgarh": "chhatisgarh",
    "dadra and nagar haveli": "dadra nagar haveli daman diu",
    "daman and diu": "dadra nagar haveli daman diu",
    "lakshdweep": "lakshadweep ut",
    "tamilnadu": "tamil nadu",
}


@dataclass(frozen=True)
class GroundwaterWorkbook:
    """Parsed workbook rows plus flattened header labels."""

    records_df: pd.DataFrame
    header_map: dict[str, str]


def _empty_groundwater_aggregation_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "aggregation_rule",
            "source_state",
            "source_districts",
            "source_rows",
            "aggregated_source_state",
            "aggregated_source_district",
            GROUNDWATER_EXTRACTABLE_RESOURCE_COL,
            GROUNDWATER_TOTAL_EXTRACTION_COL,
            GROUNDWATER_FUTURE_AVAILABILITY_COL,
            GROUNDWATER_STAGE_COL,
        ]
    )


def _empty_groundwater_duplicate_resolution_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "canonical_state",
            "canonical_district",
            "district_key",
            "kept_source_state",
            "kept_source_district",
            "kept_match_method",
            "dropped_source_state",
            "dropped_source_district",
            "dropped_match_method",
            "resolution_reason",
        ]
    )


def _find_default_workbook() -> Path:
    data_dir = get_paths_config().data_dir
    direct = data_dir / DEFAULT_WORKBOOK_NAME
    if direct.exists():
        return direct
    candidates = sorted(data_dir.glob("CentralReport*.xlsx"))
    if candidates:
        return candidates[-1]
    return direct


def _default_groundwater_dir() -> Path:
    return get_paths_config().data_dir / "groundwater"


def _col_to_num(col: str) -> int:
    out = 0
    for ch in str(col or "").strip().upper():
        if "A" <= ch <= "Z":
            out = (out * 26) + (ord(ch) - 64)
    return out


def _num_to_col(num: int) -> str:
    if num <= 0:
        return ""
    out = ""
    n = int(num)
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _load_shared_strings(zf: ZipFile) -> list[str]:
    if SHARED_STRINGS_PATH not in zf.namelist():
        return []
    root = ET.fromstring(zf.read(SHARED_STRINGS_PATH))
    shared: list[str] = []
    for si in root.findall("a:si", XML_NS):
        text = "".join(t.text or "" for t in si.findall(".//a:t", XML_NS))
        shared.append(text)
    return shared


def _read_sheet_rows(zf: ZipFile) -> tuple[dict[int, dict[str, str]], list[tuple[int, int, int, int, str]]]:
    if WORKBOOK_SHEET_PATH not in zf.namelist():
        raise FileNotFoundError(f"Workbook does not contain {WORKBOOK_SHEET_PATH}")

    shared_strings = _load_shared_strings(zf)
    sheet_root = ET.fromstring(zf.read(WORKBOOK_SHEET_PATH))
    rows: dict[int, dict[str, str]] = {}

    for row in sheet_root.findall(".//a:sheetData/a:row", XML_NS):
        row_num = int(row.attrib["r"])
        values: dict[str, str] = {}
        for cell in row.findall("a:c", XML_NS):
            ref = cell.attrib.get("r", "")
            col = "".join(ch for ch in ref if ch.isalpha())
            cell_type = cell.attrib.get("t")
            if cell_type == "inlineStr":
                text = "".join(t.text or "" for t in cell.findall(".//a:t", XML_NS))
                values[col] = text
                continue
            raw_val = cell.find("a:v", XML_NS)
            if raw_val is None or raw_val.text is None:
                values[col] = ""
                continue
            if cell_type == "s":
                values[col] = shared_strings[int(raw_val.text)]
            else:
                values[col] = raw_val.text or ""
        rows[row_num] = values

    merges: list[tuple[int, int, int, int, str]] = []
    merge_root = sheet_root.find("a:mergeCells", XML_NS)
    if merge_root is not None:
        for merge_cell in merge_root.findall("a:mergeCell", XML_NS):
            ref = merge_cell.attrib["ref"]
            start_ref, end_ref = ref.split(":") if ":" in ref else (ref, ref)
            start_col = "".join(ch for ch in start_ref if ch.isalpha())
            start_row = int("".join(ch for ch in start_ref if ch.isdigit()))
            end_col = "".join(ch for ch in end_ref if ch.isalpha())
            end_row = int("".join(ch for ch in end_ref if ch.isdigit()))
            anchor_text = rows.get(start_row, {}).get(start_col, "")
            merges.append(
                (
                    start_row,
                    end_row,
                    _col_to_num(start_col),
                    _col_to_num(end_col),
                    anchor_text,
                )
            )
    return rows, merges


def _merged_value(
    *,
    row_num: int,
    col_num: int,
    rows: dict[int, dict[str, str]],
    merges: Iterable[tuple[int, int, int, int, str]],
) -> str:
    col = _num_to_col(col_num)
    direct = rows.get(row_num, {}).get(col, "")
    if direct:
        return direct
    for start_row, end_row, start_col, end_col, text in merges:
        if start_row <= row_num <= end_row and start_col <= col_num <= end_col:
            return text
    return ""


def flatten_header_rows(
    rows: dict[int, dict[str, str]],
    merges: Iterable[tuple[int, int, int, int, str]],
    *,
    header_rows: tuple[int, int, int] = (8, 9, 10),
) -> dict[str, str]:
    """Flatten the multi-row workbook header into one label per Excel column."""
    max_col_num = 0
    for values in rows.values():
        for col in values:
            max_col_num = max(max_col_num, _col_to_num(col))
    header_map: dict[str, str] = {}
    for col_num in range(1, max_col_num + 1):
        parts: list[str] = []
        for row_num in header_rows:
            text = str(
                _merged_value(row_num=row_num, col_num=col_num, rows=rows, merges=merges) or ""
            ).strip()
            if text and text not in parts:
                parts.append(text)
        if parts:
            header_map[_num_to_col(col_num)] = " | ".join(parts)
    return header_map


def _validate_expected_headers(header_map: dict[str, str]) -> None:
    missing: list[str] = []
    for col, fragments in EXPECTED_HEADER_FRAGMENTS.items():
        header = header_map.get(col, "")
        if not header or any(fragment not in header for fragment in fragments):
            missing.append(f"{col}: expected fragments {fragments}, found '{header}'")
    if missing:
        raise ValueError("Groundwater workbook headers do not match the expected contract: " + "; ".join(missing))


def parse_groundwater_workbook(workbook_path: Path) -> GroundwaterWorkbook:
    """Parse the groundwater workbook into a normalized source dataframe."""
    with ZipFile(workbook_path) as zf:
        rows, merges = _read_sheet_rows(zf)

    header_map = flatten_header_rows(rows, merges)
    _validate_expected_headers(header_map)

    records: list[dict[str, object]] = []
    for row_num in sorted(rows):
        if row_num < 12:
            continue
        row = rows[row_num]
        state = str(row.get("B", "") or "").strip()
        district = str(row.get("C", "") or "").strip()
        assessment_unit = str(row.get("D", "") or "").strip()
        if not state and not district and not assessment_unit:
            continue
        record: dict[str, object] = {
            "source_row": row_num,
            "source_state": state,
            "source_district": district,
            "assessment_unit": assessment_unit,
        }
        for metric_cfg in GROUNDWATER_METRICS.values():
            source_col = metric_cfg["source_col"]
            record[metric_cfg["master_col"]] = pd.to_numeric(
                pd.Series([row.get(source_col, "")]),
                errors="coerce",
            ).iloc[0]
        records.append(record)

    df = pd.DataFrame.from_records(records)
    if df.empty:
        raise ValueError(f"Workbook contains no groundwater data rows: {workbook_path}")

    required = ["source_state", "source_district"]
    for col in required:
        df[col] = df[col].astype("string").str.strip()
    df = df.loc[df["source_state"].notna() & df["source_state"].ne("")].copy()
    df = df.loc[df["source_district"].notna() & df["source_district"].ne("")].copy()
    if df.empty:
        raise ValueError(f"Workbook contains no usable source state/district rows: {workbook_path}")
    return GroundwaterWorkbook(records_df=df.reset_index(drop=True), header_map=header_map)


def _normalize_state(value: str) -> str:
    norm = normalize_name(value)
    return STATE_ALIASES.get(norm, norm).replace(" ", "")


def _normalize_district(value: str) -> str:
    return normalize_name(value).replace(" ", "")


def _tokenize(value: str) -> list[str]:
    return [tok for tok in normalize_name(value).split(" ") if tok]


def _normalized_similarity(left: str, right: str) -> float:
    left_norm = _normalize_district(left)
    right_norm = _normalize_district(right)
    if not left_norm or not right_norm:
        return 0.0
    return float(difflib.SequenceMatcher(None, left_norm, right_norm).ratio())


def _is_placeholder_groundwater_row(row: pd.Series) -> bool:
    stage = pd.to_numeric(pd.Series([row.get(GROUNDWATER_STAGE_COL)]), errors="coerce").iloc[0]
    additive_vals = [
        pd.to_numeric(pd.Series([row.get(GROUNDWATER_FUTURE_AVAILABILITY_COL)]), errors="coerce").iloc[0],
        pd.to_numeric(pd.Series([row.get(GROUNDWATER_EXTRACTABLE_RESOURCE_COL)]), errors="coerce").iloc[0],
        pd.to_numeric(pd.Series([row.get(GROUNDWATER_TOTAL_EXTRACTION_COL)]), errors="coerce").iloc[0],
    ]

    def _zero_or_missing(value: object) -> bool:
        if pd.isna(value):
            return True
        return abs(float(value)) < 1e-12

    return _zero_or_missing(stage) and all(_zero_or_missing(value) for value in additive_vals)


def _collapse_lakshadweep_source_rows(
    source_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse Lakshadweep island rows into one canonical district-grain source row."""
    source = source_df.copy()
    laksh_norm = _normalize_state("LAKSHDWEEP")
    source["source_state_norm"] = source["source_state"].map(_normalize_state)
    laksh_mask = source["source_state_norm"] == laksh_norm
    if not bool(laksh_mask.any()):
        return source_df.reset_index(drop=True), _empty_groundwater_aggregation_df()

    laksh_rows = source.loc[laksh_mask].copy()
    remaining = source.loc[~laksh_mask, source_df.columns].copy()

    extractable = pd.to_numeric(
        laksh_rows[GROUNDWATER_EXTRACTABLE_RESOURCE_COL],
        errors="coerce",
    ).sum(min_count=1)
    total_extraction = pd.to_numeric(
        laksh_rows[GROUNDWATER_TOTAL_EXTRACTION_COL],
        errors="coerce",
    ).sum(min_count=1)
    future_availability = pd.to_numeric(
        laksh_rows[GROUNDWATER_FUTURE_AVAILABILITY_COL],
        errors="coerce",
    ).sum(min_count=1)
    if pd.notna(extractable) and float(extractable) != 0.0 and pd.notna(total_extraction):
        stage_pct = (float(total_extraction) / float(extractable)) * 100.0
    else:
        stage_pct = pd.NA

    source_state = str(laksh_rows["source_state"].dropna().astype("string").iloc[0]).strip()
    aggregated_row = pd.DataFrame(
        [
            {
                "source_row": pd.NA,
                "source_state": source_state,
                "source_district": LAKSHADWEEP_CANONICAL_DISTRICT,
                "assessment_unit": "",
                GROUNDWATER_EXTRACTABLE_RESOURCE_COL: extractable,
                GROUNDWATER_TOTAL_EXTRACTION_COL: total_extraction,
                GROUNDWATER_FUTURE_AVAILABILITY_COL: future_availability,
                GROUNDWATER_STAGE_COL: stage_pct,
            }
        ]
    )
    normalized = pd.concat([remaining, aggregated_row], ignore_index=True)
    normalized = normalized[source_df.columns].sort_values(
        ["source_state", "source_district"],
        na_position="last",
    ).reset_index(drop=True)

    aggregation_df = pd.DataFrame(
        [
            {
                "aggregation_rule": "lakshadweep_island_to_district",
                "source_state": source_state,
                "source_districts": "|".join(
                    sorted(
                        str(v).strip()
                        for v in laksh_rows["source_district"].dropna().astype("string").tolist()
                        if str(v).strip()
                    )
                ),
                "source_rows": "|".join(
                    str(int(v))
                    for v in sorted(
                        int(v)
                        for v in pd.to_numeric(laksh_rows["source_row"], errors="coerce")
                        .dropna()
                        .astype(int)
                        .tolist()
                    )
                ),
                "aggregated_source_state": source_state,
                "aggregated_source_district": LAKSHADWEEP_CANONICAL_DISTRICT,
                GROUNDWATER_EXTRACTABLE_RESOURCE_COL: extractable,
                GROUNDWATER_TOTAL_EXTRACTION_COL: total_extraction,
                GROUNDWATER_FUTURE_AVAILABILITY_COL: future_availability,
                GROUNDWATER_STAGE_COL: stage_pct,
            }
        ]
    )
    return normalized, aggregation_df


def _load_alias_overrides(alias_csv_path: Path) -> pd.DataFrame:
    if not alias_csv_path.exists():
        return pd.DataFrame(
            columns=[
                "source_state",
                "source_district",
                "canonical_state",
                "canonical_district",
                "prefill_status",
            ]
        )
    df = pd.read_csv(alias_csv_path)
    required = {"source_state", "source_district", "canonical_state", "canonical_district"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"Groundwater district alias CSV is missing required columns {missing}: {alias_csv_path}"
        )
    optional_cols = [col for col in ["prefill_status"] if col in df.columns]
    out = df[list(required) + optional_cols].copy()
    for col in required:
        out[col] = out[col].astype("string").str.strip()
    if "prefill_status" in out.columns:
        out["prefill_status"] = out["prefill_status"].astype("string").str.strip().str.lower()
    else:
        out["prefill_status"] = pd.NA
    out = out.dropna(subset=["source_state", "source_district", "canonical_state", "canonical_district"]).copy()
    out = out.loc[
        out["source_state"].ne("")
        & out["source_district"].ne("")
        & out["canonical_state"].ne("")
        & out["canonical_district"].ne("")
    ].copy()
    out["source_state_norm"] = out["source_state"].map(_normalize_state)
    out["source_district_norm"] = out["source_district"].map(_normalize_district)
    out["canonical_state_norm"] = out["canonical_state"].map(_normalize_state)
    out["canonical_district_norm"] = out["canonical_district"].map(_normalize_district)
    if out.duplicated(["source_state_norm", "source_district_norm"]).any():
        dupes = out.loc[
            out.duplicated(["source_state_norm", "source_district_norm"], keep=False),
            ["source_state", "source_district", "canonical_state", "canonical_district"],
        ]
        raise ValueError(
            "Groundwater district alias CSV contains duplicate source mappings: "
            + dupes.head(10).to_dict(orient="records").__repr__()
        )
    return out.reset_index(drop=True)


def _build_canonical_districts(districts_path: Path) -> pd.DataFrame:
    districts = load_district_boundaries(districts_path)[
        ["state_name", "district_name", "district_key"]
    ].copy()
    districts["canonical_state"] = districts["state_name"].astype("string").str.strip()
    districts["canonical_district"] = districts["district_name"].astype("string").str.strip()
    districts["canonical_state_norm"] = districts["canonical_state"].map(_normalize_state)
    districts["canonical_district_norm"] = districts["canonical_district"].map(_normalize_district)
    return districts[
        [
            "canonical_state",
            "canonical_district",
            "district_key",
            "canonical_state_norm",
            "canonical_district_norm",
        ]
    ].reset_index(drop=True)


def build_groundwater_alias_template(
    unmatched_df: pd.DataFrame,
    canonical_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a reviewable alias template with state-constrained fuzzy suggestions."""
    if unmatched_df.empty:
        return pd.DataFrame(
            columns=[
                "source_state",
                "source_district",
                "suggested_canonical_district_1",
                "suggested_canonical_district_2",
                "suggested_canonical_district_3",
                "canonical_state",
                "canonical_district",
                "prefill_status",
                "prefill_method",
                "prefill_confidence",
                "notes",
            ]
        )

    def _candidate_suggestions(source_norm: str, candidates: pd.DataFrame) -> list[str]:
        candidate_names = candidates["canonical_district_norm"].tolist()
        best_norms = difflib.get_close_matches(source_norm, candidate_names, n=3, cutoff=0.55)
        suggestions: list[str] = []
        for best_norm in best_norms:
            match = candidates.loc[
                candidates["canonical_district_norm"] == best_norm,
                "canonical_district",
            ]
            if not match.empty:
                suggestion = str(match.iloc[0])
                if suggestion not in suggestions:
                    suggestions.append(suggestion)
        return suggestions

    def _prefill_match(
        *,
        source_state: str,
        source_district: str,
        source_district_norm: str,
        state_candidates: pd.DataFrame,
    ) -> tuple[str, str, str, str, str]:
        if state_candidates.empty:
            return "", "", "needs_review", "", ""

        exact = state_candidates.loc[
            state_candidates["canonical_district_norm"] == source_district_norm
        ]
        if len(exact) == 1:
            row = exact.iloc[0]
            return str(row["canonical_state"]), str(row["canonical_district"]), "auto_filled", "exact_normalized", "1.000"

        source_tokens = set(_tokenize(source_district))
        token_matches: list[pd.Series] = []
        if source_tokens:
            for _, candidate in state_candidates.iterrows():
                candidate_tokens = set(_tokenize(str(candidate["canonical_district"])))
                if source_tokens and source_tokens.issubset(candidate_tokens):
                    token_matches.append(candidate)
            if len(token_matches) == 1:
                row = token_matches[0]
                return str(row["canonical_state"]), str(row["canonical_district"]), "auto_filled", "token_subset_unique", "1.000"

        scored: list[tuple[float, str, str]] = []
        for _, candidate in state_candidates.iterrows():
            canonical_norm = str(candidate["canonical_district_norm"] or "")
            score = difflib.SequenceMatcher(None, source_district_norm, canonical_norm).ratio()
            scored.append((float(score), str(candidate["canonical_state"]), str(candidate["canonical_district"])))
        scored.sort(key=lambda x: (-x[0], x[2]))
        if not scored:
            return "", "", "needs_review", "", ""

        best_score, best_state, best_district = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0
        if best_score >= 0.83 and (best_score - second_score) >= 0.08:
            return best_state, best_district, "auto_filled", "fuzzy_single_candidate", f"{best_score:.3f}"

        return "", "", "needs_review", "", ""

    rows: list[dict[str, str]] = []
    for source_state, source_district, source_state_norm, source_district_norm in (
        unmatched_df[
            ["source_state", "source_district", "source_state_norm", "source_district_norm"]
        ]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    ):
        state_candidates = canonical_df.loc[
            canonical_df["canonical_state_norm"] == source_state_norm,
            ["canonical_state", "canonical_district", "canonical_district_norm"],
        ]
        suggestions = _candidate_suggestions(source_district_norm, state_candidates)
        canonical_state, canonical_district, prefill_status, prefill_method, prefill_confidence = _prefill_match(
            source_state=source_state,
            source_district=source_district,
            source_district_norm=source_district_norm,
            state_candidates=state_candidates,
        )
        rows.append(
            {
                "source_state": source_state,
                "source_district": source_district,
                "suggested_canonical_district_1": suggestions[0] if len(suggestions) > 0 else "",
                "suggested_canonical_district_2": suggestions[1] if len(suggestions) > 1 else "",
                "suggested_canonical_district_3": suggestions[2] if len(suggestions) > 2 else "",
                "canonical_state": canonical_state,
                "canonical_district": canonical_district,
                "prefill_status": prefill_status,
                "prefill_method": prefill_method,
                "prefill_confidence": prefill_confidence,
                "notes": "",
            }
        )
    return pd.DataFrame.from_records(rows).sort_values(["source_state", "source_district"]).reset_index(drop=True)


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _metric_specific_master(master_df: pd.DataFrame, *, metric_slug: str) -> pd.DataFrame:
    metric_cfg = GROUNDWATER_METRICS[metric_slug]
    keep = ["state", "district", "district_key", metric_cfg["master_col"]]
    return master_df[keep].copy()


def _write_state_slices(
    master_df: pd.DataFrame,
    *,
    metric_slug: str,
    data_dir: Path,
    overwrite: bool,
) -> dict[str, int]:
    processed_root = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
    out_name = get_master_csv_filename("district")
    counts: dict[str, int] = {}
    for state_name, state_df in master_df.groupby("state", dropna=False, as_index=False):
        state_label = str(state_name or "").strip()
        if not state_label:
            raise ValueError(f"Groundwater district master contains an empty state value for {metric_slug}.")
        out_path = processed_root / state_label / out_name
        _write_csv(state_df.reset_index(drop=True), out_path, overwrite=overwrite)
        counts[state_label] = int(state_df.shape[0])
    return counts


def _resolve_groundwater_districts(
    source_df: pd.DataFrame,
    *,
    canonical_df: pd.DataFrame,
    alias_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source = source_df.copy()
    source["source_state_norm"] = source["source_state"].map(_normalize_state)
    source["source_district_norm"] = source["source_district"].map(_normalize_district)
    if source.duplicated(["source_state_norm", "source_district_norm"]).any():
        dupes = source.loc[
            source.duplicated(["source_state_norm", "source_district_norm"], keep=False),
            ["source_state", "source_district"],
        ].drop_duplicates()
        raise ValueError(
            "Groundwater source workbook contains duplicate state/district rows: "
            + dupes.head(10).to_dict(orient="records").__repr__()
        )

    direct = source.merge(
        canonical_df,
        left_on=["source_state_norm", "source_district_norm"],
        right_on=["canonical_state_norm", "canonical_district_norm"],
        how="left",
        suffixes=("", "_canonical"),
    )
    direct["match_method"] = pd.Series(
        ["exact" if pd.notna(v) and str(v).strip() else "" for v in direct["district_key"]],
        index=direct.index,
        dtype="string",
    )

    if not alias_df.empty:
        source_pairs = source[["source_state_norm", "source_district_norm"]].drop_duplicates()
        alias_df = alias_df.merge(
            source_pairs,
            on=["source_state_norm", "source_district_norm"],
            how="inner",
        )
        alias_lookup = alias_df.merge(
            canonical_df,
            on=["canonical_state_norm", "canonical_district_norm"],
            how="left",
            suffixes=("_alias", "_canonical"),
        )
        bad_aliases = alias_lookup.loc[alias_lookup["district_key"].isna()].copy()
        ignorable_bad_aliases = bad_aliases.loc[
            bad_aliases["prefill_status"].fillna("").eq("needs_review")
        ].copy()
        if not ignorable_bad_aliases.empty:
            alias_lookup = alias_lookup.drop(index=ignorable_bad_aliases.index)
        fatal_bad_aliases = bad_aliases.drop(index=ignorable_bad_aliases.index, errors="ignore")
        if not fatal_bad_aliases.empty:
            raise ValueError(
                "Groundwater district alias CSV points at canonical districts that do not exist: "
                + fatal_bad_aliases[
                    ["source_state", "source_district", "canonical_state_alias", "canonical_district_alias"]
                ].head(10).to_dict(orient="records").__repr__()
            )
        direct = direct.merge(
            alias_lookup[
                [
                    "source_state_norm",
                    "source_district_norm",
                    "canonical_state_alias",
                    "canonical_district_alias",
                    "district_key",
                ]
            ].rename(
                columns={
                    "canonical_state_alias": "alias_canonical_state",
                    "canonical_district_alias": "alias_canonical_district",
                    "district_key": "alias_district_key",
                }
            ),
            on=["source_state_norm", "source_district_norm"],
            how="left",
        )
    else:
        direct["alias_canonical_state"] = pd.NA
        direct["alias_canonical_district"] = pd.NA
        direct["alias_district_key"] = pd.NA

    unresolved = direct["district_key"].isna()
    direct.loc[unresolved, "canonical_state"] = direct.loc[unresolved, "alias_canonical_state"]
    direct.loc[unresolved, "canonical_district"] = direct.loc[unresolved, "alias_canonical_district"]
    direct.loc[unresolved, "district_key"] = direct.loc[unresolved, "alias_district_key"]
    direct.loc[unresolved & direct["district_key"].notna(), "match_method"] = "alias_csv"

    matched = direct.loc[direct["district_key"].notna()].copy()
    unmatched = direct.loc[direct["district_key"].isna()].copy()

    duplicate_targets = matched.loc[
        matched.duplicated(["district_key"], keep=False),
        ["source_state", "source_district", "canonical_state", "canonical_district", "district_key", "match_method"],
    ].sort_values(["canonical_state", "canonical_district", "source_state", "source_district"])
    return matched.reset_index(drop=True), unmatched.reset_index(drop=True), duplicate_targets.reset_index(drop=True)


def _resolve_placeholder_duplicate_targets(
    matched_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Drop obvious zero/blank placeholder duplicates while preserving true conflicts."""
    if matched_df.empty:
        empty_dupes = matched_df.iloc[0:0].copy()
        return matched_df.copy(), empty_dupes, _empty_groundwater_duplicate_resolution_df()

    kept_groups: list[pd.DataFrame] = []
    unresolved_groups: list[pd.DataFrame] = []
    resolution_rows: list[dict[str, object]] = []

    for _, group in matched_df.groupby("district_key", sort=True, dropna=False):
        group = group.copy().reset_index(drop=True)
        if len(group) <= 1:
            kept_groups.append(group)
            continue

        placeholder_mask = group.apply(_is_placeholder_groundwater_row, axis=1)
        placeholder_rows = group.loc[placeholder_mask].copy()
        real_rows = group.loc[~placeholder_mask].copy()

        if len(real_rows) == 1 and not placeholder_rows.empty:
            keep_row = real_rows.iloc[0]
            safe_drop_rows: list[pd.Series] = []
            unresolved_placeholder = False
            for _, drop_row in placeholder_rows.iterrows():
                keep_match = str(keep_row.get("match_method") or "").strip().lower()
                drop_match = str(drop_row.get("match_method") or "").strip().lower()
                source_similarity = _normalized_similarity(
                    str(keep_row.get("source_district") or ""),
                    str(drop_row.get("source_district") or ""),
                )
                if keep_match == "exact" and drop_match != "exact":
                    safe_drop_rows.append(drop_row)
                    continue
                if source_similarity >= 0.90:
                    safe_drop_rows.append(drop_row)
                    continue
                unresolved_placeholder = True
                break

            if not unresolved_placeholder:
                kept_groups.append(real_rows.reset_index(drop=True))
                for _, drop_row in pd.DataFrame(safe_drop_rows).iterrows():
                    resolution_rows.append(
                        {
                            "canonical_state": keep_row["canonical_state"],
                            "canonical_district": keep_row["canonical_district"],
                            "district_key": keep_row["district_key"],
                            "kept_source_state": keep_row["source_state"],
                            "kept_source_district": keep_row["source_district"],
                            "kept_match_method": keep_row["match_method"],
                            "dropped_source_state": drop_row["source_state"],
                            "dropped_source_district": drop_row["source_district"],
                            "dropped_match_method": drop_row["match_method"],
                            "resolution_reason": "drop_zero_placeholder_duplicate",
                        }
                    )
                continue

        kept_groups.append(group)
        unresolved_groups.append(group)

    resolved_matched_df = (
        pd.concat(kept_groups, ignore_index=True)
        if kept_groups
        else matched_df.iloc[0:0].copy()
    )
    unresolved_duplicate_df = (
        pd.concat(unresolved_groups, ignore_index=True)
        if unresolved_groups
        else matched_df.iloc[0:0].copy()
    )
    resolution_df = (
        pd.DataFrame.from_records(resolution_rows).sort_values(
            ["canonical_state", "canonical_district", "dropped_source_state", "dropped_source_district"]
        ).reset_index(drop=True)
        if resolution_rows
        else _empty_groundwater_duplicate_resolution_df()
    )
    return resolved_matched_df, unresolved_duplicate_df, resolution_df


def _aggregate_bengaluru_groundwater_rows(
    matched_df: pd.DataFrame,
    aggregation_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate Bengaluru (Urban) + Bengaluru South into canonical Bengaluru (Urban)."""
    if matched_df.empty:
        return matched_df.copy(), aggregation_df.copy()

    source_keys = (
        matched_df[["source_state", "source_district"]]
        .astype("string")
        .fillna("")
        .assign(
            source_state_norm=lambda df: df["source_state"].map(_normalize_state),
            source_district_norm=lambda df: df["source_district"].map(_normalize_district),
        )
    )
    bengaluru_mask = (
        source_keys["source_state_norm"].eq(_normalize_state("KARNATAKA"))
        & source_keys["source_district_norm"].isin(
            {
                _normalize_district("Bengaluru (Urban)"),
                _normalize_district("Bengaluru South"),
            }
        )
    )
    bengaluru_rows = matched_df.loc[bengaluru_mask].copy()
    if bengaluru_rows.empty:
        return matched_df.copy(), aggregation_df.copy()

    expected_keys = {
        (_normalize_state("KARNATAKA"), _normalize_district("Bengaluru (Urban)")),
        (_normalize_state("KARNATAKA"), _normalize_district("Bengaluru South")),
    }
    actual_keys = {
        (_normalize_state(str(row["source_state"] or "")), _normalize_district(str(row["source_district"] or "")))
        for _, row in bengaluru_rows.iterrows()
    }
    if len(bengaluru_rows) != 2 or actual_keys != expected_keys or bengaluru_rows["district_key"].nunique() != 1:
        return matched_df.copy(), aggregation_df.copy()

    keep_candidates = bengaluru_rows.loc[
        bengaluru_rows["source_district"].astype("string").fillna("").eq("Bengaluru (Urban)")
    ]
    keep_row = keep_candidates.iloc[0] if not keep_candidates.empty else bengaluru_rows.iloc[0]

    extractable = pd.to_numeric(
        bengaluru_rows[GROUNDWATER_EXTRACTABLE_RESOURCE_COL],
        errors="coerce",
    ).sum(min_count=1)
    total_extraction = pd.to_numeric(
        bengaluru_rows[GROUNDWATER_TOTAL_EXTRACTION_COL],
        errors="coerce",
    ).sum(min_count=1)
    future_availability = pd.to_numeric(
        bengaluru_rows[GROUNDWATER_FUTURE_AVAILABILITY_COL],
        errors="coerce",
    ).sum(min_count=1)
    if pd.notna(extractable) and float(extractable) != 0.0 and pd.notna(total_extraction):
        stage_pct = (float(total_extraction) / float(extractable)) * 100.0
    else:
        stage_pct = pd.NA

    aggregated_row = keep_row.copy()
    aggregated_row[GROUNDWATER_EXTRACTABLE_RESOURCE_COL] = extractable
    aggregated_row[GROUNDWATER_TOTAL_EXTRACTION_COL] = total_extraction
    aggregated_row[GROUNDWATER_FUTURE_AVAILABILITY_COL] = future_availability
    aggregated_row[GROUNDWATER_STAGE_COL] = stage_pct

    resolved_matched_df = pd.concat(
        [
            matched_df.loc[~bengaluru_mask].copy(),
            pd.DataFrame([aggregated_row]),
        ],
        ignore_index=True,
    )

    bengaluru_aggregation_row = pd.DataFrame(
        [
            {
                "aggregation_rule": "bengaluru_urban_plus_south_to_urban",
                "source_state": "KARNATAKA",
                "source_districts": "Bengaluru (Urban)|Bengaluru South",
                "source_rows": "|".join(
                    str(int(v))
                    for v in sorted(
                        int(v)
                        for v in pd.to_numeric(bengaluru_rows["source_row"], errors="coerce")
                        .dropna()
                        .astype(int)
                        .tolist()
                    )
                ),
                "aggregated_source_state": str(keep_row["canonical_state"]),
                "aggregated_source_district": str(keep_row["canonical_district"]),
                GROUNDWATER_EXTRACTABLE_RESOURCE_COL: extractable,
                GROUNDWATER_TOTAL_EXTRACTION_COL: total_extraction,
                GROUNDWATER_FUTURE_AVAILABILITY_COL: future_availability,
                GROUNDWATER_STAGE_COL: stage_pct,
            }
        ]
    )
    out_aggregation_df = pd.concat(
        [aggregation_df.copy(), bengaluru_aggregation_row],
        ignore_index=True,
    )
    return resolved_matched_df.reset_index(drop=True), out_aggregation_df.reset_index(drop=True)


def build_groundwater_district_outputs(
    *,
    workbook_path: Path,
    districts_path: Path,
    qa_dir: Path,
    alias_csv_path: Path,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Build the full groundwater district outputs and QA artifacts."""
    workbook = parse_groundwater_workbook(workbook_path)
    raw_source_df = workbook.records_df.copy()
    source_df, aggregation_df = _collapse_lakshadweep_source_rows(raw_source_df)
    canonical_df = _build_canonical_districts(districts_path)
    alias_df = _load_alias_overrides(alias_csv_path)
    data_dir = get_paths_config().data_dir

    matched_df, unmatched_df, duplicate_targets_df = _resolve_groundwater_districts(
        source_df,
        canonical_df=canonical_df,
        alias_df=alias_df,
    )
    matched_df, duplicate_targets_df, duplicate_resolution_df = _resolve_placeholder_duplicate_targets(
        matched_df
    )
    matched_df, aggregation_df = _aggregate_bengaluru_groundwater_rows(
        matched_df,
        aggregation_df,
    )
    duplicate_targets_df = matched_df.loc[
        matched_df.duplicated(["district_key"], keep=False),
        [
            "source_state",
            "source_district",
            "canonical_state",
            "canonical_district",
            "district_key",
            "match_method",
        ],
    ].sort_values(["canonical_state", "canonical_district", "source_state", "source_district"])

    alias_template_df = build_groundwater_alias_template(unmatched_df, canonical_df)

    crosswalk_df = matched_df[
        [
            "source_state",
            "source_district",
            "canonical_state",
            "canonical_district",
            "district_key",
            "match_method",
        ]
    ].sort_values(["source_state", "source_district"]).reset_index(drop=True)

    if not dry_run:
        _write_csv(source_df.sort_values(["source_state", "source_district"]).reset_index(drop=True), qa_dir / "groundwater_source_extract.csv", overwrite=overwrite)
        _write_csv(aggregation_df, qa_dir / "groundwater_source_aggregations.csv", overwrite=overwrite)
        _write_csv(duplicate_resolution_df, qa_dir / "groundwater_duplicate_resolution.csv", overwrite=overwrite)
        _write_csv(crosswalk_df, qa_dir / "groundwater_district_crosswalk.csv", overwrite=overwrite)
        _write_csv(unmatched_df[["source_state", "source_district"]].drop_duplicates().sort_values(["source_state", "source_district"]).reset_index(drop=True), qa_dir / "groundwater_unmatched_districts.csv", overwrite=overwrite)
        _write_csv(duplicate_targets_df, qa_dir / "groundwater_duplicate_canonical_matches.csv", overwrite=overwrite)
        _write_csv(alias_template_df, qa_dir / "groundwater_district_alias_template.csv", overwrite=overwrite)

    if not unmatched_df.empty:
        raise ValueError(
            "Groundwater district onboarding has unmatched source districts after alias resolution. "
            f"Inspect {qa_dir / 'groundwater_unmatched_districts.csv'} and fill "
            f"{qa_dir / 'groundwater_district_alias_template.csv'} or {alias_csv_path}."
        )
    if not duplicate_targets_df.empty:
        raise ValueError(
            "Groundwater district onboarding mapped multiple source rows onto the same canonical district. "
            f"Inspect {qa_dir / 'groundwater_duplicate_canonical_matches.csv'}."
        )

    master_df = matched_df[
        [
            "canonical_state",
            "canonical_district",
            "district_key",
            *[cfg["master_col"] for cfg in GROUNDWATER_METRICS.values()],
        ]
    ].rename(columns={"canonical_state": "state", "canonical_district": "district"}).sort_values(["state", "district"]).reset_index(drop=True)

    state_counts: dict[str, int]
    if not dry_run:
        state_counts = {}
        for metric_slug in GROUNDWATER_METRICS:
            counts = _write_state_slices(
                _metric_specific_master(master_df, metric_slug=metric_slug),
                metric_slug=metric_slug,
                data_dir=data_dir,
                overwrite=overwrite,
            )
            state_counts = counts
    else:
        state_counts = (
            master_df.groupby("state", as_index=False)
            .size()
            .set_index("state")["size"]
            .astype(int)
            .to_dict()
        )

    summary_df = pd.DataFrame(
        [
            {
                "workbook_rows": int(raw_source_df.shape[0]),
                "normalized_source_rows": int(source_df.shape[0]),
                "aggregation_rows": int(aggregation_df.shape[0]),
                "duplicate_resolution_rows": int(duplicate_resolution_df.shape[0]),
                "matched_rows": int(matched_df.shape[0]),
                "unique_states": int(master_df["state"].nunique()),
                "period": GROUNDWATER_PERIOD,
                "alias_rows": int(alias_df.shape[0]),
            }
        ]
    )
    if not dry_run:
        _write_csv(summary_df, qa_dir / "groundwater_summary.csv", overwrite=overwrite)

    return {
        "raw_source_df": raw_source_df,
        "source_df": source_df,
        "aggregation_df": aggregation_df,
        "duplicate_resolution_df": duplicate_resolution_df,
        "crosswalk_df": crosswalk_df,
        "master_df": master_df,
        "summary_df": summary_df,
        "state_counts": state_counts,
        "header_map": workbook.header_map,
    }


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build canonical district groundwater masters from the 2024-2025 GEC workbook."
    )
    parser.add_argument("--workbook", type=str, default=str(_find_default_workbook()))
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_groundwater_dir()))
    parser.add_argument(
        "--district-alias-csv",
        type=str,
        default=str(_default_groundwater_dir() / "groundwater_district_aliases.csv"),
        help="Optional manual source->canonical district alias CSV.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and validate without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    workbook_path = Path(args.workbook).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()
    alias_csv_path = Path(args.district_alias_csv).expanduser().resolve()

    if not workbook_path.exists():
        raise FileNotFoundError(f"Groundwater workbook not found: {workbook_path}")
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")

    outputs = build_groundwater_district_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        alias_csv_path=alias_csv_path,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )
    summary_df = outputs["summary_df"]
    state_counts = outputs["state_counts"]
    row = summary_df.iloc[0]

    print("GROUNDWATER DISTRICT MASTERS")
    print(f"workbook: {workbook_path}")
    print(f"workbook_rows: {int(row['workbook_rows'])}")
    print(f"matched_rows: {int(row['matched_rows'])}")
    print(f"unique_states: {int(row['unique_states'])}")
    print(f"period: {row['period']}")
    print(
        "district_states: "
        + ", ".join(f"{state}:{count}" for state, count in sorted(state_counts.items())[:8])
        + (" ..." if len(state_counts) > 8 else "")
    )
    if bool(args.dry_run):
        print("dry_run: True")
    else:
        print(f"qa_dir: {qa_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
