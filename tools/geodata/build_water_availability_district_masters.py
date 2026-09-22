#!/usr/bin/env python3
"""
Build canonical district water-scarcity masters from the NITI Aayog ICED
*Per Capita Water Availability, 2025 & 2050* workbook.

The source is an ordinal, 4-class per-district water-scarcity dataset with a
present-day (2025) class and a 2050 projection. This tool parses the workbook,
encodes each class to an integer code ``1..4`` (higher = worse), reconciles the
source ``(state, district)`` grain onto the canonical IRT district layer via a
curated state/district alias table plus a worst-class collision rule, left-joins
onto the full canonical roster (so every district appears, NaN where no source),
computes a 2050-minus-2025 deterioration delta, and writes one state-sliced
district master CSV per onboarded water-scarcity metric.

The composite (``composite_water_risk``) later scores the present-day class with
an absolute pre-scaled ordinal mapping; this builder only produces the class-code
masters and QA artifacts.

Design mirrors ``build_groundwater_district_masters.py`` (canonical reconciliation,
fail-fast, QA CSVs) but the source is a simple single-sheet xlsx rather than the
multi-row GEC header.
"""

from __future__ import annotations

import argparse
import difflib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from india_resilience_tool.utils.naming import normalize_name
from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import load_district_boundaries


DEFAULT_WORKBOOK_NAME = "Per_Capita_Water_Availability_2025_&_2050_1779792004085.xlsx"
WORKBOOK_SHEET_NAME = "Water Availability"
WATER_PERIOD_TOKEN = "Current"

STATE_COL = "State"
DISTRICT_COL = "District"
VALUE_2025_COL = "Value for 2025"
VALUE_2050_COL = "Value for 2050"

# Ordinal class -> integer code (higher = worse). Keyed on the descriptor prefix
# (text before the "[...]" magnitude bracket), so it is robust to the Unicode
# minus (U+2212) used inside the bracket ranges.
CLASS_LABEL_TO_CODE: dict[str, int] = {
    "no stress": 1,
    "stress": 2,
    "scarcity": 3,
    "absolute scarcity": 4,
}

# Onboarded metric slugs and their static-snapshot master columns.
SCARCITY_2025_SLUG = "water_scarcity_percapita"
SCARCITY_2050_SLUG = "water_scarcity_percapita_2050"
DETERIORATION_SLUG = "water_scarcity_deterioration_2050"


def _master_col(slug: str) -> str:
    """Return the canonical static-snapshot master column for one metric slug."""
    return f"{slug}__snapshot__{WATER_PERIOD_TOKEN}__mean"


SCARCITY_2025_COL = _master_col(SCARCITY_2025_SLUG)
SCARCITY_2050_COL = _master_col(SCARCITY_2050_SLUG)
DETERIORATION_COL = _master_col(DETERIORATION_SLUG)

WATER_METRIC_COLUMNS: dict[str, str] = {
    SCARCITY_2025_SLUG: SCARCITY_2025_COL,
    SCARCITY_2050_SLUG: SCARCITY_2050_COL,
    DETERIORATION_SLUG: DETERIORATION_COL,
}

# State aliases (NITI spaced-normalized form -> canonical spaced-normalized form).
# ``normalize_name`` drops "&" but keeps the word "and", so the combined UTs and
# J&K need explicit aliases. Applied before space-compaction (see _normalize_state).
STATE_ALIASES: dict[str, str] = {
    "jammu and kashmir": "jammu kashmir",
    "dadra and nagar haveli": "dadra nagar haveli daman diu",
    "daman and diu": "dadra nagar haveli daman diu",
}

# Curated district aliases keyed by (source NITI state, source NITI district) ->
# canonical district display name. Normalized in code (see _build_district_alias_map).
# J&K / Dadra-Nagar-Haveli-Daman-Diu districts need no district alias: once the
# state alias aligns them, their district names match canonical exactly.
DISTRICT_ALIASES: dict[tuple[str, str], str] = {
    # Andhra Pradesh
    ("Andhra Pradesh", "Ysr Kadapa"): "Y.S.R.",
    ("Andhra Pradesh", "Ananthapuramu"): "Anantapur",
    ("Andhra Pradesh", "Visakhapatnam"): "Visakhapatanam",
    ("Andhra Pradesh", "Sri Potti Sriramulu Nellore"): "Spsr Nellore",
    # Assam
    ("Assam", "Sribhumi"): "Karimganj",
    # Chhattisgarh
    ("Chhattisgarh", "Kabeerdham"): "Kabirdham",
    ("Chhattisgarh", "Uttar Bastar Kanker"): "Kanker",
    ("Chhattisgarh", "Dakshin Bastar Dantewada"): "Dantewada",
    ("Chhattisgarh", "Balrampur-Ramanujganj"): "Balrampur",
    ("Chhattisgarh", "Balodabazar-Bhatapara"): "Baloda Bazar",
    # Delhi (NITI appends " Delhi" to the compass-quadrant district names)
    ("Delhi", "North East Delhi"): "North East",
    ("Delhi", "South Delhi"): "South",
    ("Delhi", "North Delhi"): "North",
    ("Delhi", "North West Delhi"): "North West",
    ("Delhi", "East Delhi"): "East",
    ("Delhi", "South West Delhi"): "South West",
    ("Delhi", "South East Delhi"): "South East",
    ("Delhi", "West Delhi"): "West",
    ("Delhi", "Central Delhi"): "Central",
    # Gujarat
    ("Gujarat", "Dahod"): "Dohad",
    ("Gujarat", "Dangs"): "Dang",
    ("Gujarat", "Ahmedabad"): "Ahmadabad",
    # Haryana
    ("Haryana", "Mewat"): "Nuh",
    ("Haryana", "Charkhi Dadri"): "Charki Dadri",
    # Himachal Pradesh
    ("Himachal Pradesh", "Lahaul and Spiti"): "Lahul and Spiti",
    # Jharkhand
    ("Jharkhand", "Sahibganj"): "Sahebganj",
    ("Jharkhand", "East-Singhbhum"): "East Singhbum",
    # Karnataka (Bengaluru South collides onto canonical Bengaluru Urban)
    ("Karnataka", "Bijapur"): "Vijayapura",
    ("Karnataka", "Davanagere"): "Davangere",
    ("Karnataka", "Bengaluru South"): "Bengaluru Urban",
    ("Karnataka", "Chamarajanagar"): "Chamarajanagara",
    # Madhya Pradesh
    ("Madhya Pradesh", "Khandwa (East Nimar)"): "East Nimar",
    ("Madhya Pradesh", "Narsimhapur"): "Narsinghpur",
    ("Madhya Pradesh", "Khargone (West Nimar)"): "Khargone",
    # Maharashtra (renamed districts)
    ("Maharashtra", "Chhatrapati Sambhajinagar"): "Aurangabad",
    ("Maharashtra", "Ahilyanagar"): "Ahmednagar",
    ("Maharashtra", "Dharashiv"): "Osmanabad",
    # Mizoram
    ("Mizoram", "Siaha"): "Saiha",
    # Puducherry
    ("Puducherry", "Puducherry"): "Pondicherry",
    # Sikkim (NITI uses the legacy 4 directional districts; Pakyong/Soreng are
    # newer splits and remain no-source coverage gaps)
    ("Sikkim", "North Sikkim"): "Mangan",
    ("Sikkim", "East Sikkim"): "Gangtok",
    ("Sikkim", "South Sikkim"): "Namchi",
    ("Sikkim", "West Sikkim"): "Gyalshing",
    # Tamil Nadu
    ("Tamil Nadu", "Thoothukkudi"): "Tuticorin",
    ("Tamil Nadu", "Viluppuram"): "Villupuram",
    ("Tamil Nadu", "Kancheepuram"): "Kanchipuram",
    # Telangana (Warangal split)
    ("Telangana", "Warangal Rural"): "Warangal",
    ("Telangana", "Warangal Urban"): "Hanumakonda",
    # Uttar Pradesh
    ("Uttar Pradesh", "Shrawasti"): "Shravasti",
    ("Uttar Pradesh", "Sant Kabir Nagar"): "Sant Kabeer Nagar",
    ("Uttar Pradesh", "Mahrajganj"): "Maharajganj",
    ("Uttar Pradesh", "Sant Ravidas Nagar"): "Bhadohi",
    # Uttarakhand
    ("Uttarakhand", "Udham Singh Nagar"): "Udam Singh Nagar",
    # West Bengal
    ("West Bengal", "Purba Medinipur"): "Medinipur East",
    ("West Bengal", "Uttar Dinajpur"): "Dinajpur Uttar",
    ("West Bengal", "South 24 Parganas"): "24 Paraganas South",
    ("West Bengal", "Paschim Medinipur"): "Medinipur West",
    ("West Bengal", "Dakshin Dinajpur"): "Dinajpur Dakshin",
    ("West Bengal", "Malda"): "Maldah",
    ("West Bengal", "North 24 Parganas"): "24 Paraganas North",
}


@dataclass(frozen=True)
class WaterReconciliationResult:
    """Full reconciliation output plus QA frames and split coverage counters."""

    master_df: pd.DataFrame
    crosswalk_df: pd.DataFrame
    unmatched_df: pd.DataFrame
    alias_template_df: pd.DataFrame
    duplicate_targets_df: pd.DataFrame
    collisions_df: pd.DataFrame
    no_source_df: pd.DataFrame
    monotonicity_df: pd.DataFrame
    summary_df: pd.DataFrame


def _find_default_workbook() -> Path:
    data_dir = get_paths_config().data_dir
    direct = data_dir / DEFAULT_WORKBOOK_NAME
    if direct.exists():
        return direct
    candidates = sorted(data_dir.glob("Per_Capita_Water_Availability*.xlsx"))
    if candidates:
        return candidates[-1]
    return direct


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "water_availability" / "qa"


def _normalize_state(value: object) -> str:
    norm = normalize_name(value if value is not None else "")
    return STATE_ALIASES.get(norm, norm).replace(" ", "")


def _normalize_district(value: object) -> str:
    return normalize_name(value if value is not None else "").replace(" ", "")


def _class_to_code(value: object) -> int:
    """Map one ordinal class string to its integer code (raise if unrecognized)."""
    raw = str(value if value is not None else "").strip()
    label = raw.split("[", 1)[0].strip().lower()
    code = CLASS_LABEL_TO_CODE.get(label)
    if code is None:
        raise ValueError(
            f"Unrecognized water-scarcity class string: {raw!r} "
            f"(parsed label {label!r}); expected one of {sorted(CLASS_LABEL_TO_CODE)}."
        )
    return int(code)


def _build_district_alias_map() -> dict[tuple[str, str], str]:
    """Return DISTRICT_ALIASES keyed by (state_norm, source_district_norm) -> canonical_district_norm."""
    out: dict[tuple[str, str], str] = {}
    for (src_state, src_district), canonical_district in DISTRICT_ALIASES.items():
        key = (_normalize_state(src_state), _normalize_district(src_district))
        out[key] = _normalize_district(canonical_district)
    return out


def parse_water_workbook(workbook_path: Path) -> pd.DataFrame:
    """Parse the water-availability workbook into a normalized source dataframe."""
    df = pd.read_excel(workbook_path, sheet_name=WORKBOOK_SHEET_NAME, header=0)
    expected = {STATE_COL, DISTRICT_COL, VALUE_2025_COL, VALUE_2050_COL}
    missing = sorted(expected - set(df.columns))
    if missing:
        raise ValueError(
            f"Water-availability workbook is missing required columns {missing}: {workbook_path}"
        )

    df = df.dropna(how="all").copy()
    df[STATE_COL] = df[STATE_COL].astype("string").str.strip()
    df[DISTRICT_COL] = df[DISTRICT_COL].astype("string").str.strip()
    # Drop footer rows (Copyright / URL / disclaimer live in the State column with
    # blank district/value cells) and any row lacking a district or class value.
    df = df.loc[
        df[STATE_COL].notna()
        & df[STATE_COL].ne("")
        & df[DISTRICT_COL].notna()
        & df[DISTRICT_COL].ne("")
        & df[VALUE_2025_COL].notna()
        & df[VALUE_2050_COL].notna()
    ].copy()
    if df.empty:
        raise ValueError(f"Water-availability workbook has no usable data rows: {workbook_path}")

    df["code_2025"] = df[VALUE_2025_COL].map(_class_to_code).astype(int)
    df["code_2050"] = df[VALUE_2050_COL].map(_class_to_code).astype(int)
    out = df.rename(columns={STATE_COL: "source_state", DISTRICT_COL: "source_district"})[
        ["source_state", "source_district", "code_2025", "code_2050"]
    ].reset_index(drop=True)

    if out.duplicated(["source_state", "source_district"]).any():
        dupes = out.loc[
            out.duplicated(["source_state", "source_district"], keep=False),
            ["source_state", "source_district"],
        ].drop_duplicates()
        raise ValueError(
            "Water-availability workbook contains duplicate source (state, district) rows: "
            + dupes.head(10).to_dict(orient="records").__repr__()
        )
    return out


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


def _resolve_sources(
    source_df: pd.DataFrame,
    *,
    canonical_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve source rows onto canonical districts via normalization + aliases."""
    alias_map = _build_district_alias_map()
    canonical_lookup = canonical_df.set_index(
        ["canonical_state_norm", "canonical_district_norm"]
    )

    source = source_df.copy()
    source["source_state_norm"] = source["source_state"].map(_normalize_state)
    source["source_district_norm"] = source["source_district"].map(_normalize_district)

    resolved_rows: list[dict[str, object]] = []
    unmatched_rows: list[dict[str, object]] = []
    for row in source.itertuples(index=False):
        state_norm = row.source_state_norm
        alias_target = alias_map.get((state_norm, row.source_district_norm))
        district_norm = alias_target if alias_target is not None else row.source_district_norm
        match_method = "alias" if alias_target is not None else "exact"
        key = (state_norm, district_norm)
        if key in canonical_lookup.index:
            canonical = canonical_lookup.loc[key]
            if isinstance(canonical, pd.DataFrame):  # defensive: ambiguous canonical key
                canonical = canonical.iloc[0]
            resolved_rows.append(
                {
                    "source_state": row.source_state,
                    "source_district": row.source_district,
                    "canonical_state": canonical["canonical_state"],
                    "canonical_district": canonical["canonical_district"],
                    "district_key": canonical["district_key"],
                    "match_method": match_method,
                    "code_2025": int(row.code_2025),
                    "code_2050": int(row.code_2050),
                }
            )
        else:
            unmatched_rows.append(
                {
                    "source_state": row.source_state,
                    "source_district": row.source_district,
                    "source_state_norm": state_norm,
                    "source_district_norm": row.source_district_norm,
                }
            )

    matched_df = pd.DataFrame.from_records(
        resolved_rows,
        columns=[
            "source_state",
            "source_district",
            "canonical_state",
            "canonical_district",
            "district_key",
            "match_method",
            "code_2025",
            "code_2050",
        ],
    )
    unmatched_df = pd.DataFrame.from_records(
        unmatched_rows,
        columns=["source_state", "source_district", "source_state_norm", "source_district_norm"],
    )
    return matched_df.reset_index(drop=True), unmatched_df.reset_index(drop=True)


def _aggregate_worst_class_collisions(
    matched_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse multiple source rows onto one canonical district by worst (max) class.

    Applied independently to 2025 and 2050 codes, before the duplicate-target
    check. Each collapsed group is logged. Monotonicity (2050 >= 2025) is
    preserved because ``max`` is applied per year over rows that are each monotone.
    """
    if matched_df.empty:
        return matched_df.copy(), pd.DataFrame(
            columns=[
                "canonical_state",
                "canonical_district",
                "district_key",
                "source_districts",
                "n_source_rows",
                "code_2025",
                "code_2050",
            ]
        )

    kept_rows: list[pd.Series] = []
    collision_rows: list[dict[str, object]] = []
    for _, group in matched_df.groupby("district_key", sort=True, dropna=False):
        group = group.reset_index(drop=True)
        if len(group) == 1:
            kept_rows.append(group.iloc[0])
            continue
        worst_2025 = int(group["code_2025"].max())
        worst_2050 = int(group["code_2050"].max())
        keep = group.iloc[0].copy()
        keep["code_2025"] = worst_2025
        keep["code_2050"] = worst_2050
        keep["match_method"] = "worst_class_collision"
        kept_rows.append(keep)
        collision_rows.append(
            {
                "canonical_state": keep["canonical_state"],
                "canonical_district": keep["canonical_district"],
                "district_key": keep["district_key"],
                "source_districts": "|".join(
                    sorted(str(v) for v in group["source_district"].tolist())
                ),
                "n_source_rows": int(len(group)),
                "code_2025": worst_2025,
                "code_2050": worst_2050,
            }
        )

    resolved_df = pd.DataFrame(kept_rows).reset_index(drop=True)
    collisions_df = pd.DataFrame.from_records(
        collision_rows,
        columns=[
            "canonical_state",
            "canonical_district",
            "district_key",
            "source_districts",
            "n_source_rows",
            "code_2025",
            "code_2050",
        ],
    )
    return resolved_df, collisions_df


def _build_alias_template(
    unmatched_df: pd.DataFrame,
    canonical_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a reviewable alias template with state-scoped fuzzy suggestions."""
    columns = [
        "source_state",
        "source_district",
        "suggested_canonical_district_1",
        "suggested_canonical_district_2",
        "suggested_canonical_district_3",
    ]
    if unmatched_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []
    for row in unmatched_df.itertuples(index=False):
        candidates = canonical_df.loc[
            canonical_df["canonical_state_norm"] == row.source_state_norm
        ]
        near = difflib.get_close_matches(
            row.source_district_norm,
            candidates["canonical_district_norm"].tolist(),
            n=3,
            cutoff=0.5,
        )
        names = [
            str(candidates.loc[candidates["canonical_district_norm"] == n, "canonical_district"].iloc[0])
            for n in near
        ]
        rows.append(
            {
                "source_state": row.source_state,
                "source_district": row.source_district,
                "suggested_canonical_district_1": names[0] if len(names) > 0 else "",
                "suggested_canonical_district_2": names[1] if len(names) > 1 else "",
                "suggested_canonical_district_3": names[2] if len(names) > 2 else "",
            }
        )
    return pd.DataFrame.from_records(rows, columns=columns).sort_values(
        ["source_state", "source_district"]
    ).reset_index(drop=True)


def reconcile_water_availability(
    *,
    source_df: pd.DataFrame,
    canonical_df: pd.DataFrame,
    allow_unmatched: bool,
    qa_dir: Path,
) -> WaterReconciliationResult:
    """Resolve, collision-aggregate, roster-join, and QA the water-scarcity source."""
    matched_df, unmatched_df = _resolve_sources(source_df, canonical_df=canonical_df)
    matched_df, collisions_df = _aggregate_worst_class_collisions(matched_df)
    alias_template_df = _build_alias_template(unmatched_df, canonical_df)

    duplicate_targets_df = matched_df.loc[
        matched_df.duplicated(["district_key"], keep=False),
        ["source_state", "source_district", "canonical_state", "canonical_district", "district_key"],
    ].sort_values(["canonical_state", "canonical_district"]).reset_index(drop=True)

    monotonicity_df = matched_df.loc[
        matched_df["code_2050"] < matched_df["code_2025"],
        ["canonical_state", "canonical_district", "district_key", "code_2025", "code_2050"],
    ].reset_index(drop=True)

    n_source = int(source_df.shape[0])
    n_resolved = int(source_df.shape[0] - unmatched_df.shape[0])

    # Fail-fast gates (source resolution + integrity), unless --allow-unmatched.
    if not unmatched_df.empty and not allow_unmatched:
        raise ValueError(
            f"Water-availability onboarding has {unmatched_df.shape[0]} unmatched source district(s) "
            f"after alias resolution. Inspect {qa_dir / 'water_unmatched_source.csv'} and fill "
            f"{qa_dir / 'water_district_alias_template.csv'}."
        )
    if not duplicate_targets_df.empty:
        raise ValueError(
            "Water-availability onboarding mapped multiple source rows onto the same canonical "
            f"district after worst-class aggregation. Inspect {qa_dir / 'water_duplicate_targets.csv'}."
        )
    if not monotonicity_df.empty:
        raise ValueError(
            f"Water-availability source violates monotone degradation (2050 class better than 2025) for "
            f"{monotonicity_df.shape[0]} district(s). Inspect {qa_dir / 'water_monotonicity_violations.csv'}."
        )

    # Left-join onto the full canonical roster so every district appears.
    resolved_values = matched_df[
        ["district_key", "code_2025", "code_2050"]
    ].drop_duplicates("district_key")
    roster = canonical_df[["canonical_state", "canonical_district", "district_key"]].copy()
    master = roster.merge(resolved_values, on="district_key", how="left")
    master[DETERIORATION_COL] = master["code_2050"] - master["code_2025"]
    master = master.rename(
        columns={
            "canonical_state": "state",
            "canonical_district": "district",
            "code_2025": SCARCITY_2025_COL,
            "code_2050": SCARCITY_2050_COL,
        }
    )[["state", "district", "district_key", SCARCITY_2025_COL, SCARCITY_2050_COL, DETERIORATION_COL]]
    master = master.sort_values(["state", "district"]).reset_index(drop=True)

    no_source_df = master.loc[
        master[SCARCITY_2025_COL].isna(), ["state", "district", "district_key"]
    ].reset_index(drop=True)

    crosswalk_df = matched_df[
        ["source_state", "source_district", "canonical_state", "canonical_district", "district_key", "match_method"]
    ].sort_values(["source_state", "source_district"]).reset_index(drop=True)

    n_canonical = int(master.shape[0])
    n_with_source = int(master[SCARCITY_2025_COL].notna().sum())
    summary_df = pd.DataFrame(
        [
            {
                "source_rows_total": n_source,
                "source_rows_resolved": n_resolved,
                "canonical_rows_total": n_canonical,
                "canonical_rows_with_source": n_with_source,
                "unmatched_source_rows": int(unmatched_df.shape[0]),
                "collision_groups": int(collisions_df.shape[0]),
                "no_source_canonical_rows": int(no_source_df.shape[0]),
                "period": WATER_PERIOD_TOKEN,
            }
        ]
    )

    return WaterReconciliationResult(
        master_df=master,
        crosswalk_df=crosswalk_df,
        unmatched_df=unmatched_df[["source_state", "source_district"]].reset_index(drop=True),
        alias_template_df=alias_template_df,
        duplicate_targets_df=duplicate_targets_df,
        collisions_df=collisions_df,
        no_source_df=no_source_df,
        monotonicity_df=monotonicity_df,
        summary_df=summary_df,
    )


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    """Write a master CSV plus a Parquet companion for faster runtime reads."""
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(p) for p in (path, parquet_path) if p.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}"
            )
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


def _write_state_slices(
    master_df: pd.DataFrame,
    *,
    metric_slug: str,
    data_dir: Path,
    overwrite: bool,
) -> dict[str, int]:
    """Write one district master per state under the metric's portfolio processed root."""
    metric_col = WATER_METRIC_COLUMNS[metric_slug]
    metric_master = master_df[["state", "district", "district_key", metric_col]].copy()
    processed_root = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
    out_name = get_master_csv_filename("district")
    counts: dict[str, int] = {}
    for state_name, state_df in metric_master.groupby("state", dropna=False, as_index=False):
        state_label = str(state_name or "").strip()
        if not state_label:
            raise ValueError(f"Water-availability master has an empty state value for {metric_slug}.")
        out_path = processed_root / state_label / out_name
        _write_master_table(state_df.reset_index(drop=True), out_path, overwrite=overwrite)
        counts[state_label] = int(state_df.shape[0])
    return counts


def _write_qa(result: WaterReconciliationResult, *, qa_dir: Path, overwrite: bool) -> None:
    _write_csv(result.crosswalk_df, qa_dir / "water_district_crosswalk.csv", overwrite=overwrite)
    _write_csv(result.unmatched_df, qa_dir / "water_unmatched_source.csv", overwrite=overwrite)
    _write_csv(result.alias_template_df, qa_dir / "water_district_alias_template.csv", overwrite=overwrite)
    _write_csv(result.duplicate_targets_df, qa_dir / "water_duplicate_targets.csv", overwrite=overwrite)
    _write_csv(result.collisions_df, qa_dir / "water_collision_aggregations.csv", overwrite=overwrite)
    _write_csv(result.no_source_df, qa_dir / "water_no_source_canonical.csv", overwrite=overwrite)
    _write_csv(result.monotonicity_df, qa_dir / "water_monotonicity_violations.csv", overwrite=overwrite)
    _write_csv(result.summary_df, qa_dir / "water_summary.csv", overwrite=overwrite)


def build_water_availability_outputs(
    *,
    workbook_path: Path,
    districts_path: Path,
    qa_dir: Path,
    overwrite: bool,
    dry_run: bool,
    allow_unmatched: bool,
) -> WaterReconciliationResult:
    """Build the full water-scarcity district outputs and QA artifacts."""
    source_df = parse_water_workbook(workbook_path)
    canonical_df = _build_canonical_districts(districts_path)
    result = reconcile_water_availability(
        source_df=source_df,
        canonical_df=canonical_df,
        allow_unmatched=allow_unmatched,
        qa_dir=qa_dir,
    )

    if not dry_run:
        _write_qa(result, qa_dir=qa_dir, overwrite=overwrite)
        data_dir = get_paths_config().data_dir
        for metric_slug in WATER_METRIC_COLUMNS:
            _write_state_slices(
                result.master_df,
                metric_slug=metric_slug,
                data_dir=data_dir,
                overwrite=overwrite,
            )
    return result


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build canonical district water-scarcity masters from the NITI per-capita water workbook."
    )
    parser.add_argument("--workbook", type=str, default=str(_find_default_workbook()))
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_qa_dir()))
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and validate without writing files.")
    parser.add_argument(
        "--allow-unmatched",
        action="store_true",
        help="Downgrade unmatched source districts from a fail-fast error to a warning.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    workbook_path = Path(args.workbook).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()

    if not workbook_path.exists():
        raise FileNotFoundError(f"Water-availability workbook not found: {workbook_path}")
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")

    result = build_water_availability_outputs(
        workbook_path=workbook_path,
        districts_path=districts_path,
        qa_dir=qa_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        allow_unmatched=bool(args.allow_unmatched),
    )
    row = result.summary_df.iloc[0]

    print("WATER AVAILABILITY DISTRICT MASTERS")
    print(f"workbook: {workbook_path}")
    print(f"source_rows_resolved: {int(row['source_rows_resolved'])}/{int(row['source_rows_total'])}")
    print(f"canonical_rows_with_source: {int(row['canonical_rows_with_source'])}/{int(row['canonical_rows_total'])}")
    print(f"unmatched_source_rows: {int(row['unmatched_source_rows'])}")
    print(f"collision_groups: {int(row['collision_groups'])}")
    print(f"no_source_canonical_rows: {int(row['no_source_canonical_rows'])}")
    print(f"period: {row['period']}")
    if bool(args.dry_run):
        print("dry_run: True")
    else:
        print(f"qa_dir: {qa_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
