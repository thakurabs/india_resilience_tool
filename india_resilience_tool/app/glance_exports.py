"""
Export helpers for Glance rankings answer packs.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from io import BytesIO
import re
from typing import Mapping, Optional

import pandas as pd

from india_resilience_tool.utils.naming import alias

DRIVER_UNAVAILABLE_NOTE = "Driver rows were not available for this exported scope."


def _as_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _clean_driver_label(value: object) -> str:
    text = _as_text(value)
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _format_rank(value: object) -> str:
    if pd.isna(value):
        return "unranked"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return _as_text(value) or "unranked"
    return str(int(numeric)) if numeric.is_integer() else f"{numeric:g}"


def _format_count(value: object, fallback: int) -> str:
    if pd.isna(value):
        return str(fallback)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return _as_text(value) or str(fallback)
    return str(int(numeric)) if numeric.is_integer() else f"{numeric:g}"


def _row_drivers(row: pd.Series) -> list[str]:
    return [
        _clean_driver_label(row.get(f"top_driver_{idx}"))
        for idx in range(1, 4)
        if _clean_driver_label(row.get(f"top_driver_{idx}"))
    ]


def _scope_noun(scope: str, *, plural: bool) -> str:
    singular = {"state": "state", "district": "district", "block": "block"}.get(scope, "unit")
    if not plural:
        return singular
    return {"state": "states", "district": "districts", "block": "blocks"}.get(scope, "units")


def _join_alias(value: object) -> str:
    text = re.sub(r"[\u2010-\u2015-]+", " ", str(value))
    return alias(text)


def _compound_join_alias(value: object) -> str:
    return "|".join(_join_alias(part) for part in str(value).split("|"))


def _key_series(df: pd.DataFrame, key_col: str, fallback_cols: tuple[str, ...]) -> pd.Series:
    if key_col in df.columns:
        return df[key_col].map(_compound_join_alias)
    parts = []
    for col in fallback_cols:
        if col in df.columns:
            parts.append(df[col].map(_join_alias))
        else:
            parts.append(pd.Series([""] * len(df), index=df.index, dtype=object))
    if not parts:
        return pd.Series([""] * len(df), index=df.index, dtype=object)
    out = parts[0].astype(str)
    for part in parts[1:]:
        out = out + "|" + part.astype(str)
    return out


def _driver_join_key(df: pd.DataFrame, unit_scope: str) -> pd.Series:
    if unit_scope == "state":
        return _key_series(df, "__state_key", ("state_name",))
    if unit_scope == "district":
        return _key_series(df, "__district_key", ("state_name", "district_name"))
    return _key_series(df, "__block_key", ("state_name", "district_name", "block_name"))


def _top_driver_frame(drivers: pd.DataFrame, unit_scope: str) -> pd.DataFrame:
    if drivers is None or drivers.empty or "scope_level" not in drivers.columns:
        return pd.DataFrame(columns=["__join_key"])
    scoped = drivers[drivers["scope_level"].astype(str) == unit_scope].copy()
    if scoped.empty:
        return pd.DataFrame(columns=["__join_key"])
    if "driver_rank" in scoped.columns:
        scoped["driver_rank"] = pd.to_numeric(scoped["driver_rank"], errors="coerce")
        scoped = scoped[scoped["driver_rank"].between(1, 3, inclusive="both")].copy()
    else:
        scoped["driver_rank"] = scoped.groupby(_driver_join_key(scoped, unit_scope)).cumcount() + 1
        scoped = scoped[scoped["driver_rank"] <= 3].copy()
    if scoped.empty:
        return pd.DataFrame(columns=["__join_key"])
    scoped["__join_key"] = _driver_join_key(scoped, unit_scope)
    scoped = scoped.sort_values(["__join_key", "driver_rank"], kind="stable")
    rows: dict[str, dict[str, object]] = {}
    for _, row in scoped.iterrows():
        join_key = str(row.get("__join_key", ""))
        rank = int(row.get("driver_rank"))
        rows.setdefault(join_key, {"__join_key": join_key})
        rows[join_key][f"top_driver_{rank}"] = row.get("driver_label", "")
        rows[join_key][f"top_driver_{rank}_score"] = row.get("driver_score", "")
    return pd.DataFrame(list(rows.values()))


def build_glance_export_frame(visible_rows: pd.DataFrame, drivers: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Return visible Glance rankings enriched with top-three persisted drivers."""
    if visible_rows is None or visible_rows.empty:
        return pd.DataFrame(), ""
    export = visible_rows.copy()
    unit_scope = str(export["unit_scope"].dropna().astype(str).head(1).iloc[0] or "")
    export["__join_key"] = _driver_join_key(export, unit_scope)
    driver_wide = _top_driver_frame(drivers, unit_scope)
    note = ""
    if driver_wide.empty:
        note = DRIVER_UNAVAILABLE_NOTE
    export = export.merge(driver_wide, on="__join_key", how="left")
    for idx in range(1, 4):
        for col in (f"top_driver_{idx}", f"top_driver_{idx}_score"):
            if col not in export.columns:
                export[col] = ""
    export = export.drop(columns=["__join_key"], errors="ignore")
    return export, note


def build_glance_answer_text(
    export_frame: pd.DataFrame,
    *,
    bundle_label: str,
    scenario_label: str,
    period_label: str,
    geography_label: str,
    is_projection: bool,
    driver_note: str = "",
) -> str:
    """Build short copyable prose from the currently visible Glance ranking rows."""
    if export_frame is None or export_frame.empty:
        return "No Glance ranking rows are visible for the current selection."
    scope = str(export_frame["unit_scope"].iloc[0])
    noun = _scope_noun(scope, plural=True)
    timing = f"under {scenario_label}, {period_label}" if is_projection else f"for {period_label}"
    top = export_frame.sort_values("rank", kind="stable").head(3)
    names = ", ".join(_as_text(value) for value in top["unit_name"].tolist() if _as_text(value))
    focus_rows = pd.DataFrame()
    if "is_current_focus" in export_frame.columns:
        focus_mask = export_frame["is_current_focus"].fillna(False).astype(bool)
        focus_rows = export_frame[focus_mask].sort_values("rank", kind="stable").head(1)
    if not focus_rows.empty:
        focus = focus_rows.iloc[0]
        comparison_group = _as_text(focus.get("comparison_group")) or geography_label
        comparison_count = _format_count(focus.get("comparison_count"), len(export_frame))
        focus_name = _as_text(focus.get("unit_name")) or "The selected unit"
        score = _as_text(focus.get("bundle_score_display")) or _as_text(focus.get("bundle_score")) or "unavailable"
        band = _as_text(focus.get("score_band"))
        band_clause = f" and {band} risk band" if band else ""
        answer = (
            f"For {comparison_group} {_scope_noun(scope, plural=False)} rankings, "
            f"{focus_name} is the current focus. It ranks "
            f"{_format_rank(focus.get('rank'))} / {comparison_count} for {bundle_label} {timing}, "
            f"with a bundle score of {score}{band_clause}."
        )
        drivers = _row_drivers(focus)
        if drivers:
            answer += f" Its leading drivers are {', '.join(drivers)}."
        if names:
            answer += f" The highest-ranked {noun} in the visible table are {names}."
    else:
        answer = (
            f"For {geography_label}, the visible Glance ranking for {bundle_label} {timing} "
            f"shows the highest-ranked {noun} as: {names}."
        )
        first = top.iloc[0]
        drivers = _row_drivers(first)
        if drivers:
            answer += f" The leading drivers for {first.get('unit_name')} are {', '.join(drivers)}."
    if driver_note:
        answer += f" {driver_note}"
    return answer


def build_glance_csv_bytes(export_frame: pd.DataFrame) -> bytes:
    """Return UTF-8-SIG CSV bytes for the currently visible Glance rankings."""
    return export_frame.to_csv(index=False).encode("utf-8-sig")


def build_glance_answer_pack_xlsx(
    *,
    answer_text: str,
    export_frame: pd.DataFrame,
    metadata: Mapping[str, object],
    driver_note: str = "",
) -> bytes:
    """Return an Excel answer pack for currently visible Glance scopes only."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame({"answer": [answer_text]}).to_excel(writer, sheet_name="Answer", index=False)
        export_frame.to_excel(writer, sheet_name="Ranking", index=False)
        driver_cols = [
            col
            for col in export_frame.columns
            if col.startswith("top_driver_") or col in {"unit_name", "unit_scope", "rank"}
        ]
        export_frame[driver_cols].to_excel(writer, sheet_name="Drivers", index=False)
        meta_rows = [{"field": key, "value": value} for key, value in metadata.items()]
        if driver_note:
            meta_rows.append({"field": "driver_note", "value": driver_note})
        pd.DataFrame(meta_rows).to_excel(writer, sheet_name="Metadata", index=False)
        pd.DataFrame(
            {
                "note": [
                    "Scores and ranks are read from persisted optimized Glance artifacts.",
                    "Exports include only rows currently visible in the Glance Rankings table.",
                    "Higher bundle scores indicate higher hazard signal unless the source artifact states otherwise.",
                    "Missing values remain blank; rankings use persisted ranks when available.",
                ]
            }
        ).to_excel(writer, sheet_name="Method Notes", index=False)
    return output.getvalue()


def glance_export_filename(
    *,
    kind: str,
    bundle_slug: str,
    unit_scope: str,
    scenario: str,
    period: str,
    geography: str,
    band_filter: Optional[str] = None,
) -> str:
    """Build a stable sanitized Glance export filename."""
    def token(value: object) -> str:
        text = re.sub(r"[^A-Za-z0-9]+", "_", _as_text(value).lower()).strip("_")
        return text or "all"

    parts = ["irt_glance"]
    if kind == "xlsx":
        parts.append("answer_pack")
    parts.extend([token(bundle_slug), token(unit_scope), token(scenario), token(period), token(geography)])
    if band_filter:
        parts.append(token(band_filter))
    suffix = "xlsx" if kind == "xlsx" else "csv"
    return "_".join(parts) + f".{suffix}"
