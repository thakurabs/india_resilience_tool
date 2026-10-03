"""Audit thematic bundle calculation completeness against docs and processed files.

The tool is intentionally non-destructive. It cross-checks the active thematic
dashboard bundles against:

- `docs/bundle_calculation_audit.md` section coverage
- configured bundle/component relationships
- available component master files under `processed/<metric_slug>/...`
- available composite master files under `processed/<composite_slug>/...`
- scenario/period pair parity between source thematic inputs and persisted
  thematic composites

This is useful on operator machines where the processed data exists but Codex is
not available. The report is designed to answer whether the thematic bundle
calculations appear complete for the processed scope currently on disk.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.config.dashboard_bundles import THEMATIC_DASHBOARD_BUNDLES
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import find_repo_root, get_paths_config
from india_resilience_tool.data.master_columns import resolve_metric_column
from india_resilience_tool.data.master_loader import normalize_master_columns, resolve_preferred_master_path

SUPPORTED_SCENARIOS = ("historical", "ssp245", "ssp585", "snapshot")
SUPPORTED_PERIODS = ("1990-2010", "2020-2040", "2040-2060", "2060-2080", "Current")
SUPPORTED_STAT = "mean"
MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
}


@dataclass(frozen=True)
class AuditDocSection:
    """Parsed audit-doc metadata for one numbered section."""

    section_number: int
    title: str
    is_placeholder: bool
    has_bundle_definition: bool


@dataclass(frozen=True)
class BundleStateAuditRow:
    """Flat audit result for one thematic bundle + level + state."""

    bundle_name: str
    selector_label: str
    composite_slug: str
    level: str
    state: str
    status: str
    doc_section_present: bool
    doc_section_placeholder: bool
    required_source_metrics: tuple[str, ...]
    scoring_metrics: tuple[str, ...]
    attribute_metrics: tuple[str, ...]
    missing_source_metrics: tuple[str, ...]
    missing_attribute_metrics: tuple[str, ...]
    source_shared_pairs: tuple[str, ...]
    composite_pairs: tuple[str, ...]
    missing_composite_pairs: tuple[str, ...]
    extra_composite_pairs: tuple[str, ...]


def _pair_label(scenario: str, period: str) -> str:
    return f"{scenario}:{period}"


def _base_processed_root(*, processed_root: Path | None, data_dir: Path | None) -> Path:
    if processed_root is not None:
        return processed_root.resolve()
    if data_dir is not None:
        return (data_dir / "processed").resolve()
    return get_paths_config().base_output_root.resolve()


def _slug_root(slug: str, *, processed_root: Path | None, data_dir: Path | None) -> Path:
    return _base_processed_root(processed_root=processed_root, data_dir=data_dir) / slug


def _candidate_states_for_slug_root(slug_root: Path) -> list[str]:
    return list_available_states_from_processed_root(str(slug_root))


def _collect_candidate_states(
    *,
    component_metric_slugs: Iterable[str],
    composite_slug: str,
    processed_root: Path | None,
    data_dir: Path | None,
) -> list[str]:
    states: set[str] = set()
    for slug in tuple(component_metric_slugs) + (composite_slug,):
        states.update(_candidate_states_for_slug_root(_slug_root(slug, processed_root=processed_root, data_dir=data_dir)))
    return sorted(states)


def _section_is_placeholder(body: str) -> bool:
    lowered = body.lower()
    if "placeholder" in lowered:
        return True
    return "###" not in body


def parse_bundle_calculation_audit(doc_path: Path) -> dict[str, AuditDocSection]:
    """Parse numbered bundle sections from `docs/bundle_calculation_audit.md`."""
    text = doc_path.read_text(encoding="utf-8")
    pattern = re.compile(r"^##\s+(?P<num>\d+)\.\s+(?P<title>.+?)\s*$", flags=re.MULTILINE)
    matches = list(pattern.finditer(text))
    sections: dict[str, AuditDocSection] = {}
    for idx, match in enumerate(matches):
        title = match.group("title").strip()
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        body = text[start:end]
        sections[title] = AuditDocSection(
            section_number=int(match.group("num")),
            title=title,
            is_placeholder=_section_is_placeholder(body),
            has_bundle_definition="Bundle Definition" in body,
        )
    return sections


def _preferred_master_path(
    slug: str,
    *,
    level: str,
    state: str,
    processed_root: Path | None,
    data_dir: Path | None,
) -> Path:
    csv_path = _slug_root(slug, processed_root=processed_root, data_dir=data_dir) / state / MASTER_FILENAMES[level]
    return resolve_preferred_master_path(csv_path)


def _read_header_columns(path: Path) -> list[str]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq
        except Exception:
            return list(pd.read_parquet(path).columns)
        return list(pq.read_schema(path).names)
    frame = pd.read_csv(path, nrows=0)
    frame = normalize_master_columns(frame)
    return [str(col) for col in frame.columns]


def _available_pairs_for_metric(columns: Sequence[str], *, metric_slug: str) -> tuple[str, ...]:
    registry_spec = METRICS_BY_SLUG[metric_slug]
    candidates = []
    for candidate in (registry_spec.periods_metric_col, registry_spec.value_col, metric_slug):
        value = str(candidate or "").strip()
        if value and value not in candidates:
            candidates.append(value)

    pairs: list[str] = []
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            resolved = None
            for candidate in candidates:
                resolved = resolve_metric_column(columns, candidate, scenario, period, SUPPORTED_STAT)
                if resolved:
                    break
            if resolved and f"__{period.lower()}__" in resolved.lower():
                pairs.append(_pair_label(scenario, period))
    return tuple(pairs)


def _intersect_pair_sets(pair_sets: Iterable[tuple[str, ...]]) -> tuple[str, ...]:
    sets = [set(values) for values in pair_sets]
    if not sets:
        return ()
    intersection = set.intersection(*sets)
    ordered: list[str] = []
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            label = _pair_label(scenario, period)
            if label in intersection:
                ordered.append(label)
    return tuple(ordered)


def _status_for_row(
    *,
    doc_section_present: bool,
    doc_section_placeholder: bool,
    missing_source_metrics: tuple[str, ...],
    source_shared_pairs: tuple[str, ...],
    composite_pairs: tuple[str, ...],
    missing_composite_pairs: tuple[str, ...],
) -> str:
    if not doc_section_present:
        return "missing_doc_section"
    if doc_section_placeholder:
        return "doc_placeholder"
    if missing_source_metrics:
        return "missing_source_masters"
    if not source_shared_pairs:
        return "no_shared_source_pairs"
    if not composite_pairs:
        return "missing_composite_master"
    if missing_composite_pairs:
        return "composite_pair_gap"
    return "complete"


def audit_thematic_bundles(
    *,
    audit_doc_path: Path,
    processed_root: Path | None = None,
    data_dir: Path | None = None,
    bundle_names: Sequence[str] | None = None,
    levels: Sequence[str] = ("district", "block"),
    states: Sequence[str] | None = None,
) -> tuple[dict[str, AuditDocSection], list[BundleStateAuditRow]]:
    """Audit thematic bundles against the selected processed scope."""
    sections = parse_bundle_calculation_audit(audit_doc_path)
    requested_bundles = {name.strip() for name in (bundle_names or ()) if str(name).strip()}
    requested_states = tuple(dict.fromkeys(str(state).strip() for state in (states or ()) if str(state).strip()))

    rows: list[BundleStateAuditRow] = []
    for dashboard_spec in THEMATIC_DASHBOARD_BUNDLES:
        if requested_bundles and dashboard_spec.canonical_bundle not in requested_bundles:
            continue
        composite_spec = get_composite_metric_for_bundle(dashboard_spec.canonical_bundle)
        if composite_spec is None:
            raise ValueError(f"Missing composite metric config for bundle {dashboard_spec.canonical_bundle!r}")

        weights = get_bundle_weights(dashboard_spec.canonical_bundle)
        required_source_metrics = tuple(entry.metric_slug for entry in weights)
        scoring_metrics = tuple(entry.metric_slug for entry in weights if not entry.is_attribute)
        attribute_metrics = tuple(entry.metric_slug for entry in weights if entry.is_attribute)

        level_selection = tuple(level for level in levels if level in dashboard_spec.supported_levels)
        state_selection = list(requested_states) or _collect_candidate_states(
            component_metric_slugs=required_source_metrics,
            composite_slug=dashboard_spec.composite_slug,
            processed_root=processed_root,
            data_dir=data_dir,
        )
        if not state_selection:
            state_selection = ["<none-found>"]

        section = sections.get(dashboard_spec.selector_label)
        doc_section_present = section is not None
        doc_section_placeholder = False if section is None else section.is_placeholder

        for level in level_selection:
            for state in state_selection:
                source_headers: dict[str, tuple[str, ...]] = {}
                missing_source_metrics: list[str] = []
                missing_attribute_metrics: list[str] = []

                for metric_slug in required_source_metrics:
                    path = _preferred_master_path(
                        metric_slug,
                        level=level,
                        state=state,
                        processed_root=processed_root,
                        data_dir=data_dir,
                    )
                    columns = tuple(_read_header_columns(path))
                    if not columns:
                        missing_source_metrics.append(metric_slug)
                        if metric_slug in attribute_metrics:
                            missing_attribute_metrics.append(metric_slug)
                        continue
                    source_headers[metric_slug] = columns

                source_pair_sets = [
                    _available_pairs_for_metric(source_headers[metric_slug], metric_slug=metric_slug)
                    for metric_slug in scoring_metrics
                    if metric_slug in source_headers
                ]
                source_shared_pairs = _intersect_pair_sets(source_pair_sets) if len(source_pair_sets) == len(scoring_metrics) else ()

                composite_path = _preferred_master_path(
                    dashboard_spec.composite_slug,
                    level=level,
                    state=state,
                    processed_root=processed_root,
                    data_dir=data_dir,
                )
                composite_columns = tuple(_read_header_columns(composite_path))
                composite_pairs = (
                    _available_pairs_for_metric(composite_columns, metric_slug=dashboard_spec.composite_slug)
                    if composite_columns
                    else ()
                )
                missing_composite_pairs = tuple(pair for pair in source_shared_pairs if pair not in composite_pairs)
                extra_composite_pairs = tuple(pair for pair in composite_pairs if pair not in source_shared_pairs)

                rows.append(
                    BundleStateAuditRow(
                        bundle_name=dashboard_spec.canonical_bundle,
                        selector_label=dashboard_spec.selector_label,
                        composite_slug=dashboard_spec.composite_slug,
                        level=level,
                        state=state,
                        status=_status_for_row(
                            doc_section_present=doc_section_present,
                            doc_section_placeholder=doc_section_placeholder,
                            missing_source_metrics=tuple(missing_source_metrics),
                            source_shared_pairs=source_shared_pairs,
                            composite_pairs=composite_pairs,
                            missing_composite_pairs=missing_composite_pairs,
                        ),
                        doc_section_present=doc_section_present,
                        doc_section_placeholder=doc_section_placeholder,
                        required_source_metrics=required_source_metrics,
                        scoring_metrics=scoring_metrics,
                        attribute_metrics=attribute_metrics,
                        missing_source_metrics=tuple(missing_source_metrics),
                        missing_attribute_metrics=tuple(missing_attribute_metrics),
                        source_shared_pairs=source_shared_pairs,
                        composite_pairs=composite_pairs,
                        missing_composite_pairs=missing_composite_pairs,
                        extra_composite_pairs=extra_composite_pairs,
                    )
                )
    return sections, rows


def _summary_rows(rows: Sequence[BundleStateAuditRow]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    seen_keys: list[tuple[str, str]] = []
    for row in rows:
        key = (row.bundle_name, row.level)
        if key not in seen_keys:
            seen_keys.append(key)

    for bundle_name, level in seen_keys:
        subset = [row for row in rows if row.bundle_name == bundle_name and row.level == level]
        status_counts: dict[str, int] = {}
        for row in subset:
            status_counts[row.status] = status_counts.get(row.status, 0) + 1
        out.append(
            {
                "bundle_name": bundle_name,
                "level": level,
                "states_checked": len(subset),
                "complete_states": sum(1 for row in subset if row.status == "complete"),
                "status_counts": status_counts,
            }
        )
    return out


def _overall_status(rows: Sequence[BundleStateAuditRow]) -> str:
    statuses = {row.status for row in rows}
    if not rows:
        return "no_rows"
    if statuses == {"complete"}:
        return "complete"
    if "complete" in statuses:
        return "partial"
    return "incomplete"


def _print_report(
    *,
    audit_doc_path: Path,
    processed_root: Path,
    sections: dict[str, AuditDocSection],
    rows: Sequence[BundleStateAuditRow],
    verbose: bool,
) -> None:
    thematic_sections = [spec.selector_label for spec in THEMATIC_DASHBOARD_BUNDLES]
    documented_thematic = sum(1 for label in thematic_sections if label in sections)
    print("Thematic Bundle Completeness Audit")
    print(f"Audit doc: {audit_doc_path}")
    print(f"Processed root: {processed_root}")
    print(f"Thematic doc sections found: {documented_thematic}/{len(thematic_sections)}")
    print(f"Overall status: {_overall_status(rows)}")
    print("")
    for summary in _summary_rows(rows):
        status_counts = ", ".join(f"{key}={value}" for key, value in sorted(summary["status_counts"].items()))
        print(
            f"{summary['bundle_name']} | {summary['level']} | "
            f"states={summary['states_checked']} | complete={summary['complete_states']} | {status_counts}"
        )
        if not verbose:
            continue
        for row in [item for item in rows if item.bundle_name == summary["bundle_name"] and item.level == summary["level"]]:
            issue_bits: list[str] = []
            if row.missing_source_metrics:
                issue_bits.append(f"missing_sources={','.join(row.missing_source_metrics)}")
            if row.missing_composite_pairs:
                issue_bits.append(f"missing_composite_pairs={','.join(row.missing_composite_pairs)}")
            if not issue_bits and row.extra_composite_pairs:
                issue_bits.append(f"extra_composite_pairs={','.join(row.extra_composite_pairs)}")
            issue_text = "; ".join(issue_bits) if issue_bits else "no gaps"
            print(f"  - {row.state}: {row.status} | shared_pairs={','.join(row.source_shared_pairs) or '-'} | {issue_text}")


def _write_json(
    *,
    output_path: Path,
    audit_doc_path: Path,
    processed_root: Path,
    sections: dict[str, AuditDocSection],
    rows: Sequence[BundleStateAuditRow],
) -> None:
    payload = {
        "audit_doc_path": str(audit_doc_path),
        "processed_root": str(processed_root),
        "overall_status": _overall_status(rows),
        "doc_sections": {key: asdict(value) for key, value in sections.items()},
        "summary": _summary_rows(rows),
        "rows": [asdict(row) for row in rows],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_csv(output_path: Path, rows: Sequence[BundleStateAuditRow]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(asdict(BundleStateAuditRow(
        bundle_name="",
        selector_label="",
        composite_slug="",
        level="",
        state="",
        status="",
        doc_section_present=False,
        doc_section_placeholder=False,
        required_source_metrics=(),
        scoring_metrics=(),
        attribute_metrics=(),
        missing_source_metrics=(),
        missing_attribute_metrics=(),
        source_shared_pairs=(),
        composite_pairs=(),
        missing_composite_pairs=(),
        extra_composite_pairs=(),
    )).keys())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            for key, value in payload.items():
                if isinstance(value, tuple):
                    payload[key] = "|".join(str(item) for item in value)
            writer.writerow(payload)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the thematic completeness audit."""
    repo_root = find_repo_root()
    bundle_choices = [spec.canonical_bundle for spec in THEMATIC_DASHBOARD_BUNDLES]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audit-doc",
        type=Path,
        default=repo_root / "docs" / "bundle_calculation_audit.md",
        help="Path to the dashboard bundle calculation audit markdown.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="IRT data directory containing the `processed/` tree. Defaults to IRT_DATA_DIR/current repo config.",
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        help="Explicit processed root containing per-slug directories. Overrides --data-dir.",
    )
    parser.add_argument(
        "--bundle",
        action="append",
        choices=bundle_choices,
        help="Restrict the audit to one or more thematic bundle names.",
    )
    parser.add_argument(
        "--level",
        choices=("district", "block", "admin"),
        default="admin",
        help="Audit district, block, or both admin levels.",
    )
    parser.add_argument(
        "--state",
        action="append",
        help="Restrict the audit to one or more state directory names.",
    )
    parser.add_argument("--output-json", type=Path, help="Optional JSON report path.")
    parser.add_argument("--output-csv", type=Path, help="Optional flat CSV report path.")
    parser.add_argument("--verbose", action="store_true", help="Print per-state details.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    level_selection = ("district", "block") if args.level == "admin" else (args.level,)
    processed_root = _base_processed_root(processed_root=args.processed_root, data_dir=args.data_dir)
    sections, rows = audit_thematic_bundles(
        audit_doc_path=args.audit_doc,
        processed_root=args.processed_root,
        data_dir=args.data_dir,
        bundle_names=args.bundle,
        levels=level_selection,
        states=args.state,
    )
    _print_report(
        audit_doc_path=args.audit_doc,
        processed_root=processed_root,
        sections=sections,
        rows=rows,
        verbose=args.verbose,
    )
    if args.output_json:
        _write_json(
            output_path=args.output_json,
            audit_doc_path=args.audit_doc,
            processed_root=processed_root,
            sections=sections,
            rows=rows,
        )
    if args.output_csv:
        _write_csv(args.output_csv, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
