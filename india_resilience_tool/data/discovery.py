"""
Robust file discovery for IRT processed outputs.

Focus:
- District yearly ensemble CSV discovery
- State yearly ensemble stats CSV discovery

Streamlit-free: caching belongs in app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import difflib
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

PathLike = Union[str, Path]


def slugify_fs(text: str) -> str:
    """
    Filesystem-safe slug for matching folder names.
    """
    s = (
        unicodedata.normalize("NFKD", str(text))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    s = re.sub(r"[^A-Za-z0-9]+", "_", s.strip())
    return re.sub(r"_+", "_", s).strip("_").lower()


def _dedupe(seq: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for s in seq:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def build_district_yearly_candidate_paths(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
) -> list[Path]:
    """
    Format the registry templates to concrete candidate file paths.
    """
    ts_root_p = Path(ts_root)
    disp = str(district_display).strip()
    scenario = str(scenario_name).strip()
    district_underscored = disp.replace(" ", "_")

    candidates: list[str] = []
    for pat in varcfg.get("district_yearly_candidates", []) or []:
        try:
            candidates.append(
                pat.format(
                    root=str(ts_root_p),
                    state=state_dir,
                    district=disp,
                    district_underscored=district_underscored,
                    scenario=scenario,
                    district_slug=slugify_fs(disp),
                )
            )
        except Exception:
            continue

    return [Path(p) for p in _dedupe(candidates)]


def discover_district_yearly_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    aliases: Optional[dict[str, str]] = None,
    normalize_fn: Optional[callable] = None,
) -> Optional[Path]:
    """
    Discover a district yearly CSV using:
      1) registry candidate templates
      2) direct folder-name variants under <ts_root>/<state_dir>/<district_folder>/
         looking for district_yearly_ensemble_stats.csv
      3) fuzzy match closest folder name

    Returns:
        Path if found, else None
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir
    if not base.exists():
        return None

    disp = str(district_display).strip()
    scenario = str(scenario_name).strip()

    # 1) Direct registry candidates (highest priority)
    for f in build_district_yearly_candidate_paths(
        ts_root=ts_root_p,
        state_dir=state_dir,
        district_display=disp,
        scenario_name=scenario,
        varcfg=varcfg,
    ):
        if f.exists():
            return f

    # list existing district dirs once
    try:
        existing_dirs = [p for p in base.iterdir() if p.is_dir()]
    except Exception:
        existing_dirs = []

    # 2) Folder-name variants
    norm = normalize_fn or (lambda s: str(s).strip().lower())
    disp_norm = norm(disp)

    cand_names = [
        disp,
        disp.replace(" ", "_"),
        disp.replace("_", " "),
        re.sub(r"\s+", "_", disp_norm),
        disp_norm,
        slugify_fs(disp),
    ]

    aliases = aliases or {}
    ali = aliases.get(disp_norm)
    if ali:
        ali_norm = norm(ali)
        cand_names += [
            ali,
            ali.replace(" ", "_"),
            re.sub(r"\s+", "_", ali_norm),
            ali_norm,
            slugify_fs(ali),
        ]

    cand_names = _dedupe([str(c).strip() for c in cand_names if str(c).strip()])

    for name in cand_names:
        p = base / name
        f = p / "district_yearly_ensemble_stats.csv"
        if f.exists():
            return f

    # 3) Fuzzy match best directory name
    folder_names = [p.name for p in existing_dirs]
    best = difflib.get_close_matches(disp, folder_names, n=1, cutoff=0.72)
    if best:
        f = (base / best[0]) / "district_yearly_ensemble_stats.csv"
        if f.exists():
            return f

    return None


def discover_state_yearly_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    varcfg: Optional[dict[str, Any]] = None,
) -> Optional[Path]:
    """
    Discover state yearly ensemble stats CSV.

    Priority:
      1) registry templates (if provided)
      2) default <ts_root>/<state_dir>/state_yearly_ensemble_stats.csv
    """
    ts_root_p = Path(ts_root)

    if varcfg:
        cands: list[str] = []
        for pat in varcfg.get("state_yearly_candidates", []) or []:
            try:
                cands.append(
                    pat.format(
                        root=str(ts_root_p),
                        state=state_dir,
                    )
                )
            except Exception:
                continue
        for p in _dedupe(cands):
            f = Path(p)
            if f.exists():
                return f

    f_default = ts_root_p / state_dir / "state_yearly_ensemble_stats.csv"
    return f_default if f_default.exists() else None
