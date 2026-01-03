"""
Robust file discovery for IRT processed outputs.

Focus:
- District yearly ensemble CSV discovery
- Block yearly ensemble CSV discovery  
- State yearly ensemble stats CSV discovery

Supports the actual data structure:
- District: {state}/districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv
- Block: {state}/blocks/ensembles/{district}/{block}/{scenario}/{block}_yearly_ensemble.csv
- Legacy: {state}/{district}/district_yearly_ensemble_stats.csv

Streamlit-free: caching belongs in app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import difflib
import re
import unicodedata
from pathlib import Path
from typing import Any, Literal, Mapping, Optional, Sequence, Union

PathLike = Union[str, Path]
AdminLevel = Literal["district", "block"]


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


def _generate_name_variants(name: str) -> list[str]:
    """Generate various name variants for fuzzy matching."""
    disp = str(name).strip()
    disp_lower = disp.lower()
    disp_upper = disp.upper()
    
    variants = [
        disp,
        disp_upper,
        disp_lower,
        disp.replace(" ", "_"),
        disp.replace(" ", "_").upper(),
        disp.replace("_", " "),
        re.sub(r"\s+", "_", disp_lower),
        slugify_fs(disp),
        disp.title(),
        disp.title().replace(" ", "_"),
    ]
    
    return _dedupe([v for v in variants if v])


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
    Discover a district yearly CSV.
    
    Search order:
      1) New structure: {state}/districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv
      2) Registry candidate templates from varcfg
      3) Legacy structure: {state}/{district}/district_yearly_ensemble_stats.csv
      4) Fuzzy match on directory names

    Returns:
        Path if found, else None
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir
    if not base.exists():
        return None

    disp = str(district_display).strip()
    scenario = str(scenario_name).strip()
    
    # Generate name variants for matching
    name_variants = _generate_name_variants(disp)
    
    # Also check aliases
    norm = normalize_fn or (lambda s: str(s).strip().lower())
    disp_norm = norm(disp)
    aliases = aliases or {}
    if disp_norm in aliases:
        aliased = aliases[disp_norm]
        name_variants.extend(_generate_name_variants(aliased))
        name_variants = _dedupe(name_variants)

    # 1) Try NEW structure: {state}/districts/ensembles/{district}/{scenario}/
    ensembles_path = base / "districts" / "ensembles"
    if ensembles_path.exists():
        for name in name_variants:
            # Try exact path
            scenario_path = ensembles_path / name / scenario
            if scenario_path.exists():
                # Try different filename patterns
                for filename in [
                    f"{name}_yearly_ensemble.csv",
                    f"{name.upper()}_yearly_ensemble.csv",
                    f"{name.lower()}_yearly_ensemble.csv",
                    "district_yearly_ensemble_stats.csv",
                ]:
                    f = scenario_path / filename
                    if f.exists():
                        return f
                
                # Also try any CSV file in the directory
                csv_files = list(scenario_path.glob("*_yearly_ensemble.csv"))
                if csv_files:
                    return csv_files[0]
        
        # Fuzzy match on district folder names in ensembles
        try:
            district_dirs = [d.name for d in ensembles_path.iterdir() if d.is_dir()]
            best = difflib.get_close_matches(disp.upper(), district_dirs, n=1, cutoff=0.7)
            if not best:
                best = difflib.get_close_matches(disp, district_dirs, n=1, cutoff=0.7)
            
            if best:
                matched_name = best[0]
                scenario_path = ensembles_path / matched_name / scenario
                if scenario_path.exists():
                    for f in scenario_path.glob("*_yearly_ensemble.csv"):
                        return f
        except Exception:
            pass

    # 2) Try registry candidate templates
    district_underscored = disp.replace(" ", "_")
    for pat in varcfg.get("district_yearly_candidates", []) or []:
        try:
            candidate = pat.format(
                root=str(ts_root_p),
                state=state_dir,
                district=disp,
                district_underscored=district_underscored,
                scenario=scenario,
                district_slug=slugify_fs(disp),
            )
            f = Path(candidate)
            if f.exists():
                return f
        except Exception:
            continue

    # 3) Try LEGACY structure: {state}/{district}/district_yearly_ensemble_stats.csv
    for name in name_variants:
        f = base / name / "district_yearly_ensemble_stats.csv"
        if f.exists():
            return f

    # 4) Fuzzy match on top-level directory names (legacy structure)
    try:
        skip_dirs = {"districts", "blocks", "ensembles"}
        existing_dirs = [
            p for p in base.iterdir() 
            if p.is_dir() and p.name.lower() not in skip_dirs
        ]
        folder_names = [p.name for p in existing_dirs]
        best = difflib.get_close_matches(disp, folder_names, n=1, cutoff=0.72)
        if best:
            f = base / best[0] / "district_yearly_ensemble_stats.csv"
            if f.exists():
                return f
    except Exception:
        pass

    return None


def discover_block_yearly_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    block_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    aliases: Optional[dict[str, str]] = None,
    normalize_fn: Optional[callable] = None,
) -> Optional[Path]:
    """
    Discover a block yearly ensemble CSV.
    
    Structure: {state}/blocks/ensembles/{district}/{block}/{scenario}/{block}_yearly_ensemble.csv

    Returns:
        Path if found, else None
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir / "blocks" / "ensembles"
    if not base.exists():
        return None

    scenario = str(scenario_name).strip()
    
    # Generate name variants for both district and block
    district_variants = _generate_name_variants(district_display)
    block_variants = _generate_name_variants(block_display)
    
    # Try all combinations
    for district_name in district_variants:
        district_path = base / district_name
        if not district_path.exists():
            continue
            
        for block_name in block_variants:
            scenario_path = district_path / block_name / scenario
            if not scenario_path.exists():
                continue
            
            # Try different filename patterns
            for filename in [
                f"{block_name}_yearly_ensemble.csv",
                f"{block_name.upper()}_yearly_ensemble.csv",
                f"{block_name.lower()}_yearly_ensemble.csv",
                "block_yearly_ensemble_stats.csv",
            ]:
                f = scenario_path / filename
                if f.exists():
                    return f
            
            # Try any CSV file
            csv_files = list(scenario_path.glob("*_yearly_ensemble.csv"))
            if csv_files:
                return csv_files[0]

    # Fuzzy match on directory names
    try:
        district_dirs = [d.name for d in base.iterdir() if d.is_dir()]
        district_match = difflib.get_close_matches(
            district_display.upper(), district_dirs, n=1, cutoff=0.7
        )
        if district_match:
            district_path = base / district_match[0]
            block_dirs = [d.name for d in district_path.iterdir() if d.is_dir()]
            block_match = difflib.get_close_matches(
                block_display.upper(), block_dirs, n=1, cutoff=0.7
            )
            if block_match:
                scenario_path = district_path / block_match[0] / scenario
                if scenario_path.exists():
                    for f in scenario_path.glob("*_yearly_ensemble.csv"):
                        return f
    except Exception:
        pass

    return None


def discover_state_yearly_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    varcfg: Optional[dict[str, Any]] = None,
    level: AdminLevel = "district",
) -> Optional[Path]:
    """
    Discover state yearly ensemble stats CSV.

    Priority:
      1) registry templates (if provided)
      2) level-specific: {state}/state_yearly_ensemble_stats_{level}.csv
      3) default: {state}/state_yearly_ensemble_stats.csv
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

    # Try level-specific file
    f_level = ts_root_p / state_dir / f"state_yearly_ensemble_stats_{level}.csv"
    if f_level.exists():
        return f_level

    # Fall back to default
    f_default = ts_root_p / state_dir / "state_yearly_ensemble_stats.csv"
    return f_default if f_default.exists() else None