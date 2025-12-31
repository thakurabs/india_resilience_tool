"""
Robust file discovery for IRT processed outputs.

Focus:
- District yearly ensemble CSV discovery
- Block yearly ensemble CSV discovery
- State yearly ensemble stats CSV discovery

Supports BOTH folder structures:
- OLD: {state}/{district}/ensembles/{scenario}/
- NEW: {state}/districts/ensembles/{district}/{scenario}/

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

# Folder names for clean separation
DISTRICT_FOLDER = "districts"
BLOCK_FOLDER = "blocks"


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


def _get_level_folder(level: AdminLevel) -> str:
    """Get the subfolder name for a given level."""
    return BLOCK_FOLDER if level == "block" else DISTRICT_FOLDER


def _generate_district_name_variants(district_display: str) -> list[str]:
    """Generate various name variants for fuzzy matching."""
    disp = str(district_display).strip()
    disp_lower = disp.lower()
    
    variants = [
        disp,
        disp.upper(),
        disp.replace(" ", "_"),
        disp.replace(" ", "_").upper(),
        disp.replace("_", " "),
        re.sub(r"\s+", "_", disp_lower),
        disp_lower,
        slugify_fs(disp),
        # Title case variants
        disp.title(),
        disp.title().replace(" ", "_"),
    ]
    
    return _dedupe([v for v in variants if v])


def build_district_yearly_candidate_paths(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    level: AdminLevel = "district",
) -> list[Path]:
    """
    Build candidate file paths for district yearly ensemble CSVs.
    Includes paths for both old and new folder structures.
    """
    ts_root_p = Path(ts_root)
    scenario = str(scenario_name).strip()
    level_folder = _get_level_folder(level)
    
    # Generate name variants
    name_variants = _generate_district_name_variants(district_display)
    
    candidates: list[str] = []
    
    for name in name_variants:
        # NEW structure: {state}/districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv
        new_path = (
            ts_root_p / state_dir / level_folder / "ensembles" / name / scenario /
            f"{name}_yearly_ensemble.csv"
        )
        candidates.append(str(new_path))
        
        # OLD structure: {state}/{district}/ensembles/{scenario}/{district}_yearly_ensemble.csv
        old_path = (
            ts_root_p / state_dir / name / "ensembles" / scenario /
            f"{name}_yearly_ensemble.csv"
        )
        candidates.append(str(old_path))
        
        # Legacy: {state}/{district}/district_yearly_ensemble_stats.csv
        legacy_path = ts_root_p / state_dir / name / "district_yearly_ensemble_stats.csv"
        candidates.append(str(legacy_path))

    # Also add registry templates if provided
    disp = str(district_display).strip()
    district_underscored = disp.replace(" ", "_")
    
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
    level: AdminLevel = "district",
) -> Optional[Path]:
    """
    Discover a district yearly ensemble CSV.
    
    Search order:
      1) Direct candidate paths (both old and new structures)
      2) Scan ensembles directory for fuzzy match
      3) Legacy fallbacks

    Returns:
        Path if found, else None
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir
    if not base.exists():
        return None

    disp = str(district_display).strip()
    scenario = str(scenario_name).strip()
    level_folder = _get_level_folder(level)

    # 1) Try direct candidate paths first
    for f in build_district_yearly_candidate_paths(
        ts_root=ts_root_p,
        state_dir=state_dir,
        district_display=disp,
        scenario_name=scenario,
        varcfg=varcfg,
        level=level,
    ):
        if f.exists():
            return f

    # 2) Check aliases
    norm = normalize_fn or (lambda s: str(s).strip().lower())
    disp_norm = norm(disp)
    aliases = aliases or {}
    
    if disp_norm in aliases:
        aliased = aliases[disp_norm]
        for f in build_district_yearly_candidate_paths(
            ts_root=ts_root_p,
            state_dir=state_dir,
            district_display=aliased,
            scenario_name=scenario,
            varcfg=varcfg,
            level=level,
        ):
            if f.exists():
                return f

    # 3) Scan the ensembles directory for fuzzy match
    # NEW structure: ensembles are at {state}/districts/ensembles/
    new_ensembles = base / level_folder / "ensembles"
    old_ensembles_base = base  # OLD: each district has its own ensembles folder
    
    # Try new structure first
    if new_ensembles.exists():
        try:
            district_dirs = [d.name for d in new_ensembles.iterdir() if d.is_dir()]
            best = difflib.get_close_matches(disp.upper(), district_dirs, n=1, cutoff=0.7)
            if not best:
                best = difflib.get_close_matches(disp, district_dirs, n=1, cutoff=0.7)
            
            if best:
                matched_name = best[0]
                # Try various filename patterns
                for filename in [
                    f"{matched_name}_yearly_ensemble.csv",
                    f"{matched_name.lower()}_yearly_ensemble.csv",
                    "district_yearly_ensemble_stats.csv",
                ]:
                    f = new_ensembles / matched_name / scenario / filename
                    if f.exists():
                        return f
        except Exception:
            pass

    # 4) Try old structure - scan district directories
    try:
        skip_dirs = {DISTRICT_FOLDER, BLOCK_FOLDER, "ensembles"}
        district_dirs = [
            d for d in base.iterdir() 
            if d.is_dir() and d.name not in skip_dirs
        ]
        district_names = [d.name for d in district_dirs]
        
        best = difflib.get_close_matches(disp.upper(), district_names, n=1, cutoff=0.7)
        if not best:
            best = difflib.get_close_matches(disp, district_names, n=1, cutoff=0.7)
        
        if best:
            matched_name = best[0]
            matched_dir = base / matched_name / "ensembles" / scenario
            
            for filename in [
                f"{matched_name}_yearly_ensemble.csv",
                "district_yearly_ensemble_stats.csv",
            ]:
                f = matched_dir / filename
                if f.exists():
                    return f
            
            # Also check legacy path
            f = base / matched_name / "district_yearly_ensemble_stats.csv"
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
    base = ts_root_p / state_dir / BLOCK_FOLDER / "ensembles"
    if not base.exists():
        return None

    scenario = str(scenario_name).strip()
    
    # Generate name variants for both district and block
    district_variants = _generate_district_name_variants(district_display)
    block_variants = _generate_district_name_variants(block_display)
    
    # Try all combinations
    for district_name in district_variants:
        for block_name in block_variants:
            for filename in [
                f"{block_name}_yearly_ensemble.csv",
                "block_yearly_ensemble_stats.csv",
            ]:
                f = base / district_name / block_name / scenario / filename
                if f.exists():
                    return f

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
      2) level-specific summary: {state}/state_yearly_ensemble_stats_{level}.csv
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

    # Try level-specific file first
    f_level = ts_root_p / state_dir / f"state_yearly_ensemble_stats_{level}.csv"
    if f_level.exists():
        return f_level
    
    # Fall back to default
    f_default = ts_root_p / state_dir / "state_yearly_ensemble_stats.csv"
    return f_default if f_default.exists() else None


# -----------------------------------------------------------------------------
# Utility functions for level-aware discovery
# -----------------------------------------------------------------------------

def get_available_units(
    ts_root: PathLike,
    state_dir: str,
    level: AdminLevel = "district",
) -> list[str]:
    """
    Get list of available units (districts or blocks) for a state.
    
    Returns list of unit names found in the data directory.
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir
    
    if not base.exists():
        return []
    
    level_folder = _get_level_folder(level)
    
    # Check new structure first
    new_data_path = base / level_folder
    if new_data_path.exists():
        skip_dirs = {"ensembles"}
        units = []
        try:
            for item in new_data_path.iterdir():
                if item.is_dir() and item.name not in skip_dirs:
                    units.append(item.name)
        except Exception:
            pass
        if units:
            return sorted(units)
    
    # Fall back to old structure
    skip_dirs = {DISTRICT_FOLDER, BLOCK_FOLDER, "ensembles"}
    units = []
    try:
        for item in base.iterdir():
            if item.is_dir() and item.name not in skip_dirs:
                units.append(item.name)
    except Exception:
        pass
    
    return sorted(units)


def get_available_scenarios(
    ts_root: PathLike,
    state_dir: str,
    unit_name: str,
    level: AdminLevel = "district",
    parent_district: Optional[str] = None,
) -> list[str]:
    """
    Get list of available scenarios for a unit.
    
    For blocks, parent_district must be provided.
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir
    
    if not base.exists():
        return []
    
    level_folder = _get_level_folder(level)
    scenarios = set()
    
    # Generate name variants
    name_variants = _generate_district_name_variants(unit_name)
    
    # Try new structure: {state}/districts/ensembles/{district}/{scenario}/
    new_ensembles = base / level_folder / "ensembles"
    if new_ensembles.exists():
        for name in name_variants:
            if level == "block" and parent_district:
                parent_variants = _generate_district_name_variants(parent_district)
                for pname in parent_variants:
                    unit_path = new_ensembles / pname / name
                    if unit_path.exists():
                        for item in unit_path.iterdir():
                            if item.is_dir() and item.name in {"historical", "ssp245", "ssp585"}:
                                scenarios.add(item.name)
            else:
                unit_path = new_ensembles / name
                if unit_path.exists():
                    for item in unit_path.iterdir():
                        if item.is_dir() and item.name in {"historical", "ssp245", "ssp585"}:
                            scenarios.add(item.name)
    
    # Try old structure: {state}/{district}/ensembles/{scenario}/
    if not scenarios and level == "district":
        for name in name_variants:
            old_ensembles = base / name / "ensembles"
            if old_ensembles.exists():
                try:
                    for item in old_ensembles.iterdir():
                        if item.is_dir() and item.name in {"historical", "ssp245", "ssp585"}:
                            scenarios.add(item.name)
                except Exception:
                    pass
    
    return sorted(scenarios)