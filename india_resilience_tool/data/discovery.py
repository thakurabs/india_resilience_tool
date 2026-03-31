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

from india_resilience_tool.utils.naming import hydro_fs_token
from india_resilience_tool.utils.processed_io import glob_paths, path_exists

PathLike = Union[str, Path]
AdminLevel = Literal["district", "block", "basin", "sub_basin"]


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
        hydro_fs_token(disp),
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
                        level=level,
                    )
                )
            except Exception:
                continue
        for p in _dedupe(cands):
            f = Path(p)
            if f.exists():
                return f

    f_level = ts_root_p / state_dir / f"state_yearly_ensemble_stats_{level}.csv"
    return f_level if f_level.exists() else None


def discover_hydro_yearly_file(
    *,
    ts_root: PathLike,
    level: Literal["basin", "sub_basin"],
    basin_display: str,
    subbasin_display: Optional[str],
    scenario_name: str,
) -> Optional[Path]:
    """Discover a hydro yearly ensemble CSV under processed/{metric}/hydro/."""
    root = Path(ts_root) / "hydro"
    if not path_exists(root):
        return None

    scenario = str(scenario_name).strip()
    basin_variants = _generate_name_variants(basin_display)
    subbasin_variants = _generate_name_variants(subbasin_display or "")

    if level == "basin":
        base = root / "basins" / "ensembles"
        for basin_name in basin_variants:
            scen_dir = base / basin_name / scenario
            if not path_exists(scen_dir):
                continue
            for filename in (
                f"{basin_name}_yearly_ensemble.csv",
                f"{basin_name.upper()}_yearly_ensemble.csv",
                f"{basin_name.lower()}_yearly_ensemble.csv",
            ):
                f = scen_dir / filename
                if path_exists(f):
                    return f
            csvs = glob_paths(scen_dir, "*_yearly_ensemble.csv")
            if csvs:
                return csvs[0]
        return None

    base = root / "sub_basins" / "ensembles"
    for basin_name in basin_variants:
        basin_dir = base / basin_name
        if not path_exists(basin_dir):
            continue
        for subbasin_name in subbasin_variants:
            scen_dir = basin_dir / subbasin_name / scenario
            if not path_exists(scen_dir):
                continue
            for filename in (
                f"{subbasin_name}_yearly_ensemble.csv",
                f"{subbasin_name.upper()}_yearly_ensemble.csv",
                f"{subbasin_name.lower()}_yearly_ensemble.csv",
            ):
                f = scen_dir / filename
                if path_exists(f):
                    return f
            csvs = glob_paths(scen_dir, "*_yearly_ensemble.csv")
            if csvs:
                return csvs[0]
    return None


def discover_state_yearly_model_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    level: AdminLevel = "district",
) -> Optional[Path]:
    """Discover state yearly model averages CSV for the given level."""
    f = Path(ts_root) / state_dir / f"state_yearly_model_averages_{level}.csv"
    return f if f.exists() else None


def discover_state_period_ensemble_file(
    *,
    ts_root: PathLike,
    state_dir: str,
    level: AdminLevel = "district",
) -> Optional[Path]:
    """Discover state period ensemble stats CSV for the given level."""
    f = Path(ts_root) / state_dir / f"state_ensemble_stats_{level}.csv"
    return f if f.exists() else None


def _first_matching_ensemble_csv(directory: Path, *, fallback_name: str) -> Optional[Path]:
    """Return the preferred yearly-ensemble CSV inside a scenario directory."""
    if not path_exists(directory):
        return None
    explicit = directory / fallback_name
    if path_exists(explicit):
        return explicit
    csvs = glob_paths(directory, "*_yearly_ensemble.csv")
    if csvs:
        return csvs[0]
    return None


def _first_matching_model_yearly_csv(directory: Path, *, fallback_name: str) -> Optional[Path]:
    """Return the preferred per-model yearly CSV inside a scenario directory."""
    if not directory.exists():
        return None
    explicit = directory / fallback_name
    if explicit.exists():
        return explicit
    csvs = sorted(
        [
            p
            for p in directory.glob("*_yearly.csv")
            if not p.name.endswith("_yearly_ensemble.csv")
        ],
        key=lambda p: p.name.lower(),
    )
    if csvs:
        return csvs[0]
    return None


def iter_district_yearly_ensemble_files(
    *,
    ts_root: PathLike,
    state_dir: str,
) -> list[tuple[str, str, Path]]:
    """
    Enumerate district yearly-ensemble CSVs for one state.

    Returns tuples of:
      - district directory name
      - scenario directory name
      - CSV path
    """
    base = Path(ts_root) / state_dir / "districts" / "ensembles"
    if not base.exists():
        return []

    out: list[tuple[str, str, Path]] = []
    for district_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
        for scenario_dir in sorted([p for p in district_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            csv_path = _first_matching_ensemble_csv(
                scenario_dir,
                fallback_name="district_yearly_ensemble_stats.csv",
            )
            if csv_path is not None:
                out.append((district_dir.name, scenario_dir.name, csv_path))
    return out


def iter_block_yearly_ensemble_files(
    *,
    ts_root: PathLike,
    state_dir: str,
) -> list[tuple[str, str, str, Path]]:
    """
    Enumerate block yearly-ensemble CSVs for one state.

    Returns tuples of:
      - district directory name
      - block directory name
      - scenario directory name
      - CSV path
    """
    base = Path(ts_root) / state_dir / "blocks" / "ensembles"
    if not base.exists():
        return []

    out: list[tuple[str, str, str, Path]] = []
    for district_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
        for block_dir in sorted([p for p in district_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            for scenario_dir in sorted([p for p in block_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                csv_path = _first_matching_ensemble_csv(
                    scenario_dir,
                    fallback_name="block_yearly_ensemble_stats.csv",
                )
                if csv_path is not None:
                    out.append((district_dir.name, block_dir.name, scenario_dir.name, csv_path))
    return out


def iter_hydro_yearly_ensemble_files(
    *,
    ts_root: PathLike,
    level: Literal["basin", "sub_basin"],
) -> list[tuple[str, Optional[str], str, Path]]:
    """
    Enumerate hydro yearly-ensemble CSVs.

    Returns tuples of:
      - basin directory name
      - sub-basin directory name or None
      - scenario directory name
      - CSV path
    """
    root = Path(ts_root) / "hydro"
    if not root.exists():
        return []

    if str(level).strip().lower() == "basin":
        base = root / "basins" / "ensembles"
        if not base.exists():
            return []
        out: list[tuple[str, Optional[str], str, Path]] = []
        for basin_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            for scenario_dir in sorted([p for p in basin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                csv_path = _first_matching_ensemble_csv(
                    scenario_dir,
                    fallback_name=f"{basin_dir.name}_yearly_ensemble.csv",
                )
                if csv_path is not None:
                    out.append((basin_dir.name, None, scenario_dir.name, csv_path))
        return out

    base = root / "sub_basins" / "ensembles"
    if not base.exists():
        return []
    out = []
    for basin_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
        for subbasin_dir in sorted([p for p in basin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            for scenario_dir in sorted([p for p in subbasin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                csv_path = _first_matching_ensemble_csv(
                    scenario_dir,
                    fallback_name=f"{subbasin_dir.name}_yearly_ensemble.csv",
                )
                if csv_path is not None:
                    out.append((basin_dir.name, subbasin_dir.name, scenario_dir.name, csv_path))
    return out


def iter_hydro_yearly_model_files(
    *,
    ts_root: PathLike,
    level: Literal["basin", "sub_basin"],
) -> list[tuple[str, Optional[str], str, str, Path]]:
    """
    Enumerate hydro per-model yearly CSVs.

    Returns tuples of:
      - basin directory name
      - sub-basin directory name or None
      - model directory name
      - scenario directory name
      - CSV path
    """
    root = Path(ts_root) / "hydro"
    if not root.exists():
        return []

    if str(level).strip().lower() == "basin":
        base = root / "basins"
        if not base.exists():
            return []
        out: list[tuple[str, Optional[str], str, str, Path]] = []
        for basin_dir in sorted(
            [p for p in base.iterdir() if p.is_dir() and p.name.lower() != "ensembles"],
            key=lambda p: p.name.lower(),
        ):
            for model_dir in sorted([p for p in basin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                for scenario_dir in sorted([p for p in model_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                    csv_path = _first_matching_model_yearly_csv(
                        scenario_dir,
                        fallback_name=f"{basin_dir.name}_yearly.csv",
                    )
                    if csv_path is not None:
                        out.append((basin_dir.name, None, model_dir.name, scenario_dir.name, csv_path))
        return out

    base = root / "sub_basins"
    if not base.exists():
        return []
    out: list[tuple[str, Optional[str], str, str, Path]] = []
    for basin_dir in sorted(
        [p for p in base.iterdir() if p.is_dir() and p.name.lower() != "ensembles"],
        key=lambda p: p.name.lower(),
    ):
        for subbasin_dir in sorted([p for p in basin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            for model_dir in sorted([p for p in subbasin_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                for scenario_dir in sorted([p for p in model_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
                    csv_path = _first_matching_model_yearly_csv(
                        scenario_dir,
                        fallback_name=f"{subbasin_dir.name}_yearly.csv",
                    )
                    if csv_path is not None:
                        out.append((basin_dir.name, subbasin_dir.name, model_dir.name, scenario_dir.name, csv_path))
    return out


def discover_district_model_yearly_files(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    aliases: Optional[dict[str, str]] = None,
    normalize_fn: Optional[callable] = None,
) -> dict[str, Path]:
    """
    Discover per-model district yearly CSVs for a given district+scenario.

    Expected structure (compute pipeline):
      {root}/{state}/districts/{district}/{model}/{scenario}/{district}_yearly.csv

    Returns:
        Mapping of model_name -> Path to the per-model yearly CSV.
        Returns an empty dict when not found.
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir / "districts"
    if not base.exists():
        return {}

    disp = str(district_display).strip()
    scenario = str(scenario_name).strip()

    # Generate name variants for matching (and aliases when present)
    name_variants = _generate_name_variants(disp)
    norm = normalize_fn or (lambda s: str(s).strip().lower())
    disp_norm = norm(disp)
    aliases = aliases or {}
    if disp_norm in aliases:
        name_variants.extend(_generate_name_variants(aliases[disp_norm]))
        name_variants = _dedupe(name_variants)

    # Resolve district directory (skip the "ensembles" folder).
    district_dir: Optional[Path] = None
    for name in name_variants:
        cand = base / name
        if cand.exists() and cand.is_dir() and cand.name.lower() != "ensembles":
            district_dir = cand
            break

    if district_dir is None:
        try:
            district_dirs = [d for d in base.iterdir() if d.is_dir() and d.name.lower() != "ensembles"]
            folder_names = [d.name for d in district_dirs]
            best = difflib.get_close_matches(disp.upper(), folder_names, n=1, cutoff=0.7)
            if not best:
                best = difflib.get_close_matches(disp, folder_names, n=1, cutoff=0.7)
            if best:
                district_dir = base / best[0]
        except Exception:
            district_dir = None

    if district_dir is None:
        return {}

    unit_key = district_dir.name
    out: dict[str, Path] = {}
    try:
        model_dirs = sorted(
            [p for p in district_dir.iterdir() if p.is_dir()],
            key=lambda p: p.name.lower(),
        )
    except Exception:
        model_dirs = []

    for mdir in model_dirs:
        scen_dir = mdir / scenario
        if not scen_dir.exists():
            continue

        cands = [
            scen_dir / f"{unit_key}_yearly.csv",
            scen_dir / f"{unit_key.upper()}_yearly.csv",
            scen_dir / f"{unit_key.lower()}_yearly.csv",
        ]
        f: Optional[Path] = None
        for c in cands:
            if c.exists():
                f = c
                break

        if f is None:
            try:
                csvs = sorted(
                    [
                        p
                        for p in scen_dir.glob("*_yearly.csv")
                        if not p.name.endswith("_yearly_ensemble.csv")
                    ],
                    key=lambda p: p.name.lower(),
                )
                if csvs:
                    f = csvs[0]
            except Exception:
                f = None

        if f is not None and f.exists():
            out[mdir.name] = f

    return out


def discover_block_model_yearly_files(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    block_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    aliases: Optional[dict[str, str]] = None,
    normalize_fn: Optional[callable] = None,
) -> dict[str, Path]:
    """
    Discover per-model block yearly CSVs for a given district+block+scenario.

    Expected structure (compute pipeline):
      {root}/{state}/blocks/{district}/{block}/{model}/{scenario}/{block}_yearly.csv

    Returns:
        Mapping of model_name -> Path to the per-model yearly CSV.
        Returns an empty dict when not found.
    """
    ts_root_p = Path(ts_root)
    base = ts_root_p / state_dir / "blocks"
    if not base.exists():
        return {}

    scenario = str(scenario_name).strip()
    aliases = aliases or {}

    # Resolve district directory (skip "ensembles")
    district_variants = _generate_name_variants(district_display)
    norm = normalize_fn or (lambda s: str(s).strip().lower())
    dist_norm = norm(str(district_display).strip())
    if dist_norm in aliases:
        district_variants.extend(_generate_name_variants(aliases[dist_norm]))
        district_variants = _dedupe(district_variants)

    district_dir: Optional[Path] = None
    for dn in district_variants:
        cand = base / dn
        if cand.exists() and cand.is_dir() and cand.name.lower() != "ensembles":
            district_dir = cand
            break

    if district_dir is None:
        try:
            district_dirs = [d for d in base.iterdir() if d.is_dir() and d.name.lower() != "ensembles"]
            folder_names = [d.name for d in district_dirs]
            best = difflib.get_close_matches(str(district_display).upper(), folder_names, n=1, cutoff=0.7)
            if best:
                district_dir = base / best[0]
        except Exception:
            district_dir = None

    if district_dir is None:
        return {}

    # Resolve block directory within district
    block_variants = _generate_name_variants(block_display)
    block_dir: Optional[Path] = None
    for bn in block_variants:
        cand = district_dir / bn
        if cand.exists() and cand.is_dir():
            block_dir = cand
            break

    if block_dir is None:
        try:
            block_dirs = [d for d in district_dir.iterdir() if d.is_dir()]
            folder_names = [d.name for d in block_dirs]
            best = difflib.get_close_matches(str(block_display).upper(), folder_names, n=1, cutoff=0.7)
            if not best:
                best = difflib.get_close_matches(str(block_display), folder_names, n=1, cutoff=0.7)
            if best:
                block_dir = district_dir / best[0]
        except Exception:
            block_dir = None

    if block_dir is None:
        return {}

    unit_key = block_dir.name
    out: dict[str, Path] = {}
    try:
        model_dirs = sorted(
            [p for p in block_dir.iterdir() if p.is_dir()],
            key=lambda p: p.name.lower(),
        )
    except Exception:
        model_dirs = []

    for mdir in model_dirs:
        scen_dir = mdir / scenario
        if not scen_dir.exists():
            continue

        cands = [
            scen_dir / f"{unit_key}_yearly.csv",
            scen_dir / f"{unit_key.upper()}_yearly.csv",
            scen_dir / f"{unit_key.lower()}_yearly.csv",
        ]
        f: Optional[Path] = None
        for c in cands:
            if c.exists():
                f = c
                break

        if f is None:
            try:
                csvs = sorted(
                    [
                        p
                        for p in scen_dir.glob("*_yearly.csv")
                        if not p.name.endswith("_yearly_ensemble.csv")
                    ],
                    key=lambda p: p.name.lower(),
                )
                if csvs:
                    f = csvs[0]
            except Exception:
                f = None

        if f is not None and f.exists():
            out[mdir.name] = f

    return out
