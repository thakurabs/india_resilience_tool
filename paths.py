from pathlib import Path

def find_repo_root(start: Path | None = None) -> Path:
    """
    Walk up from `start` until we find something that looks like the repo root.
    Falls back to the directory of this file if no marker is found.
    """
    if start is None:
        start = Path(__file__).resolve()

    # Markers that usually live in the repo root
    markers = [".git", "pyproject.toml", "setup.cfg", "requirements.txt"]

    for parent in [start] + list(start.parents):
        if any((parent / m).exists() for m in markers):
            return parent

    # Fallback: directory containing this file
    return Path(__file__).resolve().parent


# 1. Repo root: e.g. D:\projects\india_resilience_tool
REPO_ROOT = find_repo_root()

# 2. Parent of repo root: e.g. D:\projects
PROJECTS_ROOT = REPO_ROOT.parent

# 3. Data dir: sibling folder of the repo: e.g. D:\projects\irt_data
DATA_DIR = (PROJECTS_ROOT / "irt_data").resolve()


# 4. All other paths built *relative* to DATA_DIR
DATA_ROOT = DATA_DIR / "r1i1p1f1"
DISTRICTS_PATH = DATA_DIR / "districts_4326.geojson"
BASE_OUTPUT_ROOT = DATA_DIR / "processed"
