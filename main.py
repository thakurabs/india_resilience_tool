"""
Repo-root Streamlit entrypoint for the India Resilience Tool (IRT).

This keeps the simplest runtime command:

  streamlit run main.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Streamlit executes scripts with the script's directory on sys.path.
# Ensure repo root is present so `import india_resilience_tool...` works reliably.
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from india_resilience_tool.app.main import run


if __name__ == "__main__":
    run()

