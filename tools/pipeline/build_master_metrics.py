#!/usr/bin/env python3
"""
CLI wrapper for building master CSVs for IRT processed outputs.

The implementation lives in `india_resilience_tool.compute.master_builder` so:
- tests and the runtime UI (when needed) can import it without importing tools/
- ops usage remains under tools/ for a clean runtime layout
"""

from __future__ import annotations

from india_resilience_tool.compute.master_builder import main


if __name__ == "__main__":
    main()

