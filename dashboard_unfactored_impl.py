#!/usr/bin/env python3
"""
Legacy dashboard implementation shim (Step 22).

This file is intentionally tiny and is kept for backward compatibility with the
existing dashboard runner. The actual dashboard logic is executed via:

    india_resilience_tool.app.orchestrator:run_app()

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.orchestrator import run_app

# IMPORTANT: no __name__ == "__main__" guard.
# This file is executed each rerun by the Step 21 loader, and it must run on import.
run_app()
