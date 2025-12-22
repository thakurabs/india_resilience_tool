#!/usr/bin/env python3
"""
Backward-compatible shim for the Streamlit dashboard.

This file remains at repo root so existing workflows like:
  streamlit run dashboard_unfactored.py
continue to work.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.main import run


if __name__ == "__main__":
    run()
