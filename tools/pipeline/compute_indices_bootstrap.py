"""Thin bootstrap CLI for the climate compute pipeline."""

from __future__ import annotations

import importlib
import os
import sys
from typing import Optional, Sequence

from india_resilience_tool.config.metrics_registry import PIPELINE_METRICS_RAW
from tools.pipeline.compute_indices_cli_common import (
    discover_models_lightweight,
    emit_startup_banner,
    parse_args,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Parse args, print a startup banner, then import the heavy runtime module."""
    args = parse_args(argv)
    if args.list_metrics:
        print("Available metrics:")
        for metric in PIPELINE_METRICS_RAW:
            print(f"  {metric['slug']}: {metric['name']}")
        print(f"Total: {len(PIPELINE_METRICS_RAW)}")
        return 0
    if args.list_models:
        print("Discovered models:")
        models = discover_models_lightweight()
        for model in models:
            print(f"  {model}")
        print(f"Total: {len(models)}")
        return 0
    if not args.list_metrics:
        emit_startup_banner(args, stream=sys.stderr)

    os.environ["_IRT_CMP_BOOTSTRAPPED"] = "1"
    runtime = importlib.import_module("tools.pipeline.compute_indices_multiprocess")

    return runtime.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
