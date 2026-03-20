"""
Compute module for the India Resilience Tool (IRT).

This module provides adapters and utilities for computing climate indices,
including integration with external packages like `climate-indices`.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from .spi_adapter import (
    compute_spi_climate_indices,
    compute_spi_for_unit,
    SPIResult,
    Distribution,
)

__all__ = [
    "compute_spi_climate_indices",
    "compute_spi_for_unit",
    "SPIResult",
    "Distribution",
]