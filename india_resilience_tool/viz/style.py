"""
Shared matplotlib styling helpers for India Resilience Tool (IRT) visualizations.

This module defines a small style contract for figures generated in:
- Streamlit dashboard panels (single district / portfolio panels)
- PDF exports (district yearly plot, case-study pages)

Goals:
- consistent typography (font sizes)
- consistent default aspect ratio for dashboard figures (16:9)
- optional Resilience Actions logo placement on figures/pages

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, Union

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnnotationBbox, OffsetImage

PathLike = Union[str, Path]


@dataclass(frozen=True)
class IRTFigureStyle:
    """Style contract for IRT figures."""

    # Defaults for Streamlit panels (16:9)
    panel_figsize: Tuple[float, float] = (8.0, 4.5)
    mini_figsize: Tuple[float, float] = (6.4, 3.6)

    # Raster density
    fig_dpi: int = 150
    savefig_dpi: int = 150

    # Typography
    title_size: int = 12
    label_size: int = 10
    tick_size: int = 9
    legend_size: int = 9

    # Common plot geometry
    line_width: float = 2.0
    grid_alpha: float = 0.25
    grid_linestyle: str = "--"
    grid_linewidth: float = 0.6


def ensure_16x9_figsize(
    figsize: Tuple[float, float],
    *,
    mode: str = "fit_width",
) -> Tuple[float, float]:
    """Return a 16:9 figure size derived from an existing (w, h).

    Args:
        figsize: (width_inches, height_inches)
        mode: "fit_width" keeps width and adjusts height;
              "fit_height" keeps height and adjusts width.

    Returns:
        A (width_inches, height_inches) tuple with aspect 16:9.
    """
    w, h = float(figsize[0]), float(figsize[1])
    if w <= 0 or h <= 0:
        return (8.0, 4.5)

    target = 16.0 / 9.0
    if mode == "fit_height":
        return (h * target, h)
    return (w, w / target)


def irt_rcparams(style: Optional[IRTFigureStyle] = None) -> Dict[str, Any]:
    """Matplotlib rcParams used across IRT figures."""
    s = style or IRTFigureStyle()
    return {
        "figure.dpi": s.fig_dpi,
        "savefig.dpi": s.savefig_dpi,
        "axes.titlesize": s.title_size,
        "axes.labelsize": s.label_size,
        "xtick.labelsize": s.tick_size,
        "ytick.labelsize": s.tick_size,
        "legend.fontsize": s.legend_size,
        "axes.titlepad": 6.0,
    }


@contextmanager
def irt_style_context(style: Optional[IRTFigureStyle] = None) -> Iterator[None]:
    """Context manager that applies IRT rcParams for consistent styling."""
    with plt.rc_context(irt_rcparams(style)):
        yield


def strip_spines(ax: Any) -> None:
    """Hide all spines for a cleaner dashboard look."""
    try:
        for spine in ax.spines.values():
            spine.set_visible(False)
    except Exception:
        return


def add_ra_logo(
    fig: Any,
    logo_path: Optional[PathLike],
    *,
    width_frac: float = 0.12,
    pad_frac: float = 0.012,
    alpha: float = 0.95,
) -> None:
    """Add the Resilience Actions logo to the top-right corner of a figure.

    The logo is placed in figure fraction coordinates and overlays the figure.
    This is safe for both dashboard figures and full-page PDF figures.

    Args:
        fig: Matplotlib figure.
        logo_path: Path to logo image (png/jpg). If None/missing, no-op.
        width_frac: Desired logo width as fraction of figure width.
        pad_frac: Padding from top-right corner (figure fraction).
        alpha: Logo alpha.
    """
    if fig is None or not logo_path:
        return

    try:
        p = Path(logo_path).expanduser()
    except Exception:
        return

    if not p.exists():
        return

    try:
        img = mpimg.imread(str(p))
    except Exception:
        return

    try:
        fig_w_px = float(fig.get_figwidth()) * float(fig.dpi)
        desired_w_px = max(1.0, fig_w_px * float(width_frac))
        img_w_px = float(getattr(img, "shape", [0, 0])[1] or 0.0)
        if img_w_px <= 0:
            return
        zoom = desired_w_px / img_w_px
    except Exception:
        zoom = 0.2

    oi = OffsetImage(img, zoom=zoom)
    oi.set_alpha(alpha)

    ab = AnnotationBbox(
        oi,
        (1.0 - pad_frac, 1.0 - pad_frac),
        xycoords="figure fraction",
        frameon=False,
        box_alignment=(1.0, 1.0),
        zorder=50,
    )
    try:
        fig.add_artist(ab)
    except Exception:
        return
