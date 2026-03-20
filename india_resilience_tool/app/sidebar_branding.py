"""
Sidebar branding block (logo + link + CSS).

Extracted to keep the dashboard orchestrator focused on UI flow orchestration.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Optional

import streamlit as st


@st.cache_data
def _logo_data_url(*, path: str, mtime: float) -> str | None:
    _ = mtime  # used only to invalidate Streamlit's cache
    try:
        raw = Path(path).read_bytes()
    except Exception:
        return None

    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/png;base64,{b64}"


def render_sidebar_branding(*, logo_path: str | Path, website_url: str = "https://www.resilience.org.in/") -> None:
    logo_url: Optional[str] = None
    try:
        p = Path(logo_path)
        if p.exists():
            logo_url = _logo_data_url(path=str(p), mtime=float(p.stat().st_mtime))
    except Exception:
        logo_url = None

    st.markdown(
        """
        <style>
        .irt-topcluster {
            width: 100%;
            display: flex;
            flex-direction: column;
            align-items: center;
            margin-bottom: 0.1rem;
        }
        .irt-logo {
            width: 220px;
            max-width: 100%;
            height: auto;
            display: block;
            border-radius: 0 !important;
        }
        .irt-link {
            margin-top: 0.25rem;
            margin-bottom: 0.6rem;
            font-size: 0.74rem;
            font-weight: 500;
            line-height: 1.2;
            letter-spacing: 0;
            color: rgba(85, 92, 102, 0.95);
            text-align: center;
            text-decoration: underline;
            text-underline-offset: 0.08em;
            text-decoration-thickness: 1px;
        }
        .irt-link:hover {
            color: rgba(85, 92, 102, 0.95);
            text-decoration-thickness: 2px;
        }
        /* District/Block toggle: keep horizontal labels readable (no letter-by-letter wrapping). */
        [data-testid="stSidebar"] [data-baseweb="radio-group"] {
            flex-wrap: wrap !important; /* allow wrap by option on very narrow sidebars */
            justify-content: center !important;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] label {
            white-space: nowrap !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    logo_html = f'<img class="irt-logo" src="{logo_url}" alt="Resilience Actions logo" />' if logo_url else ""

    st.markdown(
        f"""
        <div class="irt-topcluster">
          {logo_html}
          <a class="irt-link" href="{website_url}" target="_blank" rel="noopener noreferrer">
            www.resilience.org.in/
          </a>
        </div>
        """,
        unsafe_allow_html=True,
    )

