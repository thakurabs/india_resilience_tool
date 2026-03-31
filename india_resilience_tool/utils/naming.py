"""
Name normalization and alias helpers for IRT.

This module centralizes the canonical behavior used across:
- ADM2↔master join keying (district/state normalization)
- portfolio keying
- time-series discovery fallbacks

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Mapping, Optional


# Keep this default mapping aligned with what the dashboard previously used.
# You can expand it safely over time, but avoid changing existing keys/values
# unless you also rebuild masters / verify joins.
NAME_ALIASES: dict[str, str] = {
    "hanamkonda": "hanumakonda",
    "j b r bhupalpally": "jayashankar bhupalpalli",
    "jayashankar bhupalpally": "jayashankar bhupalpalli",
    "b r ambedkar bhupalpally": "jayashankar bhupalpalli",
    "bhadradri kothagudem": "bhadradri kothagudem",
    "jogulamba gadwal": "jogulamba gadwal",
}


def normalize_name(s: str) -> str:
    """
    Normalize a name into a canonical comparison form.

    Contract (behavior-preserving with the legacy dashboard):
      - ASCII fold (NFKD -> ascii)
      - lowercase
      - convert underscores/hyphens to spaces
      - remove non-alphanum except spaces
      - collapse multiple spaces

    Args:
        s: Input string

    Returns:
        Normalized string (may be empty).
    """
    if s is None:
        return ""

    s2 = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s2 = s2.lower()
    s2 = re.sub(r"[_\-]+", " ", s2)
    s2 = re.sub(r"[^a-z0-9 ]+", "", s2)
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2


def alias(s: str, aliases: Optional[Mapping[str, str]] = None) -> str:
    """
    Normalize then apply aliases.

    Args:
        s: Raw input string
        aliases: Optional mapping applied after normalization. If None, uses NAME_ALIASES.

    Returns:
        Aliased normalized string.
    """
    k = normalize_name(s)
    amap = NAME_ALIASES if aliases is None else dict(aliases)
    return amap.get(k, k)


def normalize_compact(s: str, aliases: Optional[Mapping[str, str]] = None) -> str:
    """
    Normalize + alias then remove spaces.

    This matches portfolio-style normalization (handles "Sanga Reddy" vs "Sangareddy").

    Args:
        s: Raw input string
        aliases: Optional alias mapping override

    Returns:
        Compact normalized string.
    """
    return alias(s, aliases=aliases).replace(" ", "")


def safe_fs_component(s: str) -> str:
    """
    Return the legacy filesystem-safe token used by processed output folders.

    This preserves the historical hydro/admin folder convention of replacing
    spaces and slashes with underscores without changing case.
    """
    return str(s).strip().replace(" ", "_").replace("/", "_")


def hydro_fs_token(s: str, *, max_length: int = 48) -> str:
    """
    Return a deterministic hydro folder token that stays Windows-path friendly.

    For short names this preserves the legacy token exactly. For long names it
    appends a stable hash suffix after truncation so folder names remain unique
    while avoiding `MAX_PATH` failures on Windows.
    """
    token = safe_fs_component(s)
    if len(token) <= max_length:
        return token

    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:8]
    prefix_len = max(1, max_length - len(digest) - 1)
    prefix = token[:prefix_len].rstrip("_") or token[:prefix_len]
    return f"{prefix}_{digest}"
