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
