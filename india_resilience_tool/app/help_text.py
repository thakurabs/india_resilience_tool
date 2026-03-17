"""
Help-text utilities for Streamlit widget tooltips.
"""

from __future__ import annotations


def help_md_to_plain_text(md: str) -> str:
    """
    Convert short Markdown help snippets into plain text for Streamlit widget tooltips.

    Notes:
      - Streamlit widget `help=` is plain text (not Markdown).
      - Keep this intentionally lightweight; it's not a full Markdown renderer.
    """
    txt = str(md or "").replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    for raw in txt.split("\n"):
        line = raw.strip()
        if not line:
            lines.append("")
            continue
        if line.startswith("#"):
            line = line.lstrip("#").strip()
        line = line.replace("**", "").replace("`", "")
        lines.append(line)
    return "\n".join(lines).strip()


RIBBON_HELP_MD: dict[str, str] = {
    "assessment_pillar": (
        "### Assessment pillar\n"
        "An assessment pillar is the broadest thematic layer in the dashboard "
        "(for example, Climate Hazards or Bio-physical Hazards).\n\n"
        "**How to use**\n"
        "- Start here to choose the kind of layer you want to explore."
    ),
    "risk_domain": (
        "### Domain\n"
        "A domain groups related metrics into a narrower theme within the selected assessment pillar.\n\n"
        "**How to use**\n"
        "- Choose a domain to narrow the metric list to what you care about."
    ),
    "metric": (
        "### Metric\n"
        "A metric is the specific climate index you are mapping (e.g., TXx, hot days, wet spells).\n\n"
        "**How to use**\n"
        "- Pick the metric that matches your question (extremes vs averages, heat vs rainfall, etc.)."
    ),
    "scenario": (
        "### Scenario\n"
        "Scenarios describe different global development and emissions pathways used for future climate projections.\n\n"
        "**Options in this tool**\n"
        "- Middle-of-the-road (SSP2-4.5)\n"
        "- Fossil-fuelled development (SSP5-8.5)\n\n"
        "**Tip**\n"
        "- Use SSP2-4.5 for baseline planning and SSP5-8.5 to stress-test."
    ),
    "period": (
        "### Period\n"
        "The multi-year time window over which values are summarized.\n\n"
        "**Periods**\n"
        "- Baseline: 1995–2014\n"
        "- Early century: 2021–2040\n"
        "- Mid century: 2041–2060\n"
        "- Late century: 2061–2080\n"
        "- End century: 2081–2100"
    ),
    "statistic": (
        "### Statistic\n"
        "- **Mean (average):** sensitive to extreme values.\n"
        "- **Median (typical):** more robust when values are skewed or have outliers.\n\n"
        "**Tip**\n"
        "- If results look pulled by extremes, try Median."
    ),
    "map_mode": (
        "### Map mode\n"
        "- **Absolute value:** maps the metric as-is for the selected period.\n"
        "- **Change from baseline:** maps (future period − baseline) to show how conditions shift.\n\n"
        "**Example**\n"
        "If TXx is 42°C in 2041–2060 and 40°C in baseline, change = +2°C."
    ),
}
