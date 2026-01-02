"""
State summary view for IRT.

This module renders the state summary panel shown when:
- analysis_mode is "Single district focus"
- selected_district is "All"
- selected_state is not "All"

It includes:
- State summary header with index/scenario/period
- District-wise distribution boxplot
- State summary statistics
- Per-model state averages
- Trend over time (state average)

Widget keys preserved:
- btn_state_boxplot_{variable_slug}_{state}_{scenario}_{period}_{stat}
- btn_state_trend_{variable_slug}_{state}_{scenario}

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Optional

import pandas as pd


def render_state_summary_view(
    *,
    # State/selection context
    selected_state: str,
    selected_district: str = "All",
    # Variable/metric context
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    # Pre-computed metrics
    ensemble: dict,
    per_model_df: pd.DataFrame,
    sel_districts_gdf: Any,  # GeoDataFrame or None (districts OR blocks depending on level)
    # Config
    processed_root: Path,
    pilot_state: str,
    # Callable dependencies
    make_state_boxplot_fn: Callable[..., Any],
    # Block-level support
    level: str = "district",
) -> None:
    """
    Render the state summary view (shown when no district is selected).

    This is shown in Single district focus mode when selected_district == "All"
    but selected_state != "All".
    """
    import matplotlib.pyplot as plt
    import streamlit as st

    variable_label = variables.get(variable_slug, {}).get("label", variable_slug)

    level_norm = str(level).strip().lower()
    is_block = level_norm == "block"

    if is_block and selected_district != "All":
        st.subheader(f"{selected_district} — District summary (Blocks)")
        st.markdown(
            f"**State:** {selected_state}  \n"
            f"**District:** {selected_district}  \n"
            f"**Index:** {variable_label}  \n"
            f"**Scenario:** {sel_scenario}  \n"
            f"**Period:** {sel_period}"
        )
        n_units = (
            ensemble.get("n_blocks")
            or ensemble.get("n_units")
            or ensemble.get("n_districts")
            or 0
        )
        st.caption(f"Showing {int(n_units)} blocks in {selected_district} district")
        exp_title = "Block-wise distribution across models"
        btn_label = "Generate block-wise boxplot"
        btn_key = f"btn_block_boxplot_{variable_slug}_{selected_state}_{selected_district}_{sel_scenario}_{sel_period}_{sel_stat}"
    else:
        st.subheader(f"{selected_state} — State summary")
        st.markdown(
            f"**Index:** {variable_label}  \n"
            f"**Scenario:** {sel_scenario}  \n"
            f"**Period:** {sel_period}"
        )
        exp_title = "District-wise distribution across models"
        btn_label = "Generate district-wise boxplot"
        btn_key = f"btn_state_boxplot_{variable_slug}_{selected_state}_{sel_scenario}_{sel_period}_{sel_stat}"

    # --- Expander 1: Unit-wise distribution across models (boxplot) ---
    with st.expander(exp_title, expanded=False):
        st.caption(
            "This figure can be slow to generate because it uses per-model "
            "distributions for each district."
        )
        if st.button(
            btn_label,
            key=btn_key,
        ):
            fig_box = make_state_boxplot_fn(
                sel_districts_gdf=sel_districts_gdf,
                metric_col=metric_col,
                metric_label=variable_label,
                sel_state=selected_state,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
            )
            if fig_box is not None:
                st.pyplot(fig_box, use_container_width=True)
            else:
                st.info(
                    "Per-model district data is not available for this index, "
                    "so the boxplot could not be generated."
                )

    # --- Expander 2: State summary statistics ---
    with st.expander("State summary statistics", expanded=False):
        if ensemble.get("n_districts", 0) > 0:
            stat_rows = [
                {"Statistic": "mean", "Value": f"{ensemble['mean']:.2f}"},
                {"Statistic": "median", "Value": f"{ensemble['median']:.2f}"},
                {"Statistic": "p05", "Value": f"{ensemble['p05']:.2f}"},
                {"Statistic": "p95", "Value": f"{ensemble['p95']:.2f}"},
                {"Statistic": "std", "Value": f"{ensemble['std']:.2f}"},
                {
                    "Statistic": "n_districts",
                    "Value": str(int(ensemble["n_districts"])),
                },
            ]
            st.table(pd.DataFrame(stat_rows).set_index("Statistic"))
        else:
            st.info("No numeric district values found for this state & selection.")

    # --- Expander 3: Per-model state averages ---
    with st.expander("Per-model state averages", expanded=False):
        if per_model_df is not None and not per_model_df.empty:
            st.dataframe(
                per_model_df.rename(
                    columns={"value": "state_avg", "n_districts": "n_districts_used"}
                ),
                use_container_width=True,
            )

        if sel_districts_gdf is not None and not sel_districts_gdf.empty:
            st.caption(f"Districts used: {len(sel_districts_gdf)}")

    # --- Expander 4: Trend over time (state average) ---
    with st.expander("Trend over time (state average)", expanded=False):
        st.caption(
            "Generates a state-average yearly trend plot and PDF for the "
            "selected index, scenario, and period."
        )

        if st.button(
            "Generate state-average trend PDF",
            key=f"btn_state_trend_{variable_slug}_{selected_state}_{sel_scenario}",
        ):
            _yearly_df = _load_state_yearly_cached(str(processed_root), pilot_state)
            pdf_path = _make_state_yearly_pdf(
                _yearly_df,
                selected_state,
                sel_scenario,
                variable_label,
                processed_root / "pdf_plots",
            )
            if pdf_path is not None and pdf_path.exists():
                with open(pdf_path, "rb") as fh:
                    st.download_button(
                        "⬇️ Download state-average time-series (PDF)",
                        fh.read(),
                        file_name=pdf_path.name,
                        mime="application/pdf",
                    )
            else:
                st.info(
                    "State-average yearly time-series is not available for this combination."
                )


def _load_state_yearly_cached(ts_root_str: str, state_dir: str) -> pd.DataFrame:
    """Load state yearly data with Streamlit caching."""
    import streamlit as st

    @st.cache_data
    def _inner(ts_root_str: str, state_dir: str) -> pd.DataFrame:
        from india_resilience_tool.analysis.timeseries import load_state_yearly

        return load_state_yearly(
            ts_root=Path(ts_root_str),
            state_dir=state_dir,
            varcfg=None,
        )

    return _inner(ts_root_str, state_dir)


def _make_state_yearly_pdf(
    df_yearly: pd.DataFrame,
    state_name: str,
    scenario_name: str,
    metric_label: str,
    out_dir: Path,
) -> Optional[Path]:
    """Generate a state-average yearly time-series PDF."""
    import matplotlib.pyplot as plt

    if df_yearly is None or df_yearly.empty:
        return None
    d = df_yearly.copy()
    
    # Handle both 'state' and 'state_name' column naming conventions
    state_col = "state" if "state" in d.columns else ("state_name" if "state_name" in d.columns else None)
    scenario_col = "scenario" if "scenario" in d.columns else None
    
    if state_col is None or scenario_col is None:
        return None
    
    d = d[
        (d[state_col].astype(str).str.strip().str.lower() == state_name.strip().lower())
        & (
            d[scenario_col]
            .astype(str)
            .str.strip()
            .str.lower()
            == scenario_name.strip().lower()
        )
    ]
    if d.empty:
        return None
    for c in ("year", "mean", "p05", "p95"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["year"]).sort_values("year")
    if d.empty:
        return None

    fig, ax = plt.subplots(figsize=(7.5, 4.5), dpi=150)
    x = d["year"]
    y = d["mean"]
    ax.plot(x, y, marker="o", linewidth=1.5, label="Mean")

    if "p05" in d.columns and "p95" in d.columns:
        ax.fill_between(
            x,
            d["p05"],
            d["p95"],
            alpha=0.2,
            label="5–95% range",
        )

    ax.set_xlabel("Year")
    ax.set_ylabel(metric_label)
    ax.set_title(f"{state_name} — {metric_label} ({scenario_name})")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
    ax.legend(frameon=False, ncol=3, fontsize=9)
    out_dir.mkdir(parents=True, exist_ok=True)

    def _safe(s: str) -> str:
        return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(s))

    pdf_path = (
        out_dir
        / f"{_safe(state_name)}__{_safe(metric_label)}__{_safe(scenario_name)}__yearly_timeseries.pdf"
    )
    fig.tight_layout()
    fig.savefig(pdf_path, format="pdf")
    plt.close(fig)
    return pdf_path