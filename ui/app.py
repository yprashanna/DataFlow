"""DataFlow — Streamlit monitoring dashboard.

Run with: streamlit run ui/app.py
Deploy for free on Streamlit Cloud (streamlit.io/cloud).

Dashboard sections:
  1. System health overview (KPI cards)
  2. Pipeline run history table
  3. Success/failure rate per pipeline
  4. Latency trend charts
  5. Data quality scores over time
  6. Manual pipeline trigger
"""

import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Make sure imports work whether running from root or ui/ dir
sys.path.insert(0, str(Path(__file__).parent.parent))

from monitoring.metadata import MetadataStore
from monitoring.health import HealthMonitor
from orchestrator.config_parser import load_pipeline_config, list_pipeline_configs
from orchestrator.runner import PipelineRunner

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataFlow Monitor",
    page_icon="🔀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Minimal custom CSS ───────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .metric-card {
        background: #1e1e2e;
        border: 1px solid #313244;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.5rem;
    }
    .status-success { color: #a6e3a1; font-weight: 600; }
    .status-failed  { color: #f38ba8; font-weight: 600; }
    .status-running { color: #fab387; font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Init stores ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_stores():
    return MetadataStore(), HealthMonitor()

metadata_store, health_monitor = get_stores()

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔀 DataFlow")
    st.caption("Data Pipeline Orchestration")
    st.divider()

    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if auto_refresh:
        time.sleep(30)
        st.rerun()

    st.divider()
    st.subheader("▶ Trigger Pipeline")

    config_files = list_pipeline_configs("configs")
    pipeline_options = {}
    for cf in config_files:
        try:
            cfg = load_pipeline_config(cf)
            pipeline_options[cfg["name"]] = cf
        except Exception:
            pass

    if pipeline_options:
        selected_pipeline = st.selectbox("Select pipeline", list(pipeline_options.keys()))
        if st.button("Run Now", type="primary", use_container_width=True):
            with st.spinner(f"Running {selected_pipeline}…"):
                try:
                    cfg = load_pipeline_config(pipeline_options[selected_pipeline])
                    runner = PipelineRunner(cfg)
                    result = runner.run()
                    if result["status"] == "success":
                        st.success(
                            f"✅ Done in {result.get('total_latency_ms', 0):.0f}ms — "
                            f"{result.get('rows_loaded', 0)} rows loaded"
                        )
                    else:
                        st.error(f"❌ Failed: {result.get('error', 'unknown error')}")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Error: {exc}")
    else:
        st.info("No pipeline configs found in configs/")

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Main content ─────────────────────────────────────────────────────────────
st.title("📊 DataFlow Monitoring Dashboard")

# ─ 1. System health KPIs ────────────────────────────────────────────────────
overall = health_monitor.get_overall_health()

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Total Pipelines",
    overall.get("total_pipelines", 0),
)
c2.metric(
    "Total Runs",
    overall.get("total_runs", 0),
)
success_rate = overall.get("overall_success_rate")
c3.metric(
    "Success Rate",
    f"{success_rate:.1f}%" if success_rate is not None else "—",
)
quality = overall.get("avg_quality_score")
c4.metric(
    "Avg Quality Score",
    f"{quality:.1f}%" if quality is not None else "—",
)

st.divider()

# ─ 2. Pipeline stats table ───────────────────────────────────────────────────
st.subheader("📋 Pipeline Summary")
stats_df = metadata_store.get_pipeline_stats()

if stats_df.empty:
    st.info(
        "No pipeline runs yet. Use the sidebar to trigger a pipeline, "
        "or run `make run-pipeline` from the terminal."
    )
else:
    # Compute success rate column
    stats_df["success_rate_%"] = (
        stats_df["successful_runs"] / stats_df["total_runs"] * 100
    ).round(1)

    display_cols = [
        "pipeline_name", "total_runs", "successful_runs", "failed_runs",
        "success_rate_%", "avg_quality_score", "avg_latency_ms",
        "total_rows_loaded", "last_run_at",
    ]
    display_df = stats_df[[c for c in display_cols if c in stats_df.columns]]

    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "pipeline_name": st.column_config.TextColumn("Pipeline"),
            "total_runs": st.column_config.NumberColumn("Total Runs"),
            "successful_runs": st.column_config.NumberColumn("✅ Success"),
            "failed_runs": st.column_config.NumberColumn("❌ Failed"),
            "success_rate_%": st.column_config.NumberColumn("Success Rate %", format="%.1f%%"),
            "avg_quality_score": st.column_config.NumberColumn("Avg Quality Score", format="%.1f"),
            "avg_latency_ms": st.column_config.NumberColumn("Avg Latency (ms)", format="%.0f"),
            "total_rows_loaded": st.column_config.NumberColumn("Total Rows Loaded"),
            "last_run_at": st.column_config.TextColumn("Last Run"),
        },
        hide_index=True,
    )

    st.divider()

    # ─ 3. Charts per pipeline ─────────────────────────────────────────────────
    pipeline_names = metadata_store.get_all_pipeline_names()

    if pipeline_names:
        st.subheader("📈 Pipeline Trends")
        selected = st.selectbox("Select pipeline for trend charts", pipeline_names, key="trend_sel")

        trend_df = metadata_store.get_latency_trend(selected, limit=30)

        if not trend_df.empty:
            col_a, col_b = st.columns(2)

            with col_a:
                st.markdown("**Latency Over Time (ms)**")
                fig_lat = px.line(
                    trend_df,
                    x="started_at",
                    y="total_latency_ms",
                    color="status",
                    color_discrete_map={"success": "#a6e3a1", "failed": "#f38ba8"},
                    markers=True,
                    template="plotly_dark",
                )
                fig_lat.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                    showlegend=True,
                    xaxis_title="",
                    yaxis_title="ms",
                )
                st.plotly_chart(fig_lat, use_container_width=True)

            with col_b:
                st.markdown("**Data Quality Score Over Time**")
                fig_qual = px.line(
                    trend_df,
                    x="started_at",
                    y="quality_score",
                    markers=True,
                    template="plotly_dark",
                    color_discrete_sequence=["#89b4fa"],
                )
                fig_qual.add_hline(
                    y=80,
                    line_dash="dot",
                    line_color="#f38ba8",
                    annotation_text="Alert threshold (80%)",
                )
                fig_qual.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                    yaxis_range=[0, 105],
                    xaxis_title="",
                    yaxis_title="Quality Score",
                )
                st.plotly_chart(fig_qual, use_container_width=True)

            # ─ 4. Success/fail pie ──────────────────────────────────────────
            col_c, col_d = st.columns(2)

            with col_c:
                st.markdown("**Run Status Distribution**")
                status_counts = trend_df["status"].value_counts().reset_index()
                status_counts.columns = ["status", "count"]
                fig_pie = px.pie(
                    status_counts,
                    names="status",
                    values="count",
                    color="status",
                    color_discrete_map={"success": "#a6e3a1", "failed": "#f38ba8", "running": "#fab387"},
                    template="plotly_dark",
                    hole=0.45,
                )
                fig_pie.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig_pie, use_container_width=True)

            with col_d:
                st.markdown("**Rows Loaded Per Run**")
                fig_rows = px.bar(
                    trend_df,
                    x="started_at",
                    y="rows_loaded",
                    color="status",
                    color_discrete_map={"success": "#a6e3a1", "failed": "#f38ba8"},
                    template="plotly_dark",
                )
                fig_rows.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=10, b=0),
                    xaxis_title="",
                    yaxis_title="Rows",
                    showlegend=False,
                )
                st.plotly_chart(fig_rows, use_container_width=True)

        else:
            st.info(f"No trend data yet for '{selected}'")

    st.divider()

    # ─ 5. Recent runs log ─────────────────────────────────────────────────────
    st.subheader("📜 Recent Run Log")
    recent_df = metadata_store.get_recent_runs(limit=50)

    if not recent_df.empty:
        log_cols = ["run_id", "pipeline_name", "status", "rows_ingested", "rows_loaded",
                    "quality_score", "total_latency_ms", "error", "started_at"]
        log_display = recent_df[[c for c in log_cols if c in recent_df.columns]]

        st.dataframe(
            log_display,
            use_container_width=True,
            column_config={
                "status": st.column_config.TextColumn("Status"),
                "quality_score": st.column_config.NumberColumn("Quality %", format="%.1f"),
                "total_latency_ms": st.column_config.NumberColumn("Latency (ms)", format="%.0f"),
            },
            hide_index=True,
        )

st.caption(
    "DataFlow — 100% free stack. "
    "SQLite · Pandas · APScheduler · FastAPI · Streamlit"
)
