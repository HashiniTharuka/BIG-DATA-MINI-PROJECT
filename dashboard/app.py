"""Consolidated report/dashboard deliverable (Streamlit).

Reads directly from the Postgres serving tables that both the Lambda speed
layer (vitals_realtime, alerts) and batch layer (daily_risk_report) write
to. Three views: a live ward view answering "what's happening right now",
a daily risk report answering "how does yesterday's lab data change the
risk picture" (the business question from the use case brief), and a
pipeline health view for observability.
"""
from __future__ import annotations

import os
import sys
import time
import warnings
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.db import get_conn_params  # noqa: E402

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

st.set_page_config(page_title="Hospital Ward Monitor", layout="wide", page_icon="🏥")

# ---------------------------------------------------------------------------
# Palette (validated: CVD-safe categorical order + fixed status roles)
# ---------------------------------------------------------------------------
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
SURFACE = "#fcfcfb"
GRID = "#e1e0d9"

STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

VITALS_STATUS_COLOR = {"normal": STATUS["good"], "warning": STATUS["warning"], "critical": STATUS["critical"]}
RISK_COLOR = {"low": STATUS["good"], "medium": STATUS["warning"], "high": STATUS["serious"], "critical": STATUS["critical"]}
SEVERITY_COLOR = {"warning": STATUS["warning"], "critical": STATUS["critical"]}

st.markdown(
    f"""
    <style>
    .stApp {{ background-color: #f9f9f7; color: {INK_PRIMARY}; }}
    .stApp p, .stApp span, .stApp label, .stApp li, .stApp div,
    .stApp td, .stApp th, .stApp caption, .stMarkdown, .stCaption,
    [data-testid="stCaptionContainer"], [data-testid="stMetricValue"],
    [data-testid="stWidgetLabel"], .stTabs [data-baseweb="tab"] {{ color: {INK_PRIMARY}; }}
    .stApp .badge {{ color: #ffffff; }}
    [data-testid="stMetricLabel"], [data-testid="stCaptionContainer"] {{ color: {INK_SECONDARY}; }}
    .stApp table {{ background: {SURFACE}; border-collapse: collapse; width: 100%; }}
    .stApp th {{ background: #ecebe6; text-align: left; padding: 6px 10px; }}
    .stApp td {{ padding: 6px 10px; border-bottom: 1px solid {GRID}; }}
    [data-testid="stSidebar"] {{ background-color: {SURFACE}; }}
    [data-testid="stHeader"] {{ background: transparent; }}
    .block-container {{ padding-top: 1.6rem; max-width: 1200px; }}
    h1, h2, h3 {{ color: {INK_PRIMARY}; font-weight: 700; }}
    [data-testid="stMetric"] {{
        background: {SURFACE};
        border: 1px solid {GRID};
        border-radius: 10px;
        padding: 14px 16px 10px 16px;
    }}
    [data-testid="stMetricLabel"] {{ color: {INK_SECONDARY}; font-size: 0.85rem; }}
    .badge {{
        display: inline-block; padding: 2px 10px; border-radius: 999px;
        font-size: 0.78rem; font-weight: 600; color: white; text-align: center;
    }}
    .health-card {{
        background: {SURFACE}; border: 1px solid {GRID}; border-radius: 10px;
        padding: 12px 16px; margin-bottom: 8px; display: flex; align-items: center;
        justify-content: space-between;
    }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 4px; }}
    .stTabs [data-baseweb="tab"] {{
        background: {SURFACE}; border-radius: 8px 8px 0 0; padding: 8px 18px;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_conn():
    return psycopg2.connect(**get_conn_params())


def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    except psycopg2.Error:
        conn.rollback()
        # Reconnect once on a broken connection (e.g. transient network blip).
        st.cache_resource.clear()
        conn = get_conn()
        return pd.read_sql(sql, conn, params=params)


def badge(text: str, color: str) -> str:
    return f'<span class="badge" style="background-color:{color}">{text.title()}</span>'


def chart_layout(fig: go.Figure, title: str, height: int = 320) -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(size=15, color=INK_PRIMARY)),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(color=INK_SECONDARY, family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        height=height,
        margin=dict(t=48, l=10, r=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID, color=INK_MUTED)
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=GRID, color=INK_MUTED)
    return fig


def status_donut(counts: dict, color_map: dict, title: str) -> go.Figure:
    labels = [k for k, v in counts.items() if v > 0]
    values = [v for v in counts.values() if v > 0]
    colors = [color_map[label] for label in labels]
    if not labels:
        labels, values, colors = ["no data"], [1], [GRID]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=[label.title() for label in labels],
                values=values,
                hole=0.62,
                marker=dict(colors=colors, line=dict(color=SURFACE, width=2)),
                textinfo="value",
                textfont=dict(color="white", size=13),
            )
        ]
    )
    return chart_layout(fig, title, height=280)


def render():
    st.title("🏥 Hospital Ward Monitor")
    st.caption(
        "**Lambda architecture** · speed layer (Spark Structured Streaming) drives the live ward view; "
        "batch layer (Airflow) drives the daily risk report."
    )

    tab_live, tab_risk, tab_health = st.tabs(["📡 Live Ward", "📋 Daily Risk Report", "⚙️ Pipeline Health"])

    # ---------------- Live ward view (speed layer) ----------------
    with tab_live:
        realtime = run_query("""
            SELECT patient_id, avg_heart_rate, avg_spo2, avg_systolic_bp, avg_diastolic_bp,
                   avg_temperature, reading_count, abnormal_count, status, updated_at
            FROM vitals_realtime ORDER BY patient_id
        """)
        alerts_recent = run_query("""
            SELECT patient_id, metric, value, severity, reason, event_time
            FROM alerts WHERE created_at > now() - interval '15 minutes'
            ORDER BY event_time DESC LIMIT 50
        """)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Active patients", len(realtime))
        n_normal = int((realtime["status"] == "normal").sum()) if not realtime.empty else 0
        n_warn = int((realtime["status"] == "warning").sum()) if not realtime.empty else 0
        n_crit = int((realtime["status"] == "critical").sum()) if not realtime.empty else 0
        col2.metric("Normal", n_normal)
        col3.metric("Warning", n_warn)
        col4.metric("Critical", n_crit)
        col5.metric("Alerts (15 min)", len(alerts_recent))

        if realtime.empty:
            st.info("Waiting for the first streaming window... (Spark aggregates every ~20s)")
        else:
            left, right = st.columns([2, 1])
            with left:
                st.subheader("Ward roster")
                q = st.text_input("Filter by patient ID", "", placeholder="e.g. P0007")
                view = realtime.copy()
                if q:
                    view = view[view["patient_id"].str.contains(q, case=False, na=False)]
                view.insert(
                    len(view.columns) - 1, "status_badge",
                    view["status"].apply(lambda s: badge(s, VITALS_STATUS_COLOR.get(s, INK_MUTED))),
                )
                display_cols = [
                    "patient_id", "avg_heart_rate", "avg_spo2", "avg_systolic_bp",
                    "avg_diastolic_bp", "avg_temperature", "reading_count", "abnormal_count",
                    "status_badge", "updated_at",
                ]
                st.write(
                    view[display_cols].rename(columns={"status_badge": "status"}).to_html(
                        escape=False, index=False
                    ),
                    unsafe_allow_html=True,
                )
            with right:
                counts = {"normal": n_normal, "warning": n_warn, "critical": n_crit}
                st.plotly_chart(
                    status_donut(counts, VITALS_STATUS_COLOR, "Ward status mix"),
                    use_container_width=True,
                )

        st.divider()
        st.subheader("Recent alerts (last 15 min)")
        if alerts_recent.empty:
            st.success("No alerts in the last 15 minutes.")
        else:
            a = alerts_recent.copy()
            a["severity_badge"] = a["severity"].apply(lambda s: badge(s, SEVERITY_COLOR.get(s, INK_MUTED)))
            cols = ["patient_id", "metric", "value", "severity_badge", "reason", "event_time"]
            st.write(
                a[cols].rename(columns={"severity_badge": "severity"}).to_html(escape=False, index=False),
                unsafe_allow_html=True,
            )

    # ---------------- Daily risk report (batch layer) ----------------
    with tab_risk:
        dates = run_query("SELECT DISTINCT batch_date FROM daily_risk_report ORDER BY batch_date DESC")
        if dates.empty:
            st.info("No daily risk report yet — waits for the first simulated day's Airflow DAG run.")
        else:
            selected_date = st.selectbox("Batch date", dates["batch_date"].astype(str).tolist())
            report = run_query("""
                SELECT patient_id, reading_count, abnormal_ratio, max_heart_rate, min_spo2,
                       max_systolic_bp, vitals_trend, lab_flags, lab_flag_count,
                       risk_score, risk_category, generated_at
                FROM daily_risk_report
                WHERE batch_date = %s
                ORDER BY risk_score DESC
            """, (selected_date,))

            cat_counts = report["risk_category"].value_counts().to_dict()
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Patients reported", len(report))
            k2.metric("Low risk", cat_counts.get("low", 0))
            k3.metric("Medium risk", cat_counts.get("medium", 0))
            k4.metric("High risk", cat_counts.get("high", 0))
            k5.metric("Critical risk", cat_counts.get("critical", 0))

            left, right = st.columns([2, 1])
            with left:
                bar = go.Figure(
                    go.Bar(
                        x=report["patient_id"],
                        y=report["risk_score"],
                        marker=dict(
                            color=[RISK_COLOR.get(c, INK_MUTED) for c in report["risk_category"]],
                            line=dict(color=SURFACE, width=1),
                        ),
                        hovertext=report["risk_category"],
                    )
                )
                st.plotly_chart(chart_layout(bar, "Risk score by patient", height=340), use_container_width=True)
            with right:
                st.plotly_chart(
                    status_donut(
                        {k: cat_counts.get(k, 0) for k in ["low", "medium", "high", "critical"]},
                        RISK_COLOR,
                        "Risk category mix",
                    ),
                    use_container_width=True,
                )

            if len(dates) > 1:
                st.subheader("Risk trend across days")
                pid = st.selectbox("Patient", sorted(report["patient_id"].unique().tolist()))
                trend = run_query("""
                    SELECT batch_date, risk_score, risk_category
                    FROM daily_risk_report WHERE patient_id = %s ORDER BY batch_date
                """, (pid,))
                line = go.Figure(
                    go.Scatter(
                        x=trend["batch_date"].astype(str), y=trend["risk_score"],
                        mode="lines+markers",
                        line=dict(color=CATEGORICAL[0], width=2),
                        marker=dict(size=9, color=[RISK_COLOR.get(c, INK_MUTED) for c in trend["risk_category"]]),
                    )
                )
                st.plotly_chart(chart_layout(line, f"{pid} — risk score over time", height=280), use_container_width=True)

            st.subheader("Full report")
            report_display = report.copy()
            report_display["risk_category"] = report_display["risk_category"].apply(
                lambda c: badge(c, RISK_COLOR.get(c, INK_MUTED))
            )
            st.write(report_display.to_html(escape=False, index=False), unsafe_allow_html=True)

    # ---------------- Pipeline health ----------------
    with tab_health:
        health = run_query("SELECT stage, last_success_at, last_status, detail FROM pipeline_health ORDER BY stage")
        if health.empty:
            st.warning("No pipeline components have reported a heartbeat yet.")
        else:
            now = datetime.now(timezone.utc)
            for _, row in health.iterrows():
                age = round((now - row["last_success_at"].to_pydatetime().astimezone(timezone.utc)).total_seconds(), 1)
                ok = row["last_status"] == "ok"
                dot_color = STATUS["good"] if ok else STATUS["critical"]
                st.markdown(
                    f"""
                    <div class="health-card">
                        <div>
                            <span style="height:10px;width:10px;background-color:{dot_color};
                                  border-radius:50%;display:inline-block;margin-right:10px;"></span>
                            <strong>{row['stage']}</strong>
                            <span style="color:{INK_MUTED};margin-left:10px;">{row['detail'] or ''}</span>
                        </div>
                        <div style="color:{INK_SECONDARY};">
                            {badge(row['last_status'], dot_color)}
                            <span style="margin-left:10px;">last success {age:.0f}s ago</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.caption(f"Last refreshed: {datetime.now(timezone.utc).isoformat()}")


render()

with st.sidebar:
    st.header("Settings")
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (s)", 5, 60, 10)

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
