"""Consolidated report/dashboard deliverable (Streamlit).

Reads directly from the Postgres serving tables that both the Lambda speed
layer (vitals_realtime, alerts) and batch layer (daily_risk_report) write
to. Two halves: a live ward view answering "what's happening right now",
and a daily risk report answering "how does yesterday's lab data change
the risk picture" - the business question from the use case brief.
"""
from __future__ import annotations

import os
import sys
import time
import warnings
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.db import get_conn_params  # noqa: E402

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

st.set_page_config(page_title="Hospital Ward Monitor", layout="wide")


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


STATUS_COLORS = {"normal": "#d4f7dc", "warning": "#fff3cd", "critical": "#f8d7da"}
RISK_COLORS = {"low": "#d4f7dc", "medium": "#fff3cd", "high": "#ffe0b3", "critical": "#f8d7da"}


def style_status(row: pd.Series):
    color = STATUS_COLORS.get(row.get("status"), "")
    return [f"background-color: {color}"] * len(row)


def style_risk(row: pd.Series):
    color = RISK_COLORS.get(row.get("risk_category"), "")
    return [f"background-color: {color}"] * len(row)


def render():
    st.title("🏥 Hospital Ward — Real-Time Vitals & Daily Risk Report")
    st.caption(
        "Lambda architecture: speed layer (Spark Structured Streaming) drives the live view below; "
        "batch layer (Airflow) drives the daily risk report."
    )

    # ---------------- Live ward view (speed layer) ----------------
    st.header("Live Ward Status")
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

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active patients", len(realtime))
    col2.metric("Normal", int((realtime["status"] == "normal").sum()) if not realtime.empty else 0)
    col3.metric("Warning / Critical",
                int((realtime["status"] != "normal").sum()) if not realtime.empty else 0)
    col4.metric("Alerts (last 15 min)", len(alerts_recent))

    if realtime.empty:
        st.info("Waiting for the first streaming window... (Spark aggregates every ~20s)")
    else:
        st.dataframe(realtime.style.apply(style_status, axis=1), use_container_width=True, hide_index=True)

    with st.expander("Recent alerts (last 15 min)", expanded=not alerts_recent.empty):
        if alerts_recent.empty:
            st.write("No alerts in the last 15 minutes.")
        else:
            st.dataframe(alerts_recent, use_container_width=True, hide_index=True)

    st.divider()

    # ---------------- Daily risk report (batch layer) ----------------
    st.header("Daily Patient Risk Report")
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
        st.dataframe(report.style.apply(style_risk, axis=1), use_container_width=True, hide_index=True)
        st.bar_chart(report.set_index("patient_id")["risk_score"])

    st.divider()

    # ---------------- Pipeline health ----------------
    st.header("Pipeline Health")
    health = run_query("SELECT stage, last_success_at, last_status, detail FROM pipeline_health ORDER BY stage")
    if health.empty:
        st.warning("No pipeline components have reported a heartbeat yet.")
    else:
        now = datetime.now(timezone.utc)
        health["age_seconds"] = health["last_success_at"].apply(
            lambda t: round((now - t.to_pydatetime().astimezone(timezone.utc)).total_seconds(), 1)
        )
        st.dataframe(health, use_container_width=True, hide_index=True)

    st.caption(f"Last refreshed: {datetime.now(timezone.utc).isoformat()}")


render()

with st.sidebar:
    st.header("Settings")
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (s)", 5, 60, 10)

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
