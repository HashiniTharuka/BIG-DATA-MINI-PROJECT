"""Serving layer: FastAPI.

Read-only API over the Postgres serving tables that both the speed layer
(vitals_realtime, alerts) and batch layer (daily_risk_report) write to -
the Lambda merge point. Also exposes /health (used by the pipeline's own
health-check/alerting story) and /metrics (scraped natively by Prometheus,
unlike the batch components which push to Pushgateway).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.db import get_connection, dict_cursor  # noqa: E402
from common.logging_config import get_logger, log_event  # noqa: E402

from fastapi import FastAPI, HTTPException, Query
from prometheus_fastapi_instrumentator import Instrumentator

logger = get_logger("api")

app = FastAPI(title="Hospital Vitals Pipeline API", version="1.0.0")
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# Stages expected to report a heartbeat at roughly this cadence (seconds).
# Used by /health to flag staleness. Kept generous relative to each
# component's real send interval so normal jitter doesn't false-positive.
HEALTH_STALE_AFTER_SECONDS = {
    "vitals_producer": 60,
    "spark_streaming": 90,
    "lab_source": int(os.environ.get("SIMULATED_DAY_SECONDS", "300")) * 3,
    "airflow_dag": int(os.environ.get("SIMULATED_DAY_SECONDS", "300")) * 3,
}


@app.get("/health")
def health():
    with get_connection() as conn:
        with dict_cursor(conn) as cur:
            cur.execute("SELECT stage, last_success_at, last_status, detail FROM pipeline_health")
            rows = cur.fetchall()

    now = datetime.now(timezone.utc)
    stages = {}
    overall_ok = True
    for row in rows:
        stage = row["stage"]
        age_seconds = (now - row["last_success_at"]).total_seconds()
        stale_after = HEALTH_STALE_AFTER_SECONDS.get(stage, 120)
        is_stale = age_seconds > stale_after
        if is_stale or row["last_status"] != "ok":
            overall_ok = False
        stages[stage] = {
            "last_success_at": row["last_success_at"].isoformat(),
            "age_seconds": round(age_seconds, 1),
            "status": row["last_status"],
            "stale": is_stale,
            "detail": row["detail"],
        }

    for expected_stage in HEALTH_STALE_AFTER_SECONDS:
        if expected_stage not in stages:
            overall_ok = False
            stages[expected_stage] = {"status": "never_reported", "stale": True}

    log_event(logger, 20, "health check", healthy=overall_ok)
    return {"healthy": overall_ok, "stages": stages, "checked_at": now.isoformat()}


@app.get("/ward/status")
def ward_status():
    with get_connection() as conn:
        with dict_cursor(conn) as cur:
            cur.execute("""
                SELECT patient_id, window_start, window_end, avg_heart_rate, avg_spo2,
                       avg_systolic_bp, avg_diastolic_bp, avg_temperature,
                       reading_count, abnormal_count, status, updated_at
                FROM vitals_realtime
                ORDER BY patient_id
            """)
            patients = cur.fetchall()

            cur.execute("""
                SELECT count(*) AS alert_count
                FROM alerts
                WHERE created_at > now() - interval '15 minutes'
            """)
            recent_alert_count = cur.fetchone()["alert_count"]

    status_counts = {"normal": 0, "warning": 0, "critical": 0}
    for p in patients:
        status_counts[p["status"]] = status_counts.get(p["status"], 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "active_patients": len(patients),
        "status_breakdown": status_counts,
        "recent_alert_count_15min": recent_alert_count,
        "patients": patients,
    }


@app.get("/patient/{patient_id}/risk")
def patient_risk(patient_id: str):
    with get_connection() as conn:
        with dict_cursor(conn) as cur:
            cur.execute("""
                SELECT * FROM daily_risk_report
                WHERE patient_id = %s
                ORDER BY batch_date DESC
                LIMIT 7
            """, (patient_id,))
            history = cur.fetchall()

            cur.execute("SELECT * FROM vitals_realtime WHERE patient_id = %s", (patient_id,))
            realtime = cur.fetchone()

    if not history and not realtime:
        raise HTTPException(status_code=404, detail=f"No data found for patient {patient_id}")

    return {
        "patient_id": patient_id,
        "current_vitals": realtime,
        "latest_risk_report": history[0] if history else None,
        "risk_history": history,
    }


@app.get("/alerts/recent")
def recent_alerts(minutes: int = Query(15, ge=1, le=1440)):
    with get_connection() as conn:
        with dict_cursor(conn) as cur:
            cur.execute("""
                SELECT id, patient_id, metric, value, severity, reason, event_time, created_at
                FROM alerts
                WHERE created_at > now() - (%s || ' minutes')::interval
                ORDER BY created_at DESC
                LIMIT 200
            """, (minutes,))
            alerts = cur.fetchall()
    return {"window_minutes": minutes, "count": len(alerts), "alerts": alerts}


@app.get("/")
def root():
    return {
        "service": "hospital-vitals-pipeline-api",
        "endpoints": ["/health", "/ward/status", "/patient/{patient_id}/risk", "/alerts/recent", "/metrics", "/docs"],
    }
