"""Batch layer: daily patient risk reconciliation.

Lambda batch layer for the hospital use case. Once per simulated day this
DAG:
  1. finds the oldest lab-results file the lab batch source has dropped
     that hasn't been reconciled yet,
  2. loads + cleans it into `lab_results`,
  3. recomputes that day's per-patient vitals trend by re-reading the raw
     Parquet archive the Spark speed layer wrote (the "recompute from the
     immutable raw log" step that defines the batch layer in Lambda),
  4. joins the recomputed vitals trend with that day's lab flags to produce
     a risk score (common.risk_scoring - the same module used by tests),
  5. upserts the consolidated result into `daily_risk_report`, the
     Lambda serving-layer merge point alongside the speed layer's
     `vitals_realtime` table.

Schedule is derived from SIMULATED_DAY_SECONDS so the DAG cadence tracks
whatever simulated-day compression the demo is using.
"""
from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, "/opt/airflow")
from common.db import get_connection, upsert_health  # noqa: E402
from common.risk_scoring import compute_daily_trend, compute_risk  # noqa: E402

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

LAB_DROP_DIR = os.environ.get("LAB_DROP_DIR", "/data/lab_drops")
PARQUET_LAKE_PATH = os.environ.get("PARQUET_LAKE_PATH", "/data/parquet_lake/vitals")
DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))

_schedule_minutes = max(1, round(DAY_SECONDS / 60))
SCHEDULE_INTERVAL = f"*/{_schedule_minutes} * * * *"

default_args = {
    "owner": "hospital-pipeline",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def find_unprocessed_batch(**context) -> None:
    success_markers = sorted(glob.glob(os.path.join(LAB_DROP_DIR, "lab_results_*.json._SUCCESS")))
    if not success_markers:
        raise AirflowSkipException("No lab files dropped yet")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT batch_date::text FROM daily_risk_report")
            already_done = {row[0] for row in cur.fetchall()}

    for marker in success_markers:
        filename = os.path.basename(marker)[: -len("._SUCCESS")]
        batch_date = filename.replace("lab_results_", "").replace(".json", "")
        if batch_date not in already_done:
            filepath = os.path.join(LAB_DROP_DIR, filename)
            context["ti"].xcom_push(key="batch_date", value=batch_date)
            context["ti"].xcom_push(key="lab_filepath", value=filepath)
            return

    raise AirflowSkipException("All dropped lab files already reconciled")


def load_lab_results(**context) -> None:
    ti = context["ti"]
    batch_date = ti.xcom_pull(key="batch_date", task_ids="find_unprocessed_batch")
    filepath = ti.xcom_pull(key="lab_filepath", task_ids="find_unprocessed_batch")

    with open(filepath, "r", encoding="utf-8") as f:
        records = json.load(f)

    rows = []
    for r in records:
        is_out_of_range = not (r["reference_low"] <= r["result_value"] <= r["reference_high"])
        rows.append((
            r["patient_id"], r["test_type"], r["result_value"],
            r["reference_low"], r["reference_high"], is_out_of_range,
            r["collected_at"], r["batch_date"],
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """INSERT INTO lab_results
                           (patient_id, test_type, result_value, reference_low,
                            reference_high, is_out_of_range, collected_at, batch_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (patient_id, test_type, batch_date) DO UPDATE SET
                           result_value = EXCLUDED.result_value,
                           is_out_of_range = EXCLUDED.is_out_of_range,
                           collected_at = EXCLUDED.collected_at,
                           loaded_at = now()""",
                    row,
                )

    upsert_health("airflow_dag", "ok", detail=f"loaded {len(rows)} lab results for {batch_date}")
    ti.xcom_push(key="lab_row_count", value=len(rows))


def compute_and_write_risk_report(**context) -> None:
    ti = context["ti"]
    batch_date = ti.xcom_pull(key="batch_date", task_ids="find_unprocessed_batch")

    # --- Recompute vitals trend for the day from the raw Parquet archive ---
    day_path = os.path.join(PARQUET_LAKE_PATH, f"event_date={batch_date}")
    if os.path.isdir(day_path):
        vitals_df = pd.read_parquet(day_path)
    else:
        vitals_df = pd.DataFrame(columns=["patient_id", "heart_rate", "spo2",
                                           "systolic_bp", "diastolic_bp", "temperature", "event_time"])

    trends_by_patient = {}
    for patient_id, group in vitals_df.groupby("patient_id"):
        readings = group.sort_values("event_time").to_dict("records")
        trends_by_patient[patient_id] = compute_daily_trend(readings)

    # --- Pull that day's lab flags per patient ---
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT patient_id, test_type FROM lab_results "
                "WHERE batch_date = %s AND is_out_of_range = TRUE",
                (batch_date,),
            )
            lab_flags_by_patient: dict[str, list[str]] = {}
            for patient_id, test_type in cur.fetchall():
                lab_flags_by_patient.setdefault(patient_id, []).append(test_type)

    all_patients = set(trends_by_patient) | set(lab_flags_by_patient)
    if not all_patients:
        raise AirflowSkipException(f"No vitals or lab data available for {batch_date}")

    report_rows = []
    for patient_id in all_patients:
        trend = trends_by_patient.get(patient_id) or compute_daily_trend([])
        lab_flags = lab_flags_by_patient.get(patient_id, [])
        risk = compute_risk(trend, lab_flags)
        report_rows.append((
            patient_id, batch_date, trend.reading_count, trend.abnormal_ratio,
            trend.max_heart_rate, trend.min_spo2, trend.max_systolic_bp,
            trend.vitals_trend, lab_flags, len(lab_flags), risk.risk_score, risk.risk_category,
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in report_rows:
                cur.execute(
                    """INSERT INTO daily_risk_report
                           (patient_id, batch_date, reading_count, abnormal_ratio,
                            max_heart_rate, min_spo2, max_systolic_bp, vitals_trend,
                            lab_flags, lab_flag_count, risk_score, risk_category, generated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                       ON CONFLICT (patient_id, batch_date) DO UPDATE SET
                           reading_count = EXCLUDED.reading_count,
                           abnormal_ratio = EXCLUDED.abnormal_ratio,
                           max_heart_rate = EXCLUDED.max_heart_rate,
                           min_spo2 = EXCLUDED.min_spo2,
                           max_systolic_bp = EXCLUDED.max_systolic_bp,
                           vitals_trend = EXCLUDED.vitals_trend,
                           lab_flags = EXCLUDED.lab_flags,
                           lab_flag_count = EXCLUDED.lab_flag_count,
                           risk_score = EXCLUDED.risk_score,
                           risk_category = EXCLUDED.risk_category,
                           generated_at = now()""",
                    row,
                )

    upsert_health("airflow_dag", "ok",
                   detail=f"daily_risk_report written for {batch_date}: {len(report_rows)} patients")


with DAG(
    dag_id="daily_risk_report_dag",
    description="Batch layer: reconcile daily lab results with recomputed vitals trends into a risk report",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch-layer", "lambda-architecture"],
) as dag:

    t1 = PythonOperator(
        task_id="find_unprocessed_batch",
        python_callable=find_unprocessed_batch,
    )

    t2 = PythonOperator(
        task_id="load_lab_results",
        python_callable=load_lab_results,
    )

    t3 = PythonOperator(
        task_id="compute_and_write_risk_report",
        python_callable=compute_and_write_risk_report,
    )

    t1 >> t2 >> t3
