"""Simulated pathology lab daily extract.

Once per simulated day (SIMULATED_DAY_SECONDS real seconds), drops one JSON
file of lab results (patient_id, test_type, result_value, reference_range,
collected_at) into the shared landing-zone volume, followed by a `_SUCCESS`
marker. The Airflow DAG's FileSensor watches for that marker.

Simulated day boundaries map to real calendar dates (today + day_index) so
`batch_date` behaves like a genuine daily partition key downstream.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logging_config import get_logger, log_event  # noqa: E402
from common.metrics import push_metrics  # noqa: E402
from common.db import upsert_health  # noqa: E402

logger = get_logger("lab_batch_source")

DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))
NUM_PATIENTS = int(os.environ.get("NUM_PATIENTS", "12"))
OUTPUT_DIR = os.environ.get("LAB_DROP_DIR", "/data/lab_drops")
INITIAL_DELAY_SECONDS = float(os.environ.get("LAB_INITIAL_DELAY_SECONDS", "15"))

PATIENT_IDS = [f"P{str(i).zfill(3)}" for i in range(1, NUM_PATIENTS + 1)]

# test_type -> (reference_low, reference_high, normal_mean, normal_std, abnormal_probability)
TEST_PANEL = {
    "hemoglobin":  (12.0, 17.5, 14.5, 1.0, 0.08),
    "wbc_count":   (4.0, 11.0, 7.0, 1.2, 0.10),
    "potassium":   (3.5, 5.1, 4.2, 0.3, 0.07),
    "creatinine":  (0.6, 1.3, 0.9, 0.15, 0.09),
    "troponin":    (0.0, 0.04, 0.01, 0.01, 0.05),
    "glucose":     (70, 140, 100, 15, 0.12),
    "sodium":      (135, 145, 140, 2.5, 0.06),
}


def _generate_result(test_type: str) -> dict:
    low, high, mean, std, abnormal_prob = TEST_PANEL[test_type]
    if random.random() < abnormal_prob:
        # push the value outside the reference range
        value = random.choice([
            low - abs(random.gauss(0, std * 1.5)) - 0.01,
            high + abs(random.gauss(0, std * 1.5)) + 0.01,
        ])
    else:
        value = random.gauss(mean, std)
        value = max(low, min(high, value))
    return {
        "reference_low": low,
        "reference_high": high,
        "result_value": round(value, 3),
    }


def build_daily_file(batch_date: str) -> list[dict]:
    records = []
    now = datetime.now(timezone.utc)
    for patient_id in PATIENT_IDS:
        panel_size = random.randint(3, len(TEST_PANEL))
        tests_today = random.sample(list(TEST_PANEL.keys()), panel_size)
        for test_type in tests_today:
            result = _generate_result(test_type)
            collected_at = now - timedelta(hours=random.uniform(1, 6))
            records.append({
                "patient_id": patient_id,
                "test_type": test_type,
                "result_value": result["result_value"],
                "reference_low": result["reference_low"],
                "reference_high": result["reference_high"],
                "collected_at": collected_at.isoformat(),
                "batch_date": batch_date,
            })
    return records


def drop_file(day_index: int) -> None:
    batch_date = (datetime.now(timezone.utc).date() + timedelta(days=day_index)).isoformat()
    records = build_daily_file(batch_date)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"lab_results_{batch_date}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    tmp_path = filepath + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    os.replace(tmp_path, filepath)  # atomic-ish rename so the sensor never sees a partial file

    success_path = os.path.join(OUTPUT_DIR, f"{filename}._SUCCESS")
    with open(success_path, "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())

    log_event(logger, 20, "dropped daily lab file", file=filename, records=len(records), batch_date=batch_date)
    push_metrics(
        job="lab_batch_source",
        counters={"lab_batch_files_dropped_total": day_index + 1},
        gauges={"lab_batch_last_drop_timestamp": time.time(), "lab_batch_last_record_count": len(records)},
    )
    try:
        upsert_health("lab_source", "ok", detail=f"batch_date={batch_date} records={len(records)}")
    except Exception as exc:  # noqa: BLE001
        log_event(logger, 30, "health upsert failed", error=str(exc))


def main() -> None:
    log_event(logger, 20, "lab batch source starting", day_seconds=DAY_SECONDS,
              num_patients=NUM_PATIENTS, output_dir=OUTPUT_DIR)
    time.sleep(INITIAL_DELAY_SECONDS)

    day_index = 0
    while True:
        try:
            drop_file(day_index)
        except Exception as exc:  # noqa: BLE001
            log_event(logger, 40, "failed to drop daily lab file", error=str(exc), day_index=day_index)
        day_index += 1
        time.sleep(DAY_SECONDS)


if __name__ == "__main__":
    main()
