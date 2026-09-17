"""Simulated bedside vitals monitors.

Continuously emits one Kafka message per patient every VITALS_INTERVAL_SECONDS
with fields (patient_id, heart_rate, spo2, systolic_bp, diastolic_bp,
temperature, timestamp) as specified by the use case brief. Each patient's
vitals follow a small random walk around a personal baseline so trends look
realistic, with occasional injected abnormal spikes (ABNORMAL_SPIKE_PROBABILITY)
to exercise the speed-layer alerting logic.
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logging_config import get_logger, log_event  # noqa: E402
from common.metrics import push_metrics  # noqa: E402
from common.db import upsert_health  # noqa: E402

logger = get_logger("vitals_producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_VITALS", "vitals-stream")
NUM_PATIENTS = int(os.environ.get("NUM_PATIENTS", "12"))
INTERVAL_SECONDS = float(os.environ.get("VITALS_INTERVAL_SECONDS", "3"))
SPIKE_PROBABILITY = float(os.environ.get("ABNORMAL_SPIKE_PROBABILITY", "0.06"))

PATIENT_IDS = [f"P{str(i).zfill(3)}" for i in range(1, NUM_PATIENTS + 1)]


class PatientState:
    """Per-patient baseline + random-walk state so readings drift smoothly
    instead of jumping randomly every tick."""

    def __init__(self, patient_id: str):
        self.patient_id = patient_id
        self.heart_rate = random.uniform(65, 85)
        self.spo2 = random.uniform(96, 99)
        self.systolic_bp = random.uniform(105, 125)
        self.diastolic_bp = random.uniform(70, 82)
        self.temperature = random.uniform(36.4, 37.1)

    def _drift(self, value: float, step: float, lo: float, hi: float) -> float:
        value += random.uniform(-step, step)
        return max(lo, min(hi, value))

    def next_reading(self) -> dict:
        self.heart_rate = self._drift(self.heart_rate, 2.5, 45, 140)
        self.spo2 = self._drift(self.spo2, 0.5, 85, 100)
        self.systolic_bp = self._drift(self.systolic_bp, 3.0, 75, 180)
        self.diastolic_bp = self._drift(self.diastolic_bp, 2.0, 40, 115)
        self.temperature = self._drift(self.temperature, 0.15, 34.5, 40.0)

        reading = {
            "patient_id": self.patient_id,
            "heart_rate": round(self.heart_rate, 1),
            "spo2": round(self.spo2, 1),
            "systolic_bp": round(self.systolic_bp, 1),
            "diastolic_bp": round(self.diastolic_bp, 1),
            "temperature": round(self.temperature, 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if random.random() < SPIKE_PROBABILITY:
            reading = _inject_spike(reading)

        return reading


def _inject_spike(reading: dict) -> dict:
    """Occasionally push one metric sharply out of range to simulate a real
    clinical event and exercise the alerting path end-to-end."""
    metric = random.choice(["heart_rate", "spo2", "systolic_bp", "temperature"])
    if metric == "heart_rate":
        reading["heart_rate"] = round(random.choice([random.uniform(140, 170), random.uniform(30, 44)]), 1)
    elif metric == "spo2":
        reading["spo2"] = round(random.uniform(78, 88), 1)
    elif metric == "systolic_bp":
        reading["systolic_bp"] = round(random.choice([random.uniform(175, 200), random.uniform(65, 79)]), 1)
    elif metric == "temperature":
        reading["temperature"] = round(random.choice([random.uniform(39.6, 41.0), random.uniform(33.5, 34.4)]), 1)
    return reading


def connect_producer(retries: int = 30, delay: float = 2.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                linger_ms=50,
            )
            log_event(logger, 20, "connected to kafka", bootstrap_servers=BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log_event(logger, 30, "kafka not ready, retrying", attempt=attempt, retries=retries)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS} after {retries} attempts")


def main() -> None:
    producer = connect_producer()
    patients = [PatientState(pid) for pid in PATIENT_IDS]

    events_sent = 0
    errors = 0
    log_event(logger, 20, "vitals producer starting", num_patients=NUM_PATIENTS,
              interval_seconds=INTERVAL_SECONDS, topic=TOPIC)

    tick = 0
    while True:
        tick += 1
        for patient in patients:
            reading = patient.next_reading()
            try:
                producer.send(TOPIC, key=reading["patient_id"], value=reading)
                events_sent += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                log_event(logger, 40, "failed to send vitals event", error=str(exc),
                          patient_id=reading["patient_id"])

        producer.flush()

        if tick % 5 == 0:
            log_event(logger, 20, "heartbeat", events_sent=events_sent, errors=errors, tick=tick)
            push_metrics(
                job="vitals_producer",
                counters={"vitals_producer_events_sent_total": events_sent,
                          "vitals_producer_errors_total": errors},
                gauges={"vitals_producer_last_tick_timestamp": time.time()},
            )
            try:
                upsert_health("vitals_producer", "ok", detail=f"events_sent={events_sent}")
            except Exception as exc:  # noqa: BLE001
                log_event(logger, 30, "health upsert failed", error=str(exc))

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
