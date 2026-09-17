"""Prometheus metrics helper for components that are short-lived or batch
in nature (producer loop iterations, Spark micro-batches, Airflow tasks) and
therefore push to Pushgateway rather than being scraped directly (unlike the
API, which exposes /metrics natively via prometheus_fastapi_instrumentator).
"""
from __future__ import annotations

import os

from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://localhost:9091")


def push_metrics(job: str, counters: dict[str, float] | None = None,
                  gauges: dict[str, float] | None = None,
                  grouping_key: dict[str, str] | None = None) -> None:
    """Push a fresh set of counter/gauge values for `job` to the Pushgateway.

    Each call creates its own registry (Pushgateway model: the pushed group
    is replaced wholesale), so callers pass the *current* values they want
    recorded, not deltas.
    """
    registry = CollectorRegistry()
    for name, value in (counters or {}).items():
        Counter(name, f"{name} (pushed by {job})", registry=registry).inc(value)
    for name, value in (gauges or {}).items():
        Gauge(name, f"{name} (pushed by {job})", registry=registry).set(value)
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry, grouping_key=grouping_key)
    except Exception:
        # Metrics must never take down the pipeline stage they're observing.
        pass
