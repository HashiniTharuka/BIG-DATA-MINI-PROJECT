"""Pure threshold logic for classifying a single vital-signs reading.

Deliberately dependency-free (no Spark/Kafka/DB) so it can be unit tested in
isolation and imported unchanged by both the Spark streaming job (via a
pandas UDF / plain call inside foreachBatch) and any offline analysis.

Clinically-inspired but simplified thresholds for a student project — not
real medical guidance.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Breach:
    metric: str
    value: float
    severity: str  # "warning" | "critical"
    reason: str


@dataclass
class VitalsAssessment:
    severity: str  # "normal" | "warning" | "critical"
    breaches: list[Breach] = field(default_factory=list)

    @property
    def is_abnormal(self) -> bool:
        return self.severity != "normal"


# (metric, warning_low, critical_low, warning_high, critical_high)
_THRESHOLDS = [
    ("heart_rate", 60, 50, 100, 130),
    ("spo2", 94, 90, 101, 101),          # only a "too low" side matters for SpO2
    ("systolic_bp", 90, 80, 140, 170),
    ("diastolic_bp", 60, 45, 90, 110),
    ("temperature", 36.0, 35.0, 38.0, 39.5),
]


def _classify_metric(metric: str, value: float | None,
                      warn_low: float, crit_low: float,
                      warn_high: float, crit_high: float) -> Breach | None:
    if value is None:
        return None
    if value <= crit_low or value >= crit_high:
        return Breach(metric, value, "critical", f"{metric}={value} outside critical range")
    if value <= warn_low or value >= warn_high:
        return Breach(metric, value, "warning", f"{metric}={value} outside normal range")
    return None


def assess_reading(reading: dict) -> VitalsAssessment:
    """reading: dict with optional keys heart_rate, spo2, systolic_bp,
    diastolic_bp, temperature (missing keys are skipped, not flagged)."""
    breaches: list[Breach] = []
    for metric, warn_low, crit_low, warn_high, crit_high in _THRESHOLDS:
        breach = _classify_metric(metric, reading.get(metric), warn_low, crit_low, warn_high, crit_high)
        if breach:
            breaches.append(breach)

    if any(b.severity == "critical" for b in breaches):
        severity = "critical"
    elif breaches:
        severity = "warning"
    else:
        severity = "normal"
    return VitalsAssessment(severity=severity, breaches=breaches)
