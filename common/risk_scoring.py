"""Pure business logic for the batch-layer daily patient risk report.

Joins a day's worth of vitals-trend aggregates (recomputed from the raw
Parquet archive) with that day's lab-result flags (from the daily batch
file) into a single risk score/category. Dependency-free so it is
unit-tested directly and imported unchanged by the Airflow DAG.
"""
from __future__ import annotations

from dataclasses import dataclass

from common.vitals_rules import assess_reading

# Lab tests that carry extra weight in the risk score when out of range,
# because clinically they signal acute risk rather than a minor deviation.
CRITICAL_LAB_TESTS = {"troponin", "potassium", "creatinine", "wbc_count"}


@dataclass
class DailyVitalsTrend:
    reading_count: int
    abnormal_ratio: float          # fraction of the day's readings that were warning/critical
    max_heart_rate: float | None
    min_spo2: float | None
    max_systolic_bp: float | None
    vitals_trend: str              # "improving" | "stable" | "worsening"


def compute_daily_trend(readings: list[dict]) -> DailyVitalsTrend:
    """readings: chronologically ordered list of raw vitals dicts for one
    patient for one simulated day (as archived to the Parquet data lake)."""
    if not readings:
        return DailyVitalsTrend(0, 0.0, None, None, None, "stable")

    assessments = [assess_reading(r) for r in readings]
    abnormal_count = sum(1 for a in assessments if a.is_abnormal)
    abnormal_ratio = abnormal_count / len(readings)

    heart_rates = [r["heart_rate"] for r in readings if r.get("heart_rate") is not None]
    spo2s = [r["spo2"] for r in readings if r.get("spo2") is not None]
    systolics = [r["systolic_bp"] for r in readings if r.get("systolic_bp") is not None]

    # Trend: compare abnormal ratio in the first half of the day vs. the second half.
    midpoint = len(assessments) // 2
    first_half, second_half = assessments[:midpoint], assessments[midpoint:]

    def _ratio(chunk: list) -> float:
        return (sum(1 for a in chunk if a.is_abnormal) / len(chunk)) if chunk else 0.0

    delta = _ratio(second_half) - _ratio(first_half)
    if delta > 0.1:
        trend = "worsening"
    elif delta < -0.1:
        trend = "improving"
    else:
        trend = "stable"

    return DailyVitalsTrend(
        reading_count=len(readings),
        abnormal_ratio=round(abnormal_ratio, 4),
        max_heart_rate=max(heart_rates) if heart_rates else None,
        min_spo2=min(spo2s) if spo2s else None,
        max_systolic_bp=max(systolics) if systolics else None,
        vitals_trend=trend,
    )


@dataclass
class RiskResult:
    risk_score: float
    risk_category: str  # "low" | "medium" | "high" | "critical"


def compute_risk(trend: DailyVitalsTrend, lab_flags: list[str]) -> RiskResult:
    score = trend.abnormal_ratio * 50.0

    if trend.min_spo2 is not None and trend.min_spo2 < 90:
        score += 15
    if trend.max_heart_rate is not None and trend.max_heart_rate > 130:
        score += 10
    if trend.max_systolic_bp is not None and trend.max_systolic_bp > 170:
        score += 10

    for test in lab_flags:
        score += 12 if test in CRITICAL_LAB_TESTS else 6

    if trend.vitals_trend == "worsening":
        score += 10
    elif trend.vitals_trend == "improving":
        score -= 5

    score = max(0.0, min(100.0, score))

    if score >= 70:
        category = "critical"
    elif score >= 45:
        category = "high"
    elif score >= 20:
        category = "medium"
    else:
        category = "low"

    return RiskResult(risk_score=round(score, 2), risk_category=category)
