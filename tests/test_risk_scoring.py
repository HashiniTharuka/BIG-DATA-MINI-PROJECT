from common.risk_scoring import compute_daily_trend, compute_risk

NORMAL = {"heart_rate": 75, "spo2": 98, "systolic_bp": 115, "diastolic_bp": 75, "temperature": 36.8}
CRITICAL = {"heart_rate": 150, "spo2": 82, "systolic_bp": 190, "diastolic_bp": 75, "temperature": 36.8}


def test_empty_readings_produce_zeroed_stable_trend():
    trend = compute_daily_trend([])
    assert trend.reading_count == 0
    assert trend.abnormal_ratio == 0.0
    assert trend.vitals_trend == "stable"


def test_all_normal_readings_give_low_risk():
    readings = [dict(NORMAL) for _ in range(20)]
    trend = compute_daily_trend(readings)
    risk = compute_risk(trend, lab_flags=[])
    assert trend.abnormal_ratio == 0.0
    assert risk.risk_category == "low"


def test_all_critical_readings_give_high_abnormal_ratio_and_elevated_risk():
    readings = [dict(CRITICAL) for _ in range(20)]
    trend = compute_daily_trend(readings)
    risk = compute_risk(trend, lab_flags=[])
    assert trend.abnormal_ratio == 1.0
    assert risk.risk_category in ("high", "critical")


def test_trend_detects_worsening_pattern():
    readings = [dict(NORMAL) for _ in range(10)] + [dict(CRITICAL) for _ in range(10)]
    trend = compute_daily_trend(readings)
    assert trend.vitals_trend == "worsening"


def test_trend_detects_improving_pattern():
    readings = [dict(CRITICAL) for _ in range(10)] + [dict(NORMAL) for _ in range(10)]
    trend = compute_daily_trend(readings)
    assert trend.vitals_trend == "improving"


def test_lab_flags_increase_risk_score():
    readings = [dict(NORMAL) for _ in range(20)]
    trend = compute_daily_trend(readings)
    baseline = compute_risk(trend, lab_flags=[])
    with_flags = compute_risk(trend, lab_flags=["glucose"])
    assert with_flags.risk_score > baseline.risk_score


def test_critical_lab_test_weighted_more_than_routine_test():
    readings = [dict(NORMAL) for _ in range(20)]
    trend = compute_daily_trend(readings)
    routine = compute_risk(trend, lab_flags=["glucose"])
    critical = compute_risk(trend, lab_flags=["troponin"])
    assert critical.risk_score > routine.risk_score


def test_risk_score_is_clamped_to_100():
    readings = [dict(CRITICAL) for _ in range(20)]
    trend = compute_daily_trend(readings)
    risk = compute_risk(trend, lab_flags=["troponin", "potassium", "creatinine", "wbc_count"])
    assert risk.risk_score <= 100.0
    assert risk.risk_category == "critical"
