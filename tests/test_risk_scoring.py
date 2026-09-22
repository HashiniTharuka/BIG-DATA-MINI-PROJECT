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


def test_improving_trend_reduces_risk_score_compared_to_stable():
    """The improving trend adjustment (-5) must produce a lower score than the same
    trend data when labelled 'stable' (no adjustment). Uses direct DailyVitalsTrend
    construction to make the comparison fully deterministic."""
    from common.risk_scoring import DailyVitalsTrend
    shared_kwargs = dict(
        reading_count=20, abnormal_ratio=0.3,
        max_heart_rate=90, min_spo2=97, max_systolic_bp=120,
    )
    score_improving = compute_risk(
        DailyVitalsTrend(**shared_kwargs, vitals_trend="improving"), lab_flags=[]
    ).risk_score
    score_stable = compute_risk(
        DailyVitalsTrend(**shared_kwargs, vitals_trend="stable"), lab_flags=[]
    ).risk_score
    # improving = stable - 5; stable has no adjustment
    assert score_improving < score_stable
    assert abs(score_stable - score_improving - 5.0) < 0.01


def test_improving_trend_adjustment_is_minus_5():
    """Quantitative check: compute_risk with an improving trend must produce a score
    exactly 5 points lower than the same trend object with the worsening penalty swapped
    out, within float tolerance."""
    from common.risk_scoring import DailyVitalsTrend
    base_trend = DailyVitalsTrend(
        reading_count=20, abnormal_ratio=0.2,
        max_heart_rate=90, min_spo2=97, max_systolic_bp=120,
        vitals_trend="stable",
    )
    improving_trend = DailyVitalsTrend(
        reading_count=20, abnormal_ratio=0.2,
        max_heart_rate=90, min_spo2=97, max_systolic_bp=120,
        vitals_trend="improving",
    )
    score_stable = compute_risk(base_trend, lab_flags=[]).risk_score
    score_improving = compute_risk(improving_trend, lab_flags=[]).risk_score
    assert abs(score_stable - score_improving - 5.0) < 0.01


def test_risk_score_floor_is_zero():
    """An extremely healthy patient with improving trend and no lab flags must not
    produce a negative score (the max(0, score) clamp must be in effect)."""
    from common.risk_scoring import DailyVitalsTrend
    healthy_trend = DailyVitalsTrend(
        reading_count=100, abnormal_ratio=0.0,
        max_heart_rate=75, min_spo2=99, max_systolic_bp=110,
        vitals_trend="improving",  # -5 adjustment applied
    )
    risk = compute_risk(healthy_trend, lab_flags=[])
    assert risk.risk_score >= 0.0
    assert risk.risk_category == "low"


def test_worsening_trend_scores_higher_than_stable_trend():
    """Worsening trend adds +10 to score; stable adds 0. All else equal, worsening
    must produce a strictly higher score."""
    from common.risk_scoring import DailyVitalsTrend
    shared_kwargs = dict(
        reading_count=20, abnormal_ratio=0.3,
        max_heart_rate=90, min_spo2=97, max_systolic_bp=120,
    )
    score_worsening = compute_risk(
        DailyVitalsTrend(**shared_kwargs, vitals_trend="worsening"), lab_flags=[]
    ).risk_score
    score_stable = compute_risk(
        DailyVitalsTrend(**shared_kwargs, vitals_trend="stable"), lab_flags=[]
    ).risk_score
    assert score_worsening == score_stable + 10.0
