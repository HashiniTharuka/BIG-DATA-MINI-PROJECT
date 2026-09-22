from common.vitals_rules import assess_reading

NORMAL_READING = {
    "heart_rate": 75,
    "spo2": 98,
    "systolic_bp": 115,
    "diastolic_bp": 75,
    "temperature": 36.8,
}


def test_normal_reading_has_no_breaches():
    result = assess_reading(NORMAL_READING)
    assert result.severity == "normal"
    assert result.breaches == []
    assert not result.is_abnormal


def test_low_spo2_is_critical():
    reading = dict(NORMAL_READING, spo2=85)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "spo2" for b in result.breaches)


def test_mild_tachycardia_is_warning_not_critical():
    reading = dict(NORMAL_READING, heart_rate=110)
    result = assess_reading(reading)
    assert result.severity == "warning"


def test_severe_tachycardia_is_critical():
    reading = dict(NORMAL_READING, heart_rate=145)
    result = assess_reading(reading)
    assert result.severity == "critical"


def test_multiple_breaches_take_worst_severity():
    reading = dict(NORMAL_READING, heart_rate=110, spo2=85)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert len(result.breaches) == 2


def test_missing_metrics_are_skipped_not_flagged():
    result = assess_reading({"heart_rate": 75})
    assert result.severity == "normal"
    assert result.breaches == []


def test_high_fever_is_critical():
    reading = dict(NORMAL_READING, temperature=40.0)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "temperature" for b in result.breaches)


def test_hypothermia_is_critical():
    """Temperature at or below critical_low (35.0) must be classified critical."""
    reading = dict(NORMAL_READING, temperature=34.5)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "temperature" and b.severity == "critical" for b in result.breaches)


def test_low_heart_rate_bradycardia_is_critical():
    """Heart rate at or below critical_low (50) must be classified critical, not just warning."""
    reading = dict(NORMAL_READING, heart_rate=48)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "heart_rate" and b.severity == "critical" for b in result.breaches)


def test_high_diastolic_bp_is_warning():
    """Diastolic BP between warn_high (90) and crit_high (110) is a warning breach."""
    reading = dict(NORMAL_READING, diastolic_bp=95)
    result = assess_reading(reading)
    assert result.severity == "warning"
    assert any(b.metric == "diastolic_bp" for b in result.breaches)


def test_very_high_diastolic_bp_is_critical():
    """Diastolic BP at or above critical_high (110) must be classified critical."""
    reading = dict(NORMAL_READING, diastolic_bp=115)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "diastolic_bp" and b.severity == "critical" for b in result.breaches)


def test_high_systolic_bp_is_warning():
    """Systolic BP between warn_high (140) and crit_high (170) is a warning breach."""
    reading = dict(NORMAL_READING, systolic_bp=155)
    result = assess_reading(reading)
    assert result.severity == "warning"
    assert any(b.metric == "systolic_bp" for b in result.breaches)


def test_value_exactly_at_critical_low_threshold_is_critical():
    """Boundary value: a reading exactly equal to crit_low triggers critical, not just warning.
    SpO2 crit_low is 90 — value of 90 satisfies `value <= crit_low`."""
    reading = dict(NORMAL_READING, spo2=90)
    result = assess_reading(reading)
    assert result.severity == "critical"
    assert any(b.metric == "spo2" and b.severity == "critical" for b in result.breaches)
