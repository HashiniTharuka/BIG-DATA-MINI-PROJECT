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
