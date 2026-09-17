-- Hospital pipeline schema. Applied once on Postgres container init
-- (mounted into /docker-entrypoint-initdb.d/).

-- Speed-layer output: latest per-patient rolling window snapshot.
-- Upserted every Spark Structured Streaming micro-batch.
CREATE TABLE IF NOT EXISTS vitals_realtime (
    patient_id          TEXT PRIMARY KEY,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    avg_heart_rate      DOUBLE PRECISION,
    avg_spo2            DOUBLE PRECISION,
    avg_systolic_bp     DOUBLE PRECISION,
    avg_diastolic_bp    DOUBLE PRECISION,
    avg_temperature     DOUBLE PRECISION,
    reading_count       INTEGER NOT NULL,
    abnormal_count      INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'normal',  -- normal | warning | critical
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speed-layer threshold-breach alert log (append-only).
CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    metric          TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    severity        TEXT NOT NULL,           -- warning | critical
    reason          TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_patient_time ON alerts (patient_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at DESC);

-- Batch-layer ingested lab results (one simulated day of results per load).
CREATE TABLE IF NOT EXISTS lab_results (
    id                  BIGSERIAL PRIMARY KEY,
    patient_id          TEXT NOT NULL,
    test_type           TEXT NOT NULL,
    result_value        DOUBLE PRECISION NOT NULL,
    reference_low       DOUBLE PRECISION,
    reference_high      DOUBLE PRECISION,
    is_out_of_range     BOOLEAN NOT NULL DEFAULT FALSE,
    collected_at        TIMESTAMPTZ NOT NULL,
    batch_date          DATE NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (patient_id, test_type, batch_date)
);
CREATE INDEX IF NOT EXISTS idx_lab_patient_batch ON lab_results (patient_id, batch_date);

-- Batch-layer consolidated output: Lambda serving-layer merge point.
-- One row per patient per batch_date.
CREATE TABLE IF NOT EXISTS daily_risk_report (
    patient_id              TEXT NOT NULL,
    batch_date               DATE NOT NULL,
    reading_count             INTEGER NOT NULL,
    abnormal_ratio            DOUBLE PRECISION NOT NULL,
    max_heart_rate            DOUBLE PRECISION,
    min_spo2                  DOUBLE PRECISION,
    max_systolic_bp           DOUBLE PRECISION,
    vitals_trend              TEXT,             -- improving | stable | worsening
    lab_flags                 TEXT[],           -- test_type list that were out-of-range
    lab_flag_count             INTEGER NOT NULL DEFAULT 0,
    risk_score                DOUBLE PRECISION NOT NULL,
    risk_category              TEXT NOT NULL,   -- low | medium | high | critical
    generated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, batch_date)
);

-- Observability: heartbeat/health rows written by each pipeline stage so
-- the API /health endpoint and Prometheus alert rules can detect staleness.
CREATE TABLE IF NOT EXISTS pipeline_health (
    stage           TEXT PRIMARY KEY,   -- vitals_producer | lab_source | spark_streaming | airflow_dag
    last_success_at TIMESTAMPTZ NOT NULL,
    last_status     TEXT NOT NULL,      -- ok | error
    detail          TEXT
);
