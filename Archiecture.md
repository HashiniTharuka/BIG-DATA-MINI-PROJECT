# Hospital Patient Vital Signs Monitoring — Full Architecture

> **Course:** EC8202 Big Data and Analytics — Mini Project
> **Use Case 2:** Hospital Patient Vital Signs Monitoring
> **Architectural Pattern:** Lambda Architecture

---

## Table of Contents

1. [High-Level Overview](#1-high-level-overview)
2. [Architecture Pattern: Lambda vs Kappa](#2-architecture-pattern-lambda-vs-kappa)
3. [System Components](#3-system-components)
4. [Data Flow Diagram](#4-data-flow-diagram)
5. [Layer-by-Layer Breakdown](#5-layer-by-layer-breakdown)
   - 5.1 [Data Sources (Simulation Layer)](#51-data-sources-simulation-layer)
   - 5.2 [Ingestion Layer — Apache Kafka](#52-ingestion-layer--apache-kafka)
   - 5.3 [Speed Layer — Spark Structured Streaming](#53-speed-layer--spark-structured-streaming)
   - 5.4 [Batch Layer — Apache Airflow](#54-batch-layer--apache-airflow)
   - 5.5 [Storage Layer — PostgreSQL & Parquet Data Lake](#55-storage-layer--postgresql--parquet-data-lake)
   - 5.6 [Serving Layer — FastAPI & Streamlit](#56-serving-layer--fastapi--streamlit)
   - 5.7 [Observability Layer](#57-observability-layer)
6. [Database Schema](#6-database-schema)
7. [Business Logic Modules](#7-business-logic-modules)
8. [Containerisation & Infrastructure](#8-containerisation--infrastructure)
9. [Configuration & Environment](#9-configuration--environment)
10. [Data Lifecycle](#10-data-lifecycle)
11. [Fault Tolerance & Idempotency](#11-fault-tolerance--idempotency)
12. [Known Limitations](#12-known-limitations)

---

## 1. High-Level Overview

This system implements a **Lambda Architecture** for continuous hospital patient vital signs monitoring. It fuses two fundamentally different data cadences into a single coherent view:

| Data Stream | Cadence | Layer |
|---|---|---|
| Bedside monitor vitals (heart rate, SpO2, BP, temperature) | Every 3 seconds per patient | **Speed Layer** |
| Pathology lab results (troponin, potassium, creatinine, etc.) | Once per simulated day (~5 min) | **Batch Layer** |

**The core output is twofold:**
- **Real-time ward monitoring** — live per-patient vitals, colour-coded status, and instant threshold-breach alerts.
- **Daily consolidated risk report** — a clinically-weighted composite risk score per patient, merging vitals trends with lab abnormalities.

All services are containerised with Docker Compose and run on a single host. The simulated clock compresses one hospital day into 300 seconds (configurable via `SIMULATED_DAY_SECONDS`).

---

## 2. Architecture Pattern: Lambda vs Kappa

### Why Lambda?

The two data sources have **genuinely different cadences and semantics**:

- The vitals stream is a **continuous, high-frequency sensor feed** — naturally suited to a streaming processor.
- The lab results are an **inherently once-daily batch extract** — they arrive as a file drop and cannot be replayed from Kafka because they were never published there.

A **Kappa architecture** would require a single streaming pipeline to handle both, which is awkward when one source is fundamentally periodic and file-based. Lambda's explicit separation of Speed and Batch layers is a natural fit.

### Lambda Layers in This System

```
+------------------------------------------------------------------+
|                       LAMBDA ARCHITECTURE                        |
|                                                                  |
|  +------------------+        +------------------------------+   |
|  |   SPEED LAYER    |        |       BATCH LAYER            |   |
|  |  (low latency)   |        |  (high accuracy / recompute) |   |
|  |                  |        |                              |   |
|  | Kafka + Spark    |        | File Drop -> Airflow DAG     |   |
|  | Structured       |        | -> Parquet recompute         |   |
|  | Streaming        |        | -> Lab join -> Risk Score    |   |
|  +--------+---------+        +--------------+---------------+   |
|           |                                 |                   |
|           +-----------------+---------------+                   |
|                             |                                   |
|                    +--------v--------+                          |
|                    |  SERVING LAYER  |                          |
|                    |  FastAPI        |                          |
|                    |  Streamlit      |                          |
|                    +-----------------+                          |
+------------------------------------------------------------------+
```

The **key Lambda distinction**: the batch layer recomputes from an **immutable raw Parquet archive** written by the speed layer, not from replaying the Kafka log. This gives a durable, queryable source of truth independent of Kafka's retention window.

---

## 3. System Components

| Service | Image / Tech | Role | Port |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` (KRaft mode) | Message broker — vitals-stream topic | 9092 |
| `kafka-init` | `apache/kafka:3.7.0` | One-shot topic creator | — |
| `postgres` | `postgres:16` | Persistent relational store for all pipeline outputs | 5432 |
| `vitals-producer` | Python (custom) | Simulates 12 bedside monitors -> Kafka | — |
| `lab-source` | Python (custom) | Simulates daily lab batch file drops | — |
| `spark-streaming` | PySpark (custom) | Speed layer: consumes Kafka, writes Parquet + Postgres | — |
| `airflow` | Python + Airflow (custom) | Orchestrates daily batch DAG | 8080 |
| `api` | FastAPI (custom) | Unified REST serving layer | 8000 |
| `dashboard` | Streamlit (custom) | Visual dashboard for ward view + risk reports | 8501 |
| `pushgateway` | `prom/pushgateway:v1.9.0` | Metrics collection point for batch/streaming jobs | 9091 |
| `prometheus` | `prom/prometheus:v2.53.0` | Metrics storage + alerting rules | 9090 |
| `alertmanager` | `prom/alertmanager:v0.27.0` | Alert routing & notification | 9093 |
| `grafana` | `grafana/grafana:11.1.0` | Metrics visualisation dashboards | 3000 |

---

## 4. Data Flow Diagram

```
+============================================================================+
|                      DATA SOURCES (SIMULATION)                             |
|                                                                            |
|  +-------------------------------------+  +----------------------------+  |
|  | vitals-producer (Python)            |  | lab-source (Python)        |  |
|  | 12 patients x every 3 seconds       |  | Once per simulated day     |  |
|  | {patient_id, heart_rate, spo2,      |  | {patient_id, test_type,    |  |
|  |  systolic_bp, diastolic_bp, temp,   |  |  result_value, ref_low,    |  |
|  |  timestamp}                         |  |  ref_high, collected_at}   |  |
|  +-----------------+-------------------+  +-------------+--------------+  |
+=====================|===============================================|=======+
                      | JSON over Kafka                  | CSV file drop
                      v                                  v
+=======================================+  +==================================+
|  INGESTION LAYER                      |  |  BATCH ORCHESTRATION LAYER       |
|                                       |  |                                  |
|  Apache Kafka 3.7 (KRaft)             |  |  Apache Airflow                  |
|  Topic: vitals-stream                 |  |  DAG: daily_risk_report_dag      |
|  3 partitions, replication factor: 1  |  |                                  |
|                                       |  |  Task 1: find_unprocessed_batch  |
+===============|=======================+  |  Task 2: load_lab_results        |
                |                          |  Task 3: compute_and_write_risk  |
                v                          |  Task 4: record_batch_health     |
+=======================================+  +============|=====================+
|  SPEED LAYER                          |               |
|                                       |    reads Parquet archive
|  Spark Structured Streaming           | <-------------+
|                                       |
|  (1) Parse + validate JSON            +---> /data/parquet_lake/  [archive]
|  (2) Apply vitals_rules UDF           +---> alerts         (Postgres, append)
|  (3) 1-min tumbling window agg        +---> vitals_realtime (Postgres, upsert)
|  (4) Parquet archive write            |
+=======================================+

+============================================================================+
|  STORAGE LAYER  (PostgreSQL 16 + Parquet Data Lake)                        |
|                                                                            |
|  +--------------------+  +--------------+  +-------------------------+    |
|  | vitals_realtime    |  | alerts       |  | daily_risk_report       |    |
|  | (speed layer out)  |  | (alert log)  |  | (batch layer output)    |    |
|  | 1 row per patient  |  | append-only  |  | 1 row / patient / day   |    |
|  +--------------------+  +--------------+  +-------------------------+    |
|  +--------------------+  +--------------+                                 |
|  | lab_results        |  | pipeline_    |                                 |
|  | (batch input)      |  | health       |                                 |
|  +--------------------+  +--------------+                                 |
+============================================================================+
                      |
                      v
+============================================================================+
|  SERVING LAYER                                                             |
|                                                                            |
|  +----------------------------------+  +--------------------------------+  |
|  | FastAPI  (port 8000)             |  | Streamlit Dashboard (8501)     |  |
|  | GET /ward/status                 |  | Tab 1: Live Ward View          |  |
|  | GET /patient/{id}/risk           |  | Tab 2: Alert Log Feed          |  |
|  | GET /alerts/recent               |  | Tab 3: Daily Risk Report       |  |
|  | GET /health                      |  |                                |  |
|  | GET /metrics (Prometheus)        |  |                                |  |
|  +----------------------------------+  +--------------------------------+  |
+============================================================================+
                      |
                      v
+============================================================================+
|  OBSERVABILITY LAYER                                                       |
|                                                                            |
|  Prometheus (9090) <-- Pushgateway (9091) <-- vitals-producer/spark/af    |
|  Prometheus (9090) <-- native scrape <-------- FastAPI /metrics            |
|  Alertmanager (9093) <-- Prometheus rules (5 alert rules)                  |
|  Grafana (3000) <-- Prometheus data source                                 |
+============================================================================+
```

---

## 5. Layer-by-Layer Breakdown

### 5.1 Data Sources (Simulation Layer)

#### `vitals-producer` — `sources/vitals_producer.py`

Simulates bedside monitoring hardware for `NUM_PATIENTS` (default: 12) patients.

| Property | Value |
|---|---|
| Output | JSON messages on Kafka topic `vitals-stream` |
| Rate | One reading per patient every `VITALS_INTERVAL_SECONDS` (default: 3s) |
| Abnormal spike probability | `ABNORMAL_SPIKE_PROBABILITY` (default: 6%) |
| Metrics pushed | `vitals_producer_events_sent_total`, `vitals_producer_errors_total`, `vitals_producer_last_tick_timestamp` |

**Message schema:**
```json
{
  "patient_id": "P001",
  "heart_rate": 82.4,
  "spo2": 97.1,
  "systolic_bp": 118.3,
  "diastolic_bp": 75.6,
  "temperature": 36.8,
  "timestamp": "2024-01-15T10:23:45.123Z"
}
```

#### `lab-source` — `sources/lab_batch_source.py`

Simulates a hospital pathology information system dropping daily lab result files.

| Property | Value |
|---|---|
| Output | CSV files in `/data/lab_drops/` |
| Rate | Once per `SIMULATED_DAY_SECONDS` (default: every 5 minutes) |
| Lab tests simulated | troponin, potassium, sodium, creatinine, haemoglobin, WBC count, glucose |
| Metrics pushed | `lab_batch_last_drop_timestamp`, `lab_batch_files_produced_total` |

---

### 5.2 Ingestion Layer — Apache Kafka

| Property | Value |
|---|---|
| Image | `apache/kafka:3.7.0` (KRaft mode — no ZooKeeper) |
| Topic | `vitals-stream` |
| Partitions | 3 |
| Replication factor | 1 (single-broker demo) |
| Consumer | Spark Structured Streaming (startingOffsets: earliest) |
| Durability | Ephemeral (no named volume) — Parquet archive is the durable record |

**Why KRaft mode?** The Bitnami rolling Kafka tags were removed in 2025. The upstream Apache Kafka image in KRaft mode removes the ZooKeeper dependency entirely, simplifying the container graph.

---

### 5.3 Speed Layer — Spark Structured Streaming

**File:** `streaming/vitals_stream_processor.py`

The speed layer is a single PySpark job running **three parallel streaming queries** from one Kafka source.

#### Processing Pipeline

```
Kafka Source
    |
    v  F.from_json() with VITALS_SCHEMA
Parse & Deserialise
    |
    v  Range filters (heart_rate 0-300, spo2 0-100, etc.)
Validate & Filter  (drop malformed events)
    |
    +-----------------------------------------------+
    |                                               |
    v  (all rows)               v  classify_udf()  (all rows)
Query 1: Parquet Archive    Classified DataFrame
  .format("parquet")              |
  .partitionBy("event_date")      +----------------------------+
  .trigger(30s)                   |                            |
  -> /data/parquet_lake/     v  filter severity != "normal"    v  groupBy(1-min window, patient_id)
     vitals/event_date=...  Query 2: Alerts            Query 3: vitals_realtime
     *.parquet               .foreachBatch()            .outputMode("update")
                             .trigger(10s)              .trigger(20s)
                             -> alerts (Postgres,INS)   -> vitals_realtime (Postgres, UPSERT)
```

#### Classify UDF (`_classify`)

Wraps `common.vitals_rules.assess_reading()` as a Spark UDF. For each reading it returns:
- `severity`: `"normal"` | `"warning"` | `"critical"`
- `metric`: the primary breaching metric name
- `value`: its numeric value
- `reasons`: concatenated breach descriptions

#### Query 1 — Parquet Archive

- **Mode:** `append`
- **Trigger:** every 30 seconds
- **Partitioning:** `event_date` (one directory per simulated day)
- **Checkpoint:** `/data/spark_checkpoints/archive`
- **Purpose:** Provides the immutable historical record that Airflow reads. This is the key Lambda/Kappa distinction — the batch layer reads a queryable Parquet lake rather than replaying an ephemeral Kafka log.

#### Query 2 — Threshold Alerts

- **Mode:** `foreachBatch` (custom Postgres INSERT via psycopg2)
- **Trigger:** every 10 seconds
- **Filter:** only rows where `severity != "normal"`
- **Checkpoint:** `/data/spark_checkpoints/alerts`
- **Postgres table:** `alerts` (append-only, indexed by `patient_id + event_time`)

#### Query 3 — Per-Patient 1-Minute Windowed Aggregates

- **Mode:** `update` with `foreachBatch` Postgres upsert
- **Window:** 1-minute tumbling window with 30-second watermark
- **Trigger:** every 20 seconds
- **Aggregations per window per patient:** `avg_heart_rate`, `avg_spo2`, `avg_systolic_bp`, `avg_diastolic_bp`, `avg_temperature`, `reading_count`, `abnormal_count`, `severity_rank`
- **Postgres table:** `vitals_realtime` (upsert on `patient_id`)
- **Deduplication note:** Only the most recent window per patient is kept per batch to avoid PostgreSQL `CardinalityViolation` in multi-row upserts.

---

### 5.4 Batch Layer — Apache Airflow

**DAG file:** `airflow/dags/daily_risk_report_dag.py`

Runs once per simulated day. Reconciles accumulated vitals from the Parquet data lake with that day's lab results to produce a composite patient risk score.

#### DAG: `daily_risk_report_dag`

```
find_unprocessed_batch
        |
        v
load_lab_results
        |
        v
compute_and_write_risk_report
        |
        v
record_batch_health
```

**Task 1 — `find_unprocessed_batch`**
- Scans `/data/lab_drops/` for new CSV files.
- Checks `lab_results` table to skip files already loaded (idempotent).
- Passes batch date and file path downstream via XCom.

**Task 2 — `load_lab_results`**
- Reads the CSV lab file for the batch date.
- Determines out-of-range status from embedded reference low/high values.
- Inserts rows into `lab_results` with `UNIQUE(patient_id, test_type, batch_date)` for idempotency.

**Task 3 — `compute_and_write_risk_report`**
- Reads the Parquet archive filtered to the batch date partition.
- Calls `common.risk_scoring.compute_daily_trend(readings)` per patient.
- Queries `lab_results` for that day's out-of-range flags.
- Calls `common.risk_scoring.compute_risk(trend, lab_flags)` to produce a 0–100 risk score.
- Upserts into `daily_risk_report` table.

**Task 4 — `record_batch_health`**
- Writes a success heartbeat to `pipeline_health` (stage: `airflow_dag`).
- Pushes metrics to Prometheus Pushgateway.

**Airflow mode:** `standalone` (SQLite + SequentialExecutor) — appropriate for a demo, not production.

---

### 5.5 Storage Layer — PostgreSQL & Parquet Data Lake

#### PostgreSQL 16

Single-instance relational store for all pipeline outputs. Schema auto-applied at container start via `common/schema.sql` mounted into `docker-entrypoint-initdb.d/`.

| Table | Written by | Read by | Notes |
|---|---|---|---|
| `vitals_realtime` | Spark (upsert) | FastAPI, Streamlit | Latest 1-min window per patient |
| `alerts` | Spark (append) | FastAPI, Streamlit | All threshold breach events |
| `lab_results` | Airflow batch | Airflow batch | Daily lab data, UNIQUE constraint |
| `daily_risk_report` | Airflow batch | FastAPI, Streamlit | Per-patient risk score per day |
| `pipeline_health` | All stages | FastAPI `/health` | Heartbeat/liveness rows |

#### Parquet Data Lake

| Property | Value |
|---|---|
| Path | `/data/parquet_lake/vitals/` (bind-mounted) |
| Written by | Spark Query 1 |
| Read by | Airflow batch layer |
| Partitioning | `event_date=YYYY-MM-DD` |
| Format | Apache Parquet (columnar, compressed) |
| Retention | Persists across container restarts (`down` without `-v`) |

---

### 5.6 Serving Layer — FastAPI & Streamlit

#### FastAPI — `api/`

Unified REST API serving the Lambda merge point — reads from Postgres which holds both speed-layer and batch-layer outputs.

| Endpoint | Description |
|---|---|
| `GET /ward/status` | Latest realtime vitals snapshot for all patients |
| `GET /patient/{patient_id}/risk` | Most recent daily risk score breakdown for a patient |
| `GET /alerts/recent` | Latest threshold-breach alerts (configurable window) |
| `GET /health` | Pipeline stage health based on `pipeline_health` table |
| `GET /metrics` | Prometheus metrics (native scrape endpoint) |

#### Streamlit Dashboard — `dashboard/`

Three-tab visual interface reading directly from Postgres:

| Tab | Content |
|---|---|
| **Live Ward View** | Colour-coded realtime vitals table (normal=green, warning=amber, critical=red) |
| **Alert Log** | Real-time feed of threshold breaches with patient ID, metric, value, severity |
| **Daily Risk Report** | Bar chart of risk scores + summary table per patient per batch date |

---

### 5.7 Observability Layer

#### Prometheus Metrics Collection

Two paths feed Prometheus:

1. **Pushgateway** (`pushgateway:9091`): Batch-style services without a persistent HTTP server (vitals-producer, Spark job, Airflow DAG) push metrics here via `common/metrics.py`.
2. **Native scrape**: FastAPI exposes `/metrics` directly and Prometheus scrapes it on a schedule.

#### Key Metrics

| Metric | Type | Produced by |
|---|---|---|
| `vitals_producer_events_sent_total` | Counter | vitals-producer |
| `vitals_producer_errors_total` | Counter | vitals-producer |
| `vitals_producer_last_tick_timestamp` | Gauge | vitals-producer |
| `spark_alerts_written_total` | Counter | Spark streaming |
| `spark_realtime_last_batch_timestamp` | Gauge | Spark streaming |
| `lab_batch_last_drop_timestamp` | Gauge | lab-source |
| `lab_batch_files_produced_total` | Counter | lab-source |

#### Alert Rules (5 rules — `observability/alert_rules.yml`)

| Alert | Expression | Severity | Meaning |
|---|---|---|---|
| `VitalsIngestionStalled` | `time() - vitals_producer_last_tick_timestamp > 120` | critical | No vitals produced for >2 min |
| `SparkStreamingStalled` | `time() - spark_realtime_last_batch_timestamp > 120` | critical | Speed layer not producing windows |
| `HighAbnormalVitalsRate` | `rate(spark_alerts_written_total[5m]) > 0.5` | warning | >0.5 alerts/sec sustained over 5 min |
| `BatchJobOverdue` | `time() - lab_batch_last_drop_timestamp > 900` | warning | Lab file not seen for >15 min |
| `ErrorRateElevated` | `increase(vitals_producer_errors_total[5m]) > 5` | warning | >5 Kafka publish errors in 5 min |

#### Grafana

Pre-provisioned dashboard **"Hospital Vital Signs Pipeline"** with panels for vitals ingestion rate, Spark micro-batch latency, alert event rate, and pipeline health status.

---

## 6. Database Schema

### `vitals_realtime` — Speed Layer Output

```sql
CREATE TABLE vitals_realtime (
    patient_id       TEXT PRIMARY KEY,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    avg_heart_rate   DOUBLE PRECISION,
    avg_spo2         DOUBLE PRECISION,
    avg_systolic_bp  DOUBLE PRECISION,
    avg_diastolic_bp DOUBLE PRECISION,
    avg_temperature  DOUBLE PRECISION,
    reading_count    INTEGER NOT NULL,
    abnormal_count   INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'normal',  -- normal | warning | critical
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `alerts` — Speed Layer Alert Log (Append-Only)

```sql
CREATE TABLE alerts (
    id          BIGSERIAL PRIMARY KEY,
    patient_id  TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    severity    TEXT NOT NULL,   -- warning | critical
    reason      TEXT NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Indexes
CREATE INDEX idx_alerts_patient_time ON alerts (patient_id, event_time DESC);
CREATE INDEX idx_alerts_created      ON alerts (created_at DESC);
```

### `lab_results` — Batch Layer Input

```sql
CREATE TABLE lab_results (
    id              BIGSERIAL PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    test_type       TEXT NOT NULL,
    result_value    DOUBLE PRECISION NOT NULL,
    reference_low   DOUBLE PRECISION,
    reference_high  DOUBLE PRECISION,
    is_out_of_range BOOLEAN NOT NULL DEFAULT FALSE,
    collected_at    TIMESTAMPTZ NOT NULL,
    batch_date      DATE NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (patient_id, test_type, batch_date)
);
```

### `daily_risk_report` — Lambda Serving Layer Merge Point

```sql
CREATE TABLE daily_risk_report (
    patient_id       TEXT NOT NULL,
    batch_date       DATE NOT NULL,
    reading_count    INTEGER NOT NULL,
    abnormal_ratio   DOUBLE PRECISION NOT NULL,
    max_heart_rate   DOUBLE PRECISION,
    min_spo2         DOUBLE PRECISION,
    max_systolic_bp  DOUBLE PRECISION,
    vitals_trend     TEXT,          -- improving | stable | worsening
    lab_flags        TEXT[],        -- out-of-range test_type names
    lab_flag_count   INTEGER NOT NULL DEFAULT 0,
    risk_score       DOUBLE PRECISION NOT NULL,
    risk_category    TEXT NOT NULL, -- low | medium | high | critical
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (patient_id, batch_date)
);
```

### `pipeline_health` — Observability Heartbeat

```sql
CREATE TABLE pipeline_health (
    stage           TEXT PRIMARY KEY,  -- vitals_producer | lab_source | spark_streaming | airflow_dag
    last_success_at TIMESTAMPTZ NOT NULL,
    last_status     TEXT NOT NULL,     -- ok | error
    detail          TEXT
);
```

---

## 7. Business Logic Modules

Both modules are **pure Python with zero external dependencies** — unit-tested directly and imported unchanged by both Spark and Airflow.

### `common/vitals_rules.py` — Real-Time Threshold Classification

Classifies a single vitals reading against clinically-inspired thresholds via `assess_reading(reading: dict) -> VitalsAssessment`.

#### Thresholds

| Metric | Warning Low | Critical Low | Warning High | Critical High |
|---|---|---|---|---|
| Heart Rate (bpm) | 60 | 50 | 100 | 130 |
| SpO2 (%) | 94 | 90 | 101 | 101 |
| Systolic BP (mmHg) | 90 | 80 | 140 | 170 |
| Diastolic BP (mmHg) | 60 | 45 | 90 | 110 |
| Temperature (deg C) | 36.0 | 35.0 | 38.0 | 39.5 |

#### Output — `VitalsAssessment`

```python
@dataclass
class VitalsAssessment:
    severity: str          # "normal" | "warning" | "critical"
    breaches: list[Breach] # list of all threshold violations
```

### `common/risk_scoring.py` — Batch Daily Risk Score

Computes a composite 0–100 risk score for a patient for a given simulated day.

#### Step 1 — `compute_daily_trend(readings)`

Produces a `DailyVitalsTrend` from chronologically-ordered raw vitals dicts (as archived in Parquet):
- `abnormal_ratio` — fraction of the day's readings classified as warning/critical
- `max_heart_rate`, `min_spo2`, `max_systolic_bp` — extreme values seen during the day
- `vitals_trend` — `"improving"` | `"stable"` | `"worsening"`, determined by comparing the abnormal ratio in the first half of the day vs. the second half (delta > 0.1 = worsening, < -0.1 = improving)

#### Step 2 — `compute_risk(trend, lab_flags)`

| Condition | Points |
|---|---|
| `abnormal_ratio x 50` | 0 – 50 |
| `min_spo2 < 90` | +15 |
| `max_heart_rate > 130` | +10 |
| `max_systolic_bp > 170` | +10 |
| Critical lab test out-of-range (troponin, potassium, creatinine, wbc_count) | +12 each |
| Any other lab test out-of-range | +6 each |
| `vitals_trend == "worsening"` | +10 |
| `vitals_trend == "improving"` | -5 |

**Score is clamped to [0, 100].**

#### Risk Categories

| Score | Category |
|---|---|
| >= 70 | `critical` |
| 45 – 69 | `high` |
| 20 – 44 | `medium` |
| < 20 | `low` |

---

## 8. Containerisation & Infrastructure

### Startup Order (via `depends_on` + health conditions)

```
postgres (healthy)
    |
    +-- kafka (healthy)
    |       |
    |       +-- kafka-init (completed_successfully)
    |                   |
    |                   +-- vitals-producer
    |                   +-- spark-streaming
    |
    +-- lab-source
    +-- airflow
    +-- api
    +-- dashboard

prometheus
    +-- grafana
pushgateway  (independent, no depends_on)
alertmanager (independent, no depends_on)
```

### Named Volumes

| Volume | Purpose |
|---|---|
| `postgres_data` | PostgreSQL data directory (persists across `down`) |
| `spark_ivy_cache` | Maven dependency cache for Spark Kafka connector jars |
| `prometheus_data` | Prometheus TSDB storage |
| `grafana_data` | Grafana dashboard state |

### Bind Mounts

| Host Path | Container Path | Used by |
|---|---|---|
| `./data/lab_drops/` | `/data/lab_drops` | lab-source (write), airflow (read) |
| `./data/parquet_lake/` | `/data/parquet_lake` | spark-streaming (write), airflow (read) |
| `./data/spark_checkpoints/` | `/data/spark_checkpoints` | spark-streaming |
| `./common/schema.sql` | `/docker-entrypoint-initdb.d/01-schema.sql` | postgres (init) |
| `./observability/prometheus.yml` | `/etc/prometheus/prometheus.yml` | prometheus |
| `./observability/alert_rules.yml` | `/etc/prometheus/alert_rules.yml` | prometheus |
| `./observability/alertmanager.yml` | `/etc/alertmanager/alertmanager.yml` | alertmanager |
| `./observability/grafana/` | `/etc/grafana/provisioning/` | grafana |

---

## 9. Configuration & Environment

All configurable values are set in `.env` (copy from `.env.example`):

| Variable | Default | Effect |
|---|---|---|
| `SIMULATED_DAY_SECONDS` | `300` | 1 simulated day = 5 real minutes |
| `NUM_PATIENTS` | `12` | Number of simulated ward beds/patients |
| `VITALS_INTERVAL_SECONDS` | `3` | Sensor reading cadence per patient |
| `ABNORMAL_SPIKE_PROBABILITY` | `0.06` | 6% chance of generating an out-of-range reading |
| `KAFKA_TOPIC_VITALS` | `vitals-stream` | Kafka topic name |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `POSTGRES_DB` | `hospital` | Database name |
| `POSTGRES_USER` | `hospital` | Database user |
| `POSTGRES_PASSWORD` | `hospital_pw` | Database password |
| `PUSHGATEWAY_URL` | `http://pushgateway:9091` | Prometheus Pushgateway endpoint |
| `PARQUET_LAKE_PATH` | `/data/parquet_lake/vitals` | Parquet archive path inside containers |

---

## 10. Data Lifecycle

### Every 3 seconds (continuous)
1. `vitals-producer` emits one JSON reading per patient onto Kafka.
2. Spark reads and parses the event stream continuously.

### Every 10 seconds (Spark alerts trigger)
3. Abnormal readings are flushed to the `alerts` table in Postgres.

### Every 20 seconds (Spark realtime trigger)
4. Per-patient 1-minute windowed aggregates are upserted to `vitals_realtime`.
5. Streamlit **Live Ward View** and `GET /ward/status` reflect the latest state.

### Every 30 seconds (Spark Parquet trigger)
6. Raw parsed events are appended to the Parquet data lake, partitioned by `event_date`.

### Every simulated day (~5 minutes)
7. `lab-source` drops a new CSV into `/data/lab_drops/`.
8. Airflow DAG triggers, detects the new file.
9. Loads lab results into `lab_results` table.
10. Reads that day's Parquet partition and recomputes the vitals trend per patient.
11. Joins trend with lab flags and computes the 0–100 risk score.
12. Upserts into `daily_risk_report` table.
13. Streamlit **Daily Risk Report** tab and `GET /patient/{id}/risk` reflect the new scores.

---

## 11. Fault Tolerance & Idempotency

| Concern | Mechanism |
|---|---|
| Spark job crash | Checkpoint directories in `/data/spark_checkpoints/`; job resumes from last committed Kafka offset on restart |
| Kafka message replay | `startingOffsets: earliest` + Spark checkpoints ensure no data is lost or double-processed |
| Duplicate lab file processing | `UNIQUE(patient_id, test_type, batch_date)` constraint + `find_unprocessed_batch` task skips already-loaded files |
| Duplicate risk report rows | `PRIMARY KEY(patient_id, batch_date)` with `ON CONFLICT DO UPDATE` |
| Duplicate vitals_realtime rows | `ON CONFLICT (patient_id) DO UPDATE` — always reflects the latest window |
| Airflow metadata reset | Airflow SQLite is ephemeral, but pipeline correctness lives in the app Postgres tables |
| Container health checks | All critical services declare `healthcheck` blocks; dependent services wait for `service_healthy` |
| Stale stage detection | `pipeline_health` table + Prometheus alert rules catch any stage that stops reporting within 2 minutes |

---

## 12. Known Limitations

> These are intentional simplifications for a course-scale demo.

| Limitation | Production Alternative |
|---|---|
| Single-broker Kafka (replication factor 1) | Multi-broker cluster with replication factor >= 3 |
| Single Postgres instance (no HA) | Postgres streaming replication or a managed cloud DB (e.g. Cloud SQL, RDS) |
| Airflow `standalone` mode (SQLite + SequentialExecutor) | CeleryExecutor or KubernetesExecutor with dedicated Postgres metadata DB |
| Batch vitals recompute uses pandas, not Spark | `spark-submit` batch job for large historical archives |
| No Kafka log retention configured | Set `log.retention.hours`, enable log compaction for long-lived topics |
| No TLS or authentication on any service | mTLS for Kafka, SSL for Postgres, OAuth/API keys for serving endpoints |
| Grafana anonymous viewer access enabled | Disable `GF_AUTH_ANONYMOUS_ENABLED`, enforce user accounts |
| Unit tests cover only pure business logic | Integration tests with testcontainers for Kafka/Postgres interactions |

---

## Repository Layout

```
BIG-DATA-MINI-PROJECT/
+-- common/                  Shared library (imported by all Python services)
|   +-- db.py                PostgreSQL connection helper + upsert_health()
|   +-- logging_config.py    Structured JSON logging setup
|   +-- metrics.py           Prometheus Pushgateway push helper
|   +-- risk_scoring.py      Pure batch risk scoring logic (unit tested)
|   +-- schema.sql           PostgreSQL schema (applied at container init)
|   +-- vitals_rules.py      Pure threshold classification logic (unit tested)
+-- sources/                 Simulated data producers
|   +-- vitals_producer.py   Bedside monitor simulator -> Kafka
|   +-- lab_batch_source.py  Pathology lab file drop simulator
|   +-- Dockerfile
+-- streaming/               Speed layer
|   +-- vitals_stream_processor.py   Spark Structured Streaming job (3 queries)
|   +-- Dockerfile
+-- airflow/                 Batch layer orchestration
|   +-- dags/
|   |   +-- daily_risk_report_dag.py  4-task Airflow DAG
|   +-- Dockerfile
+-- api/                     Serving layer - REST API (FastAPI)
+-- dashboard/               Serving layer - Streamlit UI
+-- observability/           Monitoring stack configuration
|   +-- prometheus.yml       Scrape configs
|   +-- alert_rules.yml      5 Prometheus alert rules
|   +-- alertmanager.yml     Alertmanager routing
|   +-- grafana/             Provisioning + dashboard JSON
+-- tests/                   Unit tests (pytest, no external dependencies)
|   +-- test_risk_scoring.py 25 tests for risk_scoring + vitals_rules
+-- data/                    Runtime bind-mount volumes
|   +-- lab_drops/           Lab CSV file drop zone
|   +-- parquet_lake/        Immutable raw vitals archive (partitioned Parquet)
|   +-- spark_checkpoints/   Spark streaming state
+-- docker-compose.yml       Full service orchestration (13 services)
+-- .env.example             Configuration template
+-- README.md                Quick start guide
+-- Start.md                 Full setup, report & demo guide
+-- Archiecture.md           This document
```

