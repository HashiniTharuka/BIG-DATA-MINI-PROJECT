# Hospital Patient Vital Signs Monitoring — Lambda Architecture Pipeline

Applied Big Data Engineering — Mini Project. Use Case 2 (Hospital Patient Vital Signs
Monitoring), implemented as a **Lambda architecture**: a Kafka + Spark Structured
Streaming speed layer for real-time ward monitoring, and an Airflow-orchestrated batch
layer that reconciles daily lab results against recomputed vitals trends into a
consolidated daily risk report. See [`report/REPORT.md`](report/REPORT.md) for the full
architecture justification, stack rationale, and write-up.

## Architecture

```
Bedside monitors (sim)  --Kafka: vitals-stream (3 partitions)-->  Spark Structured Streaming
                                                                     |-- vitals_realtime (Postgres, upsert)
                                                                     |-- alerts (Postgres, append)
                                                                     \-- raw Parquet archive (data lake)

Pathology lab (sim, 1x/simulated day) --file drop--> Airflow DAG (daily_risk_report_dag)
                                                                     |-- lab_results (Postgres)
                                                                     |-- recompute vitals trend from Parquet
                                                                     |-- join + score
                                                                     \-- daily_risk_report (Postgres)

Serving: FastAPI (/ward/status, /patient/{id}/risk, /alerts/recent, /health, /metrics)
         Streamlit dashboard (live ward view + daily risk report + pipeline health)

Observability: structured JSON logs (all stages) + Prometheus (Pushgateway for
producer/Spark/Airflow, native scrape for the API) + Alertmanager rules + Grafana
```

Why Lambda and not Kappa, and why this stack: see `report/REPORT.md` — short version:
the two sources have genuinely different cadences (continuous sensor stream vs. an
inherently-daily lab extract), so the batch layer recomputes from an immutable raw
Parquet archive rather than replaying the Kafka log, which is what distinguishes Lambda
from Kappa here.

## Simulated clock

One simulated **day = 300 seconds** (5 minutes) by default — set `SIMULATED_DAY_SECONDS`
in `.env` to change it. The vitals stream itself is **not** compressed: it represents
genuine continuous monitoring (one reading per patient every `VITALS_INTERVAL_SECONDS`,
default 3s). Only the lab batch feed and the Airflow DAG schedule follow the simulated
day.

## Prerequisites

- Docker Desktop (with at least ~6GB RAM allocated — Settings → Resources)
- Internet access on first `docker compose up` (Spark pulls the Kafka/Postgres
  connector jars from Maven Central once; cached afterwards in a named volume)

## Running it

```bash
cp .env.example .env        # adjust SIMULATED_DAY_SECONDS / NUM_PATIENTS if you like
docker compose up -d --build
docker compose ps           # everything should be "running" (kafka-init exits 0, that's expected)
```

First boot takes a few minutes (Spark downloading connector jars, Airflow initializing
its metadata DB). Give it ~2-3 minutes, then:

| Service | URL | Notes |
|---|---|---|
| Streamlit dashboard (the consolidated report) | http://localhost:8501 | live ward view + daily risk report |
| API | http://localhost:8000/docs | Swagger UI |
| Airflow UI | http://localhost:8080 | run `docker compose logs airflow \| grep -i password` for the auto-generated admin password |
| Grafana | http://localhost:3000 | admin/admin, or anonymous viewer access |
| Prometheus | http://localhost:9090 | Status → Rules to see alert rules |
| Alertmanager | http://localhost:9093 | fired alerts appear here |

Within a couple of minutes you should see the live ward table populate in the
dashboard. The first daily risk report appears after one full `SIMULATED_DAY_SECONDS`
interval once the lab source drops its first file and the Airflow DAG runs (check
Airflow's `daily_risk_report_dag` in the UI, or just wait — it's scheduled automatically).

## Reproducing the observability results for the report/demo

- **Structured logs**: `docker compose logs -f vitals-producer spark-streaming lab-source api`
  — every line is a JSON object with `stage`, `level`, `timestamp`.
- **Metrics**: http://localhost:9090/graph — try `rate(vitals_producer_events_sent_total[2m])`
  or `time() - spark_realtime_last_batch_timestamp`.
- **Alert demo**: `docker compose stop vitals-producer` and wait ~2 minutes — the
  `VitalsIngestionStalled` alert fires (visible in Prometheus → Alerts and in
  Alertmanager). `docker compose start vitals-producer` to resolve it.
- **Health check**: `curl http://localhost:8000/health`

## Running the unit tests

Core business logic (threshold classification, daily risk scoring) is pure Python with
no Spark/Kafka/DB dependency, so it's tested directly:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

## Repository layout

```
common/       shared code: DB helper, schema, structured logging, Prometheus push
              helper, and the two pure business-logic modules (vitals_rules.py,
              risk_scoring.py) that the unit tests exercise directly.
sources/      simulated data sources (vitals_producer.py, lab_batch_source.py)
streaming/    Spark Structured Streaming speed-layer job
airflow/      Airflow DAG for the batch layer
api/          FastAPI serving layer
dashboard/    Streamlit consolidated report/dashboard
observability/ Prometheus, Alertmanager, Grafana configs
tests/        pytest unit tests for common/risk_scoring.py and common/vitals_rules.py
report/       REPORT.md — the written report draft
data/         bind-mounted volumes: lab file drop zone, Parquet data lake, Spark checkpoints
```

## Known limitations (see report for the full discussion)

- Airflow runs in `airflow standalone` mode (SQLite + SequentialExecutor) for
  simplicity — a production deployment would use CeleryExecutor with a dedicated
  metadata Postgres database for concurrency and durability.
- The batch layer recomputes vitals trends with pandas over the Parquet archive rather
  than a distributed Spark batch job — appropriate at this data volume; a production
  system with a much larger historical archive would use `spark-submit` for that step too.
- Single-broker Kafka and single Postgres instance — no replication/HA, acceptable for
  a class-scale demo.
