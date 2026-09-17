"""Speed layer: Spark Structured Streaming job.

Reads raw vitals events from Kafka and produces three continuous outputs:
  1. vitals_realtime  (Postgres) - per-patient 1-minute tumbling-window
     aggregates, upserted every micro-batch. Powers the live ward view.
  2. alerts            (Postgres) - one row per reading that breaches a
     vital-sign threshold (common.vitals_rules), for immediate visibility.
  3. Parquet archive (data lake) - the immutable raw event log that the
     Airflow batch layer recomputes daily trends from. This separation
     (materialize to a batch-friendly store rather than replaying the Kafka
     log itself) is what makes this a Lambda, not a Kappa, architecture.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, "/app")
from common.logging_config import get_logger, log_event  # noqa: E402
from common.metrics import push_metrics  # noqa: E402
from common.db import get_conn_params, upsert_health  # noqa: E402
from common.vitals_rules import assess_reading  # noqa: E402

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

logger = get_logger("spark_streaming")

# Running cumulative totals across all foreachBatch calls, so the pushed
# Prometheus counters are genuinely monotonic (Pushgateway replaces the
# previous pushed value wholesale on every push, so per-batch counts would
# make a "_total" counter jump around instead of accumulate).
_alerts_written_total = 0
_realtime_rows_written_total = 0

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_VITALS", "vitals-stream")
PARQUET_LAKE_PATH = os.environ.get("PARQUET_LAKE_PATH", "/data/parquet_lake/vitals")
CHECKPOINT_ROOT = os.environ.get("SPARK_CHECKPOINT_ROOT", "/data/spark_checkpoints")

VITALS_SCHEMA = StructType([
    StructField("patient_id", StringType()),
    StructField("heart_rate", DoubleType()),
    StructField("spo2", DoubleType()),
    StructField("systolic_bp", DoubleType()),
    StructField("diastolic_bp", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", StringType()),
])

CLASSIFICATION_SCHEMA = StructType([
    StructField("severity", StringType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("reasons", StringType()),
])


def _classify(heart_rate, spo2, systolic_bp, diastolic_bp, temperature):
    assessment = assess_reading({
        "heart_rate": heart_rate, "spo2": spo2, "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp, "temperature": temperature,
    })
    if not assessment.breaches:
        return (assessment.severity, None, None, None)
    critical = [b for b in assessment.breaches if b.severity == "critical"]
    primary = critical[0] if critical else assessment.breaches[0]
    reasons = "; ".join(b.reason for b in assessment.breaches)
    return (assessment.severity, primary.metric, float(primary.value), reasons)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("hospital-vitals-speed-layer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def write_alerts_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    records = [
        (r["patient_id"], r["metric"] or "multiple", r["value"] or 0.0, r["severity"],
         r["reasons"] or "", r["event_time"])
        for r in rows
    ]
    conn = psycopg2.connect(**get_conn_params())
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO alerts (patient_id, metric, value, severity, reason, event_time)
                   VALUES %s""",
                records,
            )
        conn.commit()
    finally:
        conn.close()

    global _alerts_written_total
    _alerts_written_total += len(records)

    log_event(logger, 30, "alerts batch written", batch_id=batch_id, count=len(records))
    push_metrics(
        job="spark_streaming_alerts",
        counters={"spark_alerts_written_total": _alerts_written_total},
        gauges={"spark_alerts_last_batch_timestamp": time.time(),
                "spark_alerts_last_batch_size": len(records)},
    )
    upsert_health("spark_streaming", "ok", detail=f"alerts_batch={batch_id} rows={len(records)}")


def write_realtime_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    # In "update" output mode a single micro-batch can contain refreshed
    # aggregates for more than one window instance of the same patient
    # (e.g. a late-arriving update for the previous window alongside the
    # new one). vitals_realtime is keyed on patient_id alone (we only want
    # the latest snapshot), so a multi-row upsert would try to touch the
    # same row twice in one statement, which Postgres rejects
    # (CardinalityViolation). Keep only the most recent window per patient.
    latest_by_patient: dict[str, object] = {}
    for r in rows:
        existing = latest_by_patient.get(r["patient_id"])
        if existing is None or r["window_end"] > existing["window_end"]:
            latest_by_patient[r["patient_id"]] = r

    records = []
    for r in latest_by_patient.values():
        rank = r["severity_rank"]
        status = "critical" if rank == 2 else ("warning" if rank == 1 else "normal")
        records.append((
            r["patient_id"], r["window_start"], r["window_end"],
            r["avg_heart_rate"], r["avg_spo2"], r["avg_systolic_bp"],
            r["avg_diastolic_bp"], r["avg_temperature"], r["reading_count"],
            r["abnormal_count"], status,
        ))

    conn = psycopg2.connect(**get_conn_params())
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO vitals_realtime
                       (patient_id, window_start, window_end, avg_heart_rate, avg_spo2,
                        avg_systolic_bp, avg_diastolic_bp, avg_temperature, reading_count,
                        abnormal_count, status, updated_at)
                   VALUES %s
                   ON CONFLICT (patient_id) DO UPDATE SET
                       window_start = EXCLUDED.window_start,
                       window_end = EXCLUDED.window_end,
                       avg_heart_rate = EXCLUDED.avg_heart_rate,
                       avg_spo2 = EXCLUDED.avg_spo2,
                       avg_systolic_bp = EXCLUDED.avg_systolic_bp,
                       avg_diastolic_bp = EXCLUDED.avg_diastolic_bp,
                       avg_temperature = EXCLUDED.avg_temperature,
                       reading_count = EXCLUDED.reading_count,
                       abnormal_count = EXCLUDED.abnormal_count,
                       status = EXCLUDED.status,
                       updated_at = now()""",
                records,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())",
            )
        conn.commit()
    finally:
        conn.close()

    global _realtime_rows_written_total
    _realtime_rows_written_total += len(records)

    log_event(logger, 20, "realtime batch written", batch_id=batch_id, count=len(records))
    push_metrics(
        job="spark_streaming_realtime",
        counters={"spark_realtime_windows_written_total": _realtime_rows_written_total},
        gauges={"spark_realtime_last_batch_timestamp": time.time(),
                "spark_realtime_last_batch_size": len(records)},
    )
    upsert_health("spark_streaming", "ok", detail=f"realtime_batch={batch_id} rows={len(records)}")


def main() -> None:
    log_event(logger, 20, "spark streaming job starting", kafka_bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), VITALS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .filter(
            F.col("heart_rate").between(0, 300)
            & F.col("spo2").between(0, 100)
            & F.col("systolic_bp").between(0, 300)
            & F.col("diastolic_bp").between(0, 200)
            & F.col("temperature").between(25, 45)
        )
    )

    classify_udf = F.udf(_classify, CLASSIFICATION_SCHEMA)
    classified = (
        parsed.withColumn("classification", classify_udf(
            "heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"
        ))
        .withColumn("severity", F.col("classification.severity"))
        .withColumn("metric", F.col("classification.metric"))
        .withColumn("value", F.col("classification.value"))
        .withColumn("reasons", F.col("classification.reasons"))
        .drop("classification")
    )

    # --- Output 1: raw archive to Parquet (the Lambda batch layer's source of truth) ---
    archive_query = (
        parsed.withColumn("event_date", F.to_date("event_time"))
        .writeStream.format("parquet")
        .option("path", PARQUET_LAKE_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/archive")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Output 2: threshold alerts (append, unaggregated) ---
    alerts_query = (
        classified.filter(F.col("severity") != "normal")
        .select("patient_id", "metric", "value", "severity", "reasons", "event_time")
        .writeStream.foreachBatch(write_alerts_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Output 3: 1-minute per-patient windowed aggregates (speed-layer view) ---
    windowed = (
        classified.withWatermark("event_time", "30 seconds")
        .groupBy(F.window("event_time", "1 minute"), "patient_id")
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.avg("spo2").alias("avg_spo2"),
            F.avg("systolic_bp").alias("avg_systolic_bp"),
            F.avg("diastolic_bp").alias("avg_diastolic_bp"),
            F.avg("temperature").alias("avg_temperature"),
            F.count(F.lit(1)).alias("reading_count"),
            F.sum(F.when(F.col("severity") != "normal", 1).otherwise(0)).alias("abnormal_count"),
            F.max(F.when(F.col("severity") == "critical", 2)
                   .when(F.col("severity") == "warning", 1).otherwise(0)).alias("severity_rank"),
        )
        .select(
            "patient_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_heart_rate", "avg_spo2", "avg_systolic_bp", "avg_diastolic_bp", "avg_temperature",
            "reading_count", "abnormal_count", "severity_rank",
        )
    )

    realtime_query = (
        windowed.writeStream.foreachBatch(write_realtime_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/realtime")
        .trigger(processingTime="20 seconds")
        .start()
    )

    log_event(logger, 20, "all streaming queries started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
