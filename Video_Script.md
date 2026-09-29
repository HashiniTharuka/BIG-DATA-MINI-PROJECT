# Demo Video Script
## Hospital Patient Vital Signs Monitoring — Lambda Architecture Pipeline
### EC8202 Big Data and Analytics | Mini Project — Use Case 2

> **Target Duration:** 7–8 minutes
> **Format:** Screen recording with voice narration
> **Recording tip:** Run `docker compose up -d --build` at least **5 minutes before** recording so the system is warm, the first daily risk report has already generated, and all dashboards are populated.

---

## Pre-Recording Checklist

Before hitting Record:
- [ ] `docker compose ps` — all 12 services show **running** / **healthy** (`kafka-init` shows exited 0, that is expected)
- [ ] Streamlit dashboard at http://localhost:8501 shows live ward data
- [ ] At least one daily risk report row exists (wait one full `SIMULATED_DAY_SECONDS` = 5 min)
- [ ] Grafana at http://localhost:3000 shows populated metric panels
- [ ] Browser tabs open and arranged: Streamlit · Swagger · Airflow · Grafana · Prometheus · Alertmanager · Terminal
- [ ] Font size bumped up in browser (Ctrl +) and terminal for readability
- [ ] Screen resolution set to 1920×1080

---

## Section 1 — Introduction & Architecture Overview
### ⏱ 0:00 – 1:15 | Screen: Architecture diagram from `Archiecture.md` or `report/REPORT.md`

---

**[NARRATOR]**

> "Hello. In this demo we'll walk through our Big Data mini-project for EC8202 — a real-time hospital patient vital signs monitoring system, built on a **Lambda Architecture**.

> The use case is a hospital ward with twelve monitored beds. Every bedside monitor streams a reading — heart rate, blood oxygen saturation, blood pressure, and temperature — every three seconds. Once per simulated day, the pathology lab drops a batch file of blood test results.

> Our system has to do two things at once: raise an **instant bedside alert** the moment a patient's vitals cross a danger threshold, and produce a **daily consolidated risk score** per patient that fuses the continuous sensor data with the lab results — because knowing that a patient's troponin is elevated alongside an abnormal heart rate changes the clinical picture completely.

> Those two requirements — sub-ten-second alerting versus once-a-day high-accuracy scoring — are what drove us to **Lambda Architecture** rather than Kappa. Let me show you the design."

**[SCREEN ACTION]** Pan slowly over the architecture diagram. Point to each section as you narrate.

> "The **Speed Layer** sits on the left: Apache Kafka receives every sensor reading, and a Spark Structured Streaming job consumes them in micro-batches of ten to thirty seconds. It writes three outputs simultaneously — threshold-breach alerts into Postgres, rolling per-patient window averages into Postgres, and a raw Parquet archive onto disk.

> That Parquet archive is the bridge to the **Batch Layer** on the right. Apache Airflow runs a four-task DAG once per simulated day. It detects the new lab file, loads the results into Postgres, then reads the entire day's Parquet data to recompute a precise vitals trend — and joins it with the lab flags to produce a composite risk score from zero to one hundred.

> Both layers feed into the **Serving Layer**: a FastAPI REST API and a Streamlit dashboard. The dashboard shows the live ward view from the speed layer, and the daily risk report from the batch layer — unified in one screen.

> Finally, everything is instrumented with Prometheus metrics, Alertmanager rules, and Grafana dashboards for full observability.

> Let's see it running."

---

## Section 2 — Speed Layer in Action: Live Ward View & Alerts
### ⏱ 1:15 – 2:30 | Screen: Streamlit http://localhost:8501

---

**[SCREEN ACTION]** Switch to the Streamlit dashboard. Show the **Live Ward View** tab.

**[NARRATOR]**

> "This is our Streamlit dashboard. You're looking at the **Live Ward View** — one row per patient, updated every few seconds straight from the `vitals_realtime` table in Postgres.

> Notice the colour coding. Green rows are patients whose average readings over the last one-minute window are within normal range. Amber indicates a warning — the patient's average heart rate or blood pressure is trending outside normal bounds. Red — critical — means at least one metric has crossed the critical threshold: for example, SpO₂ below 90%, or heart rate above 130.

> The data you see here is driven by Spark's third streaming query: it groups events into one-minute tumbling windows, computes the per-patient averages, and upserts the result into Postgres on every twenty-second micro-batch trigger."

**[SCREEN ACTION]** Click the **Alert Log** tab.

> "Switching to the Alert Log tab — this is the *raw* alert feed, not the windowed aggregates. Every time a single reading breaches a threshold, Spark's second streaming query immediately appends a row to the `alerts` table. You can see the patient ID, which metric breached, the actual value, and the severity.

> For example — there's a SpO₂ reading of 87% for Patient P004, classified as critical. That row was written to Postgres within ten seconds of the sensor event arriving on Kafka."

**[SCREEN ACTION]** Open a terminal alongside and run:
```powershell
docker compose logs -f spark-streaming
```

> "Let me show you what Spark is doing behind the scenes. These are the structured JSON logs from the Spark container. You can see micro-batch processing events — alerts batch written, realtime batch written — every ten to twenty seconds, with the exact row counts. The pipeline is fully live."

---

## Section 3 — Batch Layer: Airflow DAG & Daily Risk Report
### ⏱ 2:30 – 4:00 | Screen: Airflow UI http://localhost:8080, then Streamlit Risk tab

---

**[SCREEN ACTION]** Switch to the Airflow UI. Navigate to DAGs → `daily_risk_report_dag`.

**[NARRATOR]**

> "Now for the Batch Layer. This is Apache Airflow. Our DAG is called `daily_risk_report_dag`. You can see it has already completed at least one successful run — that green bar in the run history.

> Let me walk through the four tasks in the graph view."

**[SCREEN ACTION]** Click **Graph** view to show the task chain.

> "**Task one: find_unprocessed_batch.** This task scans the lab file drop directory and cross-references the `lab_results` table to find any files not yet loaded. It's idempotent — re-running the DAG will never double-load a file, because the database has a unique constraint on patient ID, test type, and batch date.

> **Task two: load_lab_results.** Reads the CSV, checks each result against its embedded reference range, marks it as in-range or out-of-range, and bulk-inserts into Postgres.

> **Task three: compute_and_write_risk_report.** This is where the Lambda merge happens. The task reads the Parquet archive for the batch date — that's the immutable raw archive written by Spark — and recomputes the vitals trend from scratch using our pure Python `risk_scoring` module. It then pulls the out-of-range lab flags from Postgres and calls `compute_risk()` to get a weighted score from zero to one hundred. Troponin and potassium flags carry twelve points each because they signal acute cardiac risk. The vitals trend direction adds or subtracts points too — a worsening trend adds ten points; an improving one subtracts five. The result is upserted into `daily_risk_report`.

> **Task four: record_batch_health.** A simple heartbeat — writes a success timestamp to `pipeline_health` and pushes metrics to Prometheus."

**[SCREEN ACTION]** Click `compute_and_write_risk_report` → **Logs**.

> "Here are the task logs. You can see it reading the Parquet partition, iterating over twelve patients, computing trend and risk for each, and confirming the upserts. Fully traceable."

**[SCREEN ACTION]** Switch back to Streamlit → **Daily Risk Report** tab.

> "And here is the result. The Daily Risk Report tab. Each bar represents one patient's composite risk score for the last completed simulated day. The table below gives the full breakdown — abnormal vitals ratio, worst SpO₂, which lab tests were out of range, the trend direction, and the final category: low, medium, high, or critical. This is the Lambda batch view — precise, recomputed from the raw archive, not estimated from a stream."

---

## Section 4 — Serving API
### ⏱ 4:00 – 5:00 | Screen: Swagger UI http://localhost:8000/docs

---

**[SCREEN ACTION]** Switch to the FastAPI Swagger UI.

**[NARRATOR]**

> "The same data is also available programmatically through our FastAPI serving layer. This is the auto-generated Swagger UI at port 8000.

> Let me execute the `/ward/status` endpoint."

**[SCREEN ACTION]** Expand `GET /ward/status` → click **Try it out** → **Execute**.

> "The response is a JSON array — one object per patient — with their latest windowed vitals and status. This is what a nurse station application or a hospital information system would poll.

> Now let me query an individual patient risk score."

**[SCREEN ACTION]** Expand `GET /patient/{patient_id}/risk` → enter `P001` → **Execute**.

> "Patient P001. The response includes today's risk score, the category, the vitals trend, and the list of lab flags that contributed to the score. The batch and speed layer outputs are unified in one API call.

> Finally, the health endpoint."

**[SCREEN ACTION]** Expand `GET /health` → **Execute**.

> "The `/health` endpoint queries the `pipeline_health` table. It shows the last successful heartbeat timestamp for each pipeline stage — vitals producer, Spark streaming, Airflow DAG — and whether their last status was OK. This gives the serving layer real-time knowledge of the pipeline's health without coupling to any specific infrastructure."

---

## Section 5 — Unit Tests
### ⏱ 5:00 – 5:45 | Screen: Terminal

---

**[SCREEN ACTION]** Open a terminal and run:
```powershell
python -m pytest -v
```

**[NARRATOR]**

> "Our core business logic — the threshold classification rules and the risk scoring formula — is implemented in two pure Python modules: `common/vitals_rules.py` and `common/risk_scoring.py`. They have zero dependency on Spark, Kafka, or Postgres, which means we can test them directly with pytest.

> You can see twenty-five tests passing. These cover boundary conditions for every vital sign threshold — normal, warning, and critical — and the risk scoring formula under various combinations of vitals trends and lab flags.

> This separation of pure logic from infrastructure is deliberate. The same `assess_reading()` function is called from inside the Spark UDF in the streaming job, and from the Airflow batch DAG — one implementation, one test suite, two layers."

---

## Section 6 — Observability: Grafana, Prometheus & Alert Demo
### ⏱ 5:45 – 7:30 | Screen: Grafana → Prometheus → Alertmanager → Terminal → Recovery

---

**[SCREEN ACTION]** Switch to Grafana at http://localhost:3000. Navigate to Dashboards → **Hospital Vital Signs Pipeline**.

**[NARRATOR]**

> "Now for observability. This is our Grafana dashboard, pre-provisioned from configuration files in the `observability/` directory.

> The top panel shows the **vitals ingestion rate** — events per second produced by the bedside monitor simulator and confirmed as delivered to Kafka. Currently around four readings per second for twelve patients, which matches our three-second interval.

> The second panel shows the **Spark micro-batch latency** — how often the streaming job produces a realtime window update. You can see it firing roughly every twenty seconds as configured.

> The third panel shows the **alert event rate** — threshold-breach alerts written per minute. The occasional spikes correspond to the six-percent abnormal probability we configured in the simulator."

**[SCREEN ACTION]** Switch to Prometheus at http://localhost:9090. Click **Alerts**.

> "Prometheus has five alert rules defined. All are currently in the green Inactive state — meaning all pipeline stages are healthy. Let me demonstrate what happens when a stage fails."

**[SCREEN ACTION]** Switch to terminal. Run:
```powershell
docker compose stop vitals-producer
```

> "We've just stopped the vitals producer — simulating a network fault between the bedside monitors and the Kafka broker. Let's wait about ninety seconds."

**[SCREEN ACTION]** Wait 60–90 seconds, then refresh Prometheus Alerts page.

> "Prometheus evaluates its alert rules every fifteen seconds. The rule `VitalsIngestionStalled` fires when the `vitals_producer_last_tick_timestamp` gauge hasn't updated for more than two minutes. After the one-minute `for` hold period, you can see the alert is now **Firing** — shown in red."

**[SCREEN ACTION]** Switch to Alertmanager at http://localhost:9093.

> "Alertmanager has received the alert and is routing it. In a production deployment this would send a page to the on-call engineer via PagerDuty or Slack. For our demo, you can see the alert details here — the summary says 'No vitals ingested from the bedside monitor simulator in over two minutes. Patient monitoring may be blind.' — which is exactly the patient safety scenario this alert is designed to catch."

**[SCREEN ACTION]** Switch back to terminal. Run:
```powershell
docker compose start vitals-producer
```

**[NARRATOR]**

> "Now we restart the producer. Within the next minute, Prometheus will detect that the heartbeat timestamp is updating again, and the alert will resolve automatically."

**[SCREEN ACTION]** Refresh Prometheus Alerts — show alert returning to **Inactive**.

> "The alert has resolved. The pipeline self-healed without any manual intervention, because the Spark streaming job maintains its checkpoint and resumes consuming from the Kafka offset where it left off — no data is lost."

---

## Section 7 — Conclusion
### ⏱ 7:30 – 8:00 | Screen: Architecture diagram or Streamlit dashboard

---

**[SCREEN ACTION]** Return to the Streamlit dashboard — Live Ward View — with data flowing normally.

**[NARRATOR]**

> "To summarise what we've demonstrated:

> The **Speed Layer** — Kafka and Spark Structured Streaming — delivers sub-ten-second bedside alerts and a live ward view updated every twenty seconds.

> The **Batch Layer** — Airflow orchestrating a four-task DAG — reconciles an entire day's sensor history with lab results and produces a clinically-weighted risk score per patient, recomputed from the immutable Parquet archive rather than an ephemeral Kafka log. That immutable archive is the key design decision that makes this Lambda rather than Kappa.

> The **Serving Layer** — FastAPI and Streamlit — unifies both views into a single API and dashboard.

> And the **Observability Layer** — Prometheus, Alertmanager, and Grafana — gives us end-to-end pipeline health monitoring with automated fault detection and structured JSON logs from every stage.

> All thirteen services run as Docker containers, coordinated by Docker Compose, making the entire system reproducible with a single command.

> Thank you for watching."

---

## Timing Summary

| Section | Duration | Cumulative |
|---|---|---|
| 1. Introduction & Architecture | 1 min 15 sec | 1:15 |
| 2. Speed Layer — Live Ward & Alerts | 1 min 15 sec | 2:30 |
| 3. Batch Layer — Airflow & Risk Report | 1 min 30 sec | 4:00 |
| 4. Serving API (FastAPI Swagger) | 1 min 00 sec | 5:00 |
| 5. Unit Tests | 45 sec | 5:45 |
| 6. Observability & Alert Demo | 1 min 45 sec | 7:30 |
| 7. Conclusion | 30 sec | **8:00** |

---

## Recording Tips

- **Speak at 130–140 words per minute** — slightly slower than normal conversation so viewers can follow technical terms.
- **Pause 1–2 seconds** before switching tabs or executing API calls so edits are clean.
- **Zoom browser to 125%** for Swagger and Airflow so text is readable in 1080p.
- **Increase terminal font to 16pt** before recording.
- **Practice the alert demo section** — the 90-second wait is the trickiest part to keep natural; fill it with narration about what Prometheus is doing internally.
- **Edit out** the 90-second wait using video editing software (cut from "Let's wait about ninety seconds" to "Prometheus evaluates...the alert is now Firing"). This keeps the demo within the time limit.
- If demoing **live** instead of recording, skip the alert demo and simply show the Prometheus rules page with all five rules in Inactive state, then describe what happens when a stage fails.

