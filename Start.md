# Hospital Patient Vital Signs Monitoring — Complete Setup, Report & Demo Guide

This guide provides step-by-step instructions to set up, run, and verify the entire **Lambda Architecture** data pipeline, capture all required deliverables for your **report**, and record your **demo video**.

---

## 1. Prerequisites & Environment Setup

### 1.1 Prerequisites
1. **Docker Desktop**: Installed and running (ensure at least **6 GB RAM** is allocated under *Settings → Resources*).
2. **Python 3.10+**: (Optional) For running unit tests locally on your host machine.

### 1.2 Configuration
1. Open PowerShell or a terminal in the project root:
   ```powershell
   # Copy environment configuration
   cp .env.example .env
   ```
2. Key simulation parameters in `.env`:
   - `SIMULATED_DAY_SECONDS=300`: 1 simulated day = 5 minutes (adjust to 180 for faster batch cycles).
   - `NUM_PATIENTS=12`: Number of simulated ward beds.
   - `VITALS_INTERVAL_SECONDS=3`: Sensor reading cadence per patient.
   - `ABNORMAL_SPIKE_PROBABILITY=0.06`: 6% probability of abnormal vitals for alerting.

### 1.3 Launching the Complete Pipeline
Run Docker Compose to build and start all services in the background:
```powershell
docker compose up -d --build
```

Check container status:
```powershell
docker compose ps
```
> **Note**: `kafka-init` runs once to create the Kafka topic and exits with code 0 (expected). All other 12 services should show as `running` / `healthy`.

---

## 2. Accessing All System Interfaces

Allow the system **2–3 minutes** to download Spark dependencies and initialize the Airflow database. Then access:

| Component | URL | Credentials / Notes |
|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Real-time ward vitals, alerts, daily risk reports |
| **FastAPI Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive REST API documentation |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | User: `admin`<br>Password: run `docker compose logs airflow \| Select-String "password"` |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | User: `admin` / Password: `admin` |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Metric queries & alert rules ([http://localhost:9090/alerts](http://localhost:9090/alerts)) |
| **Alertmanager** | [http://localhost:9093](http://localhost:9093) | Active / fired alerts dashboard |

---

## 3. Step-by-Step Guide to Prepare the Report

The written report draft is located at [`report/REPORT.md`](report/REPORT.md). Follow these steps to collect runtime evidence:

### Step 3.1: Run and Capture Unit Tests
Execute unit tests locally to prove business logic correctness:
```powershell
python -m pip install -r requirements-dev.txt
python -m pytest
```
- **Screenshot**: Capture the terminal showing **25 passed tests**.

### Step 3.2: Capture Streamlit Dashboard Outputs
Open [http://localhost:8501](http://localhost:8501):
1. **Live Ward View**: Capture the real-time vitals table with color-coded warning/critical status.
2. **Alert Log**: Capture the real-time alert feed showing threshold breaches (e.g., Tachycardia, Hypoxia).
3. **Daily Risk Report**: Wait for the first simulated day (~5 mins) and capture the risk bar chart and summary table.

### Step 3.3: Capture Serving API Responses
Open [http://localhost:8000/docs](http://localhost:8000/docs):
1. Execute `GET /ward/status` and screenshot the JSON response.
2. Execute `GET /patient/{patient_id}/risk` (e.g., `P001`) and screenshot the detailed risk breakdown.
3. Execute `GET /health` to show pipeline stage health status.

### Step 3.4: Capture Airflow Batch Execution
Open [http://localhost:8080](http://localhost:8080):
1. Navigate to **DAGs** → `daily_risk_report_dag` → **Graph view** to show the 4 tasks:
   `find_unprocessed_batch` → `load_lab_results` → `compute_and_write_risk_report` → `record_batch_health`.
2. Click on the `compute_and_write_risk_report` task → **Logs** and capture the log showing Parquet reading and score calculation.

### Step 3.5: Capture Observability & Alert Simulation
1. **Grafana**: Open [http://localhost:3000](http://localhost:3000), navigate to Dashboards → **Hospital Vital Signs Pipeline** and screenshot the populated metric panels.
2. **Prometheus Rules**: Open [http://localhost:9090/alerts](http://localhost:9090/alerts) showing all 5 alert rules healthy/green.
3. **Trigger Alert**:
   ```powershell
   docker compose stop vitals-producer
   ```
   - Wait ~2 minutes.
   - Screenshot `VitalsIngestionStalled` firing in **Prometheus** and **Alertmanager** ([http://localhost:9093](http://localhost:9093)).
4. **Resolve Alert**:
   ```powershell
   docker compose start vitals-producer
   ```
   - Screenshot the alert resolving back to green.

### Step 3.6: Finalize the Report
1. Open [`report/REPORT.md`](report/REPORT.md):
   - Fill in Team member names and GitHub repository link.
   - Embed the captured screenshots into sections marked `[TODO: screenshot]`.
   - Write 1–2 paragraphs in **Section 6** narrating your specific run observations.
   - Add individual contributions in **Section 9**.
2. Convert `REPORT.md` to PDF (via VS Code Markdown PDF extension, Pandoc, or export via Google Docs / Word).

---

## 4. Step-by-Step Script & Structure for the Demo Video

Aim for a **5 to 7 minute** screen recording following this structure:

| Time | Section | Screen to Show | Script / Talking Points |
|---|---|---|---|
| **0:00 - 1:00** | **1. Architecture Overview** | Architecture diagram in `report/REPORT.md` | *"We implemented a Lambda Architecture for Hospital Patient Vital Signs Monitoring. It features a Speed Layer (Kafka + Spark Structured Streaming) for real-time bedside alerts, a Batch Layer (Airflow) reconciling daily pathology lab results with raw Parquet vitals, and a unified serving layer with FastAPI and Streamlit."* |
| **1:00 - 2:00** | **2. Speed Layer in Action** | Streamlit ([http://localhost:8501](http://localhost:8501)) & Terminal Logs | Show the live ward table updating every few seconds. Show the alerts table populating on abnormal heart rate or SpO2. Show `docker compose logs -f spark-streaming` displaying micro-batch processing and upserts. |
| **2:00 - 3:15** | **3. Batch Layer & Orchestration** | Airflow UI ([http://localhost:8080](http://localhost:8080)) & Streamlit Risk Tab | Show `daily_risk_report_dag`. Explain how it detects dropped lab files, reads historical raw Parquet data, joins vitals trends with out-of-range lab tests (e.g. Troponin, Potassium), and produces patient risk scores. Switch to Streamlit to show the resulting Daily Risk chart. |
| **3:15 - 4:15** | **4. Serving API & Unit Testing** | Swagger UI ([http://localhost:8000/docs](http://localhost:8000/docs)) & VS Code Terminal | Test `/ward/status` and `/patient/P001/risk` in Swagger UI. Run `python -m pytest` in terminal to demonstrate 25 unit tests validating risk scoring and threshold rules without external dependencies. |
| **4:15 - 5:30** | **5. Observability & Fault Tolerance Demo** | Grafana ([http://localhost:3000](http://localhost:3000)), Prometheus ([http://localhost:9090](http://localhost:9090)), Alertmanager ([http://localhost:9093](http://localhost:9093)) | Show ingestion rate & latency metrics in Grafana. Demonstrate live alerting by running `docker compose stop vitals-producer`. Show `VitalsIngestionStalled` alert turning red in Prometheus and firing in Alertmanager. Restart container to show recovery. |
| **5:30 - 6:00** | **6. Conclusion** | Project Repo / Architecture Summary | Summarize Lambda trade-offs (deterministic batch recalculation vs. low-latency speed layer) and wrap up. |

---

## 5. Teardown & Clean Reset Commands

When you want to stop, restart, or reset the environment:

```powershell
# Stop services without deleting stored data
docker compose stop

# Restart stopped services
docker compose start

# Graceful shutdown (removes containers & networks, preserves volume data)
docker compose down

# Full clean reset (erases all database volumes & checkpoints for a fresh start)
docker compose down -v
docker compose up -d --build
```
