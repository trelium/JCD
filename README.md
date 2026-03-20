# Biomarker Pipeline

An end-to-end production pipeline that accepts FHIR-formatted blood panel files, validates biomarkers against NHANES population reference data, scores cardiovascular/metabolic risk, and generates GP-ready clinical reports with an AI-generated narrative.

---

## Overview

The pipeline is built around a set of design properties that make it suitable for production deployment at scale.

**Asynchronous, decoupled processing.** The API accepts an upload and returns a job ID immediately — no blocking. Work is queued in Postgres and processed by independent worker services. The client polls for completion. This means the web service never becomes a bottleneck regardless of how long individual jobs take.

**Horizontal scaling via competing consumers.** Workers poll a shared Postgres job queue using `SELECT FOR UPDATE SKIP LOCKED` — a battle-tested pattern that allows any number of worker replicas to consume work concurrently with no double-processing. Increasing throughput is a single slider in the Railway dashboard. Each pipeline stage (validation, analysis, report) scales independently, so you can add capacity exactly where the bottleneck is.

**Fault tolerance and retry.** Failed jobs are automatically retried up to three times with exponential back-off. After exhausting retries the job is marked `dlq` (dead-letter) rather than silently lost, making failures visible and recoverable. Workers that crash mid-job leave the row in its current status — on restart they pick up where they left off.

**Idempotency.** Every upload is SHA-256 hashed. Duplicate submissions return the existing job instantly rather than reprocessing, making the API safe to call multiple times without side effects.

**Structured observability.** All services emit structured JSON logs via `structlog` with consistent field names across every stage transition, error, and external API call. On Railway these stream directly to the log viewer and can be forwarded to any aggregator.

**Schema-validated data contracts.** All internal data passed between pipeline stages is validated by Pydantic v2 models. Malformed FHIR input is rejected at parse time with a clear error rather than propagating corrupt data downstream.

**Zero-downtime deploys.** Each service is independently deployable. Rolling out a new report template or updated risk scoring logic requires redeploying only the relevant worker — the API and other workers keep running.

---

## Architecture

```
Upload (FHIR JSON/XML)
        │
        ▼
  FastAPI /upload
        │  returns job_id immediately
        │  stores raw bytes + metadata in Postgres
        ▼
  ┌─────────────────────────────────────────┐
  │           Postgres job queue            │
  │  status: pending → validating →         │
  │          analyzing → reporting →        │
  │          complete / dlq                 │
  └─────────────────────────────────────────┘
        │
        ▼
  Validation worker (×N replicas)
    · Parse FHIR JSON or XML
    · Look up NHANES percentiles by age + sex
    · Flag anomalies with severity + percentile position
    · Compute data quality score
        │
        ▼
  Analysis worker (×N replicas)
    · Weighted composite risk score (0–100)
    · Per-biomarker cohort percentiles
    · Plotly chart data
    · Plain-language key findings
        │
        ▼
  Report worker
    · LLM-generated clinical narrative
    · Full HTML report with embedded chart
        │
        ▼
  Job marked COMPLETE

Client polls GET /status/{job_id}
Client fetches GET /report/{job_id} → rendered HTML report
```

---

## Horizontal scaling

Each of the three worker types is a stateless process that polls the job queue independently. Scaling any of them is additive — add replicas and throughput increases linearly until the database becomes the constraint (which happens at a much higher volume than a typical clinical workload).

**On Railway**, scaling is done per service:

```
Service → Settings → Replicas → set to N
```

Workers coordinate automatically through `SELECT FOR UPDATE SKIP LOCKED` — no configuration changes required when adding replicas.

**Stage-level scaling.** The three stages have different computational profiles:

- Validation is fast (database lookups, in-memory computation) — 1–2 replicas typically sufficient
- Analysis is slightly heavier (Plotly chart serialisation) — scale to 2–3 under load
- Report generation is I/O bound on the LLM API call — scale to match your Anthropic API concurrency limit

**Identifying the bottleneck.** If jobs are piling up in a particular status (visible by querying the `jobs` table), that stage needs more replicas. A simple query:

```sql
SELECT status, COUNT(*) FROM jobs GROUP BY status ORDER BY count DESC;
```

**Database scaling.** The Postgres connection pool is set to 10 connections per worker process with a max overflow of 20. With N replicas of M workers, peak connections = N × M × 30. On Railway's free Postgres tier this is fine up to a few dozen replicas total. For higher scale, add PgBouncer as a connection pooler in front of Postgres.

---

## Deployment (Railway)

### Prerequisites

- GitHub repository with this code
- Railway account (free tier sufficient)
- Anthropic API key with active credit balance

### Services to create

Create five services in Railway, all pointing at the same GitHub repo:

| Service name | `RAILWAY_DOCKERFILE_PATH` | Start command |
|---|---|---|
| `api` | `services/api/Dockerfile` | *(from Dockerfile)* |
| `validation-worker` | `services/worker/Dockerfile` | `python -m services.worker.validation_worker` |
| `analysis-worker` | `services/worker/Dockerfile` | `python -m services.worker.analysis_worker` |
| `report-worker` | `services/worker/Dockerfile` | `python -m services.worker.report_worker` |
| `nhanes-loader` | `services/nhanes/Dockerfile` | `python services/nhanes/loader.py` |

### Environment variables

Set these on every service:

| Variable | Value |
|---|---|
| `DATABASE_URL` | From Railway Postgres plugin — change `postgresql://` to `postgresql+asyncpg://` |
| `ANTHROPIC_API_KEY` | Your Anthropic key |
| `ENV` | `production` |

### Deploy order

1. Add a **PostgreSQL** plugin to the project (Railway dashboard → New → Database → PostgreSQL)
2. Deploy **nhanes-loader** first and wait for it to complete — it seeds the reference ranges table
3. Deploy the remaining four services — they can all be deployed simultaneously

### Public URL

In the `api` service → Settings → Networking → Generate Domain. Set the port to `8000`.

---

## Project structure

```
biomarker-pipeline/
├── railway.toml                # Railway service definitions
├── docker-compose.yml          # Local dev stack
├── alembic.ini                 # DB migration config
├── sample_fhir_bundle.json     # Sample input — normal/moderate risk
│
├── requirements/
│   ├── api.txt                 # API service dependencies
│   ├── worker.txt              # Worker service dependencies
│   └── nhanes.txt              # NHANES loader dependencies
│
├── shared/
│   ├── models/models.py        # Pydantic data models for all stages
│   ├── kafka/kafka_utils.py    # Postgres job queue (poll + retry logic)
│   ├── db/database.py          # SQLAlchemy async engine + ORM
│   └── logging_config.py       # structlog JSON configuration
│
├── services/
│   ├── api/
│   │   ├── main.py             # FastAPI — upload, status, report, health
│   │   └── Dockerfile
│   ├── worker/
│   │   ├── fhir_parser.py      # FHIR R4 JSON + XML → internal models
│   │   ├── validation_worker.py # Stage 1: parse + validate vs NHANES
│   │   ├── analysis_worker.py  # Stage 2: risk scoring + Plotly charts
│   │   ├── report_worker.py    # Stage 3: LLM narrative + HTML render
│   │   └── Dockerfile
│   └── nhanes/
│       ├── loader.py           # Seeds reference_ranges table on first run
│       └── Dockerfile
│
└── infra/
    └── migrations/
        ├── env.py
        └── versions/
            └── 0001_initial_schema.py
```

---

## FHIR input format

The pipeline accepts a FHIR R4 Bundle containing one `Patient` resource and one or more LOINC-coded `Observation` resources. Both JSON and XML serialisations are supported.

The `Patient` resource is used for age/sex stratification against NHANES reference distributions. If omitted, the pipeline falls back to population-wide (unstratified) percentiles.

See `sample_fhir_bundle.json` for a complete working example.

**Supported biomarkers (LOINC codes):** Total Cholesterol (2093-3), LDL (13457-7), HDL (2085-9), Triglycerides (3043-7), Fasting Glucose (2345-7), HbA1c (4548-4), hsCRP (30522-7), Creatinine (2157-6), BUN (3094-0), ALT (1742-6), AST (1920-8), Ferritin (2276-4), Hemoglobin (718-7), WBC (6690-2), Platelets (777-3), Sodium (2947-0), Potassium (6298-4), Albumin (1751-7), Total Bilirubin (1975-2), Alkaline Phosphatase (6768-6), Calcium (17861-6), Total Protein (2885-2).

---

## Risk scoring

The composite risk score (0–100) is a weighted sum of per-biomarker risk contributions. Each biomarker's contribution is derived from its percentile position in the age/sex-stratified NHANES distribution — not from fixed clinical thresholds. This means the score reflects where the patient sits relative to the real-world population, not just whether they crossed an arbitrary cutoff.

HDL is treated as a protective factor: low HDL contributes positively to risk, high HDL contributes nothing.

| Score | Tier |
|---|---|
| 0–19 | Low |
| 20–44 | Moderate |
| 45–69 | High |
| 70–100 | Urgent |

The severity badges in the results table (Normal / Borderline / Abnormal / Critical) use different thresholds — they reflect the percentile position of each individual biomarker in isolation, independent of the composite score. A patient can show all Normal badges and still receive a Moderate composite score if several biomarkers are simultaneously in the upper quartile.

---

## Reference data

Population reference ranges are derived from the **NHANES 2017–2018** dataset (National Health and Nutrition Examination Survey, CDC), stratified by sex and age band (18–29, 30–39, 40–49, 50–59, 60–69, 70+). The percentile distributions are bundled directly in the loader rather than downloaded at runtime, making deployment independent of external data availability.

---

## Extending the pipeline

**Add a new biomarker.** Add an entry to `REFERENCE_DATA` in `services/nhanes/loader.py` with LOINC code, display name, unit, and percentile values per age/sex stratum. Re-run the nhanes-loader service.

**Add a new pipeline stage.** Add a new worker module in `services/worker/`, define the input status it polls for, update the status transition in `shared/kafka/kafka_utils.py`, and add the service to `railway.toml`.

**Change the LLM model.** Update `CLAUDE_MODEL` in `services/worker/report_worker.py`.

**Run Alembic migrations.**
```bash
DATABASE_URL=postgresql+asyncpg://... alembic upgrade head
```
